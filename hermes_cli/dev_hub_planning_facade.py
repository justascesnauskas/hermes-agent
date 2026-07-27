"""Canonical natural-language facade for Dev Hub Planning V2.

The Agent Ops plugin keeps its familiar public tool names, while this Hermes
core module owns the one durable planning implementation.  It resolves the
exact current conversation, binds private turn attachments, drives complete
preview delivery, and approves only a later fully-reviewed immutable preview.
"""

from __future__ import annotations

from collections.abc import Mapping
import json
import logging
import re
from typing import Any, Optional

from agent.auxiliary_client import get_runtime_main_route
from hermes_constants import get_hermes_home
from hermes_cli.dev_hub_planning_v2 import (
    PlanningOriginPayload,
    PlanningThreadDTO,
    PlanningV2Client,
    PlanningV2ClientError,
    PlanningV2ConfigError,
    derive_artifact_idempotency_key,
    derive_thread_id_from_origin,
)
from hermes_cli.planning_artifact_spool import (
    list_artifact_recoveries,
    register_artifact_recovery,
)
from hermes_cli.turn_origin import (
    TurnAttachmentOriginV1,
    get_current_turn_origin,
    get_current_turn_user_text,
)


FACADE_PROTOCOL = "agent-ops-planning-v2-facade/1"
_PLAN_TOOL = "agent_ops_task_plan"
_APPROVAL_TOOL = "agent_ops_task_approve_apply"
_PLAN_SCHEMA = "agent-ops-task-plan-natural/1"
_APPROVAL_SCHEMA = "agent-ops-task-approve-natural/1"
_PLAN_INTENTS = frozenset(
    {"new", "revise", "retry", "resume", "status", "show", "cancel"}
)
_ACTIVE_RUN_STATES = frozenset({"queued", "running", "waiting"})
_PRIVATE_RESULT_FIELDS = frozenset(
    {
        "local_path",
        "localPath",
        "path",
        "recovery_token",
        "recoveryToken",
        "snapshot_path",
        "snapshotPath",
    }
)
_RECOVERY_TOKEN_RE = re.compile(r"\bartrec_v2_[0-9a-f]{64}\b")
logger = logging.getLogger(__name__)


def get_facade_capabilities() -> Mapping[str, Any]:
    """Return the exact plugin/core negotiation contract."""

    return {
        "protocol": FACADE_PROTOCOL,
        "publicTools": {
            _PLAN_TOOL: {"inputSchema": _PLAN_SCHEMA},
            _APPROVAL_TOOL: {"inputSchema": _APPROVAL_SCHEMA},
        },
        "engine": "planning_v2",
        "legacyFallback": False,
        "artifactContinuation": "profile-local-durable-spool",
        "previewDelivery": "provider-confirmed-all-pages",
    }


def _json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _private_path_fragments() -> tuple[str, ...]:
    fragments: set[str] = set()
    turn = get_current_turn_origin()
    if turn is not None:
        fragments.update(
            str(attachment.local_path)
            for attachment in turn.attachments
            if attachment.local_path
        )
    try:
        fragments.add(str(get_hermes_home().expanduser().resolve()))
    except (OSError, RuntimeError):
        # Redaction is defense in depth. Failure to resolve the profile home
        # must not make an otherwise typed public failure crash.
        pass
    return tuple(sorted(fragments, key=len, reverse=True))


def _redact_private_text(
    value: str,
    *,
    private_paths: tuple[str, ...],
) -> str:
    redacted = _RECOVERY_TOKEN_RE.sub("[private recovery token]", value)
    for fragment in private_paths:
        if fragment:
            redacted = redacted.replace(fragment, "[private path]")
    return redacted


def _public_value(
    value: Any,
    *,
    private_paths: Optional[tuple[str, ...]] = None,
) -> Any:
    """Remove process-private filesystem capabilities from public results."""

    paths = (
        _private_path_fragments()
        if private_paths is None
        else private_paths
    )
    if isinstance(value, Mapping):
        return {
            str(key): _public_value(child, private_paths=paths)
            for key, child in value.items()
            if str(key) not in _PRIVATE_RESULT_FIELDS
        }
    if isinstance(value, (list, tuple)):
        return [
            _public_value(child, private_paths=paths)
            for child in value
        ]
    if isinstance(value, str):
        return _redact_private_text(value, private_paths=paths)
    return value


def _failure(
    code: str,
    *,
    detail: Any,
    state_changed: bool = False,
    **extra: Any,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": False,
        "code": code,
        "route": "planning_v2",
        "stateChanged": state_changed,
        "detail": _public_value(detail),
    }
    result.update(
        {
            str(key): _public_value(value)
            for key, value in extra.items()
        }
    )
    return result


def _success(value: Mapping[str, Any], **extra: Any) -> dict[str, Any]:
    result = dict(_public_value(value))
    result.update(
        {
            str(key): _public_value(child)
            for key, child in extra.items()
        }
    )
    result["ok"] = True
    result["route"] = "planning_v2"
    return result


def _forward(value: Mapping[str, Any], **extra: Any) -> dict[str, Any]:
    """Augment an internal result without turning a failure into success."""

    result = dict(_public_value(value))
    result.update(
        {
            str(key): _public_value(child)
            for key, child in extra.items()
        }
    )
    result["route"] = "planning_v2"
    return result


def _decode_internal(raw: Any) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        value = dict(raw)
    elif isinstance(raw, str):
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PlanningV2ConfigError(
                "planning.facade_internal_result_invalid",
                detail="The canonical Planning V2 handler returned invalid JSON.",
            ) from exc
        if not isinstance(decoded, dict):
            raise PlanningV2ConfigError(
                "planning.facade_internal_result_invalid"
            )
        value = decoded
    else:
        raise PlanningV2ConfigError(
            "planning.facade_internal_result_invalid"
        )
    return value


def _invoke_internal(
    arguments: Mapping[str, Any],
    runtime_kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    # Lazy import avoids turning facade negotiation into direct-tool exposure.
    from tools.dev_hub_planning_tool import _handle_planning_v2

    kwargs = dict(runtime_kwargs)
    kwargs["_trusted_facade"] = True
    return _decode_internal(
        _handle_planning_v2(dict(arguments), **kwargs)
    )


def _exact_user_text(runtime_kwargs: Mapping[str, Any]) -> str:
    current = get_current_turn_user_text()
    if current is not None and current.strip():
        return current
    fallback = runtime_kwargs.get("user_task")
    if isinstance(fallback, str) and fallback.strip():
        return fallback
    raise PlanningV2ConfigError(
        "planning.current_input_required",
        detail=(
            "The exact current human turn is unavailable; no planning input "
            "or approval was written."
        ),
    )


def _title_from_text(text: str) -> str:
    compact = " ".join(text.split())
    if not compact:
        return "Planning thread"
    if len(compact) <= 120:
        return compact
    return compact[:117].rstrip() + "..."


def _route_policy() -> dict[str, Any]:
    route = get_runtime_main_route()
    provider = str(route.get("provider") or "").strip()
    model = str(route.get("model") or "").strip()
    api_mode = str(route.get("api_mode") or "").strip()
    if not provider or not model or not api_mode:
        raise PlanningV2ConfigError(
            "planning.model_route_missing",
            detail=(
                "Hermes could not bind the live conversation provider, model, "
                "and API mode. No planning mutation was attempted."
            ),
        )
    return {
        "modelRoute": {
            "provider": provider,
            "model": model,
            "apiMode": api_mode,
        }
    }


def _run_policy(arguments: Mapping[str, Any]) -> dict[str, Any]:
    mode = str(arguments.get("mode") or "auto").strip().lower()
    if mode not in {"auto", "quick", "deep"}:
        raise PlanningV2ConfigError(
            "planning.facade_mode_invalid",
            detail="mode must be auto, quick, or deep.",
        )
    return {"planningMode": mode}


def _input_payload(
    intent: str,
    arguments: Mapping[str, Any],
) -> dict[str, Any]:
    # The direct handler stores exact runtime text as authority and places this
    # complete natural schema under modelNormalization. No evidence list is
    # clipped, summarized, or silently discarded here.
    return {
        "facadeProtocol": FACADE_PROTOCOL,
        "intent": intent,
        "request": dict(arguments),
    }


def _choice_label(thread: Mapping[str, Any]) -> str:
    title = _display_title(thread)
    updated = str(thread.get("updatedAt") or "").strip()
    suffix = str(thread.get("threadId") or "")[-8:]
    components = [title]
    if updated:
        components.append(updated)
    if suffix:
        components.append(f"ref {suffix}")
    return " · ".join(components)


def _display_title(thread: Mapping[str, Any]) -> str:
    return str(
        thread.get("title") or "Untitled planning thread"
    ).strip()


def _thread_choices(
    threads: list[PlanningThreadDTO],
) -> list[dict[str, Any]]:
    return [
        {
            "selection": _choice_label(thread),
            "title": _display_title(thread),
            "status": thread.get("status"),
            "updatedAt": thread.get("updatedAt"),
            "threadId": thread.get("threadId"),
        }
        for thread in threads
    ]


def _selected_thread(
    threads: list[PlanningThreadDTO],
    selected_option: Any,
) -> Optional[PlanningThreadDTO]:
    if not isinstance(selected_option, str) or not selected_option:
        return None
    selected = selected_option
    exact_id = [
        thread
        for thread in threads
        if str(thread.get("threadId") or "") == selected
    ]
    if len(exact_id) == 1:
        return exact_id[0]
    exact_label = [
        thread for thread in threads if _choice_label(thread) == selected
    ]
    if len(exact_label) == 1:
        return exact_label[0]
    title_matches = [
        thread
        for thread in threads
        if _display_title(thread) == selected
    ]
    if len(title_matches) == 1:
        return title_matches[0]
    return None


def _resolve_thread(
    client: PlanningV2Client,
    arguments: Mapping[str, Any],
) -> tuple[Optional[PlanningThreadDTO], Optional[dict[str, Any]]]:
    resolved = client.resolve_current_thread(
        origin=client.current_origin()
    ).payload
    threads = list(resolved["threads"])
    explicit_value = (
        arguments.get("thread_id")
        if arguments.get("thread_id") is not None
        else arguments.get("threadId")
    )
    if explicit_value is not None:
        if not isinstance(explicit_value, str) or not explicit_value:
            return None, _failure(
                "planning.facade_thread_selection_invalid",
                detail=(
                    "thread_id must exactly match a thread displayed for this "
                    "conversation."
                ),
                choices=_thread_choices(threads),
            )
        exact = [
            thread
            for thread in threads
            if thread.get("threadId") == explicit_value
        ]
        if len(exact) == 1:
            return exact[0], None
        return None, _failure(
            "planning.facade_thread_not_in_conversation",
            detail=(
                "The requested thread is not an active plan owned by this "
                "exact conversation and user. Nothing was read or changed "
                "through the supplied identifier."
            ),
            choices=_thread_choices(threads),
        )
    if resolved["match"] == "none":
        return None, _failure(
            "planning.facade_current_thread_not_found",
            detail=(
                "This conversation has no active planning thread. Use intent=new "
                "only when the human is starting a genuinely new outcome."
            ),
            choices=[],
        )
    selected_option = arguments.get("selectedOption")
    if selected_option is not None:
        selected = _selected_thread(threads, selected_option)
        if selected is not None:
            return selected, None
        return None, _failure(
            "planning.facade_thread_selection_invalid",
            detail=(
                "selectedOption must exactly equal one displayed title, "
                "selection label, or thread identifier. Hermes did not use "
                "a fuzzy or newest-plan fallback."
            ),
            choices=_thread_choices(threads),
        )
    if resolved["match"] == "one":
        return threads[0], None
    return None, _failure(
        "planning.facade_current_thread_ambiguous",
        detail=(
            "More than one active plan belongs to this exact conversation. "
            "Ask the human to choose one displayed title; Hermes did not pick "
            "the newest plan or mutate anything."
        ),
        choices=_thread_choices(threads),
    )


def _private_attachments() -> tuple[TurnAttachmentOriginV1, ...]:
    turn = get_current_turn_origin()
    if turn is None:
        return ()
    attachments = tuple(
        sorted(turn.attachments, key=lambda item: item.ingress_ordinal)
    )
    if any(not attachment.local_path for attachment in attachments):
        raise PlanningV2ConfigError(
            "planning.attachment_private_source_unavailable",
            detail=(
                "An attachment identity exists but its private gateway source "
                "is no longer available. No thread mutation was attempted."
            ),
        )
    return attachments


def _artifact_role(arguments: Mapping[str, Any]) -> str:
    return (
        "implementation_target"
        if arguments.get("visualRole") == "implementation_target"
        else "turn_attachment"
    )


def _stage_attachments(
    *,
    client: PlanningV2Client,
    thread_id: str,
    origin: PlanningOriginPayload,
    attachments: tuple[TurnAttachmentOriginV1, ...],
    arguments: Mapping[str, Any],
) -> tuple[str, ...]:
    role = _artifact_role(arguments)
    tokens: list[str] = []
    for attachment in attachments:
        idempotency_key = derive_artifact_idempotency_key(
            runner_id=client.runner_id,
            thread_id=thread_id,
            provider=origin["provider"],
            gateway_account_id=origin["gatewayAccountId"],
            provider_event_id=origin["providerEventId"],
            role=role,
            position=attachment.ingress_ordinal,
            attachment_identity=attachment.attachment_id,
            ingress_ordinal=attachment.ingress_ordinal,
        )
        tokens.append(
            register_artifact_recovery(
                thread_id=thread_id,
                local_path=str(attachment.local_path),
                origin=origin,
                role=role,
                position=attachment.ingress_ordinal,
                required=True,
                idempotency_key=idempotency_key,
                content_type=None,
                retain_until=None,
                attachment_identity=attachment.attachment_id,
                ingress_ordinal=attachment.ingress_ordinal,
            )
        )
    return tuple(tokens)


def _upload_pending(
    *,
    client: PlanningV2Client,
    thread_id: str,
    runtime_kwargs: Mapping[str, Any],
) -> tuple[int, Optional[dict[str, Any]]]:
    pending = list_artifact_recoveries(
        current_origin=client.current_origin(),
        thread_id=thread_id,
    )
    failures: list[dict[str, Any]] = []
    completed = 0
    for record in pending:
        result = _invoke_internal(
            {
                "action": "upload_artifact",
                "recovery_token": record.token,
            },
            runtime_kwargs,
        )
        if result.get("ok") is True:
            completed += 1
        else:
            failures.append(
                {
                    "position": record.position,
                    "code": result.get("code")
                    or "planning.artifact_upload_failed",
                    "retryable": bool(result.get("retryable")),
                    "outcomeAmbiguous": bool(
                        result.get("outcomeAmbiguous")
                    ),
                }
            )
    if failures:
        return completed, _failure(
            "planning.facade_artifacts_pending",
            detail=(
                "One or more durably spooled attachments have not converged "
                "with Dev Hub. Their private snapshots remain available for "
                "the next retry/resume; the planning run was not started."
            ),
            state_changed=True,
            threadId=thread_id,
            uploadedThisAttempt=completed,
            pending=failures,
            nextAction={"intent": "retry"},
        )
    return completed, None


def _start_run(
    *,
    thread_id: str,
    arguments: Mapping[str, Any],
    runtime_kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    return _invoke_internal(
        {
            "action": "start_run",
            "thread_id": thread_id,
            "run_policy": _run_policy(arguments),
            "route_policy": _route_policy(),
        },
        runtime_kwargs,
    )


def _status(
    *,
    thread_id: str,
    runtime_kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    return _invoke_internal(
        {"action": "status", "thread_id": thread_id},
        runtime_kwargs,
    )


def _create(
    *,
    client: PlanningV2Client,
    arguments: Mapping[str, Any],
    runtime_kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    exact_text = _exact_user_text(runtime_kwargs)
    origin = client.current_origin()
    expected_thread_id = derive_thread_id_from_origin(origin)
    attachments = _private_attachments()
    route_policy = _route_policy()

    # Snapshot every attachment before the first Dev Hub request. A process or
    # provider failure can therefore resume with identical bytes even when the
    # gateway cache entry disappears.
    _stage_attachments(
        client=client,
        thread_id=expected_thread_id,
        origin=origin,
        attachments=attachments,
        arguments=arguments,
    )
    create_result = _invoke_internal(
        {
            "action": "create",
            "start_run": False,
            "title": _title_from_text(exact_text),
            "project_id": arguments.get("laneHint"),
            "payload": _input_payload("new", arguments),
            "thread_policy": {
                "facadeProtocol": FACADE_PROTOCOL,
                "planningMode": str(arguments.get("mode") or "auto"),
            },
        },
        runtime_kwargs,
    )
    if create_result.get("ok") is not True:
        outcome_ambiguous = bool(
            create_result.get("outcomeAmbiguous")
        )
        return _forward(
            create_result,
            draftPersisted=(None if outcome_ambiguous else False),
            draftPersistence=(
                "unknown" if outcome_ambiguous else "not_persisted"
            ),
            attachmentsDurablySpooled=len(attachments),
        )
    actual_thread_id = str(create_result.get("threadId") or "")
    if actual_thread_id != expected_thread_id:
        return _failure(
            "planning.facade_thread_identity_mismatch",
            detail=(
                "Dev Hub returned a thread identity that differs from the "
                "documented immutable provider-event derivation. The spooled "
                "attachments were retained and no run was started."
            ),
            state_changed=True,
            expectedThreadId=expected_thread_id,
            actualThreadId=actual_thread_id,
        )
    uploaded, upload_failure = _upload_pending(
        client=client,
        thread_id=actual_thread_id,
        runtime_kwargs=runtime_kwargs,
    )
    if upload_failure is not None:
        return upload_failure
    run_result = _invoke_internal(
        {
            "action": "start_run",
            "thread_id": actual_thread_id,
            "run_policy": _run_policy(arguments),
            "route_policy": route_policy,
        },
        runtime_kwargs,
    )
    return _forward(
        run_result,
        intent="new",
        draftPersisted=True,
        attachmentsUploaded=uploaded,
    )


def _revise(
    *,
    client: PlanningV2Client,
    thread: PlanningThreadDTO,
    arguments: Mapping[str, Any],
    runtime_kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    _exact_user_text(runtime_kwargs)
    thread_id = str(thread["threadId"])
    origin = client.current_origin()
    attachments = _private_attachments()
    _route_policy()
    _stage_attachments(
        client=client,
        thread_id=thread_id,
        origin=origin,
        attachments=attachments,
        arguments=arguments,
    )
    revision = _invoke_internal(
        {
            "action": "continue",
            "thread_id": thread_id,
            "start_run": False,
            "payload": _input_payload("revise", arguments),
        },
        runtime_kwargs,
    )
    if revision.get("ok") is not True:
        return _forward(
            revision,
            intent="revise",
            attachmentsDurablySpooled=len(attachments),
        )
    uploaded, upload_failure = _upload_pending(
        client=client,
        thread_id=thread_id,
        runtime_kwargs=runtime_kwargs,
    )
    if upload_failure is not None:
        return upload_failure
    run = _start_run(
        thread_id=thread_id,
        arguments=arguments,
        runtime_kwargs=runtime_kwargs,
    )
    return _forward(
        run,
        intent="revise",
        inputStored=True,
        previewInvalidated=bool(revision.get("previewInvalidated")),
        attachmentsUploaded=uploaded,
    )


def _retry(
    *,
    client: PlanningV2Client,
    thread: PlanningThreadDTO,
    arguments: Mapping[str, Any],
    runtime_kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    thread_id = str(thread["threadId"])
    uploaded, upload_failure = _upload_pending(
        client=client,
        thread_id=thread_id,
        runtime_kwargs=runtime_kwargs,
    )
    if upload_failure is not None:
        return upload_failure
    current = _status(thread_id=thread_id, runtime_kwargs=runtime_kwargs)
    if current.get("ok") is not True:
        return _forward(current, intent="retry")
    if current.get("runStatus") in _ACTIVE_RUN_STATES:
        return _forward(
            current,
            intent="retry",
            retryDisposition="already_recovering",
            recoveredArtifacts=uploaded,
            instruction=(
                "The existing run is active; Hermes did not create a duplicate "
                "run. Automatic lease and delivery recovery remains in charge."
            ),
        )
    run = _start_run(
        thread_id=thread_id,
        arguments=arguments,
        runtime_kwargs=runtime_kwargs,
    )
    return _forward(
        run,
        intent="retry",
        retriedImmutableHead=True,
        appendedRetryMessage=False,
        recoveredArtifacts=uploaded,
    )


def _resume(
    *,
    client: PlanningV2Client,
    thread: PlanningThreadDTO,
    arguments: Mapping[str, Any],
    runtime_kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    thread_id = str(thread["threadId"])
    uploaded, upload_failure = _upload_pending(
        client=client,
        thread_id=thread_id,
        runtime_kwargs=runtime_kwargs,
    )
    if upload_failure is not None:
        return upload_failure
    current = _status(thread_id=thread_id, runtime_kwargs=runtime_kwargs)
    if current.get("ok") is not True:
        return _forward(
            current,
            intent="resume",
            recoveredArtifacts=uploaded,
        )
    if current.get("runStatus") in _ACTIVE_RUN_STATES:
        return _forward(
            current,
            intent="resume",
            resumeDisposition="automatic_recovery_active",
            recoveredArtifacts=uploaded,
        )
    if uploaded:
        run = _start_run(
            thread_id=thread_id,
            arguments=arguments,
            runtime_kwargs=runtime_kwargs,
        )
        return _forward(
            run,
            intent="resume",
            resumeDisposition="artifact_handoff_completed",
            recoveredArtifacts=uploaded,
        )
    return _forward(
        current,
        intent="resume",
        resumeDisposition="no_manual_resume_required",
        instruction=(
            "Planning and Jira recovery are automatic on the original durable "
            "operation; Hermes did not create a replacement run or request a "
            "second approval."
        ),
    )


def _show(
    *,
    client: PlanningV2Client,
    thread: PlanningThreadDTO,
    arguments: Mapping[str, Any],
    runtime_kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    detail = arguments.get("detail", "human")
    if detail is None:
        detail = "human"
    if not isinstance(detail, str) or detail not in {
        "human",
        "technical",
        "all",
    }:
        return _failure(
            "planning.facade_preview_detail_invalid",
            detail="detail must be human, technical, or all.",
        )
    task_position = arguments.get("taskPosition")
    if task_position is not None and (
        isinstance(task_position, bool)
        or not isinstance(task_position, int)
        or task_position < 1
    ):
        return _failure(
            "planning.facade_task_position_invalid",
            detail="taskPosition must be a positive integer.",
        )

    thread_id = str(thread["threadId"])
    status = _status(thread_id=thread_id, runtime_kwargs=runtime_kwargs)
    if status.get("ok") is not True:
        return _forward(status, intent="show")
    preview = status.get("preview")
    preview_result_id = (
        str(preview.get("versionId") or "")
        if isinstance(preview, Mapping)
        else ""
    )
    if not preview_result_id:
        return _failure(
            "planning.facade_preview_not_ready",
            detail=(
                "The current plan has no accepted preview yet. Status contains "
                "the durable planning progress; Hermes did not fabricate one."
            ),
            threadId=thread_id,
            status=status,
        )

    if task_position is not None:
        offsets: list[int] = [task_position - 1]
        page_limit = 1
    else:
        offsets = [0]
        page_limit = 200

    pinned_identity: Optional[tuple[Any, ...]] = None
    pages: list[dict[str, Any]] = []
    cursor = offsets[0]
    while True:
        page = _invoke_internal(
            {
                "action": "preview",
                "thread_id": thread_id,
                "preview_result_id": preview_result_id,
                "offset": cursor,
                "limit": page_limit,
            },
            runtime_kwargs,
        )
        if page.get("ok") is not True:
            return _forward(page, intent="show")
        if page.get("deliveryReceiptPending") is not True:
            return _failure(
                "planning.facade_preview_delivery_unavailable",
                detail=(
                    "The exact preview page could not be bound to this gateway "
                    "send generation. It remains unreviewed and cannot be "
                    "approved."
                ),
                threadId=thread_id,
                previewResultId=preview_result_id,
                offset=cursor,
            )
        identity = (
            page.get("threadId"),
            page.get("runId"),
            page.get("previewResultId"),
            page.get("previewResultHash"),
            page.get("planHash"),
            page.get("basisInputSequence"),
            page.get("taskCount"),
            page.get("acceptedAt"),
        )
        if pinned_identity is None:
            pinned_identity = identity
        elif identity != pinned_identity:
            return _failure(
                "planning.preview_revision_changed",
                detail=(
                    "The immutable preview identity changed while pages were "
                    "being prepared. No mixed revision was presented."
                ),
                state_changed=False,
            )
        returned = page.get("returned")
        page_offset = page.get("offset")
        task_count = page.get("taskCount")
        has_more = page.get("hasMore")
        if (
            isinstance(returned, bool)
            or not isinstance(returned, int)
            or returned < 1
            or returned > page_limit
            or isinstance(page_offset, bool)
            or not isinstance(page_offset, int)
            or page_offset != cursor
            or isinstance(task_count, bool)
            or not isinstance(task_count, int)
            or task_count < 1
            or cursor + returned > task_count
            or not isinstance(has_more, bool)
        ):
            return _failure(
                "planning.preview_cursor_invalid",
                detail="Dev Hub returned an empty or mismatched preview page.",
            )
        pages.append(
            {
                "offset": cursor,
                "count": returned,
                "firstTask": cursor + 1,
                "lastTask": cursor + returned,
                "pageDigest": page.get("pageDigest"),
            }
        )
        if task_position is not None:
            break
        next_offset = page.get("nextOffset")
        expected_next = cursor + returned
        if has_more:
            if (
                isinstance(next_offset, bool)
                or not isinstance(next_offset, int)
                or next_offset != expected_next
                or expected_next >= task_count
            ):
                return _failure(
                    "planning.preview_cursor_stalled",
                    detail=(
                        "Dev Hub said more preview work exists but did not "
                        "advance by the exact delivered range."
                    ),
                )
            cursor = next_offset
            continue
        if next_offset is not None or expected_next != task_count:
            return _failure(
                "planning.preview_coverage_incomplete",
                detail=(
                    "Dev Hub ended preview pagination before the immutable "
                    "task count was fully covered."
                ),
            )
        break

    if pinned_identity is None:
        return _failure(
            "planning.facade_preview_delivery_unavailable",
            detail="No immutable preview page was prepared for delivery.",
        )
    return {
        "ok": True,
        "route": "planning_v2",
        "intent": "show",
        "threadId": thread_id,
        "previewResultId": pinned_identity[2],
        "previewResultHash": pinned_identity[3],
        "planHash": pinned_identity[4],
        "basisInputSequence": pinned_identity[5],
        "taskCount": pinned_identity[6],
        "pageCount": len(pages),
        "pages": pages,
        "detail": detail,
        "completePreviewScheduledForExactDelivery": (
            task_position is None
        ),
        "approvalRemainsLockedUntilProviderDelivery": True,
    }


def _cancel(
    *,
    client: PlanningV2Client,
    thread: PlanningThreadDTO,
) -> dict[str, Any]:
    cancel = getattr(client, "cancel_thread", None)
    if not callable(cancel):
        return _failure(
            "planning.facade_cancel_unavailable",
            detail=(
                "The installed Hermes Planning V2 client does not yet expose "
                "the atomic thread cancellation contract. Nothing was changed."
            ),
        )
    response = cancel(
        str(thread["threadId"]),
        origin=client.current_origin(),
        reason="explicit_human_cancel",
    )
    return _success(
        response.payload,
        intent="cancel",
        stateChanged=not bool(response.payload["replayed"]),
    )


def _approval_thread(
    client: PlanningV2Client,
    arguments: Mapping[str, Any],
) -> tuple[
    Optional[PlanningThreadDTO],
    Optional[dict[str, Any]],
    Optional[dict[str, Any]],
]:
    resolved = client.resolve_current_thread(
        origin=client.current_origin()
    ).payload
    threads = list(resolved["threads"])
    if resolved["match"] == "none":
        return None, None, _failure(
            "planning.facade_current_thread_not_found",
            detail="This conversation has no active plan to approve.",
        )
    eligible: list[tuple[PlanningThreadDTO, dict[str, Any]]] = []
    unreadable: list[
        tuple[PlanningThreadDTO, dict[str, Any]]
    ] = []
    for thread in threads:
        preview_id = str(thread.get("activePreviewVersionId") or "")
        if not preview_id:
            continue
        try:
            page = client.get_preview_page(
                str(thread["threadId"]),
                preview_id,
                offset=0,
                limit=1,
            ).payload
        except PlanningV2ClientError as exc:
            unreadable.append((thread, exc.compact()))
            continue
        review = page.get("reviewStatus")
        if (
            page.get("approvalEligible") is True
            and isinstance(review, Mapping)
            and review.get("complete") is True
        ):
            eligible.append((thread, page))
    if len(eligible) == 1 and not unreadable:
        return eligible[0][0], eligible[0][1], None
    if unreadable:
        return None, None, _failure(
            "planning.facade_preview_status_unavailable",
            detail=(
                "Hermes could not prove the review state of every candidate "
                "preview, so it did not guess or approve another one."
            ),
            retryable=any(
                bool(failure.get("retryable"))
                for _thread, failure in unreadable
            ),
            unreadableChoices=[
                {
                    **_thread_choices([thread])[0],
                    "code": failure.get("code"),
                }
                for thread, failure in unreadable
            ],
        )
    if not eligible:
        return None, None, _failure(
            "planning.facade_preview_not_fully_reviewed",
            detail=(
                "No active preview in this conversation has complete "
                "provider-confirmed page coverage. Use intent=show first; "
                "approval was not attempted."
            ),
            unreadableChoices=[],
        )
    selected = _selected_thread(
        [item[0] for item in eligible],
        arguments.get("selectedOption"),
    )
    if selected is not None:
        for thread, page in eligible:
            if thread["threadId"] == selected["threadId"]:
                return thread, page, None
    return None, None, _failure(
        "planning.facade_approval_ambiguous",
        detail=(
            "More than one preview in this exact conversation is fully "
            "reviewed. Hermes did not guess which one the human approved."
        ),
        choices=_thread_choices([item[0] for item in eligible]),
    )


def _approve(
    *,
    client: PlanningV2Client,
    arguments: Mapping[str, Any],
    runtime_kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    exact_text = _exact_user_text(runtime_kwargs)
    supplied = str(arguments.get("approvalMessage") or "")
    evidence = arguments.get("approvalEvidence")
    if supplied != exact_text:
        return _failure(
            "planning.approval_message_mismatch",
            detail=(
                "approvalMessage is not the exact current human turn. Approval "
                "was not attempted."
            ),
        )
    if not isinstance(evidence, Mapping):
        return _failure(
            "planning.approval_evidence_invalid",
            detail="Exact structured approval evidence is required.",
        )
    exact_quote = evidence.get("exactQuote")
    if (
        evidence.get("meaning") != "approve_current_preview_exactly"
        or not isinstance(exact_quote, str)
        or not exact_quote.strip()
        or exact_quote not in exact_text
    ):
        return _failure(
            "planning.approval_evidence_invalid",
            detail=(
                "The approval evidence must quote a verbatim clause from the "
                "complete current human message."
            ),
        )
    thread, page, failure = _approval_thread(client, arguments)
    if failure is not None:
        return failure
    if thread is None or page is None:
        return _failure(
            "planning.facade_approval_identity_missing",
            detail=(
                "Hermes could not bind the approval to one immutable preview. "
                "Nothing was applied."
            ),
        )
    result = _invoke_internal(
        {
            "action": "approve_apply",
            "thread_id": thread["threadId"],
            "preview_result_id": page["previewResultId"],
            "expected_preview_hash": page["previewResultHash"],
            "expected_plan_hash": page["planHash"],
            "approval_evidence": dict(evidence),
        },
        runtime_kwargs,
    )
    return _forward(
        result,
        publicTool=_APPROVAL_TOOL,
        exactLaterTurnApproval=True,
        fullyReviewedPreview=True,
    )


def invoke_public_tasking_tool(
    *,
    tool_name: str,
    arguments: Mapping[str, Any],
    runtime_kwargs: Mapping[str, Any],
) -> str | Mapping[str, Any]:
    """Invoke one negotiated public alias against the canonical V2 engine."""

    if not isinstance(arguments, Mapping) or not isinstance(
        runtime_kwargs,
        Mapping,
    ):
        return _json(
            _failure(
                "planning.facade_request_invalid",
                detail="Facade arguments and runtime context must be objects.",
            )
        )
    try:
        client = PlanningV2Client()
        if tool_name == _APPROVAL_TOOL:
            return _json(
                _approve(
                    client=client,
                    arguments=arguments,
                    runtime_kwargs=runtime_kwargs,
                )
            )
        if tool_name != _PLAN_TOOL:
            return _json(
                _failure(
                    "planning.facade_public_tool_unsupported",
                    detail=f"Unsupported public tasking alias: {tool_name!r}.",
                )
            )
        intent = str(arguments.get("intent") or "").strip().lower()
        if intent not in _PLAN_INTENTS:
            return _json(
                _failure(
                    "planning.facade_intent_invalid",
                    detail=(
                        "intent must be new, revise, retry, resume, status, "
                        "show, or cancel."
                    ),
                )
            )
        if intent in {"new", "revise", "retry", "resume"}:
            # Validate product-policy input before any durable local or remote
            # side effect. The internal engine validates again at admission.
            _run_policy(arguments)
            _route_policy()
        if intent == "new":
            result = _create(
                client=client,
                arguments=arguments,
                runtime_kwargs=runtime_kwargs,
            )
            return _json(result)

        thread, resolution_failure = _resolve_thread(client, arguments)
        if resolution_failure is not None:
            return _json(resolution_failure)
        if thread is None:
            return _json(
                _failure(
                    "planning.facade_thread_resolution_invalid",
                    detail=(
                        "Dev Hub returned no exact planning thread. Nothing "
                        "was changed."
                    ),
                )
            )
        if intent == "revise":
            result = _revise(
                client=client,
                thread=thread,
                arguments=arguments,
                runtime_kwargs=runtime_kwargs,
            )
        elif intent == "retry":
            result = _retry(
                client=client,
                thread=thread,
                arguments=arguments,
                runtime_kwargs=runtime_kwargs,
            )
        elif intent == "resume":
            result = _resume(
                client=client,
                thread=thread,
                arguments=arguments,
                runtime_kwargs=runtime_kwargs,
            )
        elif intent == "status":
            result = _forward(
                _status(
                    thread_id=str(thread["threadId"]),
                    runtime_kwargs=runtime_kwargs,
                ),
                intent="status",
                title=thread.get("title"),
            )
        elif intent == "show":
            result = _show(
                client=client,
                thread=thread,
                arguments=arguments,
                runtime_kwargs=runtime_kwargs,
            )
        else:
            result = _cancel(client=client, thread=thread)
        return _json(result)
    except PlanningV2ClientError as exc:
        compact = exc.compact()
        return _json(
            _failure(
                str(compact.get("code") or "planning.facade_failed"),
                detail=compact.get("detail") or exc.code,
                retryable=bool(compact.get("retryable")),
                outcomeAmbiguous=bool(compact.get("outcomeAmbiguous")),
                httpStatus=compact.get("httpStatus"),
            )
        )
    except Exception:
        logger.exception(
            "Unexpected failure at the Dev Hub Planning V2 facade boundary"
        )
        return _json(
            _failure(
                "planning.facade_internal_error",
                detail=(
                    "Hermes hit an unexpected local planning error. No "
                    "unverified retry or replacement operation was created."
                ),
                retryable=True,
                outcomeAmbiguous=True,
            )
        )


__all__ = [
    "FACADE_PROTOCOL",
    "get_facade_capabilities",
    "invoke_public_tasking_tool",
]
