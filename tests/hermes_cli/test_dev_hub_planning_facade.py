from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from agent.auxiliary_client import (
    get_runtime_main_route,
    scoped_runtime_main,
)
from hermes_cli import dev_hub_planning_facade as facade
from hermes_cli.dev_hub_planning_v2 import (
    PlanningV2ConfigError,
    PlanningV2Response,
    derive_thread_id_from_origin,
)
from hermes_cli.planning_artifact_spool import ArtifactRecoveryRecord
from hermes_cli.turn_origin import (
    TurnAttachmentOriginV1,
    TurnOriginV1,
    scoped_turn_origin,
    scoped_turn_user_text,
    turn_attachment_path_fingerprint,
)


def _wire_origin(*, event: str = "event-current") -> dict[str, Any]:
    return {
        "schemaVersion": "1.0",
        "provider": "discord",
        "gatewayInstanceId": "runner-1",
        "gatewayAccountId": "account-1",
        "chatId": "chat-1",
        "threadId": "provider-thread-1",
        "messageId": f"message-{event}",
        "senderId": "user-1",
        "chatType": "channel",
        "sourceTimestamp": "2026-07-28T10:00:00Z",
        "providerEventId": event,
    }


def _turn_origin(
    *,
    event: str = "event-current",
    attachments: tuple[TurnAttachmentOriginV1, ...] = (),
) -> TurnOriginV1:
    return TurnOriginV1(
        provider="discord",
        gateway_account_id="account-1",
        chat_id="chat-1",
        thread_id="provider-thread-1",
        message_id=f"message-{event}",
        sender_id="user-1",
        chat_type="channel",
        source_timestamp="2026-07-28T10:00:00Z",
        event_id=event,
        attachments=attachments,
    )


def _thread(
    thread_id: str = "thread-1",
    *,
    title: str = "Ship stable planning",
    preview_id: str | None = None,
) -> dict[str, Any]:
    return {
        "threadId": thread_id,
        "title": title,
        "status": "active",
        "activeRunId": "run-1",
        "activePreviewVersionId": preview_id,
        "updatedAt": "2026-07-28T10:00:00Z",
    }


def _resolution(*threads: dict[str, Any]) -> PlanningV2Response:
    count = len(threads)
    return PlanningV2Response(
        status=200,
        payload={
            "match": "none" if count == 0 else "one" if count == 1 else "ambiguous",
            "matchCount": count,
            "threads": list(threads),
        },
    )


class FakeClient:
    runner_id = "runner-1"

    def __init__(
        self,
        *,
        origin: dict[str, Any] | None = None,
        resolution: PlanningV2Response | None = None,
    ) -> None:
        self.origin = origin or _wire_origin()
        self.resolution = resolution or _resolution(_thread())
        self.preview_pages: dict[str, dict[str, Any]] = {}
        self.cancel_calls: list[dict[str, Any]] = []
        self.cancel_replayed = False

    def current_origin(self):
        return dict(self.origin)

    def resolve_current_thread(self, *, origin):
        assert origin == self.origin
        return self.resolution

    def get_thread(self, thread_id: str):
        matches = [
            item
            for item in self.resolution.payload["threads"]
            if item["threadId"] == thread_id
        ]
        assert len(matches) == 1
        return PlanningV2Response(
            status=200,
            payload={
                "thread": matches[0],
                "bindings": [],
                "inputEventCount": 1,
                "semanticEventHead": 1,
            },
        )

    def get_preview_page(
        self,
        thread_id: str,
        preview_result_id: str,
        *,
        offset: int,
        limit: int,
    ):
        page = dict(self.preview_pages[thread_id])
        assert page["previewResultId"] == preview_result_id
        return PlanningV2Response(status=200, payload=page)

    def cancel_thread(self, thread_id: str, *, origin, reason: str):
        self.cancel_calls.append(
            {
                "threadId": thread_id,
                "origin": origin,
                "reason": reason,
            }
        )
        return PlanningV2Response(
            status=200,
            payload={
                "ok": True,
                "replayed": self.cancel_replayed,
                "thread": {
                    **_thread(thread_id),
                    "status": "cancelled",
                },
                "event": {
                    "threadId": thread_id,
                    "eventType": "planning_cancelled",
                },
                "cancelled": {
                    "runs": 1,
                    "workItems": 3,
                    "attempts": 1,
                    "semanticEvents": 0,
                    "semanticDeliveries": 0,
                    "applyAdmissions": 0,
                },
            },
        )


@pytest.fixture
def route(monkeypatch):
    monkeypatch.setattr(
        facade,
        "get_runtime_main_route",
        lambda: {
            "provider": "openrouter",
            "model": "anthropic/claude-sonnet-4",
            "api_mode": "chat_completions",
        },
    )


def _invoke(
    arguments: dict[str, Any],
    *,
    tool_name: str = "agent_ops_task_plan",
    text: str = "Plan this change",
) -> dict[str, Any]:
    with (
        scoped_turn_origin(_turn_origin()),
        scoped_turn_user_text(text),
    ):
        return json.loads(
            facade.invoke_public_tasking_tool(
                tool_name=tool_name,
                arguments=arguments,
                runtime_kwargs={"user_task": text, "session_id": "session-1"},
            )
        )


def test_capability_contract_is_exact_and_has_no_legacy_fallback() -> None:
    capabilities = facade.get_facade_capabilities()
    assert capabilities["protocol"] == facade.FACADE_PROTOCOL
    assert capabilities["legacyFallback"] is False
    assert capabilities["publicTools"] == {
        "agent_ops_task_plan": {
            "inputSchema": "agent-ops-task-plan-natural/1"
        },
        "agent_ops_task_approve_apply": {
            "inputSchema": "agent-ops-task-approve-natural/1"
        },
    }


def test_runtime_route_uses_exact_context_local_values_without_secret() -> None:
    secret = "must-never-leave-runtime"
    with scoped_runtime_main(
        {
            "provider": "OpenRouter",
            "model": "anthropic/claude-sonnet-4",
            "api_mode": "chat_completions",
            "base_url": "https://router.example/v1",
            "api_key": secret,
            "auth_mode": "bearer",
        }
    ):
        route = get_runtime_main_route()

    assert route == {
        "provider": "openrouter",
        "model": "anthropic/claude-sonnet-4",
        "api_mode": "chat_completions",
    }
    assert secret not in json.dumps(route)


def test_new_preserves_all_natural_evidence_and_starts_exact_route(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    expected_thread_id = derive_thread_id_from_origin(client.origin)
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(facade, "list_artifact_recoveries", lambda **_kwargs: ())
    calls: list[dict[str, Any]] = []

    def internal(arguments, _runtime):
        calls.append(dict(arguments))
        if arguments["action"] == "create":
            return {"ok": True, "threadId": expected_thread_id}
        return {
            "ok": True,
            "action": "start_run",
            "threadId": expected_thread_id,
            "runId": "run-1",
            "runStatus": "queued",
        }

    monkeypatch.setattr(facade, "_invoke_internal", internal)
    evidence = [
        {
            "type": "repository",
            "reference": f"repo:{index}",
            "summary": f"Evidence {index}",
        }
        for index in range(137)
    ]
    constraints = [f"Constraint {index}" for index in range(137)]
    request = {
        "intent": "new",
        "mode": "deep",
        "laneHint": "dev-hub",
        "evidence": evidence,
        "constraints": constraints,
    }
    result = _invoke(request, text="Build a crash-proof planner")

    assert result["ok"] is True
    assert result["draftPersisted"] is True
    assert [call["action"] for call in calls] == ["create", "start_run"]
    create = calls[0]
    assert create["project_id"] == "dev-hub"
    assert create["title"] == "Build a crash-proof planner"
    assert create["payload"]["request"]["evidence"] == evidence
    assert create["payload"]["request"]["constraints"] == constraints
    assert calls[1]["route_policy"] == {
        "modelRoute": {
            "provider": "openrouter",
            "model": "anthropic/claude-sonnet-4",
            "apiMode": "chat_completions",
        }
    }
    assert calls[1]["run_policy"] == {"planningMode": "deep"}


def test_137_attachments_are_all_spooled_before_first_hub_call_and_uploaded(
    monkeypatch,
    route,
    tmp_path: Path,
) -> None:
    paths = [tmp_path / f"artifact-{index}.png" for index in range(1, 138)]
    attachments = tuple(
        TurnAttachmentOriginV1(
            attachment_id=f"attachment-{index}",
            ingress_ordinal=index,
            path_fingerprint=turn_attachment_path_fingerprint(str(path)),
            local_path=str(path),
        )
        for index, path in enumerate(paths, 1)
    )
    turn = _turn_origin(attachments=attachments)
    client = FakeClient()
    expected_thread_id = derive_thread_id_from_origin(client.origin)
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    events: list[str] = []
    records: list[ArtifactRecoveryRecord] = []

    def register(**kwargs):
        position = kwargs["position"]
        events.append(f"stage:{position}")
        token = f"token-{position}"
        records.append(
            ArtifactRecoveryRecord(
                token=token,
                thread_id=kwargs["thread_id"],
                snapshot_path=f"/private/spool/{position}.png",
                origin=dict(kwargs["origin"]),
                role=kwargs["role"],
                position=position,
                required=True,
                idempotency_key=kwargs["idempotency_key"],
                content_type=None,
                retain_until=None,
                attachment_identity=kwargs["attachment_identity"],
                ingress_ordinal=position,
                checksum="sha256:" + "a" * 64,
                size_bytes=100,
            )
        )
        return token

    def pending(**_kwargs):
        return tuple(records)

    def internal(arguments, _runtime):
        events.append(f"hub:{arguments['action']}")
        if arguments["action"] == "create":
            return {"ok": True, "threadId": expected_thread_id}
        if arguments["action"] == "upload_artifact":
            return {"ok": True, "inputStored": True}
        return {
            "ok": True,
            "threadId": expected_thread_id,
            "runStatus": "queued",
        }

    monkeypatch.setattr(facade, "register_artifact_recovery", register)
    monkeypatch.setattr(facade, "list_artifact_recoveries", pending)
    monkeypatch.setattr(facade, "_invoke_internal", internal)
    with (
        scoped_turn_origin(turn),
        scoped_turn_user_text("Plan every attachment"),
    ):
        result = json.loads(
            facade.invoke_public_tasking_tool(
                tool_name="agent_ops_task_plan",
                arguments={"intent": "new"},
                runtime_kwargs={"user_task": "Plan every attachment"},
            )
        )

    assert result["ok"] is True
    assert result["attachmentsUploaded"] == 137
    first_hub = next(index for index, value in enumerate(events) if value.startswith("hub:"))
    assert first_hub == 137
    assert events[first_hub] == "hub:create"
    assert events.count("hub:upload_artifact") == 137
    assert "/private/spool" not in json.dumps(result)


def test_missing_live_model_route_fails_before_mutation_or_spooling(
    monkeypatch,
) -> None:
    client = FakeClient()
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(
        facade,
        "get_runtime_main_route",
        lambda: {"provider": "", "model": "", "api_mode": ""},
    )
    calls: list[str] = []
    monkeypatch.setattr(
        facade,
        "register_artifact_recovery",
        lambda **_kwargs: calls.append("stage"),
    )
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda *_args: calls.append("hub"),
    )

    result = _invoke({"intent": "new"})
    assert result["ok"] is False
    assert result["code"] == "planning.model_route_missing"
    assert result["stateChanged"] is False
    assert calls == []


def test_invalid_mode_fails_before_any_local_or_remote_side_effect(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    calls: list[str] = []
    monkeypatch.setattr(
        facade,
        "register_artifact_recovery",
        lambda **_kwargs: calls.append("stage"),
    )
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda *_args: calls.append("hub"),
    )

    result = _invoke({"intent": "new", "mode": "unbounded"})

    assert result["ok"] is False
    assert result["code"] == "planning.facade_mode_invalid"
    assert result["stateChanged"] is False
    assert calls == []


def test_ambiguous_create_never_claims_the_draft_was_not_persisted(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(
        facade,
        "list_artifact_recoveries",
        lambda **_kwargs: (),
    )
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda *_args: {
            "ok": False,
            "code": "planning.transport_timeout",
            "outcomeAmbiguous": True,
            "retryable": True,
            "detail": "The create response was not observed.",
        },
    )

    result = _invoke({"intent": "new"})

    assert result["ok"] is False
    assert result["outcomeAmbiguous"] is True
    assert result["draftPersisted"] is None
    assert result["draftPersistence"] == "unknown"


def test_none_and_ambiguous_resolution_never_guess_or_mutate(
    monkeypatch,
    route,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda *_args: calls.append("mutated"),
    )
    none_client = FakeClient(resolution=_resolution())
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: none_client)
    none = _invoke({"intent": "revise"})

    ambiguous_client = FakeClient(
        resolution=_resolution(
            _thread("thread-a", title="Planner A"),
            _thread("thread-b", title="Planner B"),
        )
    )
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: ambiguous_client)
    ambiguous = _invoke({"intent": "status"})

    assert none["code"] == "planning.facade_current_thread_not_found"
    assert ambiguous["code"] == "planning.facade_current_thread_ambiguous"
    assert [choice["title"] for choice in ambiguous["choices"]] == [
        "Planner A",
        "Planner B",
    ]
    assert calls == []


def test_exact_displayed_choice_resolves_ambiguous_thread(
    monkeypatch,
    route,
) -> None:
    threads = [
        _thread("thread-a", title="Planner A"),
        _thread("thread-b", title="Planner B"),
    ]
    client = FakeClient(resolution=_resolution(*threads))
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda arguments, _runtime: {
            "ok": True,
            "threadId": arguments["thread_id"],
            "runStatus": "running",
        },
    )
    label = facade._choice_label(threads[1])
    result = _invoke({"intent": "status", "selectedOption": label})
    assert result["ok"] is True
    assert result["threadId"] == "thread-b"


def test_thread_selection_never_uses_casefold_or_foreign_explicit_id(
    monkeypatch,
    route,
) -> None:
    threads = [
        _thread("thread-a", title="Planner A"),
        _thread("thread-b", title="Planner B"),
    ]
    client = FakeClient(resolution=_resolution(*threads))
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    calls: list[str] = []
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda *_args: calls.append("read-or-mutate"),
    )

    wrong_case = _invoke(
        {"intent": "status", "selectedOption": "planner b"}
    )
    foreign = _invoke(
        {"intent": "status", "thread_id": "thread-from-another-chat"}
    )

    assert wrong_case["code"] == "planning.facade_thread_selection_invalid"
    assert foreign["code"] == "planning.facade_thread_not_in_conversation"
    assert calls == []


def test_retry_does_not_append_and_does_not_duplicate_active_run(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(facade, "list_artifact_recoveries", lambda **_kwargs: ())
    calls: list[str] = []

    def internal(arguments, _runtime):
        calls.append(arguments["action"])
        assert arguments["action"] != "continue"
        return {
            "ok": True,
            "threadId": "thread-1",
            "runStatus": "running",
        }

    monkeypatch.setattr(facade, "_invoke_internal", internal)
    result = _invoke({"intent": "retry"}, text="Try again")
    assert result["retryDisposition"] == "already_recovering"
    assert calls == ["status"]


def test_retry_recovers_pending_spool_then_runs_same_input_head(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    record = ArtifactRecoveryRecord(
        token="recovery-token",
        thread_id="thread-1",
        snapshot_path="/private/never-public.pdf",
        origin=_wire_origin(event="old-event"),
        role="turn_attachment",
        position=1,
        required=True,
        idempotency_key="artifact-key",
        content_type="application/pdf",
        retain_until=None,
        attachment_identity="attachment-1",
        ingress_ordinal=1,
        checksum="sha256:" + "b" * 64,
        size_bytes=50,
    )
    monkeypatch.setattr(
        facade,
        "list_artifact_recoveries",
        lambda **_kwargs: (record,),
    )
    calls: list[dict[str, Any]] = []

    def internal(arguments, _runtime):
        calls.append(dict(arguments))
        if arguments["action"] == "status":
            return {
                "ok": True,
                "threadId": "thread-1",
                "runStatus": "failed",
            }
        if arguments["action"] == "upload_artifact":
            return {"ok": True, "inputStored": True}
        return {
            "ok": True,
            "threadId": "thread-1",
            "runStatus": "queued",
        }

    monkeypatch.setattr(facade, "_invoke_internal", internal)
    result = _invoke({"intent": "retry"}, text="Retry the saved draft")

    assert result["ok"] is True
    assert result["retriedImmutableHead"] is True
    assert result["appendedRetryMessage"] is False
    assert [call["action"] for call in calls] == [
        "upload_artifact",
        "status",
        "start_run",
    ]
    assert calls[0] == {
        "action": "upload_artifact",
        "recovery_token": "recovery-token",
    }
    assert "never-public" not in json.dumps(result)


def test_resume_keeps_active_automatic_recovery_authoritative(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(facade, "list_artifact_recoveries", lambda **_kwargs: ())
    calls: list[str] = []

    def internal(arguments, _runtime):
        calls.append(arguments["action"])
        return {
            "ok": True,
            "threadId": "thread-1",
            "runStatus": "waiting",
        }

    monkeypatch.setattr(facade, "_invoke_internal", internal)
    result = _invoke({"intent": "resume"})
    assert result["resumeDisposition"] == "automatic_recovery_active"
    assert calls == ["status"]


def _status_with_preview() -> dict[str, Any]:
    return {
        "ok": True,
        "threadId": "thread-1",
        "runId": "run-1",
        "runStatus": "succeeded",
        "preview": {
            "ready": True,
            "status": "ready",
            "versionId": "preview-1",
        },
    }


def _internal_preview_page(
    *,
    offset: int,
    returned: int,
    task_count: int,
    next_offset: int | None,
    preview_hash: str = "sha256:" + "a" * 64,
) -> dict[str, Any]:
    return {
        "ok": True,
        "threadId": "thread-1",
        "runId": "run-1",
        "previewResultId": "preview-1",
        "previewResultHash": preview_hash,
        "planHash": "sha256:" + "b" * 64,
        "basisInputSequence": 4,
        "taskCount": task_count,
        "acceptedAt": "2026-07-28T10:00:00Z",
        "offset": offset,
        "returned": returned,
        "pageDigest": "sha256:" + f"{offset:064x}"[-64:],
        "hasMore": next_offset is not None,
        "nextOffset": next_offset,
        "deliveryReceiptPending": True,
    }


def test_show_registers_every_ordered_page_without_total_task_cap(
    monkeypatch,
    route,
) -> None:
    client = FakeClient(
        resolution=_resolution(_thread(preview_id="preview-1"))
    )
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    calls: list[dict[str, Any]] = []

    def internal(arguments, _runtime):
        calls.append(dict(arguments))
        if arguments["action"] == "status":
            return _status_with_preview()
        offset = arguments["offset"]
        if offset == 0:
            return _internal_preview_page(
                offset=0,
                returned=200,
                task_count=401,
                next_offset=200,
            )
        if offset == 200:
            return _internal_preview_page(
                offset=200,
                returned=200,
                task_count=401,
                next_offset=400,
            )
        return _internal_preview_page(
            offset=400,
            returned=1,
            task_count=401,
            next_offset=None,
        )

    monkeypatch.setattr(facade, "_invoke_internal", internal)
    result = _invoke({"intent": "show", "detail": "all"})

    assert result["ok"] is True
    assert result["taskCount"] == 401
    assert result["pageCount"] == 3
    assert result["completePreviewScheduledForExactDelivery"] is True
    assert [call.get("offset") for call in calls[1:]] == [0, 200, 400]


def test_show_fails_closed_when_preview_identity_changes_between_pages(
    monkeypatch,
    route,
) -> None:
    client = FakeClient(
        resolution=_resolution(_thread(preview_id="preview-1"))
    )
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)

    def internal(arguments, _runtime):
        if arguments["action"] == "status":
            return _status_with_preview()
        if arguments["offset"] == 0:
            return _internal_preview_page(
                offset=0,
                returned=200,
                task_count=201,
                next_offset=200,
            )
        return _internal_preview_page(
            offset=200,
            returned=1,
            task_count=201,
            next_offset=None,
            preview_hash="sha256:" + "c" * 64,
        )

    monkeypatch.setattr(facade, "_invoke_internal", internal)
    result = _invoke({"intent": "show"})
    assert result["ok"] is False
    assert result["code"] == "planning.preview_revision_changed"


def test_show_rejects_non_contiguous_or_incomplete_server_pagination(
    monkeypatch,
    route,
) -> None:
    client = FakeClient(
        resolution=_resolution(_thread(preview_id="preview-1"))
    )
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)

    def internal(arguments, _runtime):
        if arguments["action"] == "status":
            return _status_with_preview()
        return _internal_preview_page(
            offset=0,
            returned=100,
            task_count=201,
            next_offset=200,
        )

    monkeypatch.setattr(facade, "_invoke_internal", internal)
    result = _invoke({"intent": "show"})

    assert result["ok"] is False
    assert result["code"] == "planning.preview_cursor_stalled"


def _approval_page(*, eligible: bool) -> dict[str, Any]:
    return {
        "threadId": "thread-1",
        "runId": "run-1",
        "previewResultId": "preview-1",
        "previewResultHash": "sha256:" + "a" * 64,
        "planHash": "sha256:" + "b" * 64,
        "basisInputSequence": 3,
        "taskCount": 7,
        "offset": 0,
        "limit": 1,
        "returned": 1,
        "tasks": [{}],
        "pageDigest": "sha256:" + "c" * 64,
        "reviewStatus": {
            "taskCount": 7,
            "coveredTaskCount": 7 if eligible else 0,
            "coveredRanges": [{"offset": 0, "count": 7}] if eligible else [],
            "missingRanges": [] if eligible else [{"offset": 0, "count": 7}],
            "complete": eligible,
        },
        "hasMore": True,
        "nextOffset": 1,
        "title": "Plan",
        "objective": "Ship",
        "summary": "Summary",
        "decisions": [],
        "coverage": {},
        "acceptedAt": "2026-07-28T10:00:00Z",
        "approvalEligible": eligible,
        "deliveryPayload": {},
        "deliveryPayloadDigest": "sha256:" + "d" * 64,
        "deliveryContent": "content",
        "deliveryContentDigest": "sha256:" + "e" * 64,
    }


def test_approval_reads_existing_receipts_without_redelivering_preview(
    monkeypatch,
    route,
) -> None:
    thread = _thread(preview_id="preview-1")
    client = FakeClient(resolution=_resolution(thread))
    client.preview_pages["thread-1"] = _approval_page(eligible=True)
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    calls: list[dict[str, Any]] = []

    def internal(arguments, _runtime):
        calls.append(dict(arguments))
        assert arguments["action"] == "approve_apply"
        return {
            "ok": True,
            "threadId": "thread-1",
            "previewResultId": "preview-1",
            "operationId": "operation-1",
            "status": "requested",
        }

    monkeypatch.setattr(facade, "_invoke_internal", internal)
    message = "Patvirtinu šį planą"
    result = _invoke(
        {
            "approvalMessage": message,
            "approvalEvidence": {
                "meaning": "approve_current_preview_exactly",
                "exactQuote": message,
            },
        },
        tool_name="agent_ops_task_approve_apply",
        text=message,
    )

    assert result["ok"] is True
    assert result["fullyReviewedPreview"] is True
    assert [call["action"] for call in calls] == ["approve_apply"]
    assert calls[0]["expected_preview_hash"] == "sha256:" + "a" * 64
    assert calls[0]["expected_plan_hash"] == "sha256:" + "b" * 64


def test_approval_rejects_model_transcription_before_preview_read(
    monkeypatch,
    route,
) -> None:
    client = FakeClient(
        resolution=_resolution(_thread(preview_id="preview-1"))
    )
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    calls: list[str] = []
    monkeypatch.setattr(
        client,
        "get_preview_page",
        lambda *_args, **_kwargs: calls.append("preview"),
    )
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda *_args: calls.append("mutate"),
    )
    result = _invoke(
        {
            "approvalMessage": "Model paraphrase",
            "approvalEvidence": {
                "meaning": "approve_current_preview_exactly",
                "exactQuote": "Patvirtinu",
            },
        },
        tool_name="agent_ops_task_approve_apply",
        text="Patvirtinu",
    )
    assert result["code"] == "planning.approval_message_mismatch"
    assert calls == []


def test_approval_does_not_guess_when_another_candidate_is_unreadable(
    monkeypatch,
    route,
) -> None:
    first = _thread("thread-1", title="Reviewed", preview_id="preview-1")
    second = _thread("thread-2", title="Unknown", preview_id="preview-2")
    client = FakeClient(resolution=_resolution(first, second))
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)

    def preview(thread_id, _preview_id, *, offset, limit):
        assert offset == 0
        assert limit == 1
        if thread_id == "thread-2":
            raise PlanningV2ConfigError(
                "planning.preview_temporarily_unavailable",
                retryable=True,
            )
        return PlanningV2Response(
            status=200,
            payload=_approval_page(eligible=True),
        )

    monkeypatch.setattr(client, "get_preview_page", preview)
    message = "Patvirtinu šį planą"
    result = _invoke(
        {
            "approvalMessage": message,
            "approvalEvidence": {
                "meaning": "approve_current_preview_exactly",
                "exactQuote": message,
            },
        },
        tool_name="agent_ops_task_approve_apply",
        text=message,
    )

    assert result["ok"] is False
    assert result["code"] == "planning.facade_preview_status_unavailable"
    assert result["retryable"] is True


def test_cancel_uses_exact_current_origin_and_atomic_client_contract(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    result = _invoke({"intent": "cancel"}, text="Atšauk šį planą")

    assert result["ok"] is True
    assert result["intent"] == "cancel"
    assert result["stateChanged"] is True
    assert client.cancel_calls == [
        {
            "threadId": "thread-1",
            "origin": client.origin,
            "reason": "explicit_human_cancel",
        }
    ]

    client.cancel_replayed = True
    replay = _invoke({"intent": "cancel"}, text="Atšauk šį planą")
    assert replay["ok"] is True
    assert replay["replayed"] is True
    assert replay["stateChanged"] is False


def test_public_failure_redacts_private_paths_and_recovery_capabilities(
    monkeypatch,
    route,
    tmp_path: Path,
) -> None:
    private = tmp_path / "gateway-cache" / "reference.pdf"
    private.parent.mkdir()
    private.write_bytes(b"private")
    attachment = TurnAttachmentOriginV1(
        attachment_id="attachment-1",
        ingress_ordinal=1,
        path_fingerprint=turn_attachment_path_fingerprint(str(private)),
        local_path=str(private),
    )
    client = FakeClient()
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    token = "artrec_v2_" + "a" * 64
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda *_args: {
            "ok": False,
            "code": "planning.test_failure",
            "detail": f"Could not read {private}; retry with {token}",
            "recovery": {
                "nextAction": {
                    "arguments": {
                        "recovery_token": token,
                    }
                }
            },
        },
    )

    with (
        scoped_turn_origin(_turn_origin(attachments=(attachment,))),
        scoped_turn_user_text("Show status"),
    ):
        result = json.loads(
            facade.invoke_public_tasking_tool(
                tool_name="agent_ops_task_plan",
                arguments={"intent": "status"},
                runtime_kwargs={"user_task": "Show status"},
            )
        )

    encoded = json.dumps(result)
    assert result["ok"] is False
    assert str(private) not in encoded
    assert token not in encoded
    assert "recovery_token" not in encoded


def test_unexpected_internal_exception_returns_typed_public_failure(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda *_args: (_ for _ in ()).throw(
            RuntimeError("/private/internal/path")
        ),
    )

    result = _invoke({"intent": "status"})

    assert result["ok"] is False
    assert result["code"] == "planning.facade_internal_error"
    assert result["retryable"] is True
    assert result["outcomeAmbiguous"] is True
    assert result["stateChanged"] is False
    assert "/private/internal/path" not in json.dumps(result)
