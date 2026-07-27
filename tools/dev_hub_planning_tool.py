"""Opt-in user-facing action for durable Dev Hub Planning V2 threads.

This tool does not replace normal Hermes conversation or the existing Kanban /
Agent Ops tasking paths.  It is registered only when the active profile
explicitly includes the ``planning_v2`` toolset and exact runner credentials
are available.  Invocations are direct writes, never shadow traffic.
"""

from __future__ import annotations

from collections import Counter
import json
from typing import Any, Mapping, Optional

from hermes_cli.dev_hub_planning_v2 import (
    PlanningOriginPayload,
    PlanningRunDTO,
    PlanningThreadProjection,
    PlanningV2Client,
    PlanningV2ClientError,
    PlanningV2ConfigError,
    derive_run_idempotency_key,
)
from hermes_cli.turn_origin import get_current_turn_origin
from tools.registry import registry, tool_error, tool_result


PLANNING_V2_TOOLSET = "planning_v2"
PLANNING_V2_TOOL_NAME = "agent_ops_planning_v2"


def _profile_opted_in(provider: Optional[str] = None) -> bool:
    """Require a global or provider-specific profile toolset opt-in."""

    try:
        from hermes_cli.config import load_config

        config = load_config()
    except Exception:
        return False
    if not isinstance(config, dict):
        return False

    def _contains(value: Any) -> bool:
        return (
            isinstance(value, (list, tuple, set))
            and PLANNING_V2_TOOLSET in value
        )

    if _contains(config.get("toolsets")):
        return True
    platform_toolsets = config.get("platform_toolsets")
    if not isinstance(platform_toolsets, Mapping):
        return False
    if provider is not None:
        return _contains(platform_toolsets.get(provider))
    return any(_contains(value) for value in platform_toolsets.values())


def _require_scoped_provider_opt_in() -> None:
    origin = get_current_turn_origin()
    if origin is None or _profile_opted_in(origin.provider):
        return
    raise PlanningV2ConfigError(
        "planning.tool_not_enabled_for_provider",
        detail=(
            f"Planning V2 is not enabled for the {origin.provider} "
            "gateway profile."
        ),
    )


def _check_planning_v2_requirements() -> bool:
    if not _profile_opted_in():
        return False
    try:
        PlanningV2Client()
    except PlanningV2ClientError:
        return False
    return True


def _mapping(
    args: Mapping[str, Any],
    name: str,
    *,
    default: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    value = args.get(name)
    if value is None:
        return dict(default or {})
    if not isinstance(value, dict):
        raise PlanningV2ConfigError(
            "planning.tool_argument_invalid",
            detail=f"{name} must be an object.",
        )
    return dict(value)


def _input_payload(
    args: Mapping[str, Any],
    *,
    user_task: Any = None,
) -> dict[str, Any]:
    """Build content without ever using it as identity."""

    explicit = args.get("payload")
    if explicit is not None and not isinstance(explicit, dict):
        raise PlanningV2ConfigError(
            "planning.tool_argument_invalid",
            detail="payload must be an object.",
        )
    payload = dict(explicit or {})
    message = args.get("message")
    if message is None and not payload:
        message = user_task
    if message is not None:
        text = str(message)
        if text:
            payload.setdefault("text", text)
    if not payload:
        raise PlanningV2ConfigError(
            "planning.input_required",
            detail="message or payload is required for a planning input.",
        )
    return payload


def _required_id(args: Mapping[str, Any], name: str) -> str:
    value = str(args.get(name) or "").strip()
    if not value:
        raise PlanningV2ConfigError(
            "planning.identifier_required",
            detail=f"{name} is required.",
        )
    return value


def _should_start_run(args: Mapping[str, Any]) -> bool:
    value = args.get("start_run")
    return True if value is None else bool(value)


def _binding_providers(
    thread_projection: Mapping[str, Any],
) -> list[str]:
    providers: set[str] = set()
    bindings = thread_projection.get("bindings")
    if not isinstance(bindings, list):
        return []
    for binding in bindings:
        if not isinstance(binding, Mapping):
            continue
        endpoint = binding.get("endpoint")
        if not isinstance(endpoint, Mapping):
            continue
        provider = endpoint.get("provider")
        if isinstance(provider, str) and provider:
            providers.add(provider)
    return sorted(providers)


def _work_progress(run: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    items = run.get("workItems") if isinstance(run, Mapping) else None
    work_items = [
        item for item in (items or []) if isinstance(item, Mapping)
    ]
    statuses = Counter(str(item.get("status") or "unknown") for item in work_items)
    server_progress = (
        run.get("progress") if isinstance(run, Mapping) else None
    )
    if isinstance(server_progress, Mapping):
        raw_states = server_progress.get("states")
        if isinstance(raw_states, Mapping):
            try:
                statuses = Counter(
                    {
                        str(state): max(0, int(count))
                        for state, count in raw_states.items()
                    }
                )
            except (TypeError, ValueError):
                pass
    completed_states = {"succeeded", "degraded", "superseded", "cancelled"}
    active_states = {"queued", "leased", "running", "retry_scheduled"}
    waiting_states = {"waiting", "needs_decision", "quarantined"}
    facts = []
    for item in work_items:
        progress = item.get("progress")
        status = str(item.get("status") or "unknown")
        if not progress and status not in waiting_states:
            continue
        fact: dict[str, Any] = {
            "workItemId": item.get("workItemId"),
            "workKind": item.get("workKind"),
            "scopeKey": item.get("scopeKey"),
            "status": status,
        }
        if isinstance(progress, dict) and progress:
            fact["progress"] = progress
        if item.get("terminalReason"):
            fact["reason"] = item["terminalReason"]
        facts.append(fact)
    total = sum(statuses.values())
    completed = sum(statuses[state] for state in completed_states)
    if isinstance(server_progress, Mapping):
        try:
            total = max(0, int(server_progress.get("total", total)))
            completed = max(
                0,
                int(server_progress.get("completed", completed)),
            )
        except (TypeError, ValueError):
            pass
    return {
        "total": total,
        "completed": completed,
        "active": sum(statuses[state] for state in active_states),
        "waiting": sum(statuses[state] for state in waiting_states),
        "byStatus": dict(sorted(statuses.items())),
        "facts": facts,
    }


def _needs_decision(run: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    raw_items = run.get("workItems") if isinstance(run, Mapping) else None
    items = []
    for item in raw_items or []:
        if not isinstance(item, Mapping) or item.get("status") != "needs_decision":
            continue
        fact: dict[str, Any] = {
            "workItemId": item.get("workItemId"),
            "workKind": item.get("workKind"),
            "scopeKey": item.get("scopeKey"),
        }
        if item.get("terminalReason"):
            fact["reason"] = item["terminalReason"]
        progress = item.get("progress")
        if isinstance(progress, dict) and progress:
            fact["progress"] = progress
        items.append(fact)
    return {"required": bool(items), "items": items}


def _preview_fact(
    thread: Mapping[str, Any],
    run: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    version_id = thread.get("activePreviewVersionId")
    raw_items = run.get("workItems") if isinstance(run, Mapping) else None
    preview_items = [
        item
        for item in (raw_items or [])
        if isinstance(item, Mapping)
        and (
            item.get("workKind") in {"preview", "preview_reduce"}
            or item.get("scopeKey") == "preview"
        )
    ]
    latest = preview_items[-1] if preview_items else {}
    result: dict[str, Any] = {
        "ready": bool(version_id),
        "status": (
            "ready"
            if version_id
            else str(latest.get("status") or "not_started")
        ),
    }
    if version_id:
        result["versionId"] = version_id
    for source, target in (
        ("workItemId", "workItemId"),
        ("acceptedResultId", "resultId"),
    ):
        if latest.get(source):
            result[target] = latest[source]
    progress = latest.get("progress")
    if isinstance(progress, dict) and progress:
        result["progress"] = progress
    return result


def _event_facts(payload: Optional[Mapping[str, Any]]) -> list[dict[str, Any]]:
    events = payload.get("events") if isinstance(payload, Mapping) else None
    facts: list[dict[str, Any]] = []
    for event in events or []:
        if not isinstance(event, Mapping):
            continue
        facts.append(
            {
                "sequence": event.get("sequence"),
                "type": event.get("eventType"),
                "runId": event.get("runId"),
                "payload": (
                    event.get("payload")
                    if isinstance(event.get("payload"), dict)
                    else {}
                ),
            }
        )
    return facts


def _semantic_result(
    *,
    action: str,
    thread_projection: PlanningThreadProjection,
    run: Optional[PlanningRunDTO] = None,
    events: Optional[Mapping[str, Any]] = None,
    input_duplicate: Optional[bool] = None,
    run_replayed: Optional[bool] = None,
    preview_invalidated: Optional[bool] = None,
) -> dict[str, Any]:
    thread = thread_projection.get("thread") or {}
    result: dict[str, Any] = {
        "ok": True,
        "action": action,
        "threadId": thread.get("threadId"),
        "threadStatus": thread.get("status"),
        "inputEventCount": thread_projection.get("inputEventCount", 0),
        "semanticEventHead": thread_projection.get(
            "semanticEventHead", 0
        ),
        "boundProviders": _binding_providers(thread_projection),
        "progress": _work_progress(run),
        "needsDecision": _needs_decision(run),
        "preview": _preview_fact(thread, run),
    }
    if run:
        result["runId"] = run.get("runId")
        result["runStatus"] = run.get("status")
        result["basisInputSequence"] = run.get("basisInputSequence")
    if input_duplicate is not None:
        result["inputReplayed"] = input_duplicate
    if run_replayed is not None:
        result["runReplayed"] = run_replayed
    if preview_invalidated is not None:
        result["previewInvalidated"] = preview_invalidated
    if events is not None:
        result["events"] = _event_facts(events)
        result["nextEventSequence"] = events.get("nextSequence", 0)
    return result


def _run_key(
    args: Mapping[str, Any],
    *,
    client: PlanningV2Client,
    thread_id: str,
    origin: Optional[PlanningOriginPayload],
) -> str:
    explicit = str(args.get("idempotency_key") or "").strip()
    if explicit:
        return explicit
    resolved_origin = origin or client.current_origin()
    return derive_run_idempotency_key(
        runner_id=client.runner_id,
        thread_id=thread_id,
        provider=resolved_origin["provider"],
        gateway_account_id=resolved_origin["gatewayAccountId"],
        provider_event_id=resolved_origin["providerEventId"],
    )


def _start_run(
    client: PlanningV2Client,
    args: Mapping[str, Any],
    *,
    thread_id: str,
    origin: Optional[PlanningOriginPayload],
    idempotency_key: Optional[str] = None,
):
    return client.create_run(
        thread_id,
        idempotency_key=(
            idempotency_key
            or _run_key(
                args,
                client=client,
                thread_id=thread_id,
                origin=origin,
            )
        ),
        policy=_mapping(args, "run_policy"),
        route_policy=_mapping(args, "route_policy"),
        correlation_id=(
            str(args["correlation_id"])
            if args.get("correlation_id") is not None
            else None
        ),
    )


def _handle_planning_v2(args: dict, **kwargs: Any) -> str:
    if not _profile_opted_in():
        return tool_error(
            "Dev Hub Planning V2 is opt-in. Enable planning_v2 for this "
            "surface in the profile's platform_toolsets; normal Hermes "
            "conversation is unchanged.",
            code="planning.tool_not_enabled",
        )
    action = str(args.get("action") or "").strip().lower()
    if action not in {"create", "continue", "status", "events", "start_run"}:
        return tool_error(
            "action must be create, continue, status, events, or start_run",
            code="planning.action_invalid",
        )

    partial: dict[str, Any] = {}
    try:
        client = PlanningV2Client()
        _require_scoped_provider_opt_in()

        if action == "create":
            origin = client.current_origin()
            thread_response = client.create_thread(
                origin=origin,
                payload=_input_payload(
                    args,
                    user_task=kwargs.get("user_task"),
                ),
                input_kind=str(args.get("input_kind") or "message"),
                title=(
                    str(args["title"])
                    if args.get("title") is not None
                    else None
                ),
                project_id=(
                    str(args["project_id"])
                    if args.get("project_id") is not None
                    else None
                ),
                policy_version=(
                    str(args["policy_version"])
                    if args.get("policy_version") is not None
                    else None
                ),
                policy=_mapping(args, "thread_policy"),
            )
            thread_id = str(
                thread_response.payload["thread"]["threadId"]
            )
            partial = {
                "threadId": thread_id,
                "inputStored": True,
                "inputReplayed": bool(
                    thread_response.payload.get("duplicate")
                ),
            }
            run_response = None
            if _should_start_run(args):
                run_idempotency_key = _run_key(
                    args,
                    client=client,
                    thread_id=thread_id,
                    origin=origin,
                )
                partial["runIdempotencyKey"] = run_idempotency_key
                run_response = _start_run(
                    client,
                    args,
                    thread_id=thread_id,
                    origin=origin,
                    idempotency_key=run_idempotency_key,
                )
            return tool_result(
                _semantic_result(
                    action=action,
                    thread_projection=thread_response.payload,
                    run=run_response.payload if run_response else None,
                    input_duplicate=bool(
                        thread_response.payload.get("duplicate")
                    ),
                    run_replayed=(
                        run_response.status == 200
                        if run_response is not None
                        else None
                    ),
                )
            )

        if action == "continue":
            thread_id = _required_id(args, "thread_id")
            origin = client.current_origin()
            input_response = client.append_thread_input(
                thread_id,
                origin=origin,
                payload=_input_payload(
                    args,
                    user_task=kwargs.get("user_task"),
                ),
                input_kind=str(args.get("input_kind") or "message"),
            )
            partial = {
                "threadId": thread_id,
                "inputStored": True,
                "inputReplayed": bool(
                    input_response.payload.get("duplicate")
                ),
            }
            run_response = None
            if _should_start_run(args):
                run_idempotency_key = _run_key(
                    args,
                    client=client,
                    thread_id=thread_id,
                    origin=origin,
                )
                partial["runIdempotencyKey"] = run_idempotency_key
                run_response = _start_run(
                    client,
                    args,
                    thread_id=thread_id,
                    origin=origin,
                    idempotency_key=run_idempotency_key,
                )
            return tool_result(
                _semantic_result(
                    action=action,
                    thread_projection=input_response.payload,
                    run=run_response.payload if run_response else None,
                    input_duplicate=bool(
                        input_response.payload.get("duplicate")
                    ),
                    run_replayed=(
                        run_response.status == 200
                        if run_response is not None
                        else None
                    ),
                    preview_invalidated=bool(
                        input_response.payload.get("previewInvalidated")
                    ),
                )
            )

        if action == "events":
            thread_id = _required_id(args, "thread_id")
            after_sequence = int(args.get("after_sequence") or 0)
            events_response = client.get_thread_events(
                thread_id,
                after_sequence=after_sequence,
            )
            return tool_result(
                {
                    "ok": True,
                    "action": action,
                    "threadId": thread_id,
                    "afterSequence": after_sequence,
                    "nextEventSequence": events_response.payload[
                        "nextSequence"
                    ],
                    "events": _event_facts(events_response.payload),
                }
            )

        if action == "start_run":
            thread_id = _required_id(args, "thread_id")
            run_idempotency_key = _run_key(
                args,
                client=client,
                thread_id=thread_id,
                origin=None,
            )
            run_response = _start_run(
                client,
                args,
                thread_id=thread_id,
                origin=None,
                idempotency_key=run_idempotency_key,
            )
            partial = {
                "threadId": thread_id,
                "runStarted": True,
                "runId": run_response.payload.get("runId"),
                "runReplayed": run_response.status == 200,
                "runIdempotencyKey": run_idempotency_key,
            }
            thread_response = client.get_thread(thread_id)
            return tool_result(
                _semantic_result(
                    action=action,
                    thread_projection=thread_response.payload,
                    run=run_response.payload,
                    run_replayed=run_response.status == 200,
                )
            )

        thread_id = _required_id(args, "thread_id")
        thread_response = client.get_thread(thread_id)
        requested_run_id = str(args.get("run_id") or "").strip()
        run_id = requested_run_id or str(
            thread_response.payload["thread"].get("activeRunId") or ""
        )
        run_response = client.get_run(run_id) if run_id else None
        if (
            run_response is not None
            and str(run_response.payload.get("threadId") or "") != thread_id
        ):
            raise PlanningV2ConfigError(
                "planning.run_thread_mismatch",
                detail={
                    "threadId": thread_id,
                    "runId": run_id,
                    "runThreadId": run_response.payload.get("threadId"),
                },
            )
        events_response = (
            client.get_thread_events(
                thread_id,
                after_sequence=int(args.get("after_sequence") or 0),
            )
            if bool(args.get("include_events"))
            else None
        )
        return tool_result(
            _semantic_result(
                action=action,
                thread_projection=thread_response.payload,
                run=run_response.payload if run_response else None,
                events=(
                    events_response.payload
                    if events_response is not None
                    else None
                ),
            )
        )
    except PlanningV2ClientError as exc:
        failure = exc.compact()
        if partial:
            recovery = dict(partial)
            if exc.retryable or exc.ambiguous:
                recovery_arguments = {
                    "action": "start_run",
                    "thread_id": partial["threadId"],
                }
                if partial.get("runIdempotencyKey"):
                    recovery_arguments["idempotency_key"] = partial[
                        "runIdempotencyKey"
                    ]
                recovery["nextAction"] = {
                    "tool": PLANNING_V2_TOOL_NAME,
                    "arguments": recovery_arguments,
                }
            failure["recovery"] = recovery
        return json.dumps(failure, ensure_ascii=False)


PLANNING_V2_SCHEMA = {
    "name": PLANNING_V2_TOOL_NAME,
    "description": (
        "Opt-in durable Dev Hub Planning V2 action. Call only when the user "
        "explicitly asks to create, continue, start, or inspect a saved "
        "planning thread. Do not call for ordinary discussion, brainstorming, "
        "answers, the existing Kanban/tasking flow, or shadow traffic. "
        "Cross-provider continuation requires the explicit thread_id returned "
        "by an earlier call; never infer a planning thread from chat text."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": [
                    "create",
                    "continue",
                    "status",
                    "events",
                    "start_run",
                ],
                "description": "Explicit planning operation.",
            },
            "thread_id": {
                "type": "string",
                "description": (
                    "Exact Dev Hub planning thread id. Required for every "
                    "action except create."
                ),
            },
            "run_id": {
                "type": "string",
                "description": (
                    "Optional exact run id for status; otherwise the thread's "
                    "active run is read."
                ),
            },
            "message": {
                "type": "string",
                "description": (
                    "Planning input text. It is content only and is never used "
                    "as event or thread identity."
                ),
            },
            "payload": {
                "type": "object",
                "description": (
                    "Arbitrary structured planning input, including artifact "
                    "references. Hermes imposes no artifact or task-count cap."
                ),
                "additionalProperties": True,
            },
            "input_kind": {
                "type": "string",
                "description": "Input kind; defaults to message.",
            },
            "title": {
                "type": "string",
                "description": "Optional title for a new planning thread.",
            },
            "project_id": {
                "type": "string",
                "description": "Optional Dev Hub project id.",
            },
            "policy_version": {
                "type": "string",
                "description": "Optional thread policy version.",
            },
            "thread_policy": {
                "type": "object",
                "additionalProperties": True,
                "description": "Optional durable thread policy.",
            },
            "run_policy": {
                "type": "object",
                "additionalProperties": True,
                "description": "Optional policy for the new run.",
            },
            "route_policy": {
                "type": "object",
                "additionalProperties": True,
                "description": "Optional model/worker route policy.",
            },
            "correlation_id": {
                "type": "string",
                "description": "Optional caller correlation id.",
            },
            "idempotency_key": {
                "type": "string",
                "description": (
                    "Optional exact run Idempotency-Key. When omitted, Hermes "
                    "derives it from runner + planning thread + the scoped "
                    "provider/account/event namespace, never message text."
                ),
            },
            "start_run": {
                "type": "boolean",
                "description": (
                    "For create/continue, start a run in the same explicit "
                    "action. Defaults to true."
                ),
            },
            "after_sequence": {
                "type": "integer",
                "minimum": 0,
                "description": "Semantic event cursor for events/status.",
            },
            "include_events": {
                "type": "boolean",
                "description": "Include semantic events in status.",
            },
        },
        "required": ["action"],
    },
}


registry.register(
    name=PLANNING_V2_TOOL_NAME,
    toolset=PLANNING_V2_TOOLSET,
    schema=PLANNING_V2_SCHEMA,
    handler=_handle_planning_v2,
    check_fn=_check_planning_v2_requirements,
    emoji="🧭",
)


__all__ = [
    "PLANNING_V2_SCHEMA",
    "PLANNING_V2_TOOLSET",
    "PLANNING_V2_TOOL_NAME",
]
