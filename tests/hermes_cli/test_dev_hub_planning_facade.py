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
    PlanningV2HTTPError,
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
            "returnedCount": count,
            "threads": list(threads),
            "hasMore": False,
            "nextCursor": None,
        },
    )


def _attention_item(
    token: str = "pdra_" + ("a" * 48),
    *,
    provider: str = "discord",
    account: str = "account-1",
    chat: str = "chat-1",
) -> dict[str, Any]:
    return {
        "resolutionToken": token,
        "kind": "provider_outcome_ambiguous",
        "eventType": "planning_preview_ready",
        "generation": 1,
        "target": {
            "provider": provider,
            "gatewayAccountId": account,
            "chatId": chat,
            "threadId": "provider-thread-1",
        },
        "message": "The provider may have accepted this message.",
        "actions": [
            {
                "action": "mark_delivered",
                "acknowledgement": "user_observed_original_delivery",
                "meaning": "I observed the original message.",
            },
            {
                "action": "resend_acknowledged",
                "acknowledgement": "user_accepts_possible_duplicate",
                "meaning": "I accept possible duplicate delivery.",
            },
        ],
    }


def _apply_item(
    token: str = "par_" + ("a" * 64),
    *,
    next_action: str = "automatic_resume",
) -> dict[str, Any]:
    return {
        "recoveryToken": token,
        "state": (
            "revision_required"
            if next_action == "revise"
            else "resume_ready"
        ),
        "progress": {"total": 7, "published": 0},
        "nextAction": next_action,
        "message": (
            "Jira rejected a field; correct this same plan."
            if next_action == "revise"
            else "The original Jira operation can safely resume."
        ),
        "requiresFreshPreviewApproval": next_action == "revise",
        "reusesOriginalOperation": next_action == "automatic_resume",
        "automaticWhenJiraPreflightPasses": (
            next_action == "automatic_resume"
        ),
        "recoveryReason": (
            "jira_contract_rejected"
            if next_action == "revise"
            else None
        ),
        "requestedAt": "2026-07-28T10:00:00Z",
        "updatedAt": "2026-07-28T10:01:00Z",
    }


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
        self.attention_items: list[dict[str, Any]] = []
        self.delivery_resolution_calls: list[dict[str, Any]] = []
        self.apply_items: list[dict[str, Any]] = []
        self.apply_recovery_calls: list[dict[str, Any]] = []
        self.current_thread_resolution_calls: list[dict[str, Any]] = []
        self.rejected_current_thread_cursors: dict[
            str,
            tuple[str, int],
        ] = {}

    def current_origin(self):
        return dict(self.origin)

    def resolve_current_thread(
        self,
        *,
        origin,
        cursor: str | None = None,
        page_size: int = 50,
    ):
        assert origin == self.origin
        rejection = self.rejected_current_thread_cursors.get(cursor or "")
        if rejection is not None:
            raise PlanningV2HTTPError(
                rejection[0],
                status=rejection[1],
                detail="The current-thread choice cursor is not usable.",
            )
        all_threads = self.resolution.payload["threads"]
        start = 0
        if cursor is not None:
            prefix = "current-thread-offset:"
            assert cursor.startswith(prefix)
            start = int(cursor.removeprefix(prefix))
        page = list(all_threads[start : start + page_size])
        has_more = start + len(page) < len(all_threads)
        next_cursor = (
            f"current-thread-offset:{start + len(page)}"
            if has_more and page
            else None
        )
        self.current_thread_resolution_calls.append(
            {
                "cursor": cursor,
                "pageSize": page_size,
                "returnedCount": len(page),
            }
        )
        count = len(all_threads)
        return PlanningV2Response(
            status=200,
            payload={
                "match": (
                    "none"
                    if count == 0
                    else "one"
                    if count == 1
                    else "ambiguous"
                ),
                "matchCount": count,
                "returnedCount": len(page),
                "threads": page,
                "hasMore": has_more,
                "nextCursor": next_cursor,
            },
        )

    def get_delivery_attention(
        self,
        thread_id: str,
        *,
        origin,
        after_resolution_token: str | None = None,
        limit: int = 50,
    ):
        assert origin == self.origin
        assert thread_id
        start = 0
        if after_resolution_token is not None:
            tokens = [
                item["resolutionToken"] for item in self.attention_items
            ]
            start = tokens.index(after_resolution_token) + 1
        page = self.attention_items[start : start + limit]
        has_more = start + len(page) < len(self.attention_items)
        return PlanningV2Response(
            status=200,
            payload={
                "ok": True,
                "threadId": thread_id,
                "deliveryAttention": {
                    "status": (
                        "action_required"
                        if self.attention_items
                        else "clear"
                    ),
                    "count": len(self.attention_items),
                    "returnedCount": len(page),
                    "hasMore": has_more,
                    "nextAfterResolutionToken": (
                        page[-1]["resolutionToken"]
                        if has_more and page
                        else None
                    ),
                    "requiresExplicitToken": (
                        len(self.attention_items) > 1
                    ),
                    "items": list(page),
                },
                "authorizedBinding": {
                    "provider": origin["provider"],
                    "gatewayAccountId": origin["gatewayAccountId"],
                },
            },
        )

    def resolve_delivery_attention(
        self,
        thread_id: str,
        *,
        origin,
        resolution_token: str,
        action: str,
        reason: str,
    ):
        call = {
            "threadId": thread_id,
            "origin": origin,
            "resolutionToken": resolution_token,
            "action": action,
            "reason": reason,
        }
        self.delivery_resolution_calls.append(call)
        acknowledgement = {
            "mark_delivered": "user_observed_original_delivery",
            "resend_acknowledged": "user_accepts_possible_duplicate",
        }[action]
        return PlanningV2Response(
            status=200,
            payload={
                "ok": True,
                "replayed": False,
                "threadId": thread_id,
                "resolution": {
                    "resolutionToken": resolution_token,
                    "action": action,
                    "acknowledgement": acknowledgement,
                    "status": (
                        "attested_delivered"
                        if action == "mark_delivered"
                        else "resend_scheduled"
                    ),
                    "target": self.attention_items[0]["target"],
                    "generation": (
                        1 if action == "mark_delivered" else 2
                    ),
                    "resolvedAt": "2026-07-28T10:01:00Z",
                },
            },
        )

    def get_apply_status(
        self,
        thread_id: str,
        *,
        origin,
        after_recovery_token: str | None = None,
        limit: int = 20,
    ):
        assert origin == self.origin
        assert thread_id
        start = 0
        if after_recovery_token is not None:
            tokens = [
                item["recoveryToken"] for item in self.apply_items
            ]
            start = tokens.index(after_recovery_token) + 1
        page = self.apply_items[start : start + limit]
        has_more = start + len(page) < len(self.apply_items)
        automatic_resume_count = sum(
            item["nextAction"] == "automatic_resume"
            for item in self.apply_items
        )
        revision_required_count = sum(
            item["nextAction"] == "revise"
            for item in self.apply_items
        )
        return PlanningV2Response(
            status=200,
            payload={
                "ok": True,
                "threadId": thread_id,
                "applyStatus": {
                    "count": len(self.apply_items),
                    "actionableCount": (
                        automatic_resume_count
                        + revision_required_count
                    ),
                    "automaticResumeCount": automatic_resume_count,
                    "revisionRequiredCount": revision_required_count,
                    "returnedCount": len(page),
                    "hasMore": has_more,
                    "nextAfterRecoveryToken": (
                        page[-1]["recoveryToken"]
                        if has_more and page
                        else None
                    ),
                    "items": list(page),
                },
            },
        )

    def resolve_apply_recovery(
        self,
        thread_id: str,
        *,
        origin,
        reason: str,
        recovery_token: str | None = None,
    ):
        call = {
            "threadId": thread_id,
            "origin": origin,
            "reason": reason,
            "recoveryToken": recovery_token,
        }
        self.apply_recovery_calls.append(call)
        selected = next(
            item
            for item in self.apply_items
            if item["recoveryToken"] == recovery_token
        )
        if selected["nextAction"] == "revise":
            recovery = {
                "action": "revise",
                "state": "revision_input_appended",
                "inputSequence": 2,
                "snapshotCreated": True,
                "knownJiraIdentityCount": 1,
                "candidateJiraKeys": ["IL-101"],
                "oldOperationImmutable": True,
                "requiresFreshPreviewApproval": True,
            }
            next_action = "create_preview"
        else:
            recovery = {
                "action": "automatic_resume",
                "state": "resumed",
                "failedEffectsResumed": 1,
                "publishedEffectsPreserved": 0,
                "pendingEffectsPreserved": 6,
                "reusedOriginalOperation": True,
                "createdPlan": False,
                "createdCommand": False,
                "createdApproval": False,
            }
            next_action = "wait_for_apply"
        return PlanningV2Response(
            status=200,
            payload={
                "ok": True,
                "threadId": thread_id,
                "recovery": recovery,
                "nextAction": next_action,
                "replayed": False,
            },
        )

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


@pytest.fixture(autouse=True)
def exact_gateway_admission(monkeypatch):
    """Keep facade behavior tests focused; admission has a dedicated matrix."""

    monkeypatch.setattr(
        facade,
        "current_planning_gateway_admission",
        lambda: type(
            "Admission",
            (),
            {
                "eligible": True,
                "provider": "discord",
                "reason": "live_exact_delivery_conformant",
            },
        )(),
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
    assert capabilities["deliveryRecovery"] == "bound-turn-explicit-v1"
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


def test_large_thread_history_is_bounded_and_selects_late_exact_title(
    monkeypatch,
    route,
) -> None:
    threads = [
        _thread(
            f"thread-{index:03d}",
            title=f"Planner {index:03d}",
        )
        for index in range(137)
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

    first = _invoke({"intent": "status"})

    assert first["code"] == (
        "planning.facade_current_thread_ambiguous"
    )
    assert first["matchCount"] == 137
    assert first["returnedChoiceCount"] == 50
    assert first["moreChoicesAvailable"] is True
    assert [
        choice["threadId"] for choice in first["choices"]
    ] == [f"thread-{index:03d}" for index in range(50)]
    middle_cursor = first["nextChoiceCursor"]
    assert middle_cursor == "current-thread-offset:50"
    assert client.current_thread_resolution_calls == [
        {"cursor": None, "pageSize": 50, "returnedCount": 50}
    ]

    middle = _invoke(
        {
            "intent": "status",
            "threadAfterCursor": middle_cursor,
        }
    )
    assert [
        choice["threadId"] for choice in middle["choices"]
    ] == [f"thread-{index:03d}" for index in range(50, 100)]
    final_cursor = middle["nextChoiceCursor"]
    assert final_cursor == "current-thread-offset:100"
    assert middle["moreChoicesAvailable"] is True

    final = _invoke(
        {
            "intent": "status",
            "threadAfterCursor": final_cursor,
        }
    )
    assert [
        choice["threadId"] for choice in final["choices"]
    ] == [f"thread-{index:03d}" for index in range(100, 137)]
    assert final["returnedChoiceCount"] == 37
    assert final["moreChoicesAvailable"] is False
    assert final["nextChoiceCursor"] is None

    client.current_thread_resolution_calls.clear()
    client.rejected_current_thread_cursors["must-be-ignored"] = (
        "planning.current_thread_cursor_stale",
        409,
    )
    target = threads[123]
    selected = _invoke(
        {
            "intent": "status",
            "selectedOption": target["title"],
            "threadAfterCursor": "must-be-ignored",
        }
    )

    assert selected["ok"] is True
    assert selected["threadId"] == target["threadId"]
    assert [
        call["returnedCount"]
        for call in client.current_thread_resolution_calls
    ] == [50, 50, 37]
    assert max(
        call["returnedCount"]
        for call in client.current_thread_resolution_calls
    ) == 50


def test_thread_choice_cursor_stale_or_cross_scope_fails_closed(
    monkeypatch,
    route,
) -> None:
    threads = [
        _thread(f"thread-{index:03d}", title=f"Planner {index:03d}")
        for index in range(137)
    ]
    client = FakeClient(resolution=_resolution(*threads))
    client.rejected_current_thread_cursors.update(
        {
            "stale-choice-cursor": (
                "planning.current_thread_cursor_stale",
                409,
            ),
            "cross-scope-choice-cursor": (
                "planning.current_thread_cursor_invalid",
                422,
            ),
        }
    )
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    internal_calls: list[str] = []
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda *_args: internal_calls.append("read-or-mutate"),
    )

    stale = _invoke(
        {
            "intent": "status",
            "threadAfterCursor": "stale-choice-cursor",
        }
    )
    cross_scope = _invoke(
        {
            "intent": "status",
            "threadAfterCursor": "cross-scope-choice-cursor",
        }
    )

    assert stale["code"] == "planning.current_thread_cursor_stale"
    assert stale["httpStatus"] == 409
    assert cross_scope["code"] == (
        "planning.current_thread_cursor_invalid"
    )
    assert cross_scope["httpStatus"] == 422
    assert internal_calls == []


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


def test_status_surfaces_natural_delivery_choices_without_admin_ids(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    client.attention_items = [_attention_item()]
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda arguments, _runtime: {
            "ok": True,
            "threadId": arguments["thread_id"],
            "runStatus": "succeeded",
        },
    )

    result = _invoke({"intent": "status"}, text="Kas vyksta?")

    assert result["ok"] is True
    assert result["deliveryAttention"]["status"] == "action_required"
    assert result["deliveryAttention"]["count"] == 1
    assert result["nextAction"]["intent"] == "resolve_delivery"
    choice = result["nextAction"]["choices"][0]
    assert choice["target"]["provider"] == "discord"
    assert {
        action["action"] for action in choice["actions"]
    } == {"mark_delivered", "resend_acknowledged"}
    assert "admin" not in result["instruction"].lower()


def test_status_surfaces_self_service_apply_recovery_without_admin_ids(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    item = _apply_item()
    client.apply_items = [item]
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda arguments, _runtime: {
            "ok": True,
            "threadId": arguments["thread_id"],
            "runStatus": "succeeded",
        },
    )

    result = _invoke({"intent": "status"}, text="Kas vyksta su Jira?")

    assert result["ok"] is True
    assert result["applyStatus"]["status"] == "action_required"
    assert result["applyStatus"]["actionableCount"] == 1
    assert result["nextAction"]["intent"] == "resume"
    choice = result["nextAction"]["choices"][0]
    assert choice["action"] == "automatic_resume"
    assert choice["applyRecoveryToken"] == item["recoveryToken"]
    serialized = json.dumps(result)
    assert "operationId" not in serialized
    assert "planId" not in serialized
    assert "admin" not in result["instruction"].lower()


def test_status_pages_large_delivery_attention_with_opaque_continuation(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    client.attention_items = [
        _attention_item("pdra_" + f"{index:048x}")
        for index in range(75)
    ]
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda arguments, _runtime: {
            "ok": True,
            "threadId": arguments["thread_id"],
            "runStatus": "succeeded",
        },
    )

    first = _invoke({"intent": "status"}, text="Status")
    attention = first["deliveryAttention"]
    assert attention["count"] == 75
    assert attention["returnedCount"] == 50
    assert attention["hasMore"] is True
    cursor = attention["nextAfterResolutionToken"]
    assert cursor == client.attention_items[49]["resolutionToken"]

    second = _invoke(
        {
            "intent": "status",
            "deliveryAfterResolutionToken": cursor,
        },
        text="Tęsk statusą",
    )
    attention = second["deliveryAttention"]
    assert attention["count"] == 75
    assert attention["returnedCount"] == 25
    assert attention["hasMore"] is False
    assert attention["items"][0]["resolutionToken"] == (
        client.attention_items[50]["resolutionToken"]
    )
    resolved = _invoke(
        {
            "intent": "resolve_delivery",
            "deliveryAction": "mark_delivered",
            "resolutionToken": client.attention_items[74][
                "resolutionToken"
            ],
        },
        text="I received and saw the original delivery.",
    )
    assert resolved["ok"] is True
    assert client.delivery_resolution_calls[-1]["resolutionToken"] == (
        client.attention_items[74]["resolutionToken"]
    )


def test_status_finds_actionable_apply_on_later_bounded_page(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    client.apply_items = [
        _apply_item(
            "par_" + f"{index:064x}",
            next_action=("revise" if index == 24 else "none"),
        )
        for index in range(25)
    ]
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda arguments, _runtime: {
            "ok": True,
            "threadId": arguments["thread_id"],
            "runStatus": "succeeded",
        },
    )

    first = _invoke({"intent": "status"}, text="Jira statusas")
    status = first["applyStatus"]
    assert status["count"] == 25
    assert status["actionableCount"] == 1
    assert status["returnedCount"] == 20
    assert status["hasMore"] is True
    assert first["nextAction"]["intent"] == "status"

    second = _invoke(
        {
            "intent": "status",
            "applyAfterRecoveryToken": status[
                "nextAfterRecoveryToken"
            ],
        },
        text="Tęsk Jira statusą",
    )
    choices = second["nextAction"]["choices"]
    assert len(choices) == 1
    assert choices[0]["action"] == "revise"
    assert choices[0]["applyRecoveryToken"] == (
        client.apply_items[24]["recoveryToken"]
    )
    recovered = _invoke(
        {"intent": "resume"},
        text="Correct the rejected Jira field in this same plan.",
    )
    assert recovered["resumeDisposition"] == (
        "corrective_preview_started"
    )
    assert client.apply_recovery_calls[-1]["recoveryToken"] == (
        client.apply_items[24]["recoveryToken"]
    )


def test_resume_reuses_original_apply_without_new_plan_or_approval(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    item = _apply_item()
    client.apply_items = [item]
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(
        facade,
        "list_artifact_recoveries",
        lambda **_kwargs: (),
    )
    calls: list[str] = []

    def internal(arguments, _runtime):
        calls.append(arguments["action"])
        return {
            "ok": True,
            "threadId": "thread-1",
            "runStatus": "succeeded",
        }

    monkeypatch.setattr(facade, "_invoke_internal", internal)
    text = "Jira IL jau veikia, tęsk tą pačią operaciją."
    result = _invoke({"intent": "resume"}, text=text)

    assert result["ok"] is True
    assert result["resumeDisposition"] == "original_apply_resumed"
    assert result["secondApprovalRequired"] is False
    assert result["replacementPlanCreated"] is False
    assert result["recovery"]["createdPlan"] is False
    assert result["recovery"]["createdCommand"] is False
    assert result["recovery"]["createdApproval"] is False
    assert calls == ["status"]
    assert client.apply_recovery_calls == [
        {
            "threadId": "thread-1",
            "origin": client.origin,
            "reason": text,
            "recoveryToken": item["recoveryToken"],
        }
    ]


def test_retry_routes_apply_recovery_to_resume_without_new_run(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    client.apply_items = [_apply_item()]
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(
        facade,
        "list_artifact_recoveries",
        lambda **_kwargs: (),
    )
    calls: list[str] = []

    def internal(arguments, _runtime):
        calls.append(arguments["action"])
        return {
            "ok": True,
            "threadId": "thread-1",
            "runStatus": "failed",
        }

    monkeypatch.setattr(facade, "_invoke_internal", internal)
    result = _invoke({"intent": "retry"}, text="Pabandyk dar kartą.")

    assert result["retryDisposition"] == (
        "apply_recovery_requires_resume"
    )
    assert result["nextAction"]["intent"] == "resume"
    assert calls == ["status"]
    assert client.apply_recovery_calls == []


def test_permanent_jira_correction_starts_fresh_preview_on_same_thread(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    item = _apply_item(next_action="revise")
    client.apply_items = [item]
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(
        facade,
        "list_artifact_recoveries",
        lambda **_kwargs: (),
    )
    calls: list[str] = []

    def internal(arguments, _runtime):
        calls.append(arguments["action"])
        assert arguments["action"] != "continue"
        return {
            "ok": True,
            "threadId": "thread-1",
            "runId": "corrective-run-1",
            "runStatus": "queued",
        }

    monkeypatch.setattr(facade, "_invoke_internal", internal)
    text = "Pakeisk Jira issue type į Task ir išlaikyk jau sukurtus issue."
    result = _invoke({"intent": "revise"}, text=text)

    assert result["ok"] is True
    assert result["intent"] == "revise"
    assert result["runId"] == "corrective-run-1"
    assert result["oldOperationImmutable"] is True
    assert result["freshPreviewApprovalRequired"] is True
    assert result["applyRecovery"]["candidateJiraKeys"] == ["IL-101"]
    assert calls == ["start_run"]
    assert client.apply_recovery_calls[0]["reason"] == text
    assert client.apply_recovery_calls[0]["recoveryToken"] == (
        item["recoveryToken"]
    )


def test_multiple_apply_recoveries_require_exact_model_private_choice(
    monkeypatch,
    route,
) -> None:
    first = _apply_item("par_" + ("a" * 64))
    second = _apply_item(
        "par_" + ("b" * 64),
        next_action="revise",
    )
    client = FakeClient()
    client.apply_items = [first, second]
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(
        facade,
        "list_artifact_recoveries",
        lambda **_kwargs: (),
    )
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda arguments, _runtime: {
            "ok": True,
            "threadId": "thread-1",
            "runId": "run-corrected",
            "runStatus": (
                "queued"
                if arguments["action"] == "start_run"
                else "failed"
            ),
        },
    )

    missing = _invoke({"intent": "resume"}, text="Tęsk.")
    assert missing["code"] == (
        "planning.facade_apply_recovery_selection_required"
    )
    assert client.apply_recovery_calls == []

    selected = _invoke(
        {
            "intent": "resume",
            "applyRecoveryToken": second["recoveryToken"],
        },
        text="Taisyk atmestą Jira lauką.",
    )
    assert selected["resumeDisposition"] == "corrective_preview_started"
    assert client.apply_recovery_calls[-1]["recoveryToken"] == (
        second["recoveryToken"]
    )


def test_retry_never_blind_resends_ambiguous_provider_delivery(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    client.attention_items = [_attention_item()]
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(facade, "list_artifact_recoveries", lambda **_kwargs: ())
    calls: list[str] = []

    def internal(arguments, _runtime):
        calls.append(arguments["action"])
        return {
            "ok": True,
            "threadId": "thread-1",
            "runStatus": "failed",
        }

    monkeypatch.setattr(facade, "_invoke_internal", internal)
    result = _invoke({"intent": "retry"}, text="Bandyk dar kartą")

    assert result["retryDisposition"] == "delivery_attention_required"
    assert result["deliveryAttention"]["count"] == 1
    assert calls == ["status"]
    assert client.delivery_resolution_calls == []


def test_single_delivery_resolves_from_exact_current_human_turn(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    item = _attention_item()
    client.attention_items = [item]
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    text = "Siųsk dar kartą — suprantu, kad gali atsirasti dublikatas."

    result = _invoke(
        {
            "intent": "resolve_delivery",
            "deliveryAction": "resend_acknowledged",
        },
        text=text,
    )

    assert result["ok"] is True
    assert result["resolution"]["status"] == "resend_scheduled"
    assert client.delivery_resolution_calls == [
        {
            "threadId": "thread-1",
            "origin": client.origin,
            "resolutionToken": item["resolutionToken"],
            "action": "resend_acknowledged",
            "reason": text,
        }
    ]


def test_multiple_delivery_decisions_require_exact_current_choice(
    monkeypatch,
    route,
) -> None:
    client = FakeClient()
    first = _attention_item()
    second = _attention_item(
        "pdra_" + ("b" * 48),
        provider="slack",
        account="slack-account",
        chat="slack-chat",
    )
    client.attention_items = [first, second]
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)

    missing = _invoke(
        {
            "intent": "resolve_delivery",
            "deliveryAction": "mark_delivered",
        },
        text="Mačiau Slack žinutę.",
    )
    assert missing["code"] == "planning.facade_delivery_selection_required"
    assert client.delivery_resolution_calls == []

    resolved = _invoke(
        {
            "intent": "resolve_delivery",
            "deliveryAction": "mark_delivered",
            "resolutionToken": second["resolutionToken"],
        },
        text="Mačiau Slack žinutę.",
    )
    assert resolved["ok"] is True
    assert client.delivery_resolution_calls[-1]["resolutionToken"] == (
        second["resolutionToken"]
    )


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


def test_approval_selects_late_reviewed_thread_through_bounded_pages(
    monkeypatch,
    route,
) -> None:
    threads = [
        _thread(
            f"thread-{index:03d}",
            title=f"Planner {index:03d}",
            preview_id="preview-1",
        )
        for index in range(137)
    ]
    target = threads[123]
    client = FakeClient(resolution=_resolution(*threads))
    client.preview_pages = {
        str(thread["threadId"]): _approval_page(eligible=True)
        for thread in threads
    }
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda arguments, _runtime: {
            "ok": True,
            "threadId": arguments["thread_id"],
            "previewResultId": "preview-1",
            "operationId": "operation-1",
            "status": "requested",
        },
    )
    message = "Patvirtinu tiksliai pasirinktą planą"
    approval = {
        "approvalMessage": message,
        "approvalEvidence": {
            "meaning": "approve_current_preview_exactly",
            "exactQuote": message,
        },
    }

    first = _invoke(
        approval,
        tool_name="agent_ops_task_approve_apply",
        text=message,
    )
    assert first["code"] == "planning.facade_approval_ambiguous"
    assert first["eligibleMatchCount"] == 137
    assert [
        choice["threadId"] for choice in first["choices"]
    ] == [f"thread-{index:03d}" for index in range(50)]
    assert first["nextChoiceCursor"] == "current-thread-offset:50"

    middle = _invoke(
        {
            **approval,
            "threadAfterCursor": first["nextChoiceCursor"],
        },
        tool_name="agent_ops_task_approve_apply",
        text=message,
    )
    assert [
        choice["threadId"] for choice in middle["choices"]
    ] == [f"thread-{index:03d}" for index in range(50, 100)]
    assert middle["nextChoiceCursor"] == "current-thread-offset:100"

    final = _invoke(
        {
            **approval,
            "threadAfterCursor": middle["nextChoiceCursor"],
        },
        tool_name="agent_ops_task_approve_apply",
        text=message,
    )
    assert [
        choice["threadId"] for choice in final["choices"]
    ] == [f"thread-{index:03d}" for index in range(100, 137)]
    assert final["returnedChoiceCount"] == 37
    assert final["moreChoicesAvailable"] is False
    assert final["nextChoiceCursor"] is None

    client.current_thread_resolution_calls.clear()
    client.rejected_current_thread_cursors["must-be-ignored"] = (
        "planning.current_thread_cursor_stale",
        409,
    )
    result = _invoke(
        {
            **approval,
            "selectedOption": target["title"],
            "threadAfterCursor": "must-be-ignored",
        },
        tool_name="agent_ops_task_approve_apply",
        text=message,
    )
    assert result["ok"] is True
    assert result["threadId"] == target["threadId"]
    assert [
        call["returnedCount"]
        for call in client.current_thread_resolution_calls
    ] == [50, 50, 37]


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


@pytest.mark.parametrize(
    ("internal_action", "public_intent"),
    [
        ("create", "retry"),
        ("continue", "retry"),
        ("start_run", "retry"),
        ("upload_artifact", "retry"),
        ("approve_apply", "resume"),
        ("preview", "show"),
        ("status", "resume"),
        ("events", "resume"),
        ("future_internal_action", "resume"),
    ],
)
def test_internal_failure_actions_become_callable_public_recovery(
    monkeypatch,
    route,
    internal_action: str,
    public_intent: str,
) -> None:
    client = FakeClient()
    monkeypatch.setattr(facade, "PlanningV2Client", lambda: client)
    token = "artrec_v2_" + "a" * 64
    monkeypatch.setattr(
        facade,
        "_invoke_internal",
        lambda *_args: {
            "ok": False,
            "code": "planning.retryable_test",
            "retryable": True,
            "recovery": {
                "nextAction": {
                    "tool": "agent_ops_planning_v2",
                    "arguments": {
                        "action": internal_action,
                        "thread_id": "thread-1",
                        "recovery_token": token,
                        "expected_preview_hash": "private-engine-value",
                    },
                }
            },
        },
    )

    result = _invoke({"intent": "status"}, text="Show status")

    assert result["recovery"]["nextAction"] == {
        "tool": "agent_ops_task_plan",
        "arguments": {
            "intent": public_intent,
            "threadId": "thread-1",
        },
    }
    encoded = json.dumps(result)
    assert "agent_ops_planning_v2" not in encoded
    assert token not in encoded
    assert "recovery_token" not in encoded
    assert '"action"' not in encoded
    assert "private-engine-value" not in encoded


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
