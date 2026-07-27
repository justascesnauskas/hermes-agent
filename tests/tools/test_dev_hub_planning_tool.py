"""User-facing Planning V2 tool boundaries and semantic result tests."""

from __future__ import annotations

import json
from typing import Any

from hermes_cli.dev_hub_planning_v2 import (
    PlanningV2HTTPError,
    PlanningV2Response,
    PlanningV2TransportError,
    planning_origin_from_current_turn,
)
from hermes_cli.turn_origin import (
    TurnOriginV1,
    get_current_turn_origin,
    scoped_turn_origin,
)
from tools import dev_hub_planning_tool as planning_tool
from toolsets import resolve_toolset, validate_toolset


def _turn_origin(provider: str, *, event_id: str) -> TurnOriginV1:
    return TurnOriginV1(
        provider=provider,
        gateway_account_id=f"{provider}-account",
        chat_id=f"{provider}-chat",
        thread_id=f"{provider}-native-thread",
        message_id=f"{provider}-message",
        sender_id=f"{provider}-user",
        chat_type="direct",
        source_timestamp="2026-07-27T12:30:00Z",
        event_id=event_id,
    )


def _thread_projection(
    *,
    providers: tuple[str, ...],
    input_count: int,
    active_run_id: str | None = None,
    active_preview_version_id: str | None = None,
    duplicate: bool = False,
) -> dict[str, Any]:
    return {
        "thread": {
            "threadId": "planning-thread-1",
            "status": "active",
            "activeRunId": active_run_id,
            "activePreviewVersionId": active_preview_version_id,
        },
        "bindings": [
            {
                "deliveryMode": "subscribed",
                "endpoint": {"provider": provider},
            }
            for provider in providers
        ],
        "inputEventCount": input_count,
        "semanticEventHead": input_count,
        "inputEvent": {"eventId": f"input-{input_count}"},
        "duplicate": duplicate,
    }


def _run_projection(
    *,
    run_id: str = "run-1",
    work_items: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "runId": run_id,
        "threadId": "planning-thread-1",
        "status": "running",
        "basisInputSequence": 2,
        "workItems": list(work_items or []),
    }


class _CrossProviderClient:
    runner_id = "runner-1"

    def __init__(self) -> None:
        self.create_origins: list[dict[str, Any]] = []
        self.append_origins: list[dict[str, Any]] = []
        self.append_thread_ids: list[str] = []
        self.run_keys: list[str] = []

    def current_origin(self) -> dict[str, Any]:
        return planning_origin_from_current_turn(runner_id=self.runner_id)

    def create_thread(
        self,
        *,
        origin: dict[str, Any],
        payload: dict[str, Any],
        **_kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        self.create_origins.append(origin)
        assert payload == {"text": "same planning text"}
        return PlanningV2Response(
            201,
            _thread_projection(providers=("discord",), input_count=1),
        )

    def append_thread_input(
        self,
        thread_id: str,
        *,
        origin: dict[str, Any],
        payload: dict[str, Any],
        **_kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        self.append_thread_ids.append(thread_id)
        self.append_origins.append(origin)
        assert payload == {"text": "same planning text"}
        return PlanningV2Response(
            201,
            {
                **_thread_projection(
                    providers=("discord", "slack"),
                    input_count=2,
                ),
                "previewInvalidated": True,
            },
        )

    def create_run(
        self,
        _thread_id: str,
        *,
        idempotency_key: str,
        **_kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        self.run_keys.append(idempotency_key)
        return PlanningV2Response(
            201,
            _run_projection(run_id=f"run-{len(self.run_keys)}"),
        )


def test_explicit_discord_to_slack_continuation_uses_only_scoped_origin(
    monkeypatch,
) -> None:
    fake = _CrossProviderClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    with scoped_turn_origin(_turn_origin("discord", event_id="shared-event-id")):
        created = json.loads(
            planning_tool._handle_planning_v2(
                {"action": "create", "message": "same planning text"}
            )
        )
    assert get_current_turn_origin() is None

    with scoped_turn_origin(_turn_origin("slack", event_id="shared-event-id")):
        continued = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "continue",
                    "thread_id": created["threadId"],
                    "message": "same planning text",
                }
            )
        )
    assert get_current_turn_origin() is None

    assert fake.append_thread_ids == ["planning-thread-1"]
    assert fake.create_origins[0] == {
        "schemaVersion": "1.0",
        "provider": "discord",
        "gatewayInstanceId": "runner-1",
        "gatewayAccountId": "discord-account",
        "chatId": "discord-chat",
        "threadId": "discord-native-thread",
        "messageId": "discord-message",
        "senderId": "discord-user",
        "chatType": "direct",
        "sourceTimestamp": "2026-07-27T12:30:00Z",
        "providerEventId": "shared-event-id",
    }
    assert fake.append_origins[0]["provider"] == "slack"
    assert fake.append_origins[0]["providerEventId"] == "shared-event-id"
    assert "same planning text" not in json.dumps(
        [fake.create_origins, fake.append_origins]
    )
    assert len(set(fake.run_keys)) == 2
    assert all(
        key.startswith("hermes-planning-run-v1:") for key in fake.run_keys
    )
    assert created["boundProviders"] == ["discord"]
    assert continued["boundProviders"] == ["discord", "slack"]
    assert continued["previewInvalidated"] is True

    missing_origin = json.loads(
        planning_tool._handle_planning_v2(
            {"action": "create", "message": "same planning text"}
        )
    )
    assert missing_origin["code"] == "planning.origin_missing"
    assert len(fake.create_origins) == 1


def test_missing_gateway_account_id_fails_closed_before_hub_write(
    monkeypatch,
) -> None:
    fake = _CrossProviderClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)
    incomplete = TurnOriginV1(
        provider="discord",
        chat_id="discord-chat",
        message_id="discord-message",
        sender_id="discord-user",
        source_timestamp="2026-07-27T12:30:00Z",
        event_id="discord-event",
    )

    with scoped_turn_origin(incomplete):
        result = json.loads(
            planning_tool._handle_planning_v2(
                {"action": "create", "message": "must not write"}
            )
        )

    assert result["code"] == "planning.origin_incomplete"
    assert "gateway_account_id" in result["detail"]["missing"]
    assert fake.create_origins == []


class _StatusClient:
    runner_id = "runner-1"

    def get_thread(
        self,
        _thread_id: str,
    ) -> PlanningV2Response[dict[str, Any]]:
        return PlanningV2Response(
            200,
            _thread_projection(
                providers=("discord", "slack"),
                input_count=8,
                active_run_id="run-status",
                active_preview_version_id="preview-v3",
            ),
        )

    def get_run(
        self,
        _run_id: str,
    ) -> PlanningV2Response[dict[str, Any]]:
        return PlanningV2Response(
            200,
            _run_projection(
                run_id="run-status",
                work_items=[
                    {
                        "workItemId": "research",
                        "workKind": "research",
                        "scopeKey": "market",
                        "status": "succeeded",
                    },
                    {
                        "workItemId": "architecture",
                        "workKind": "architecture",
                        "scopeKey": "system",
                        "status": "running",
                        "progress": {"phase": "failure modes", "percent": 65},
                    },
                    {
                        "workItemId": "decision",
                        "workKind": "decision",
                        "scopeKey": "storage",
                        "status": "needs_decision",
                        "terminalReason": "Choose artifact durability tier",
                        "progress": {"options": ["R2", "local durable store"]},
                    },
                    {
                        "workItemId": "preview",
                        "workKind": "preview_reduce",
                        "scopeKey": "preview",
                        "status": "succeeded",
                        "acceptedResultId": "result-preview-3",
                    },
                ],
            ),
        )


def test_status_returns_concise_progress_decision_and_preview_facts(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(
        planning_tool,
        "PlanningV2Client",
        lambda: _StatusClient(),
    )

    raw = planning_tool._handle_planning_v2(
        {"action": "status", "thread_id": "planning-thread-1"}
    )
    result = json.loads(raw)

    assert result["progress"] == {
        "total": 4,
        "completed": 2,
        "active": 1,
        "waiting": 1,
        "byStatus": {
            "needs_decision": 1,
            "running": 1,
            "succeeded": 2,
        },
        "facts": [
            {
                "workItemId": "architecture",
                "workKind": "architecture",
                "scopeKey": "system",
                "status": "running",
                "progress": {"phase": "failure modes", "percent": 65},
            },
            {
                "workItemId": "decision",
                "workKind": "decision",
                "scopeKey": "storage",
                "status": "needs_decision",
                "progress": {"options": ["R2", "local durable store"]},
                "reason": "Choose artifact durability tier",
            },
        ],
    }
    assert result["needsDecision"]["required"] is True
    assert result["needsDecision"]["items"][0]["scopeKey"] == "storage"
    assert result["preview"] == {
        "ready": True,
        "status": "ready",
        "versionId": "preview-v3",
        "workItemId": "preview",
        "resultId": "result-preview-3",
    }
    assert "workItems" not in raw


def test_status_rejects_a_run_from_another_thread(monkeypatch) -> None:
    class _MismatchedStatusClient(_StatusClient):
        def get_run(
            self,
            _run_id: str,
        ) -> PlanningV2Response[dict[str, Any]]:
            payload = _run_projection(run_id="run-from-thread-2")
            payload["threadId"] = "planning-thread-2"
            return PlanningV2Response(200, payload)

    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(
        planning_tool,
        "PlanningV2Client",
        lambda: _MismatchedStatusClient(),
    )

    result = json.loads(
        planning_tool._handle_planning_v2(
            {
                "action": "status",
                "thread_id": "planning-thread-1",
                "run_id": "run-from-thread-2",
            }
        )
    )

    assert result["code"] == "planning.run_thread_mismatch"
    assert result["detail"]["threadId"] == "planning-thread-1"
    assert result["detail"]["runThreadId"] == "planning-thread-2"


def test_run_projection_progress_survives_without_embedded_work_items() -> None:
    assert planning_tool._work_progress(
        {
            "progress": {
                "total": 5,
                "completed": 1,
                "states": {
                    "succeeded": 1,
                    "queued": 3,
                    "needs_decision": 1,
                },
            }
        }
    ) == {
        "total": 5,
        "completed": 1,
        "active": 3,
        "waiting": 1,
        "byStatus": {
            "needs_decision": 1,
            "queued": 3,
            "succeeded": 1,
        },
        "facts": [],
    }


class _LostRunResponseClient:
    runner_id = "runner-1"

    def __init__(self) -> None:
        self.run_keys: list[str] = []
        self.lose_first_response = True

    def current_origin(self) -> dict[str, Any]:
        return planning_origin_from_current_turn(runner_id=self.runner_id)

    def create_thread(
        self,
        **_kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        return PlanningV2Response(
            201,
            _thread_projection(providers=("discord",), input_count=1),
        )

    def create_run(
        self,
        _thread_id: str,
        *,
        idempotency_key: str,
        **_kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        self.run_keys.append(idempotency_key)
        if self.lose_first_response:
            self.lose_first_response = False
            raise PlanningV2TransportError(
                "planning.hub_timeout",
                detail="response lost after commit",
                ambiguous=True,
                attempts=2,
            )
        return PlanningV2Response(
            200,
            _run_projection(run_id="replayed-run"),
        )

    def get_thread(
        self,
        _thread_id: str,
    ) -> PlanningV2Response[dict[str, Any]]:
        return PlanningV2Response(
            200,
            _thread_projection(
                providers=("discord",),
                input_count=1,
                active_run_id="replayed-run",
            ),
        )


def test_lost_run_response_returns_replay_safe_recovery_action(
    monkeypatch,
) -> None:
    fake = _LostRunResponseClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    with scoped_turn_origin(_turn_origin("discord", event_id="event-1")):
        failure = json.loads(
            planning_tool._handle_planning_v2(
                {"action": "create", "message": "persist this plan"}
            )
        )

    recovery = failure["recovery"]["nextAction"]["arguments"]
    assert failure["outcomeAmbiguous"] is True
    assert failure["recovery"]["inputStored"] is True
    assert recovery["action"] == "start_run"
    assert recovery["thread_id"] == "planning-thread-1"
    assert recovery["idempotency_key"] == fake.run_keys[0]

    recovered = json.loads(planning_tool._handle_planning_v2(recovery))
    assert recovered["runId"] == "replayed-run"
    assert recovered["runReplayed"] is True
    assert fake.run_keys == [
        recovery["idempotency_key"],
        recovery["idempotency_key"],
    ]


def test_started_run_keeps_recovery_key_when_followup_read_times_out(
    monkeypatch,
) -> None:
    class _RunStartedReadLostClient:
        runner_id = "runner-1"

        def __init__(self) -> None:
            self.run_keys: list[str] = []
            self.lose_first_read = True

        def create_run(
            self,
            _thread_id: str,
            *,
            idempotency_key: str,
            **_kwargs: Any,
        ) -> PlanningV2Response[dict[str, Any]]:
            self.run_keys.append(idempotency_key)
            return PlanningV2Response(
                201 if len(self.run_keys) == 1 else 200,
                _run_projection(run_id="committed-run"),
            )

        def get_thread(
            self,
            _thread_id: str,
        ) -> PlanningV2Response[dict[str, Any]]:
            if self.lose_first_read:
                self.lose_first_read = False
                raise PlanningV2TransportError(
                    "planning.hub_timeout",
                    detail="follow-up read timed out",
                    retryable=True,
                    attempts=2,
                )
            return PlanningV2Response(
                200,
                _thread_projection(
                    providers=("discord",),
                    input_count=1,
                    active_run_id="committed-run",
                ),
            )

    fake = _RunStartedReadLostClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    failure = json.loads(
        planning_tool._handle_planning_v2(
            {
                "action": "start_run",
                "thread_id": "planning-thread-1",
                "idempotency_key": "operator-stable-key",
            }
        )
    )

    assert failure["recovery"]["runStarted"] is True
    recovery = failure["recovery"]["nextAction"]["arguments"]
    assert recovery["idempotency_key"] == "operator-stable-key"

    recovered = json.loads(planning_tool._handle_planning_v2(recovery))
    assert recovered["runId"] == "committed-run"
    assert recovered["runReplayed"] is True
    assert fake.run_keys == ["operator-stable-key", "operator-stable-key"]


class _ApprovalClient:
    runner_id = "runner-1"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def current_origin(self) -> dict[str, Any]:
        return planning_origin_from_current_turn(runner_id=self.runner_id)

    def approve_and_apply_preview(
        self,
        thread_id: str,
        preview_result_id: str,
        **kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        self.calls.append(
            {
                "threadId": thread_id,
                "previewResultId": preview_result_id,
                **kwargs,
            }
        )
        return PlanningV2Response(
            200,
            {
                "ok": True,
                "replayed": len(self.calls) > 1,
                "applyBindingId": "apply-binding-1",
                "threadId": thread_id,
                "runId": "run-1",
                "previewResultId": preview_result_id,
                "previewResultHash": kwargs["expected_preview_hash"],
                "planHash": kwargs["expected_plan_hash"],
                "operationId": "operation-1",
                "status": "requested",
                "operation": {"status": "queued"},
            },
        )


class _PreviewClient:
    runner_id = "runner-1"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def get_preview_page(
        self,
        thread_id: str,
        preview_result_id: str,
        *,
        offset: int,
        limit: int,
    ) -> PlanningV2Response[dict[str, Any]]:
        self.calls.append(
            {
                "threadId": thread_id,
                "previewResultId": preview_result_id,
                "offset": offset,
                "limit": limit,
            }
        )
        return PlanningV2Response(
            200,
            {
                "threadId": thread_id,
                "runId": "run-1",
                "previewResultId": preview_result_id,
                "previewResultHash": "sha256:preview-1",
                "planHash": "sha256:plan-1",
                "basisInputSequence": 4,
                "taskCount": 137,
                "offset": offset,
                "limit": limit,
                "returned": limit,
                "tasks": [
                    {"stableTaskId": f"task-{index}"}
                    for index in range(offset, offset + limit)
                ],
                "hasMore": True,
                "nextOffset": offset + limit,
                "title": "Planning V2 delivery",
                "objective": "Ship the accepted implementation chain",
                "summary": "Review all tasks before approval.",
                "decisions": [{"code": "ready_for_approval"}],
                "coverage": {"ready": True, "findings": []},
                "acceptedAt": "2026-07-27T12:30:00Z",
                "approvalEligible": True,
            },
        )


def test_preview_returns_exact_hashes_page_and_next_action_without_origin(
    monkeypatch,
) -> None:
    fake = _PreviewClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    result = json.loads(
        planning_tool._handle_planning_v2(
            {
                "action": "preview",
                "thread_id": "planning-thread-1",
                "preview_result_id": "preview-1",
                "offset": 0,
                "limit": 50,
            }
        )
    )

    assert fake.calls == [
        {
            "threadId": "planning-thread-1",
            "previewResultId": "preview-1",
            "offset": 0,
            "limit": 50,
        }
    ]
    assert result["previewResultHash"] == "sha256:preview-1"
    assert result["planHash"] == "sha256:plan-1"
    assert result["taskCount"] == 137
    assert len(result["tasks"]) == 50
    assert result["nextAction"] == {
        "tool": planning_tool.PLANNING_V2_TOOL_NAME,
        "arguments": {
            "action": "preview",
            "thread_id": "planning-thread-1",
            "preview_result_id": "preview-1",
            "offset": 50,
            "limit": 50,
        },
    }


def test_preview_tool_schema_requires_review_before_approval() -> None:
    schema = planning_tool.PLANNING_V2_SCHEMA
    properties = schema["parameters"]["properties"]
    description = schema["description"]

    assert "preview" in properties["action"]["enum"]
    assert properties["offset"]["minimum"] == 0
    assert properties["limit"]["minimum"] == 1
    assert "show/review every page in order" in description
    assert "Never approve tasks the user has not seen" in description
    assert "approvalEligible=false" in description


def test_approval_uses_exact_current_turn_and_cross_provider_origin(
    monkeypatch,
) -> None:
    fake = _ApprovalClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)
    exact_user_turn = "Tvirtinu būtent preview-1 planą."

    with scoped_turn_origin(_turn_origin("slack", event_id="approval-event")):
        result = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "approve_apply",
                    "thread_id": "planning-thread-1",
                    "preview_result_id": "preview-1",
                    "expected_preview_hash": "sha256:preview-1",
                    "expected_plan_hash": "sha256:plan-1",
                    "approval_message": "modelio perrašytas tekstas",
                    "approval_evidence": {"verbatimClause": "Tvirtinu"},
                },
                user_task=exact_user_turn,
            )
        )

    call = fake.calls[0]
    assert call["approval_message"] == exact_user_turn
    assert call["origin"]["provider"] == "slack"
    assert call["origin"]["gatewayAccountId"] == "slack-account"
    assert call["origin"]["providerEventId"] == "approval-event"
    assert call["idempotency_key"].startswith(
        "hermes-planning-approval-v1:"
    )
    assert exact_user_turn not in call["idempotency_key"]
    assert result == {
        "ok": True,
        "action": "approve_apply",
        "threadId": "planning-thread-1",
        "runId": "run-1",
        "previewResultId": "preview-1",
        "previewResultHash": "sha256:preview-1",
        "planHash": "sha256:plan-1",
        "applyBindingId": "apply-binding-1",
        "operationId": "operation-1",
        "status": "requested",
        "replayed": False,
        "operationStatus": "queued",
    }


def test_approval_requires_current_scoped_origin_before_hub_write(
    monkeypatch,
) -> None:
    fake = _ApprovalClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    result = json.loads(
        planning_tool._handle_planning_v2(
            {
                "action": "approve_apply",
                "thread_id": "planning-thread-1",
                "preview_result_id": "preview-1",
                "expected_preview_hash": "sha256:preview-1",
                "expected_plan_hash": "sha256:plan-1",
                "approval_message": "Tvirtinu",
            }
        )
    )

    assert result["code"] == "planning.origin_missing"
    assert fake.calls == []


def test_same_turn_approval_rejection_passes_through_without_retry(
    monkeypatch,
) -> None:
    fake = _ApprovalClient()

    def _reject(*_args: Any, **_kwargs: Any):
        raise PlanningV2HTTPError(
            "planning.same_turn_approval_forbidden",
            status=409,
            detail="Approval must arrive in a later user turn.",
        )

    monkeypatch.setattr(fake, "approve_and_apply_preview", _reject)
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    with scoped_turn_origin(_turn_origin("discord", event_id="same-turn")):
        result = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "approve_apply",
                    "thread_id": "planning-thread-1",
                    "preview_result_id": "preview-1",
                    "expected_preview_hash": "sha256:preview-1",
                    "expected_plan_hash": "sha256:plan-1",
                },
                user_task="Tvirtinu",
            )
        )

    assert result["code"] == "planning.same_turn_approval_forbidden"
    assert result["httpStatus"] == 409
    assert result["detail"] == "Approval must arrive in a later user turn."
    assert "nextAction" not in result["recovery"]


def test_lost_approval_response_has_exact_replay_recovery(
    monkeypatch,
) -> None:
    class _LostApprovalClient(_ApprovalClient):
        def approve_and_apply_preview(
            self,
            thread_id: str,
            preview_result_id: str,
            **kwargs: Any,
        ) -> PlanningV2Response[dict[str, Any]]:
            if not self.calls:
                self.calls.append(
                    {
                        "threadId": thread_id,
                        "previewResultId": preview_result_id,
                        **kwargs,
                    }
                )
                raise PlanningV2TransportError(
                    "planning.hub_timeout",
                    detail="approval response lost after commit",
                    retryable=True,
                    ambiguous=True,
                    attempts=2,
                )
            return super().approve_and_apply_preview(
                thread_id,
                preview_result_id,
                **kwargs,
            )

    fake = _LostApprovalClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)
    initial = {
        "action": "approve_apply",
        "thread_id": "planning-thread-1",
        "preview_result_id": "preview-1",
        "expected_preview_hash": "sha256:preview-1",
        "expected_plan_hash": "sha256:plan-1",
        "approval_evidence": {"verbatimClause": "Tvirtinu"},
    }

    with scoped_turn_origin(_turn_origin("discord", event_id="approval-event")):
        failed = json.loads(
            planning_tool._handle_planning_v2(
                initial,
                user_task="Tvirtinu",
            )
        )
        recovery = failed["recovery"]["nextAction"]
        recovered = json.loads(
            planning_tool._handle_planning_v2(recovery["arguments"])
        )

    assert recovery["tool"] == planning_tool.PLANNING_V2_TOOL_NAME
    assert recovery["arguments"]["action"] == "approve_apply"
    assert recovery["arguments"]["approval_message"] == "Tvirtinu"
    assert recovery["arguments"]["idempotency_key"].startswith(
        "hermes-planning-approval-v1:"
    )
    assert len(fake.calls) == 2
    assert fake.calls[0]["idempotency_key"] == fake.calls[1][
        "idempotency_key"
    ]
    assert fake.calls[0]["origin"] == fake.calls[1]["origin"]
    assert recovered["replayed"] is True
    assert recovered["operationId"] == "operation-1"


def test_approval_tool_schema_requires_later_explicit_exact_preview() -> None:
    schema = planning_tool.PLANNING_V2_SCHEMA
    properties = schema["parameters"]["properties"]

    assert "approve_apply" in properties["action"]["enum"]
    assert {
        "preview_result_id",
        "expected_preview_hash",
        "expected_plan_hash",
        "approval_message",
        "approval_evidence",
    } <= set(properties)
    description = schema["description"]
    assert "exact preview id/hash and plan hash" in description
    assert "later conversation turn" in description
    assert "never auto-approve" in description


def test_permanent_run_rejection_does_not_offer_a_retry_loop(
    monkeypatch,
) -> None:
    fake = _LostRunResponseClient()

    def _reject_run(
        _thread_id: str,
        *,
        idempotency_key: str,
        **_kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        fake.run_keys.append(idempotency_key)
        raise PlanningV2HTTPError(
            "planning.idempotency_key_conflict",
            status=409,
            detail="the key belongs to a different request",
        )

    monkeypatch.setattr(fake, "create_run", _reject_run)
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    with scoped_turn_origin(_turn_origin("discord", event_id="event-1")):
        failure = json.loads(
            planning_tool._handle_planning_v2(
                {"action": "create", "message": "persist this plan"}
            )
        )

    assert failure["code"] == "planning.idempotency_key_conflict"
    assert failure["recovery"]["inputStored"] is True
    assert "nextAction" not in failure["recovery"]


def test_tool_is_not_available_without_explicit_profile_opt_in(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: False,
    )

    def _must_not_construct():
        raise AssertionError("client must not be constructed while disabled")

    monkeypatch.setattr(
        planning_tool,
        "PlanningV2Client",
        _must_not_construct,
    )

    result = json.loads(
        planning_tool._handle_planning_v2(
            {"action": "status", "thread_id": "planning-thread-1"}
        )
    )

    assert result["code"] == "planning.tool_not_enabled"
    assert "normal Hermes conversation is unchanged" in result["error"]


def test_profile_opt_in_is_provider_scoped(monkeypatch) -> None:
    from hermes_cli import config

    monkeypatch.setattr(
        config,
        "load_config",
        lambda: {
            "platform_toolsets": {
                "discord": ["hermes-discord", "planning_v2"],
                "slack": ["hermes-slack"],
            }
        },
    )

    assert planning_tool._profile_opted_in() is True
    assert planning_tool._profile_opted_in("discord") is True
    assert planning_tool._profile_opted_in("slack") is False
    assert planning_tool._profile_opted_in("telegram") is False


def test_write_is_refused_when_current_provider_is_not_opted_in(
    monkeypatch,
) -> None:
    fake = _CrossProviderClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda provider=None: provider in {None, "discord"},
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    with scoped_turn_origin(_turn_origin("slack", event_id="slack-event")):
        result = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "continue",
                    "thread_id": "planning-thread-1",
                    "message": "do not append",
                }
            )
        )

    assert result["code"] == "planning.tool_not_enabled_for_provider"
    assert fake.append_origins == []


def test_planning_toolset_is_valid_but_not_part_of_normal_core() -> None:
    assert validate_toolset("planning_v2") is True
    assert resolve_toolset("planning_v2") == [
        planning_tool.PLANNING_V2_TOOL_NAME
    ]

    from hermes_cli.tools_config import _get_platform_tools
    from toolsets import _HERMES_CORE_TOOLS

    assert planning_tool.PLANNING_V2_TOOL_NAME not in _HERMES_CORE_TOOLS
    assert "planning_v2" not in _get_platform_tools({}, "discord")
    enabled = _get_platform_tools(
        {
            "platform_toolsets": {
                "discord": ["hermes-discord", "planning_v2"],
            }
        },
        "discord",
    )
    assert "planning_v2" in enabled
