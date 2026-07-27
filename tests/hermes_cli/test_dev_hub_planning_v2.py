"""Exact-wire and recovery tests for the Dev Hub Planning V2 client."""

from __future__ import annotations

from io import BytesIO
import json
from typing import Any
from urllib import error

import pytest

from hermes_cli.dev_hub_planning_v2 import (
    PLANNING_V2_PREFIX,
    PlanningV2Client,
    PlanningV2ConfigError,
    PlanningV2HTTPError,
    PlanningV2ProtocolError,
    PlanningV2TransportError,
)


def _origin(provider: str = "discord") -> dict[str, Any]:
    return {
        "schemaVersion": "1.0",
        "provider": provider,
        "gatewayInstanceId": "runner-1",
        "gatewayAccountId": f"{provider}-account",
        "chatId": f"{provider}-chat",
        "threadId": None,
        "messageId": f"{provider}-message",
        "senderId": f"{provider}-user",
        "chatType": "direct",
        "sourceTimestamp": "2026-07-27T12:30:00Z",
        "providerEventId": f"{provider}-event",
    }


class _Response:
    def __init__(self, status: int, payload: Any = None) -> None:
        self.status = status
        self._raw = (
            b""
            if payload is None
            else json.dumps(payload).encode("utf-8")
        )
        self.closed = False

    def read(self) -> bytes:
        return self._raw

    def close(self) -> None:
        self.closed = True


class _ScriptedTransport:
    def __init__(self, *steps: Any) -> None:
        self.steps = list(steps)
        self.calls: list[dict[str, Any]] = []

    def __call__(self, req, *, timeout: float):
        self.calls.append(
            {
                "method": req.get_method(),
                "url": req.full_url,
                "headers": {
                    key.lower(): value
                    for key, value in req.header_items()
                },
                "body": req.data,
                "timeout": timeout,
            }
        )
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        return step


def _client(transport: _ScriptedTransport) -> PlanningV2Client:
    return PlanningV2Client(
        base_url="https://hub.example.test",
        token="runner-token",
        runner_id="runner-1",
        transport=transport,
        sleep=lambda _seconds: None,
    )


def _thread_projection(
    *,
    duplicate: bool = False,
) -> dict[str, Any]:
    return {
        "thread": {
            "threadId": "thread-1",
            "status": "active",
            "activeRunId": "run-1",
            "activePreviewVersionId": None,
        },
        "bindings": [],
        "inputEventCount": 1,
        "semanticEventHead": 1,
        "inputEvent": {"eventId": "input-1"},
        "duplicate": duplicate,
    }


def _run_projection() -> dict[str, Any]:
    return {
        "runId": "run-1",
        "threadId": "thread-1",
        "status": "queued",
        "basisInputSequence": 1,
        "workItems": [],
    }


def test_client_exposes_complete_exact_runner_surface() -> None:
    transport = _ScriptedTransport(
        _Response(201, _thread_projection()),
        _Response(200, _thread_projection()),
        _Response(
            200,
            {
                "threadId": "thread-1",
                "basisInputSequence": 1,
                "inputs": [],
                "hasMore": False,
                "nextAfterSequence": 0,
            },
        ),
        _Response(201, _thread_projection()),
        _Response(
            200,
            {
                "threadId": "thread-1",
                "afterSequence": 0,
                "events": [],
                "nextSequence": 0,
            },
        ),
        _Response(201, _thread_projection()),
        _Response(201, _run_projection()),
        _Response(200, _run_projection()),
        _Response(204),
        _Response(200, {"ok": True, "progressAdvanced": True}),
        _Response(200, {"ok": True, "replayed": False}),
        _Response(200, {"ok": True, "decision": {"action": "retry"}}),
        _Response(200, {"ok": True, "workItem": {"status": "queued"}}),
    )
    client = _client(transport)
    origin = _origin()

    client.create_thread(origin=origin, payload={"text": "plan"})
    client.get_thread("thread-1")
    client.get_thread_inputs("thread-1", after_sequence=0, limit=100)
    client.append_thread_input(
        "thread-1",
        origin=origin,
        payload={"text": "continue"},
    )
    client.get_thread_events("thread-1", after_sequence=0)
    client.bind_thread(
        "thread-1",
        origin=origin,
        delivery_mode="primary",
    )
    client.create_run(
        "thread-1",
        idempotency_key="run-key-1",
        policy={"quality": "premium"},
        route_policy={"provider": "automatic"},
        correlation_id="correlation-1",
    )
    client.get_run("run-1")
    no_work = client.claim_work(
        worker_id="runner-1:planning-1",
        capabilities=["planning"],
    )
    client.heartbeat_work(
        "work-1",
        worker_id="runner-1:planning-1",
        attempt_id="attempt-1",
        lease_epoch=1,
        progress={"phase": "research"},
    )
    client.complete_work(
        "work-1",
        worker_id="runner-1:planning-1",
        attempt_id="attempt-1",
        lease_epoch=1,
        result_kind="outline.result",
        schema_version="1.0",
        result={"outline": []},
    )
    client.fail_work(
        "work-2",
        worker_id="runner-1:planning-1",
        attempt_id="attempt-2",
        lease_epoch=2,
        error_code="provider_timeout",
        failure_class="transient_infrastructure",
    )
    client.resume_work("work-2", reason="provider recovered")

    assert no_work.status == 204
    assert no_work.payload is None
    assert [call["method"] for call in transport.calls] == [
        "POST",
        "GET",
        "GET",
        "POST",
        "GET",
        "POST",
        "POST",
        "GET",
        "POST",
        "POST",
        "POST",
        "POST",
        "POST",
    ]
    assert [call["url"] for call in transport.calls] == [
        f"https://hub.example.test{PLANNING_V2_PREFIX}/threads",
        f"https://hub.example.test{PLANNING_V2_PREFIX}/threads/thread-1",
        (
            f"https://hub.example.test{PLANNING_V2_PREFIX}"
            "/threads/thread-1/inputs?afterSequence=0&limit=100"
        ),
        (
            f"https://hub.example.test{PLANNING_V2_PREFIX}"
            "/threads/thread-1/inputs"
        ),
        (
            f"https://hub.example.test{PLANNING_V2_PREFIX}"
            "/threads/thread-1/events?afterSequence=0"
        ),
        (
            f"https://hub.example.test{PLANNING_V2_PREFIX}"
            "/threads/thread-1/bindings"
        ),
        (
            f"https://hub.example.test{PLANNING_V2_PREFIX}"
            "/threads/thread-1/runs"
        ),
        f"https://hub.example.test{PLANNING_V2_PREFIX}/runs/run-1",
        f"https://hub.example.test{PLANNING_V2_PREFIX}/work/claim",
        (
            f"https://hub.example.test{PLANNING_V2_PREFIX}"
            "/work/work-1/heartbeat"
        ),
        (
            f"https://hub.example.test{PLANNING_V2_PREFIX}"
            "/work/work-1/complete"
        ),
        (
            f"https://hub.example.test{PLANNING_V2_PREFIX}"
            "/work/work-2/fail"
        ),
        (
            f"https://hub.example.test{PLANNING_V2_PREFIX}"
            "/work/work-2/resume"
        ),
    ]
    assert all(
        call["headers"]["authorization"] == "Bearer runner-token"
        for call in transport.calls
    )
    assert transport.calls[6]["headers"]["idempotency-key"] == "run-key-1"
    assert all(
        "idempotency-key" not in call["headers"]
        for index, call in enumerate(transport.calls)
        if index != 6
    )
    assert json.loads(transport.calls[8]["body"])["workerId"] == (
        "runner-1:planning-1"
    )
    assert json.loads(transport.calls[10]["body"]) == {
        "attemptId": "attempt-1",
        "degraded": False,
        "leaseEpoch": 1,
        "provenance": {},
        "result": {"outline": []},
        "resultKind": "outline.result",
        "schemaVersion": "1.0",
        "workerId": "runner-1:planning-1",
    }


def test_claim_200_preserves_run_results_and_failure_context() -> None:
    claimed = {
        "workItem": {
            "workItemId": "work-synthesis",
            "threadId": "thread-1",
            "runId": "run-1",
            "status": "leased",
        },
        "attempt": {
            "attemptId": "attempt-1",
            "workItemId": "work-synthesis",
            "leaseEpoch": 1,
            "workerId": "runner-1:planning-1",
        },
        "context": {
            "run": _run_projection(),
            "predecessors": [],
            "acceptedRunResults": [
                {
                    "workItem": {"workItemId": "work-research"},
                    "result": {
                        "resultId": "result-research",
                        "resultKind": "research.result",
                    },
                }
            ],
            "failures": [
                {
                    "failureId": "failure-1",
                    "errorCode": "provider_503",
                    "failureClass": "transient_infrastructure",
                }
            ],
        },
    }
    transport = _ScriptedTransport(_Response(200, claimed))

    response = _client(transport).claim_work()

    assert response.payload == claimed
    assert response.payload["context"]["acceptedRunResults"][0][
        "result"
    ]["resultId"] == "result-research"
    assert response.payload["context"]["failures"][0]["errorCode"] == (
        "provider_503"
    )


def test_complete_response_reports_exact_replay_marker() -> None:
    transport = _ScriptedTransport(
        _Response(200, {"ok": True, "replayed": True})
    )

    response = _client(transport).complete_work(
        "work-1",
        worker_id="runner-1:planning-1",
        attempt_id="attempt-1",
        lease_epoch=1,
        result_kind="outline.result",
        schema_version="1.0",
        result={"outline": []},
    )

    assert response.replayed is True


def test_paginated_inputs_can_be_consumed_without_client_count_cap() -> None:
    transport = _ScriptedTransport(
        _Response(
            200,
            {
                "threadId": "thread-1",
                "basisInputSequence": 2,
                "inputs": [{"sequence": 1, "payload": {"text": "one"}}],
                "hasMore": True,
                "nextAfterSequence": 1,
            },
        ),
        _Response(
            200,
            {
                "threadId": "thread-1",
                "basisInputSequence": 2,
                "inputs": [{"sequence": 2, "payload": {"text": "two"}}],
                "hasMore": False,
                "nextAfterSequence": 2,
            },
        ),
    )

    inputs = list(_client(transport).iter_thread_inputs("thread-1"))

    assert [item["sequence"] for item in inputs] == [1, 2]
    assert transport.calls[0]["url"].endswith(
        "afterSequence=0&limit=500"
    )
    assert transport.calls[1]["url"].endswith(
        "afterSequence=1&limit=500"
    )


def test_paginated_inputs_pin_the_initial_basis_during_concurrent_append() -> None:
    transport = _ScriptedTransport(
        _Response(
            200,
            {
                "threadId": "thread-1",
                "basisInputSequence": 2,
                "inputs": [{"sequence": 1, "payload": {"text": "one"}}],
                "hasMore": True,
                "nextAfterSequence": 1,
            },
        ),
        _Response(
            200,
            {
                "threadId": "thread-1",
                "basisInputSequence": 3,
                "inputs": [
                    {"sequence": 2, "payload": {"text": "two"}},
                    {"sequence": 3, "payload": {"text": "concurrent"}},
                ],
                "hasMore": False,
                "nextAfterSequence": 3,
            },
        ),
    )

    inputs = list(_client(transport).iter_thread_inputs("thread-1"))

    assert [item["sequence"] for item in inputs] == [1, 2]


def test_duplicate_provider_webhook_recovers_after_transport_timeout() -> None:
    transport = _ScriptedTransport(
        TimeoutError("timed out after server commit"),
        _Response(200, _thread_projection(duplicate=True)),
    )
    client = _client(transport)

    response = client.create_thread(
        origin=_origin(),
        payload={"text": "identical body"},
    )

    assert response.status == 200
    assert response.replayed is True
    assert response.transport_attempts == 2
    assert len(transport.calls) == 2
    assert transport.calls[0]["body"] == transport.calls[1]["body"]
    assert "idempotency-key" not in transport.calls[0]["headers"]


def test_typed_hub_failure_preserves_service_code_status_and_detail() -> None:
    payload = {
        "ok": False,
        "code": "planning.idempotency_key_conflict",
        "detail": "key already belongs to another thread",
    }
    response_body = BytesIO(json.dumps(payload).encode("utf-8"))
    transport = _ScriptedTransport(
        error.HTTPError(
            "https://hub.example.test",
            409,
            "Conflict",
            {},
            response_body,
        )
    )

    with pytest.raises(PlanningV2HTTPError) as captured:
        _client(transport).create_run(
            "thread-2",
            idempotency_key="conflicting-key",
        )

    failure = captured.value
    assert failure.status == 409
    assert failure.code == "planning.idempotency_key_conflict"
    assert failure.detail == "key already belongs to another thread"
    assert failure.payload == payload
    assert len(transport.calls) == 1
    assert response_body.closed is True


def test_malformed_success_is_a_typed_protocol_failure() -> None:
    transport = _ScriptedTransport(
        _Response(
            200,
            {
                "thread": {},
                "bindings": [],
                "inputEventCount": 0,
                "semanticEventHead": 0,
            },
        )
    )

    with pytest.raises(PlanningV2ProtocolError) as captured:
        _client(transport).get_thread("thread-1")

    assert captured.value.code == "planning.hub_response_invalid"
    assert captured.value.status == 200
    assert "thread.threadId" in str(captured.value.detail)


def test_invalid_idempotency_header_is_rejected_before_transport() -> None:
    transport = _ScriptedTransport()

    with pytest.raises(PlanningV2ConfigError) as captured:
        _client(transport).create_run(
            "thread-1",
            idempotency_key="unsafe\nheader",
        )

    assert captured.value.code == "planning.idempotency_key_invalid"
    assert transport.calls == []


def test_timeout_retry_is_bounded_to_safe_or_idempotent_operations() -> None:
    safe = _ScriptedTransport(
        TimeoutError("first read timed out"),
        _Response(200, _thread_projection()),
    )
    response = _client(safe).get_thread("thread-1")
    assert response.transport_attempts == 2

    unsafe = _ScriptedTransport(TimeoutError("claim outcome unknown"))
    with pytest.raises(PlanningV2TransportError) as captured:
        _client(unsafe).claim_work()

    assert len(unsafe.calls) == 1
    assert captured.value.code == "planning.hub_timeout"
    assert captured.value.ambiguous is True
    assert captured.value.retryable is False

    replay_safe_write = _ScriptedTransport(
        TimeoutError("run response one lost"),
        TimeoutError("run response two lost"),
    )
    with pytest.raises(PlanningV2TransportError) as captured_write:
        _client(replay_safe_write).create_run(
            "thread-1",
            idempotency_key="stable-key",
        )

    assert len(replay_safe_write.calls) == 2
    assert replay_safe_write.calls[0]["body"] == (
        replay_safe_write.calls[1]["body"]
    )
    assert captured_write.value.retryable is True
    assert captured_write.value.ambiguous is True
    assert captured_write.value.attempts == 2
