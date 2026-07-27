"""Exact-wire and recovery tests for the Dev Hub Planning V2 client."""

from __future__ import annotations

import hashlib
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
    derive_approval_idempotency_key,
    derive_artifact_idempotency_key,
    derive_artifact_input_origin,
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
        body_is_stream = hasattr(req.data, "read")
        body = req.data.read() if body_is_stream else req.data
        self.calls.append(
            {
                "method": req.get_method(),
                "url": req.full_url,
                "headers": {
                    key.lower(): value
                    for key, value in req.header_items()
                },
                "body": body,
                "bodyIsStream": body_is_stream,
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


def _approval_projection(*, replayed: bool = False) -> dict[str, Any]:
    return {
        "ok": True,
        "replayed": replayed,
        "applyBindingId": "apply-binding-1",
        "threadId": "thread-1",
        "runId": "run-1",
        "previewResultId": "preview-1",
        "previewResultHash": "sha256:preview-1",
        "planHash": "sha256:plan-1",
        "basisInputSequence": 1,
        "planId": "plan-1",
        "approvalId": "approval-1",
        "operationId": "operation-1",
        "status": "requested",
        "operation": {"status": "queued"},
        "error": None,
        "createdAt": "2026-07-27T12:30:00Z",
        "updatedAt": "2026-07-27T12:30:00Z",
    }


def _approval_key(provider: str = "discord") -> str:
    origin = _origin(provider)
    return derive_approval_idempotency_key(
        runner_id="runner-1",
        thread_id="thread-1",
        preview_result_id="preview-1",
        provider=provider,
        gateway_account_id=origin["gatewayAccountId"],
        provider_event_id=origin["providerEventId"],
    )


def _preview_page(
    *,
    offset: int,
    tasks: list[dict[str, Any]],
    task_count: int = 137,
    limit: int = 60,
    next_offset: int | None,
) -> dict[str, Any]:
    return {
        "threadId": "thread-1",
        "runId": "run-1",
        "previewResultId": "preview-1",
        "previewResultHash": "sha256:preview-1",
        "planHash": "sha256:plan-1",
        "basisInputSequence": 4,
        "taskCount": task_count,
        "offset": offset,
        "limit": limit,
        "returned": len(tasks),
        "tasks": tasks,
        "hasMore": next_offset is not None,
        "nextOffset": next_offset,
        "title": "Planning V2 delivery",
        "objective": "Ship the accepted implementation chain",
        "summary": "A complete dependency-ordered implementation plan.",
        "decisions": [{"code": "ready_for_approval"}],
        "coverage": {
            "ready": True,
            "totalSources": 12,
            "findings": [],
        },
        "acceptedAt": "2026-07-27T12:30:00Z",
        "approvalEligible": True,
    }


def _artifact_upload_projection(
    content: bytes,
    *,
    disposition: str = "committed",
    role: str = "design_reference",
    position: int = 1,
) -> dict[str, Any]:
    checksum = f"sha256:{hashlib.sha256(content).hexdigest()}"
    blob_id = "ablob_0123456789abcdef"
    artifact_ref = f"planning-artifact-v1:{blob_id}"
    return {
        "ok": True,
        "storage": {"mode": "local", "reason": "local_fallback"},
        "disposition": disposition,
        "artifact": {
            "blobId": blob_id,
            "artifactRef": artifact_ref,
            "checksum": checksum,
            "sizeBytes": len(content),
            "contentType": "image/png",
            "retentionPolicy": "reference_bound",
            "retainUntil": None,
            "createdAt": "2026-07-27T12:30:00Z",
        },
        "reference": {
            "referenceId": "aref_0123456789abcdef",
            "blobId": blob_id,
            "artifactRef": artifact_ref,
            "role": role,
            "position": position,
            "provenance": {
                "schemaVersion": "1.0",
                "source": "planning_v2_runner_upload",
                "filename": "dashboard.png",
            },
            "createdAt": "2026-07-27T12:30:00Z",
            "releasedAt": None,
        },
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


def test_preview_approval_uses_exact_hashes_origin_and_replay_header() -> None:
    transport = _ScriptedTransport(_Response(200, _approval_projection()))
    client = _client(transport)
    origin = _origin()
    key = _approval_key()

    response = client.approve_and_apply_preview(
        "thread-1",
        "preview-1",
        idempotency_key=key,
        origin=origin,
        expected_preview_hash="sha256:preview-1",
        expected_plan_hash="sha256:plan-1",
        approval_message="Tvirtinu šį tikslų planą",
        approval_evidence={"verbatimClause": "Tvirtinu"},
    )

    assert response.payload["operationId"] == "operation-1"
    call = transport.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == (
        f"https://hub.example.test{PLANNING_V2_PREFIX}/threads/thread-1"
        "/previews/preview-1/approve-apply"
    )
    assert call["headers"]["idempotency-key"] == key
    assert json.loads(call["body"]) == {
        "approvalEvidence": {"verbatimClause": "Tvirtinu"},
        "approvalMessage": "Tvirtinu šį tikslų planą",
        "expectedPlanHash": "sha256:plan-1",
        "expectedPreviewHash": "sha256:preview-1",
        "origin": origin,
    }


def test_preview_read_wire_needs_runner_auth_but_no_turn_origin() -> None:
    transport = _ScriptedTransport(
        _Response(
            200,
            _preview_page(
                offset=20,
                tasks=[{"stableTaskId": "task-21"}],
                task_count=21,
                limit=10,
                next_offset=None,
            ),
        )
    )

    response = _client(transport).get_preview_page(
        "thread-1",
        "preview-1",
        offset=20,
        limit=10,
    )

    assert response.payload["previewResultHash"] == "sha256:preview-1"
    call = transport.calls[0]
    assert call["method"] == "GET"
    assert call["url"] == (
        f"https://hub.example.test{PLANNING_V2_PREFIX}/threads/thread-1"
        "/previews/preview-1?offset=20&limit=10"
    )
    assert call["headers"]["authorization"] == "Bearer runner-token"
    assert call["body"] is None


def test_preview_iterator_reads_all_137_tasks_without_total_cap() -> None:
    tasks = [
        {"stableTaskId": f"task-{index:03d}"}
        for index in range(1, 138)
    ]
    transport = _ScriptedTransport(
        _Response(
            200,
            _preview_page(
                offset=0,
                tasks=tasks[:60],
                next_offset=60,
            ),
        ),
        _Response(
            200,
            _preview_page(
                offset=60,
                tasks=tasks[60:120],
                next_offset=120,
            ),
        ),
        _Response(
            200,
            _preview_page(
                offset=120,
                tasks=tasks[120:],
                next_offset=None,
            ),
        ),
    )

    received = list(
        _client(transport).iter_preview_tasks(
            "thread-1",
            "preview-1",
            page_size=60,
        )
    )

    assert received == tasks
    assert len(received) == 137
    assert [
        call["url"].rsplit("?", 1)[-1] for call in transport.calls
    ] == [
        "offset=0&limit=60",
        "offset=60&limit=60",
        "offset=120&limit=60",
    ]


def test_preview_schema_rejects_nullable_or_stalled_page_fields() -> None:
    malformed = _preview_page(
        offset=0,
        tasks=[{"stableTaskId": "task-1"}],
        task_count=2,
        limit=1,
        next_offset=0,
    )
    malformed["summary"] = None
    transport = _ScriptedTransport(_Response(200, malformed))

    with pytest.raises(PlanningV2ProtocolError) as captured:
        _client(transport).get_preview_page(
            "thread-1",
            "preview-1",
            limit=1,
        )

    assert captured.value.code == "planning.hub_response_invalid"
    assert "summary" in str(captured.value.detail)


def test_artifact_upload_streams_raw_file_and_replays_lost_response(
    tmp_path,
) -> None:
    content = b"\x89PNG\r\n\x1a\nraw-binary-evidence\x00\xff"
    source = tmp_path / "dashboard.png"
    source.write_bytes(content)
    projection = _artifact_upload_projection(
        content,
        disposition="replayed",
    )
    transport = _ScriptedTransport(
        TimeoutError("upload response lost after commit"),
        _Response(201, projection),
    )
    origin = _origin()
    key = derive_artifact_idempotency_key(
        runner_id="runner-1",
        thread_id="thread-1",
        provider=origin["provider"],
        gateway_account_id=origin["gatewayAccountId"],
        provider_event_id=origin["providerEventId"],
        role="design_reference",
        position=1,
    )

    response = _client(transport).upload_artifact(
        "thread-1",
        str(source),
        role="design_reference",
        position=1,
        idempotency_key=key,
    )

    assert response.payload == projection
    assert response.transport_attempts == 2
    assert [call["body"] for call in transport.calls] == [content, content]
    assert all(call["bodyIsStream"] for call in transport.calls)
    assert transport.calls[0]["url"] == (
        f"https://hub.example.test{PLANNING_V2_PREFIX}/threads/thread-1"
        "/artifacts?role=design_reference&position=1"
    )
    assert all(
        call["headers"]["idempotency-key"] == key
        for call in transport.calls
    )
    assert all(
        call["headers"]["content-type"] == "image/png"
        for call in transport.calls
    )
    assert all(
        call["headers"]["content-length"] == str(len(content))
        for call in transport.calls
    )
    assert all(
        call["headers"]["x-artifact-filename"] == "dashboard.png"
        for call in transport.calls
    )
    assert b"base64" not in transport.calls[0]["body"]


def test_artifact_upload_rejects_mismatched_success_identity(
    tmp_path,
) -> None:
    content = b"exact evidence"
    source = tmp_path / "dashboard.png"
    source.write_bytes(content)
    malformed = _artifact_upload_projection(content)
    malformed["reference"]["position"] = 2
    transport = _ScriptedTransport(_Response(201, malformed))

    with pytest.raises(PlanningV2ProtocolError) as captured:
        _client(transport).upload_artifact(
            "thread-1",
            str(source),
            role="design_reference",
            position=1,
            idempotency_key="hermes-planning-artifact-v1:stable",
        )

    assert captured.value.code == "planning.hub_response_invalid"
    assert "identity mismatch" in str(captured.value.detail)


def test_artifact_identity_is_stable_scoped_and_preserves_turn_origin() -> None:
    origin = _origin()
    first = derive_artifact_idempotency_key(
        runner_id="runner-1",
        thread_id="thread-1",
        provider=origin["provider"],
        gateway_account_id=origin["gatewayAccountId"],
        provider_event_id=origin["providerEventId"],
        role="database_schema",
        position=137,
    )
    repeated = derive_artifact_idempotency_key(
        runner_id="runner-1",
        thread_id="thread-1",
        provider=origin["provider"],
        gateway_account_id=origin["gatewayAccountId"],
        provider_event_id=origin["providerEventId"],
        role="database_schema",
        position=137,
    )
    another_position = derive_artifact_idempotency_key(
        runner_id="runner-1",
        thread_id="thread-1",
        provider=origin["provider"],
        gateway_account_id=origin["gatewayAccountId"],
        provider_event_id=origin["providerEventId"],
        role="database_schema",
        position=138,
    )
    derived_origin = derive_artifact_input_origin(
        origin,
        runner_id="runner-1",
        thread_id="thread-1",
        role="database_schema",
        position=137,
    )

    assert first == repeated
    assert first.startswith("hermes-planning-artifact-v1:")
    assert first != another_position
    assert derived_origin["providerEventId"].startswith(
        "hermes-planning-artifact-input-v1:"
    )
    assert {
        name: derived_origin[name]
        for name in origin
        if name != "providerEventId"
    } == {
        name: origin[name]
        for name in origin
        if name != "providerEventId"
    }
    assert derived_origin["messageId"] == origin["messageId"]
    assert derived_origin["senderId"] == origin["senderId"]
    assert "dashboard.png" not in first
    assert "exact evidence" not in first


def test_approval_key_is_stable_and_scoped_without_message_content() -> None:
    first = _approval_key()
    repeated = _approval_key()
    another_preview = derive_approval_idempotency_key(
        runner_id="runner-1",
        thread_id="thread-1",
        preview_result_id="preview-2",
        provider="discord",
        gateway_account_id="discord-account",
        provider_event_id="discord-event",
    )
    another_provider = derive_approval_idempotency_key(
        runner_id="runner-1",
        thread_id="thread-1",
        preview_result_id="preview-1",
        provider="slack",
        gateway_account_id="slack-account",
        provider_event_id="discord-event",
    )

    assert first == repeated
    assert first.startswith("hermes-planning-approval-v1:")
    assert first != another_preview
    assert first != another_provider
    assert "Tvirtinu" not in first


def test_approval_lost_response_replays_exact_request() -> None:
    transport = _ScriptedTransport(
        TimeoutError("approval response lost after commit"),
        _Response(200, _approval_projection(replayed=True)),
    )
    key = _approval_key()

    response = _client(transport).approve_and_apply_preview(
        "thread-1",
        "preview-1",
        idempotency_key=key,
        origin=_origin(),
        expected_preview_hash="sha256:preview-1",
        expected_plan_hash="sha256:plan-1",
        approval_message="Tvirtinu",
    )

    assert response.replayed is True
    assert response.transport_attempts == 2
    assert transport.calls[0]["headers"]["idempotency-key"] == key
    assert transport.calls[0]["body"] == transport.calls[1]["body"]


def test_approval_response_identity_mismatch_is_protocol_failure() -> None:
    malformed = _approval_projection()
    malformed["planHash"] = "sha256:another-plan"
    transport = _ScriptedTransport(_Response(200, malformed))

    with pytest.raises(PlanningV2ProtocolError) as captured:
        _client(transport).approve_and_apply_preview(
            "thread-1",
            "preview-1",
            idempotency_key=_approval_key(),
            origin=_origin(),
            expected_preview_hash="sha256:preview-1",
            expected_plan_hash="sha256:plan-1",
            approval_message="Tvirtinu",
        )

    assert captured.value.code == "planning.hub_response_invalid"
    assert "identity mismatch" in str(captured.value.detail)


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
