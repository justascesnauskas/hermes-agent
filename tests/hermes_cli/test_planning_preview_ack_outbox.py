"""Crash/retry contracts for the Planning preview Hub acknowledgement outbox."""

from __future__ import annotations

import threading
from pathlib import Path
import json
import os
import subprocess
import sys
from typing import Any

import pytest

from hermes_cli import planning_preview_ack_outbox as outbox


PROCESS_CHILD = Path(__file__).with_name(
    "process_harness_preview_ack_child.py"
)
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _private_home(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))


def _request(**updates: Any) -> dict[str, Any]:
    request: dict[str, Any] = {
        "schemaVersion": outbox.ACK_REQUEST_SCHEMA,
        "threadId": "planning-thread-1",
        "previewResultId": "preview-result-1",
        "idempotencyKey": "preview-review-key-1",
        "expectedPreviewHash": "sha256:" + "a" * 64,
        "offset": 0,
        "count": 17,
        "pageDigest": "sha256:" + "b" * 64,
        "origin": {
            "schemaVersion": "1.0",
            "provider": "discord",
            "gatewayInstanceId": "runner-1",
            "gatewayAccountId": "discord-primary",
            "chatId": "channel-1",
            "threadId": "thread-1",
            "messageId": "message-inbound-1",
            "senderId": "user-1",
            "chatType": "channel",
            "sourceTimestamp": "2026-07-28T09:00:00Z",
            "providerEventId": "event-inbound-1",
        },
        "deliveryProof": {
            "schemaVersion": "planning.preview-delivery-proof.v1",
            "deliveryNonce": "preview-delivery-nonce-1",
            "provider": "discord",
            "gatewayInstanceId": "runner-1",
            "gatewayAccountId": "discord-primary",
            "chatId": "channel-1",
            "providerMessageId": "provider-message-2",
            "providerMessageIds": [
                "provider-message-1",
                "provider-message-2",
            ],
            "deliveredAt": "2026-07-28T09:01:00Z",
            "previewResultId": "preview-result-1",
            "previewResultHash": "sha256:" + "a" * 64,
            "offset": 0,
            "count": 17,
            "pageDigest": "sha256:" + "b" * 64,
            "deliveryPayloadDigest": "sha256:" + "c" * 64,
            "deliveryContentDigest": "sha256:" + "d" * 64,
        },
    }
    request.update(updates)
    return request


def _bridge_request() -> dict[str, Any]:
    request = _request()
    proof = request.pop("deliveryProof")
    request["deliveryProofBase"] = {
        name: proof[name]
        for name in (
            "schemaVersion",
            "deliveryNonce",
            "provider",
            "gatewayInstanceId",
            "gatewayAccountId",
            "chatId",
            "previewResultId",
            "previewResultHash",
            "offset",
            "count",
            "pageDigest",
        )
    }
    request["deliveryPayloadDigest"] = proof["deliveryPayloadDigest"]
    request["deliveryContentDigest"] = proof["deliveryContentDigest"]
    return request


def test_bridge_validates_complete_ack_authority_before_persisting() -> None:
    staged = outbox.stage_preview_ack_bridge(
        [_bridge_request()],
        semantic_delivery_ids=["semantic-delivery-1"],
        semantic_scope_id="semantic-scope-1",
        provider="discord",
        gateway_account_id="discord-primary",
    )

    assert staged["state"] == "pending"
    assert staged["replayed"] is False


def test_malformed_bridge_authority_fails_before_any_outbox_write(
    tmp_path,
) -> None:
    malformed: list[dict[str, Any]] = []

    wrong_route = _bridge_request()
    wrong_route["origin"]["provider"] = "slack"
    malformed.append(wrong_route)

    wrong_account = _bridge_request()
    wrong_account["deliveryProofBase"]["gatewayAccountId"] = "other-account"
    malformed.append(wrong_account)

    wrong_preview = _bridge_request()
    wrong_preview["deliveryProofBase"]["previewResultId"] = "other-preview"
    malformed.append(wrong_preview)

    malformed_digest = _bridge_request()
    malformed_digest["deliveryContentDigest"] = "not-a-sha256"
    malformed.append(malformed_digest)

    incomplete_origin = _bridge_request()
    incomplete_origin["origin"].pop("providerEventId")
    malformed.append(incomplete_origin)

    premature_provider_proof = _bridge_request()
    premature_provider_proof["deliveryProofBase"][
        "providerMessageId"
    ] = "must-not-exist-yet"
    malformed.append(premature_provider_proof)

    for index, request in enumerate(malformed):
        path = tmp_path / f"invalid-bridge-{index}.sqlite3"
        with pytest.raises(outbox.PreviewAckOutboxError):
            outbox.stage_preview_ack_bridge(
                [request],
                semantic_delivery_ids=["semantic-delivery-1"],
                semantic_scope_id="semantic-scope-1",
                provider="discord",
                gateway_account_id="discord-primary",
                path=path,
            )
        assert not path.exists()


def test_exact_replay_is_stable_and_changed_replay_conflicts() -> None:
    first = outbox.enqueue_preview_ack(_request())
    replay = outbox.enqueue_preview_ack(_request())

    assert first["ackId"] == replay["ackId"]
    assert first["replayed"] is False
    assert replay["replayed"] is True

    changed = _request()
    changed["deliveryProof"] = {
        **changed["deliveryProof"],
        "providerMessageId": "changed-provider-message",
        "providerMessageIds": ["changed-provider-message"],
    }
    with pytest.raises(
        outbox.PreviewAckOutboxError,
        match="idempotency conflict",
    ):
        outbox.enqueue_preview_ack(changed)


def test_expired_process_claim_replays_same_request_after_lost_commit(
    monkeypatch,
) -> None:
    clock = [1_000.0]
    monkeypatch.setattr(outbox.time, "time", lambda: clock[0])
    queued = outbox.enqueue_preview_ack(_request())
    first = outbox.claim_preview_ack(
        queued["ackId"],
        now=1_000.0,
    )
    assert first is not None
    remote_commits = [first.request]

    # Simulate process death after the Hub committed but before local settlement.
    clock[0] = 1_046.0
    recovered = outbox.claim_preview_ack(
        queued["ackId"],
        now=clock[0],
    )
    assert recovered is not None
    remote_commits.append(recovered.request)
    assert recovered.request == first.request
    assert recovered.attempts == 2
    assert outbox.complete_preview_ack(
        recovered.ack_id,
        recovered.lease_token,
    )
    assert outbox.preview_ack_status(queued["ackId"])["state"] == "succeeded"
    assert remote_commits[0] == remote_commits[1]


def test_transient_failures_have_no_attempt_count_stop_condition(
    monkeypatch,
) -> None:
    clock = [2_000.0]
    monkeypatch.setattr(outbox.time, "time", lambda: clock[0])
    queued = outbox.enqueue_preview_ack(_request())

    for expected_attempt in range(1, 21):
        claim = outbox.claim_preview_ack(
            queued["ackId"],
            now=clock[0],
        )
        assert claim is not None
        assert claim.attempts == expected_attempt
        assert outbox.retry_preview_ack(
            claim.ack_id,
            claim.lease_token,
            error="hub_temporarily_unavailable",
            attempts=claim.attempts,
        )
        clock[0] += 61.0

    status = outbox.preview_ack_status(queued["ackId"])
    assert status is not None
    assert status["state"] == "retry_scheduled"
    assert status["attempts"] == 20


def test_gateway_worker_drains_persisted_request_without_user_turn() -> None:
    queued = outbox.enqueue_preview_ack(_request())
    delivered: list[dict[str, Any]] = []
    completed = threading.Event()
    stop = threading.Event()

    def deliver(request) -> None:
        delivered.append(dict(request))
        completed.set()

    worker = threading.Thread(
        target=outbox.run_preview_ack_worker,
        args=(stop,),
        kwargs={"deliver": deliver, "poll_seconds": 0.01},
        daemon=True,
    )
    worker.start()
    try:
        assert completed.wait(2.0)
    finally:
        stop.set()
        worker.join(timeout=2.0)

    assert delivered == [_request()]
    assert outbox.preview_ack_status(queued["ackId"])["state"] == "succeeded"


def test_typed_permanent_hub_rejection_is_terminal_not_a_retry_loop() -> None:
    class PermanentError(RuntimeError):
        status = 422
        code = "planning.preview_review_receipt_invalid"
        retryable = False
        ambiguous = False

    queued = outbox.enqueue_preview_ack(_request())
    result = outbox.dispatch_one_preview_ack(
        deliver=lambda _request: (_ for _ in ()).throw(PermanentError()),
    )

    assert result == {
        "ackId": queued["ackId"],
        "state": "rejected",
        "attempts": 1,
    }
    status = outbox.preview_ack_status(queued["ackId"])
    assert status is not None
    assert status["state"] == "rejected"
    assert status["lastError"] == PermanentError.code


def test_fresh_process_recovers_lost_response_without_new_user_turn(
    tmp_path,
) -> None:
    queued = outbox.enqueue_preview_ack(_request())
    observation = tmp_path / "provider-observation.json"
    child = subprocess.run(
        [
            sys.executable,
            str(PROCESS_CHILD),
            queued["ackId"],
            str(observation),
        ],
        check=False,
        cwd=REPOSITORY_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPOSITORY_ROOT),
        },
    )

    assert child.returncode == 73
    committed = json.loads(observation.read_text(encoding="utf-8"))
    recovered = outbox.claim_preview_ack(
        queued["ackId"],
        now=outbox.time.time() + 46.0,
    )
    assert recovered is not None
    assert recovered.attempts == 2
    assert recovered.request == committed["request"] == _request()
    assert outbox.complete_preview_ack(
        recovered.ack_id,
        recovered.lease_token,
    )
