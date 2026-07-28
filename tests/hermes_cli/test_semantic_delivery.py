"""Behavioral contract for crash-safe semantic message delivery."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import subprocess
import sys
import types

import hermes_cli.semantic_delivery as semantic_delivery_module
from gateway.semantic_exact_attempt import (
    provider_protocol_rejection_evidence,
    provider_rejection_evidence,
)
from hermes_cli.semantic_delivery import (
    SEMANTIC_DELIVERY_CONTRACT,
    begin_semantic_delivery,
    delivery_ledger_path,
    finish_semantic_delivery,
    provider_delivery_token,
    semantic_delivery_scope_id,
    semantic_delivery_status,
    semantic_send,
)


def _begin(tmp_path: Path, delivery_id: str = "delivery-1"):
    return begin_semantic_delivery(
        delivery_id=delivery_id,
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        ledger_path=tmp_path / "ledger.sqlite3",
    )


def test_bounded_diagnostic_marks_redaction_collapsed_long_input_truncated():
    diagnostic = "Authorization: Bearer " + ("s" * 5_000)

    evidence = semantic_delivery_module._bounded_diagnostic(diagnostic)

    assert isinstance(evidence, dict)
    assert evidence["schema_version"] == "hermes.bounded-diagnostic/1"
    assert evidence["text_sha256"] == (
        "sha256:" + hashlib.sha256(diagnostic.encode()).hexdigest()
    )
    assert evidence["truncated"] is True
    assert evidence["redacted"] is True
    assert diagnostic not in evidence["text_preview"]


def test_same_process_concurrent_claim_returns_in_flight(tmp_path) -> None:
    owner = _begin(tmp_path)
    contender = _begin(tmp_path)
    try:
        assert owner.action == "send"
        assert contender.action == "in_flight"
        assert contender.result["outcome"] == "in_flight"
    finally:
        owner.release()


def test_windows_delivery_lock_materializes_byte_before_locking(
    monkeypatch,
    tmp_path,
) -> None:
    calls: list[tuple[int, int]] = []
    lock_path = tmp_path / "locks" / "delivery.lock"

    def locking(file_descriptor: int, mode: int, count: int) -> None:
        if mode == 11:
            assert os.fstat(file_descriptor).st_size == 1
        calls.append((mode, count))

    fake_msvcrt = types.SimpleNamespace(
        LK_NBLCK=11,
        LK_UNLCK=12,
        locking=locking,
    )
    monkeypatch.setitem(sys.modules, "msvcrt", fake_msvcrt)
    monkeypatch.setattr(
        semantic_delivery_module,
        "os",
        types.SimpleNamespace(name="nt"),
    )

    lock = semantic_delivery_module._DeliveryFileLock(lock_path)
    assert lock.acquire() is True
    assert lock_path.read_bytes() == b"\0"
    lock.release()

    assert calls == [(fake_msvcrt.LK_NBLCK, 1), (fake_msvcrt.LK_UNLCK, 1)]


def test_full_checkpoint_only_runs_when_authority_changes(
    monkeypatch,
    tmp_path,
) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    statements: list[str] = []
    real_connect = sqlite3.connect

    def traced_connect(*args, **kwargs):
        connection = real_connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(
        semantic_delivery_module.sqlite3,
        "connect",
        traced_connect,
    )

    first_scope = semantic_delivery_scope_id(ledger_path=ledger)
    assert any(
        "wal_checkpoint(full)" in statement.lower()
        for statement in statements
    )

    statements.clear()
    second_scope = semantic_delivery_scope_id(ledger_path=ledger)

    assert second_scope == first_scope
    assert not any(
        "wal_checkpoint(full)" in statement.lower()
        for statement in statements
    )


def test_delivered_receipt_replays_without_provider_call(tmp_path) -> None:
    calls: list[dict] = []

    def send(payload):
        calls.append(payload)
        return {"success": True, "message_id": "171234.567"}

    first = semantic_send(
        delivery_id="delivery-replay",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        send=send,
        ledger_path=tmp_path / "ledger.sqlite3",
    )
    second = semantic_send(
        delivery_id="delivery-replay",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        send=send,
        ledger_path=tmp_path / "ledger.sqlite3",
    )

    assert first["outcome"] == "delivered"
    assert second["outcome"] == "delivered"
    assert second["replayed"] is True
    assert second["message_ids"] == ["171234.567"]
    assert len(calls) == 1


def test_changed_identity_conflicts_without_provider_call(tmp_path) -> None:
    calls: list[str] = []

    def send(_payload):
        calls.append("called")
        return {"success": True, "message_id": "message-1"}

    semantic_send(
        delivery_id="delivery-conflict",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="first body",
        send=send,
        ledger_path=tmp_path / "ledger.sqlite3",
    )
    conflict = semantic_send(
        delivery_id="delivery-conflict",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="changed body",
        send=send,
        ledger_path=tmp_path / "ledger.sqlite3",
    )

    assert conflict["outcome"] == "conflict"
    assert len(calls) == 1


def test_prewrite_retry_and_permanent_rejection_are_distinct(tmp_path) -> None:
    first = _begin(tmp_path, "delivery-prewrite")
    retryable = finish_semantic_delivery(
        first,
        {
            "error": "provider temporarily unavailable",
            "provider_write_attempted": False,
            "provider_retryable": True,
        },
    )
    assert retryable["outcome"] == "retryable"

    second = _begin(tmp_path, "delivery-prewrite")
    assert second.action == "send"
    rejected = finish_semantic_delivery(
        second,
        {
            "error": "target does not exist",
            "provider_write_attempted": False,
            "provider_retryable": False,
        },
    )
    assert rejected["outcome"] == "rejected"

    replay = _begin(tmp_path, "delivery-prewrite")
    assert replay.action == "rejected"
    assert replay.result["replayed"] is True


def test_private_ledger_stores_digest_not_message(
    monkeypatch,
    tmp_path,
) -> None:
    home = tmp_path / "profile-home"
    monkeypatch.setenv("HERMES_HOME", str(home))
    secret_message = "customer-private-content-9f5b7b"

    result = semantic_send(
        delivery_id="delivery-private",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message=secret_message,
        send=lambda _payload: {
            "success": True,
            "message_id": "message-private",
        },
    )

    ledger = delivery_ledger_path()
    assert result["outcome"] == "delivered"
    assert ledger.exists()
    assert secret_message.encode() not in ledger.read_bytes()
    assert stat.S_IMODE(ledger.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(ledger.stat().st_mode) == 0o600
    with sqlite3.connect(ledger) as connection:
        row = connection.execute(
            "SELECT payload_digest, provider_receipt_json "
            "FROM semantic_deliveries WHERE delivery_id=?",
            ("delivery-private",),
        ).fetchone()
    assert row is not None
    assert len(row[0]) == 64
    assert secret_message not in row[1]


def test_provider_tokens_are_stable_target_and_part_bound() -> None:
    base = provider_delivery_token(
        "delivery-token",
        provider="discord",
        target="discord:123",
        unit=0,
    )
    assert base == provider_delivery_token(
        "delivery-token",
        provider="discord",
        target="discord:123",
        unit=0,
    )
    assert len(base) == 25
    assert base != provider_delivery_token(
        "delivery-token",
        provider="discord",
        target="discord:123",
        unit=1,
    )
    assert base != provider_delivery_token(
        "delivery-token",
        provider="discord",
        target="discord:456",
        unit=0,
    )


def _fresh_process_status(
    *,
    ledger: Path,
    delivery_id: str,
    scope_id: str,
    provider: str,
) -> dict:
    root = Path(__file__).resolve().parents[2]
    code = (
        "import json,sys;"
        "from pathlib import Path;"
        "from hermes_cli.semantic_delivery import semantic_delivery_status;"
        "print(json.dumps(semantic_delivery_status("
        "delivery_id=sys.argv[1],"
        "contract_version='hermes-semantic-delivery/1',"
        "expected_scope_id=sys.argv[2],"
        "expected_provider=sys.argv[3],"
        "ledger_path=Path(sys.argv[4]))))"
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = (
        str(root)
        + (
            os.pathsep + environment["PYTHONPATH"]
            if environment.get("PYTHONPATH")
            else ""
        )
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
            delivery_id,
            scope_id,
            provider,
            str(ledger),
        ],
        cwd=root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def test_provider_rejection_evidence_survives_restart_readback(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "semantic.sqlite3"
    scope_id = semantic_delivery_scope_id(ledger_path=ledger)
    secret = "ghp_" + ("r" * 80)
    body = (
        '{"authorization":"Bearer '
        + secret
        + '","detail":"'
        + ("permanent provider rejection " * 30)
        + '"}'
    )
    evidence = provider_rejection_evidence(
        provider="Home Assistant",
        status=422,
        body=body,
    )
    attempt = begin_semantic_delivery(
        delivery_id="durable-rejection-evidence",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="homeassistant:notification",
        message="one exact message",
        ledger_path=ledger,
        expected_scope_id=scope_id,
    )

    settled = finish_semantic_delivery(
        attempt,
        {
            "error": f"provider rejected token {secret}",
            "provider_write_attempted": False,
            "provider_retryable": False,
            "provider_rejection": evidence,
        },
    )
    replay = _fresh_process_status(
        ledger=ledger,
        delivery_id="durable-rejection-evidence",
        scope_id=scope_id,
        provider="homeassistant",
    )

    assert settled["outcome"] == "rejected"
    assert replay["outcome"] == "rejected"
    assert replay["replayed"] is True
    assert replay["provider_rejection"] == evidence
    assert replay["provider_error"]["redacted"] is True
    with sqlite3.connect(ledger) as connection:
        stored_receipt = connection.execute(
            "SELECT provider_receipt_json FROM semantic_deliveries "
            "WHERE delivery_id='durable-rejection-evidence'"
        ).fetchone()[0]
    assert evidence["body_sha256"] in stored_receipt
    assert secret.encode("utf-8") not in ledger.read_bytes()


def test_retryable_rejection_evidence_survives_restart_readback(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "semantic.sqlite3"
    scope_id = semantic_delivery_scope_id(ledger_path=ledger)
    evidence = provider_rejection_evidence(
        provider="ntfy",
        status=429,
        body={"error": "rate limited", "request": "req-123"},
    )
    attempt = begin_semantic_delivery(
        delivery_id="durable-retry-evidence",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="ntfy:alerts",
        message="one exact message",
        ledger_path=ledger,
        expected_scope_id=scope_id,
    )

    settled = finish_semantic_delivery(
        attempt,
        {
            "error": "ntfy HTTP 429",
            "provider_write_attempted": False,
            "provider_retryable": True,
            "provider_rejection": evidence,
        },
    )
    replay = _fresh_process_status(
        ledger=ledger,
        delivery_id="durable-retry-evidence",
        scope_id=scope_id,
        provider="ntfy",
    )

    assert settled["outcome"] == "retryable"
    assert replay["outcome"] == "retryable"
    assert replay["replayed"] is True
    assert replay["provider_rejection"] == evidence
    assert replay["provider_error"] == "ntfy HTTP 429"


def test_protocol_rejection_evidence_survives_restart_readback(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "semantic.sqlite3"
    scope_id = semantic_delivery_scope_id(ledger_path=ledger)
    evidence = provider_protocol_rejection_evidence(
        provider="SimpleX",
        protocol="simplex-json-websocket",
        response={
            "type": "chatCmdError",
            "chatError": {"type": "error", "message": "denied"},
        },
    )
    attempt = begin_semantic_delivery(
        delivery_id="durable-protocol-rejection",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="simplex:42",
        message="one exact message",
        ledger_path=ledger,
        expected_scope_id=scope_id,
    )

    finish_semantic_delivery(
        attempt,
        {
            "error": "SimpleX protocol rejection",
            "provider_write_attempted": False,
            "provider_retryable": False,
            "provider_rejection": evidence,
        },
    )
    replay = _fresh_process_status(
        ledger=ledger,
        delivery_id="durable-protocol-rejection",
        scope_id=scope_id,
        provider="simplex",
    )

    assert replay["outcome"] == "rejected"
    assert replay["provider_rejection"] == evidence


def test_adapter_result_mapping_carries_sanitized_rejection_evidence() -> None:
    evidence = provider_rejection_evidence(
        provider="LINE",
        status=503,
        body="upstream unavailable",
    )
    result = types.SimpleNamespace(
        success=False,
        message_id=None,
        continuation_message_ids=(),
        error="LINE provider failed",
        raw_response={
            "provider_write_attempted": False,
            "provider_retryable": True,
            "provider_rejection": evidence,
        },
        retryable=True,
        retry_after=3,
    )

    mapped = semantic_delivery_module._send_result_mapping(result)

    assert mapped["provider_rejection"] == evidence
    assert mapped["provider_error"] == "LINE provider failed"
    assert mapped["provider_write_attempted"] is False
    assert mapped["provider_retryable"] is True


def test_long_provider_message_id_is_preserved_exactly(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "semantic.sqlite3"
    provider_id = "  teams-" + ("訊" * 2_048) + "-receipt  "

    result = semantic_send(
        delivery_id="oversized-provider-message-id",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        send=lambda _payload: {
            "success": True,
            "message_id": provider_id,
        },
        ledger_path=ledger,
    )

    assert result["outcome"] == "delivered"
    assert result["message_id"] == provider_id
    with sqlite3.connect(ledger) as connection:
        receipt = connection.execute(
            "SELECT provider_receipt_json FROM semantic_deliveries "
            "WHERE delivery_id=?",
            ("oversized-provider-message-id",),
        ).fetchone()[0]
    assert json.loads(receipt)["message_id"] == provider_id


def test_ledger_scope_survives_restart_and_database_copy(tmp_path) -> None:
    ledger = tmp_path / "source" / "ledger.sqlite3"
    first = semantic_delivery_scope_id(ledger_path=ledger)
    assert semantic_delivery_scope_id(ledger_path=ledger) == first

    copied = tmp_path / "copied" / "ledger.sqlite3"
    copied.parent.mkdir(parents=True)
    shutil.copy2(ledger, copied)
    assert semantic_delivery_scope_id(ledger_path=copied) == first

    fresh = tmp_path / "fresh" / "ledger.sqlite3"
    assert semantic_delivery_scope_id(ledger_path=fresh) != first
    assert stat.S_IMODE(fresh.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(fresh.stat().st_mode) == 0o600


def test_scope_mismatch_fails_before_provider_write(tmp_path) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    current = semantic_delivery_scope_id(ledger_path=ledger)
    calls: list[dict] = []

    result = semantic_send(
        delivery_id="delivery-scope-conflict",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        send=lambda payload: calls.append(payload) or {
            "success": True,
            "message_id": "must-not-send",
        },
        ledger_path=ledger,
        expected_scope_id="scope_stale",
        gateway_account_id="slack-primary",
    )

    assert result == {
        "delivery_contract": SEMANTIC_DELIVERY_CONTRACT,
        "delivery_id": "delivery-scope-conflict",
        "delivery_scope_id": current,
        "error": "semantic_delivery_scope_conflict",
        "gateway_account_id": "slack-primary",
        "outcome": "conflict",
        "provider": "slack",
        "replay_strategy": "none",
        "replayed": False,
        "target": "slack:C123",
    }
    assert calls == []


def test_success_without_provider_receipt_is_ambiguous(tmp_path) -> None:
    calls: list[dict] = []
    result = semantic_send(
        delivery_id="delivery-no-receipt",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        send=lambda payload: calls.append(payload) or {"success": True},
        ledger_path=tmp_path / "ledger.sqlite3",
    )

    assert result["outcome"] == "ambiguous"
    assert result["error"] == "semantic_delivery_outcome_ambiguous"
    assert len(calls) == 1


def test_read_only_status_never_reopens_provider_send(tmp_path) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    scope = semantic_delivery_scope_id(ledger_path=ledger)
    calls: list[dict] = []
    delivered = semantic_send(
        delivery_id="delivery-status",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        send=lambda payload: calls.append(payload) or {
            "success": True,
            "message_id": "slack-receipt-1",
        },
        ledger_path=ledger,
        expected_scope_id=scope,
    )

    replay = semantic_delivery_status(
        delivery_id="delivery-status",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        expected_scope_id=scope,
        ledger_path=ledger,
    )
    missing = semantic_delivery_status(
        delivery_id="delivery-missing",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        expected_scope_id=scope,
        ledger_path=ledger,
    )
    mismatch = semantic_delivery_status(
        delivery_id="delivery-status",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        expected_scope_id="scope_stale",
        ledger_path=ledger,
    )

    assert delivered["delivery_scope_id"] == scope
    assert replay["outcome"] == "delivered"
    assert replay["message_ids"] == ["slack-receipt-1"]
    assert replay["delivery_scope_id"] == scope
    assert missing["outcome"] == "retryable"
    assert missing["error"] == "semantic_delivery_missing_prewrite"
    assert missing["provider_write_attempted"] is False
    assert missing["provider_write_started"] is False
    assert mismatch["outcome"] == "conflict"
    assert len(calls) == 1


def test_read_only_status_reports_live_lock_as_in_flight(tmp_path) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    scope = semantic_delivery_scope_id(ledger_path=ledger)
    attempt = begin_semantic_delivery(
        delivery_id="delivery-unsettled",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        ledger_path=ledger,
        expected_scope_id=scope,
    )
    try:
        status = semantic_delivery_status(
            delivery_id="delivery-unsettled",
            contract_version=SEMANTIC_DELIVERY_CONTRACT,
            expected_scope_id=scope,
            ledger_path=ledger,
        )
    finally:
        attempt.release()

    assert status["outcome"] == "in_flight"
    assert status["error"] == "semantic_delivery_in_flight"
    assert status["delivery_scope_id"] == scope
    assert "provider_write_attempted" not in status
    assert "provider_write_started" not in status


def test_read_only_status_preserves_exact_durable_state_semantics(
    tmp_path,
) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    scope = semantic_delivery_scope_id(ledger_path=ledger)

    retryable = semantic_send(
        delivery_id="delivery-status-intent",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="retry later",
        send=lambda _payload: {
            "provider_write_attempted": False,
            "provider_retryable": True,
        },
        ledger_path=ledger,
        expected_scope_id=scope,
        gateway_account_id="slack-primary",
    )
    rejected = semantic_send(
        delivery_id="delivery-status-rejected",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="unsupported route",
        send=lambda _payload: {
            "provider_write_attempted": False,
            "provider_retryable": False,
        },
        ledger_path=ledger,
        expected_scope_id=scope,
        gateway_account_id="slack-primary",
    )
    ambiguous = semantic_send(
        delivery_id="delivery-status-ambiguous",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="transport uncertainty",
        send=lambda _payload: {
            "error": "transport lost",
            "provider_write_attempted": True,
        },
        ledger_path=ledger,
        expected_scope_id=scope,
        gateway_account_id="slack-primary",
    )
    bounded_sending = begin_semantic_delivery(
        delivery_id="delivery-status-bounded-sending",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="discord:C123",
        message="bounded send",
        ledger_path=ledger,
        expected_scope_id=scope,
        gateway_account_id="discord-primary",
    )
    bounded_sending.release()
    native_sending = begin_semantic_delivery(
        delivery_id="delivery-status-native-sending",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="matrix:!room:example.org",
        message="native send",
        ledger_path=ledger,
        expected_scope_id=scope,
        gateway_account_id="matrix-primary",
    )
    native_sending.release()

    def status(delivery_id, provider, account):
        return semantic_delivery_status(
            delivery_id=delivery_id,
            contract_version=SEMANTIC_DELIVERY_CONTRACT,
            expected_scope_id=scope,
            expected_provider=provider,
            gateway_account_id=account,
            ledger_path=ledger,
        )

    intent_status = status(
        "delivery-status-intent",
        "slack",
        "slack-primary",
    )
    rejected_status = status(
        "delivery-status-rejected",
        "slack",
        "slack-primary",
    )
    ambiguous_status = status(
        "delivery-status-ambiguous",
        "slack",
        "slack-primary",
    )
    bounded_status = status(
        "delivery-status-bounded-sending",
        "discord",
        "discord-primary",
    )
    native_status = status(
        "delivery-status-native-sending",
        "matrix",
        "matrix-primary",
    )

    assert retryable["outcome"] == "retryable"
    assert intent_status["outcome"] == "retryable"
    assert intent_status["provider_write_attempted"] is False
    assert intent_status["provider_write_started"] is False
    assert rejected["outcome"] == "rejected"
    assert rejected_status["outcome"] == "rejected"
    assert rejected_status["error"] == "semantic_delivery_provider_rejected"
    assert rejected_status["provider_write_attempted"] is False
    assert ambiguous["outcome"] == "ambiguous"
    assert ambiguous_status["outcome"] == "ambiguous"
    assert "provider_write_attempted" not in ambiguous_status
    assert "provider_write_started" not in ambiguous_status
    assert bounded_status["outcome"] == "ambiguous"
    assert bounded_status["replay_strategy"] == "bounded_native"
    assert "provider_write_attempted" not in bounded_status
    assert "provider_write_started" not in bounded_status
    assert native_status["outcome"] == "retryable"
    assert native_status["replay_strategy"] == "durable_native"
    assert "provider_write_attempted" not in native_status
    assert "provider_write_started" not in native_status


def test_first_success_returns_authoritative_scope_and_account(tmp_path) -> None:
    ledger = tmp_path / "ledger.sqlite3"

    result = semantic_send(
        delivery_id="delivery-first-success",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        send=lambda _payload: {
            "success": True,
            "message_id": "message-first",
        },
        ledger_path=ledger,
        gateway_account_id="slack-primary",
    )

    assert result["outcome"] == "delivered"
    assert result["gateway_account_id"] == "slack-primary"
    assert result["delivery_scope_id"] == semantic_delivery_scope_id(
        ledger_path=ledger
    )


def test_account_identity_conflicts_before_provider_write(tmp_path) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    calls: list[str] = []
    first = semantic_send(
        delivery_id="delivery-account-bound",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        send=lambda _payload: {
            "success": True,
            "message_id": "message-account",
        },
        ledger_path=ledger,
        gateway_account_id="slack-primary",
    )

    conflict = semantic_send(
        delivery_id="delivery-account-bound",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        send=lambda _payload: calls.append("called") or {
            "success": True,
            "message_id": "must-not-send",
        },
        ledger_path=ledger,
        gateway_account_id="slack-secondary",
    )

    assert first["outcome"] == "delivered"
    assert conflict["outcome"] == "conflict"
    assert conflict["gateway_account_id"] == "slack-secondary"
    assert calls == []


def test_settlement_after_ledger_replacement_is_ambiguous(tmp_path) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    attempt = begin_semantic_delivery(
        delivery_id="delivery-replaced-ledger",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        ledger_path=ledger,
        gateway_account_id="slack-primary",
    )
    assert attempt.action == "send"

    for candidate in (
        ledger,
        Path(f"{ledger}-wal"),
        Path(f"{ledger}-shm"),
    ):
        candidate.unlink(missing_ok=True)
    result = finish_semantic_delivery(
        attempt,
        {"success": True, "message_id": "provider-accepted"},
    )

    assert result["outcome"] == "ambiguous"
    assert result["error"] == "semantic_delivery_settlement_ambiguous"
    with sqlite3.connect(ledger) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM semantic_deliveries"
        ).fetchone()[0]
    assert count == 0


def test_settlement_rejects_tampered_identity_row(tmp_path) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    attempt = begin_semantic_delivery(
        delivery_id="delivery-tampered-row",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        ledger_path=ledger,
        gateway_account_id="slack-primary",
    )
    assert attempt.action == "send"
    with sqlite3.connect(ledger) as connection:
        connection.execute(
            "UPDATE semantic_deliveries SET payload_digest=? "
            "WHERE delivery_id=?",
            ("0" * 64, "delivery-tampered-row"),
        )
        connection.commit()

    result = finish_semantic_delivery(
        attempt,
        {"success": True, "message_id": "provider-accepted"},
    )

    assert result["outcome"] == "ambiguous"
    assert result["error"] == "semantic_delivery_settlement_ambiguous"
    with sqlite3.connect(ledger) as connection:
        state = connection.execute(
            "SELECT state FROM semantic_deliveries WHERE delivery_id=?",
            ("delivery-tampered-row",),
        ).fetchone()[0]
    assert state == "sending"


def test_corrupt_stored_receipt_never_claims_delivery(tmp_path) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    calls: list[str] = []
    semantic_send(
        delivery_id="delivery-corrupt-receipt",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        send=lambda _payload: {
            "success": True,
            "message_id": "message-real",
        },
        ledger_path=ledger,
        gateway_account_id="slack-primary",
    )
    with sqlite3.connect(ledger) as connection:
        connection.execute(
            "UPDATE semantic_deliveries SET provider_receipt_json=? "
            "WHERE delivery_id=?",
            ('{"outcome":"delivered","message_id":"forged"}', "delivery-corrupt-receipt"),
        )
        connection.commit()

    replay = semantic_send(
        delivery_id="delivery-corrupt-receipt",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        send=lambda _payload: calls.append("called") or {
            "success": True,
            "message_id": "must-not-send",
        },
        ledger_path=ledger,
        gateway_account_id="slack-primary",
    )

    assert replay["outcome"] == "ambiguous"
    assert replay["error"] == "semantic_delivery_receipt_invalid"
    assert replay["gateway_account_id"] == "slack-primary"
    assert calls == []


def test_status_validates_exact_provider_and_account(tmp_path) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    delivered = semantic_send(
        delivery_id="delivery-status-identity",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target="slack:C123",
        message="one exact message",
        send=lambda _payload: {
            "success": True,
            "message_id": "message-status",
        },
        ledger_path=ledger,
        gateway_account_id="slack-primary",
    )

    exact = semantic_delivery_status(
        delivery_id="delivery-status-identity",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        expected_scope_id=delivered["delivery_scope_id"],
        expected_provider="slack",
        gateway_account_id="slack-primary",
        ledger_path=ledger,
    )
    wrong_account = semantic_delivery_status(
        delivery_id="delivery-status-identity",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        expected_scope_id=delivered["delivery_scope_id"],
        expected_provider="slack",
        gateway_account_id="slack-secondary",
        ledger_path=ledger,
    )

    assert exact["outcome"] == "delivered"
    assert exact["gateway_account_id"] == "slack-primary"
    assert wrong_account["outcome"] == "conflict"
    assert wrong_account["gateway_account_id"] == "slack-secondary"
