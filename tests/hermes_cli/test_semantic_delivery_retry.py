"""Durable automatic semantic retry outbox and ordered continuation."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys
from types import SimpleNamespace

import pytest

from gateway.platform_registry import PlatformEntry, platform_registry
from gateway.semantic_exact_attempt import (
    LiveSemanticExactAttemptCapability,
    live_semantic_exact_attempt_encoding_contract,
    provider_rejection_evidence,
    semantic_exact_attempt_encoding_contract,
)
from hermes_cli.semantic_delivery import (
    SEMANTIC_DELIVERY_CONTRACT,
    begin_semantic_delivery,
    dispatch_due_semantic_delivery_retries,
    finish_semantic_delivery,
    register_semantic_retry_completion_handler,
    semantic_delivery_retry_status,
    semantic_delivery_status,
    semantic_delivery_scope_id,
    semantic_retry_turn_execution_committed,
    stage_semantic_delivery_retry,
    unregister_semantic_retry_completion_handler,
)


PROVIDER = "retry_test"
ACCOUNT = "retry-account"
TARGET = f"{PROVIDER}:preview-target-v1:stable"


class ExactRetryAdapter:
    platform = PROVIDER
    SEMANTIC_EXACT_ATTEMPT_CAPABILITY = (
        LiveSemanticExactAttemptCapability(
            provider=PROVIDER,
            contract="hermes-live-semantic-exact-attempt/1",
            segmentation_version="retry-test-logical-v1",
            max_logical_units=1000,
            length_semantics="unicode_codepoints",
            wire_encoding="retry-test-wire-v1",
        )
    )

    def __init__(self, outcomes):
        self.config = SimpleNamespace(
            extra={"gateway_account_id": ACCOUNT}
        )
        self.outcomes = outcomes
        self.calls = []

    async def send(self, *args, **kwargs):
        raise AssertionError("generic send path must not be used by scheduler")

    async def send_semantic_exact_attempt(self, request):
        self.calls.append(request)
        outcome = self.outcomes(request, len(self.calls))
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _success(message_id: str):
    return SimpleNamespace(
        success=True,
        message_id=message_id,
        continuation_message_ids=(),
        error=None,
        raw_response={},
        retryable=False,
        retry_after=None,
    )


def _prewrite_429(retry_after: float = 0):
    return SimpleNamespace(
        success=False,
        message_id=None,
        continuation_message_ids=(),
        error="provider rate limited",
        raw_response={
            "provider_write_attempted": False,
            "provider_retryable": True,
        },
        retryable=True,
        retry_after=retry_after,
    )


def _prewrite_429_with_evidence(retry_after: float = 60):
    evidence = provider_rejection_evidence(
        provider="Retry Test",
        status=429,
        body={
            "error": "rate limited",
            "request": "provider-request-123",
        },
    )
    return SimpleNamespace(
        success=False,
        message_id=None,
        continuation_message_ids=(),
        error="Retry Test HTTP 429",
        raw_response={
            "provider_write_attempted": False,
            "provider_retryable": True,
            "provider_rejection": evidence,
        },
        retryable=True,
        retry_after=retry_after,
    )


@pytest.fixture(autouse=True)
def _registered_provider():
    platform_registry.register(
        PlatformEntry(
            name=PROVIDER,
            label="Retry test",
            adapter_factory=lambda config: None,
            check_fn=lambda: True,
            semantic_exact_attempt=False,
            live_semantic_exact_attempt=True,
        )
    )
    try:
        yield
    finally:
        platform_registry.unregister(PROVIDER)


def _stage(
    ledger: Path,
    adapter: ExactRetryAdapter,
    *,
    delivery_id: str,
    message: str,
    group: str,
    unit_index: int,
    unit_count: int,
    turn_execution_ref: str = "",
    completion_contract: str = "",
    completion_ref: str = "",
) -> dict:
    return stage_semantic_delivery_retry(
        delivery_id=delivery_id,
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target=TARGET,
        message=message,
        gateway_account_id=ACCOUNT,
        adapter=adapter,
        encoding_contract=live_semantic_exact_attempt_encoding_contract(
            adapter
        ),
        chat_id="chat-123",
        thread_id="thread-456",
        reply_to="message-789" if unit_index == 0 else None,
        delivery_group_id=group,
        unit_index=unit_index,
        unit_count=unit_count,
        turn_execution_ref=turn_execution_ref,
        completion_contract=completion_contract,
        completion_ref=completion_ref,
        expected_scope_id=semantic_delivery_scope_id(
            ledger_path=ledger
        ),
        initial_delay_seconds=0,
        ledger_path=ledger,
    )


def test_ordered_group_429_resumes_without_overtake_and_completes(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    first_attempts = 0

    def outcomes(request, _call_number):
        nonlocal first_attempts
        if request.delivery_unit == 0:
            first_attempts += 1
            if first_attempts == 1:
                return _prewrite_429(0)
            return _success("provider-unit-0")
        return _success("provider-unit-1")

    adapter = ExactRetryAdapter(outcomes)
    completions: list[tuple[str, str, tuple[str, ...]]] = []

    def complete(**kwargs):
        completions.append(
            (
                kwargs["completion_ref"],
                kwargs["delivery_group_id"],
                kwargs["delivery_ids"],
            )
        )
        return {"acknowledged": True}

    register_semantic_retry_completion_handler(
        "planning-preview-ack/1",
        complete,
    )
    try:
        _stage(
            ledger,
            adapter,
            delivery_id="ordered-u0",
            message="first exact unit",
            group="ordered-group",
            unit_index=0,
            unit_count=2,
            completion_contract="planning-preview-ack/1",
            completion_ref="bridge_123",
        )
        staged = semantic_delivery_retry_status(
            delivery_id="ordered-u0",
            ledger_path=ledger,
        )
        assert staged["group_state"] == "staging"

        # A partial group is inert: no time offset can accidentally release u0.
        empty = asyncio.run(
            dispatch_due_semantic_delivery_retries(
                bound_adapters={"primary": {PROVIDER: adapter}},
                ledger_path=ledger,
                due_before=10**20,
            )
        )
        assert empty["claimed"] == 0
        assert adapter.calls == []

        _stage(
            ledger,
            adapter,
            delivery_id="ordered-u1",
            message="second exact unit",
            group="ordered-group",
            unit_index=1,
            unit_count=2,
            completion_contract="planning-preview-ack/1",
            completion_ref="bridge_123",
        )

        first_pump = asyncio.run(
            dispatch_due_semantic_delivery_retries(
                bound_adapters={
                    "profiles": {"work": {PROVIDER: adapter}}
                },
                ledger_path=ledger,
            )
        )
        assert first_pump["retryable"] == 1
        assert [call.delivery_unit for call in adapter.calls] == [0]
        assert completions == []

        second_pump = asyncio.run(
            dispatch_due_semantic_delivery_retries(
                bound_adapters={
                    "profiles": {"work": {PROVIDER: adapter}}
                },
                ledger_path=ledger,
                due_before=10**20,
            )
        )
        assert second_pump["delivered"] == 2
        assert second_pump["completion_completed"] == 1
        assert [call.delivery_unit for call in adapter.calls] == [0, 0, 1]
        assert completions == [
            (
                "bridge_123",
                "ordered-group",
                ("ordered-u0", "ordered-u1"),
            )
        ]
        first_status = semantic_delivery_retry_status(
            delivery_id="ordered-u0",
            ledger_path=ledger,
        )
        second_status = semantic_delivery_retry_status(
            delivery_id="ordered-u1",
            ledger_path=ledger,
        )
        assert first_status["state"] == "retired"
        assert first_status["attempt_count"] == 1
        assert second_status["state"] == "retired"
        assert second_status["group_state"] == "completed"
        replayed_stage = _stage(
            ledger,
            adapter,
            delivery_id="ordered-u0",
            message="first exact unit",
            group="ordered-group",
            unit_index=0,
            unit_count=2,
            completion_contract="planning-preview-ack/1",
            completion_ref="bridge_123",
        )
        assert replayed_stage["outcome"] == "retired"
        payload_dir = ledger.parent / "retry-payloads"
        assert list(payload_dir.glob("*.payload")) == []
        ledger_bytes = ledger.read_bytes()
        assert b"first exact unit" not in ledger_bytes
        assert b"second exact unit" not in ledger_bytes
        assert stat.S_IMODE(payload_dir.stat().st_mode) == 0o700
    finally:
        unregister_semantic_retry_completion_handler(
            "planning-preview-ack/1",
            handler=complete,
        )


def test_scheduler_persists_adapter_rejection_evidence_in_both_ledgers(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    adapter = ExactRetryAdapter(
        lambda _request, _call_number: _prewrite_429_with_evidence()
    )
    _stage(
        ledger,
        adapter,
        delivery_id="evidence-u0",
        message="one exact unit",
        group="evidence-group",
        unit_index=0,
        unit_count=1,
    )

    counts = asyncio.run(
        dispatch_due_semantic_delivery_retries(
            bound_adapters={PROVIDER: adapter},
            ledger_path=ledger,
        )
    )
    retry_status = semantic_delivery_retry_status(
        delivery_id="evidence-u0",
        ledger_path=ledger,
    )
    delivery_status = semantic_delivery_status(
        delivery_id="evidence-u0",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        expected_scope_id=semantic_delivery_scope_id(
            ledger_path=ledger
        ),
        expected_provider=PROVIDER,
        gateway_account_id=ACCOUNT,
        ledger_path=ledger,
    )

    assert counts["retryable"] == 1
    assert len(adapter.calls) == 1
    outbox_evidence = json.loads(retry_status["last_error"])
    assert outbox_evidence["schema_version"] == (
        "hermes.semantic-delivery-failure/1"
    )
    assert outbox_evidence["provider_rejection"]["status"] == 429
    assert delivery_status["outcome"] == "retryable"
    assert delivery_status["provider_rejection"] == (
        outbox_evidence["provider_rejection"]
    )
    assert delivery_status["provider_error"] == "Retry Test HTTP 429"


def test_no_terminal_retry_cap_and_no_user_or_admin_gate(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    adapter = ExactRetryAdapter(
        lambda request, call_number: _success("event-final")
    )
    delivery_id = "unbounded-retry"
    message = "retry exactly until provider accepts"
    _stage(
        ledger,
        adapter,
        delivery_id=delivery_id,
        message=message,
        group=delivery_id,
        unit_index=0,
        unit_count=1,
    )

    scope = semantic_delivery_scope_id(ledger_path=ledger)
    for _ in range(40):
        attempt = begin_semantic_delivery(
            delivery_id=delivery_id,
            contract_version=SEMANTIC_DELIVERY_CONTRACT,
            target=TARGET,
            message=message,
            gateway_account_id=ACCOUNT,
            expected_scope_id=scope,
            ledger_path=ledger,
        )
        assert attempt.action == "send"
        result = finish_semantic_delivery(
            attempt,
            {
                "provider_write_attempted": False,
                "provider_retryable": True,
                "retry_after": 0,
            },
        )
        assert result["outcome"] == "retryable"

    pending = semantic_delivery_retry_status(
        delivery_id=delivery_id,
        ledger_path=ledger,
    )
    assert pending["attempt_count"] == 40
    assert pending["state"] == "pending"
    assert pending["retry_limit"] is None
    assert pending["retry_automatic"] is True

    settled = asyncio.run(
        dispatch_due_semantic_delivery_retries(
            bound_adapters={PROVIDER: adapter},
            ledger_path=ledger,
            due_before=10**20,
        )
    )
    assert settled["delivered"] == 1
    assert len(adapter.calls) == 1


def test_untyped_transport_failure_becomes_ambiguous_without_resend(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    adapter = ExactRetryAdapter(
        lambda request, call_number: RuntimeError("socket vanished")
    )
    _stage(
        ledger,
        adapter,
        delivery_id="ambiguous-transport",
        message="could have reached provider",
        group="ambiguous-transport",
        unit_index=0,
        unit_count=1,
    )

    first = asyncio.run(
        dispatch_due_semantic_delivery_retries(
            bound_adapters={PROVIDER: adapter},
            ledger_path=ledger,
            due_before=10**20,
        )
    )
    second = asyncio.run(
        dispatch_due_semantic_delivery_retries(
            bound_adapters={PROVIDER: adapter},
            ledger_path=ledger,
            due_before=10**20,
        )
    )
    status = semantic_delivery_retry_status(
        delivery_id="ambiguous-transport",
        ledger_path=ledger,
    )

    assert first["ambiguous"] == 1
    assert second["claimed"] == 0
    assert len(adapter.calls) == 1
    assert status["state"] == "retired"
    assert status["group_state"] == "blocked"


def test_stage_rejects_wrong_account_and_lying_adapter(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    adapter = ExactRetryAdapter(
        lambda request, call_number: _success("unused")
    )
    adapter.config.extra["gateway_account_id"] = "different-account"
    with pytest.raises(
        ValueError,
        match="live provider capability required",
    ):
        _stage(
            ledger,
            adapter,
            delivery_id="wrong-account",
            message="must never stage",
            group="wrong-account",
            unit_index=0,
            unit_count=1,
        )

    class LyingAdapter:
        platform = PROVIDER
        config = SimpleNamespace(
            extra={"gateway_account_id": ACCOUNT}
        )

        async def send(self, *args, **kwargs):
            return _success("fake")

    with pytest.raises(
        ValueError,
        match="live provider capability required",
    ):
        _stage(
            ledger,
            LyingAdapter(),
            delivery_id="lying-adapter",
            message="must never stage",
            group="lying-adapter",
            unit_index=0,
            unit_count=1,
        )
    with sqlite3.connect(ledger) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM semantic_delivery_retry_outbox"
        ).fetchone()[0]
    assert count == 0


def test_deploy_cannot_silently_reencode_staged_delivery(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    v1_adapter = ExactRetryAdapter(
        lambda request, call_number: _success("unused-v1")
    )
    _stage(
        ledger,
        v1_adapter,
        delivery_id="frozen-encoder",
        message="raw canonical content",
        group="frozen-encoder",
        unit_index=0,
        unit_count=1,
    )
    v1_contract = live_semantic_exact_attempt_encoding_contract(v1_adapter)
    assert v1_contract is not None
    v2_capability = LiveSemanticExactAttemptCapability(
        provider=PROVIDER,
        contract="hermes-live-semantic-exact-attempt/1",
        segmentation_version="retry-test-logical-v2",
        max_logical_units=800,
        length_semantics="unicode_codepoints",
        wire_encoding="retry-test-wire-v2",
    )
    v2_contract = semantic_exact_attempt_encoding_contract(v2_capability)

    class V2OnlyAdapter:
        platform = PROVIDER
        SEMANTIC_EXACT_ATTEMPT_CAPABILITY = v2_capability

        def __init__(self):
            self.config = SimpleNamespace(
                extra={"gateway_account_id": ACCOUNT}
            )
            self.calls = []

        async def send(self, *args, **kwargs):
            raise AssertionError("generic send must remain unreachable")

        async def send_semantic_exact_attempt(self, request):
            self.calls.append(request)
            return _success("must-not-send")

    v2_only = V2OnlyAdapter()
    deferred = asyncio.run(
        dispatch_due_semantic_delivery_retries(
            bound_adapters={PROVIDER: v2_only},
            ledger_path=ledger,
        )
    )
    assert deferred["deferred"] == 1
    assert v2_only.calls == []
    pending = semantic_delivery_retry_status(
        delivery_id="frozen-encoder",
        ledger_path=ledger,
    )
    assert pending["state"] == "pending"
    assert pending["last_error"] == (
        "semantic_retry_exact_adapter_unavailable"
    )

    class BackwardCompatibleAdapter:
        platform = PROVIDER
        SEMANTIC_EXACT_ATTEMPT_CAPABILITY = v2_capability
        SEMANTIC_EXACT_ATTEMPT_ENCODING_CONTRACTS = (
            v1_contract,
            v2_contract,
        )

        def __init__(self):
            self.config = SimpleNamespace(
                extra={"gateway_account_id": ACCOUNT}
            )
            self.calls = []

        async def send(self, *args, **kwargs):
            raise AssertionError("generic send must remain unreachable")

        async def send_semantic_exact_attempt(self, request):
            self.calls.append(request)
            return _success("sent-with-v1")

    compatible = BackwardCompatibleAdapter()
    with pytest.raises(
        ValueError,
        match="semantic retry outbox identity conflict",
    ):
        _stage(
            ledger,
            compatible,
            delivery_id="frozen-encoder",
            message="raw canonical content",
            group="frozen-encoder",
            unit_index=0,
            unit_count=1,
        )
    settled = asyncio.run(
        dispatch_due_semantic_delivery_retries(
            bound_adapters={PROVIDER: compatible},
            ledger_path=ledger,
            due_before=10**20,
        )
    )
    assert settled["delivered"] == 1
    assert len(compatible.calls) == 1
    assert compatible.calls[0].encoding_contract == v1_contract


def test_turn_execution_commit_barrier_survives_fresh_process(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "ledger.sqlite3"
    adapter = ExactRetryAdapter(
        lambda request, call_number: _success("unused")
    )
    turn_ref = "qturn_planning_preview_123"
    first = _stage(
        ledger,
        adapter,
        delivery_id="turn-barrier-u0",
        message="first committed unit",
        group="turn-barrier",
        unit_index=0,
        unit_count=2,
        turn_execution_ref=turn_ref,
    )
    assert first["group_state"] == "staging"
    assert first["group_fully_staged"] is False
    assert (
        semantic_retry_turn_execution_committed(
            turn_ref,
            ledger_path=ledger,
        )
        is False
    )

    second = _stage(
        ledger,
        adapter,
        delivery_id="turn-barrier-u1",
        message="second committed unit",
        group="turn-barrier",
        unit_index=1,
        unit_count=2,
        turn_execution_ref=turn_ref,
    )
    assert second["group_state"] == "pending"
    assert second["group_fully_staged"] is True
    assert semantic_retry_turn_execution_committed(
        turn_ref,
        ledger_path=ledger,
    )

    code = (
        "from pathlib import Path\n"
        "from hermes_cli.semantic_delivery import "
        "semantic_retry_turn_execution_committed\n"
        "import sys\n"
        "print('1' if semantic_retry_turn_execution_committed("
        "sys.argv[2], ledger_path=Path(sys.argv[1])) else '0')\n"
    )
    child = subprocess.run(
        [sys.executable, "-c", code, str(ledger), turn_ref],
        cwd=Path(__file__).resolve().parents[2],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert child.stdout.strip() == "1"

    with pytest.raises(
        ValueError,
        match="turn execution identity conflict",
    ):
        _stage(
            ledger,
            adapter,
            delivery_id="other-group-u0",
            message="must not alias the turn",
            group="other-group",
            unit_index=0,
            unit_count=1,
            turn_execution_ref=turn_ref,
        )
