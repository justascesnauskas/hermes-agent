"""Release gates for provider-confirmed Planning preview review receipts."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from hermes_cli import planning_preview_ack_outbox, planning_preview_delivery
from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    SendResult,
)
from gateway.platforms.signal import SignalAdapter
from gateway.semantic_exact_attempt import (
    LiveSemanticExactAttemptCapability,
    owns_live_semantic_exact_attempt,
    provider_rejection_evidence,
    semantic_exact_attempt_encoding_contract,
    semantic_exact_attempt_via_send,
)
from gateway.session import SessionSource, build_session_key
from hermes_cli.planning_preview_delivery import (
    ProviderDeliveryReceipt,
    bind_preview_delivery_generation,
    canonical_preview_delivery_target,
    claim_preview_delivery,
    claim_preview_delivery_for_source,
    complete_preview_delivery,
    deliver_preview_for_source,
    derive_preview_delivery_nonce,
    discard_preview_delivery_intent,
    has_preview_delivery_intent,
    prepare_preview_delivery_content,
    preview_delivery_target_for_source,
    record_preview_delivery_result,
    register_preview_delivery_intent,
    reset_preview_delivery_generation,
)
from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig
from hermes_cli.semantic_delivery import (
    SEMANTIC_DELIVERY_CONTRACT,
    delivery_ledger_path,
    semantic_delivery_scope_id,
    semantic_retry_turn_execution_committed,
)


@pytest.fixture(autouse=True)
def _private_semantic_home(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    monkeypatch.setattr(
        "gateway.platform_registry.supports_live_semantic_exact_attempt",
        lambda provider, adapter=None: (
            provider in {"matrix", "signal", "telegram"}
            and owns_live_semantic_exact_attempt(adapter)
        ),
    )


def _canonical(value) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _payload() -> tuple[dict, str, str, str]:
    payload = {
        "schemaVersion": "planning.preview-delivery-payload.v1",
        "threadId": "thread-1",
        "previewResultId": "preview-1",
        "offset": 0,
        "count": 1,
        "tasks": [{"stableTaskId": "task-1", "summary": "Fix recovery"}],
    }
    digest = "sha256:" + hashlib.sha256(
        _canonical(payload).encode()
    ).hexdigest()
    content = (
        "## Implementation plan\n\n"
        "### 1. Fix recovery\n\n"
        "Ensure the operation can recover without an administrator."
    )
    content_digest = "sha256:" + hashlib.sha256(
        content.encode()
    ).hexdigest()
    return payload, digest, content, content_digest


def _register(session_key: str, generation: int, calls: list) -> None:
    payload, digest, content, content_digest = _payload()
    token = bind_preview_delivery_generation(session_key, generation)
    try:
        assert register_preview_delivery_intent(
            thread_id="thread-1",
            preview_result_id="preview-1",
            preview_result_hash="sha256:" + "a" * 64,
            offset=0,
            count=1,
            page_digest="sha256:" + "b" * 64,
            delivery_payload=payload,
            delivery_payload_digest=digest,
            delivery_content=content,
            delivery_content_digest=content_digest,
            delivery_nonce="preview-delivery-nonce-stable-0001",
            acknowledge=lambda receipt: calls.append(receipt),
            allow_process_local_ack=True,
        )
    finally:
        reset_preview_delivery_generation(token)


def _register_pages(
    session_key: str,
    generation: int,
    pages: list[dict],
) -> None:
    token = bind_preview_delivery_generation(session_key, generation)
    try:
        for index, page in enumerate(pages):
            content = str(page["content"])
            payload = {
                "schemaVersion": "planning.preview-delivery-payload.v1",
                "threadId": page.get("thread_id", "thread-envelope"),
                "previewResultId": page.get(
                    "preview_result_id",
                    "preview-envelope",
                ),
                "offset": page["offset"],
                "count": page["count"],
                "tasks": [{"stableTaskId": f"task-{index}"}],
            }
            payload_digest = (
                "sha256:"
                + hashlib.sha256(
                    _canonical(payload).encode("utf-8")
                ).hexdigest()
            )
            assert register_preview_delivery_intent(
                thread_id=payload["threadId"],
                preview_result_id=payload["previewResultId"],
                preview_result_hash=page.get(
                    "preview_result_hash",
                    "sha256:" + "e" * 64,
                ),
                offset=page["offset"],
                count=page["count"],
                page_digest=(
                    "sha256:"
                    + hashlib.sha256(
                        f"page-{index}".encode("utf-8")
                    ).hexdigest()
                ),
                delivery_payload=payload,
                delivery_payload_digest=payload_digest,
                delivery_content=content,
                delivery_content_digest=(
                    "sha256:"
                    + hashlib.sha256(content.encode("utf-8")).hexdigest()
                ),
                delivery_nonce=f"preview-envelope-page-{index}",
                acknowledge=lambda _receipt: None,
                allow_process_local_ack=True,
            )
    finally:
        reset_preview_delivery_generation(token)


def test_preview_registration_requires_durable_ack_contract_by_default() -> None:
    payload, digest, content, content_digest = _payload()
    session_key = "session-missing-durable-ack"
    generation = 31
    token = bind_preview_delivery_generation(session_key, generation)
    try:
        registered = register_preview_delivery_intent(
            thread_id="thread-1",
            preview_result_id="preview-1",
            preview_result_hash="sha256:" + "a" * 64,
            offset=0,
            count=1,
            page_digest="sha256:" + "b" * 64,
            delivery_payload=payload,
            delivery_payload_digest=digest,
            delivery_content=content,
            delivery_content_digest=content_digest,
            delivery_nonce="preview-delivery-missing-durable-ack",
            acknowledge=lambda _receipt: None,
        )
    finally:
        reset_preview_delivery_generation(token)

    assert registered is False
    assert not has_preview_delivery_intent(session_key, generation)


def test_preview_registration_rejects_ack_base_for_another_intent() -> None:
    payload, digest, content, content_digest = _payload()
    session_key = "session-mismatched-durable-ack"
    generation = 33
    token = bind_preview_delivery_generation(session_key, generation)
    try:
        registered = register_preview_delivery_intent(
            thread_id="thread-1",
            preview_result_id="preview-1",
            preview_result_hash="sha256:" + "a" * 64,
            offset=0,
            count=1,
            page_digest="sha256:" + "b" * 64,
            delivery_payload=payload,
            delivery_payload_digest=digest,
            delivery_content=content,
            delivery_content_digest=content_digest,
            delivery_nonce="preview-delivery-right-nonce",
            acknowledge=lambda _receipt: None,
            acknowledgement_request={
                "threadId": "thread-1",
                "previewResultId": "preview-1",
                "expectedPreviewHash": "sha256:" + "a" * 64,
                "offset": 0,
                "count": 1,
                "pageDigest": "sha256:" + "b" * 64,
                "deliveryProofBase": {
                    "deliveryNonce": "preview-delivery-wrong-nonce",
                    "previewResultId": "preview-1",
                    "previewResultHash": "sha256:" + "a" * 64,
                    "offset": 0,
                    "count": 1,
                    "pageDigest": "sha256:" + "b" * 64,
                },
            },
        )
    finally:
        reset_preview_delivery_generation(token)

    assert registered is False
    assert not has_preview_delivery_intent(session_key, generation)


def test_reordered_omitted_and_collapsed_pages_reject_before_ledger(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "hermes_cli.planning_preview_delivery.begin_semantic_delivery",
        lambda **_kwargs: pytest.fail(
            "invalid page order must fail before the durable ledger"
        ),
    )
    scenarios = (
        (
            "reordered",
            [
                {"offset": 0, "count": 1, "content": "First page"},
                {"offset": 1, "count": 1, "content": "Second page"},
            ],
            "Second page\n\n---\nFirst page",
        ),
        (
            "omitted",
            [
                {"offset": 0, "count": 1, "content": "First page"},
                {"offset": 1, "count": 1, "content": "Second page"},
            ],
            "First page",
        ),
        (
            "collapsed-duplicate",
            [
                {"offset": 0, "count": 1, "content": "Repeated page"},
                {"offset": 1, "count": 1, "content": "Repeated page"},
            ],
            "Repeated page",
        ),
    )
    for generation, (name, pages, delivered_content) in enumerate(
        scenarios,
        start=40,
    ):
        session_key = f"session-envelope-{name}"
        _register_pages(session_key, generation, pages)
        claim = claim_preview_delivery(
            session_key,
            generation,
            delivered_content=delivered_content,
            provider="telegram",
            target="telegram:test-target",
            gateway_account_id="telegram-account",
        )
        assert claim.action == "rejected"
        discard_preview_delivery_intent(session_key, generation)


@pytest.mark.parametrize(
    "second_page",
    [
        {
            "offset": 2,
            "count": 1,
            "content": "Second page",
            "thread_id": "another-thread",
        },
        {
            "offset": 2,
            "count": 1,
            "content": "Second page",
            "preview_result_id": "another-preview",
        },
        {
            "offset": 2,
            "count": 1,
            "content": "Second page",
            "preview_result_hash": "sha256:" + "f" * 64,
        },
        {
            "offset": 1,
            "count": 2,
            "content": "Overlapping page",
        },
    ],
)
def test_mixed_or_overlapping_page_envelope_rejects_before_ledger(
    monkeypatch,
    second_page: dict,
) -> None:
    session_key = "session-invalid-envelope"
    generation = 43
    first_page = {
        "offset": 0,
        "count": 2,
        "content": "First page",
    }
    _register_pages(
        session_key,
        generation,
        [first_page, second_page],
    )
    monkeypatch.setattr(
        "hermes_cli.planning_preview_delivery.begin_semantic_delivery",
        lambda **_kwargs: pytest.fail(
            "invalid envelope must fail before the durable ledger"
        ),
    )

    claim = claim_preview_delivery(
        session_key,
        generation,
        delivered_content=(
            f"{first_page['content']}\n\n---\n"
            f"{second_page['content']}"
        ),
        provider="telegram",
        target="telegram:test-target",
        gateway_account_id="telegram-account",
    )

    assert claim.action == "rejected"
    discard_preview_delivery_intent(session_key, generation)


def _claim(session_key: str, generation: int, content: str):
    return claim_preview_delivery(
        session_key,
        generation,
        delivered_content=content,
        provider="telegram",
        target="telegram:test-target",
        gateway_account_id="telegram-account",
    )


class _Adapter(BasePlatformAdapter):
    SEMANTIC_EXACT_ATTEMPT_CAPABILITY = LiveSemanticExactAttemptCapability(
        provider="telegram",
        contract="hermes-live-semantic-exact-attempt/1",
        segmentation_version="telegram-test-logical-v1",
        max_logical_units=1800,
        length_semantics="utf16_code_units",
        wire_encoding="telegram-test-wire-v1",
    )

    def __init__(
        self,
        result: SendResult | list[SendResult],
    ) -> None:
        super().__init__(
            PlatformConfig(
                enabled=True,
                extra={"gateway_account_id": "telegram-account"},
            ),
            Platform.TELEGRAM,
        )
        self.results = (
            list(result) if isinstance(result, list) else [result]
        )
        self.sent: list[str] = []
        self.exact_requests = []

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        return True

    async def disconnect(self) -> None:
        return None

    async def get_chat_info(self, chat_id):
        return {"id": chat_id}

    async def send(
        self,
        chat_id,
        content,
        reply_to=None,
        metadata=None,
        **_kwargs,
    ) -> SendResult:
        self.sent.append(content)
        return self.results.pop(0)

    async def send_semantic_exact_attempt(self, request):
        self.exact_requests.append(request)
        return await semantic_exact_attempt_via_send(self, request)

    def bind_semantic_exact_attempt_provider_route(
        self,
        *,
        chat_id: str,
        thread_id: str | None = None,
        reply_to: str | None = None,
    ):
        del chat_id, thread_id, reply_to
        return {"transport": "telegram-test-v1"}

    async def send_typing(self, chat_id, metadata=None) -> None:
        return None

    async def stop_typing(self, chat_id, metadata=None) -> None:
        return None


def _exact_claim_adapter(
    platform,
    *,
    account: str,
    max_logical_units: int = 1000,
    segmentation_version: str = "test-logical-v1",
    historical=(),
):
    provider = str(getattr(platform, "value", platform))
    capability = LiveSemanticExactAttemptCapability(
        provider=provider,
        contract="hermes-live-semantic-exact-attempt/1",
        segmentation_version=segmentation_version,
        max_logical_units=max_logical_units,
        length_semantics=(
            "utf16_code_units"
            if provider == "telegram"
            else "unicode_codepoints"
        ),
        wire_encoding=f"{provider}-test-wire-v1",
    )

    async def send_semantic_exact_attempt(self, request):
        raise AssertionError("claim-only adapter must not send")

    attributes = {
        "platform": platform,
        "config": PlatformConfig(
            enabled=True,
            extra={"gateway_account_id": account},
        ),
        "SEMANTIC_EXACT_ATTEMPT_CAPABILITY": capability,
        "send_semantic_exact_attempt": send_semantic_exact_attempt,
    }
    if historical:
        attributes["SEMANTIC_EXACT_ATTEMPT_ENCODING_CONTRACTS"] = (
            semantic_exact_attempt_encoding_contract(capability),
            *tuple(historical),
        )
    return type("ExactClaimAdapter", (), attributes)()


def _event() -> MessageEvent:
    return MessageEvent(
        text="show preview",
        source=SessionSource(
            platform=Platform.TELEGRAM,
            chat_id="chat-1",
            chat_type="direct",
            gateway_account_id="telegram-account",
        ),
        message_id="incoming-1",
    )


async def _run_base_delivery(
    result: SendResult | list[SendResult],
    *,
    generation: int = 9,
    response_text: str = "Human-readable preview.",
) -> tuple[_Adapter, list[ProviderDeliveryReceipt]]:
    adapter = _Adapter(result)
    event = _event()
    session_key = build_session_key(event.source)
    calls: list[ProviderDeliveryReceipt] = []

    async def handler(_event):
        _register(session_key, generation, calls)
        return response_text

    adapter.set_message_handler(handler)
    active = asyncio.Event()
    setattr(active, "_hermes_run_generation", generation)
    adapter._active_sessions[session_key] = active
    await adapter._process_message_background(event, session_key)
    return adapter, calls


@pytest.mark.asyncio
async def test_401_tasks_cross_real_signal_adapter_as_ordered_units(
    monkeypatch,
) -> None:
    """The public no-cap journey must exercise the provider adapter, not a stub."""

    session_key = "signal:account:direct:+37060000000"
    generation = 401
    receipts: list[ProviderDeliveryReceipt] = []
    token = bind_preview_delivery_generation(session_key, generation)
    try:
        for offset, count in ((0, 200), (200, 200), (400, 1)):
            tasks = [
                {
                    "stableTaskId": f"task-{index + 1:03d}",
                    "summary": (
                        f"Implement recovery slice {index + 1:03d} with "
                        "observable acceptance evidence"
                    ),
                }
                for index in range(offset, offset + count)
            ]
            payload = {
                "schemaVersion": "planning.preview-delivery-payload.v1",
                "threadId": "thread-401",
                "previewResultId": "preview-401",
                "offset": offset,
                "count": count,
                "tasks": tasks,
            }
            payload_digest = "sha256:" + hashlib.sha256(
                _canonical(payload).encode("utf-8")
            ).hexdigest()
            content = "\n".join(
                f"{task['stableTaskId']} — {task['summary']}"
                for task in tasks
            )
            content_digest = "sha256:" + hashlib.sha256(
                content.encode("utf-8")
            ).hexdigest()
            assert register_preview_delivery_intent(
                thread_id="thread-401",
                preview_result_id="preview-401",
                preview_result_hash="sha256:" + "4" * 64,
                offset=offset,
                count=count,
                page_digest=(
                    "sha256:"
                    + hashlib.sha256(content.encode("utf-8")).hexdigest()
                ),
                delivery_payload=payload,
                delivery_payload_digest=payload_digest,
                delivery_content=content,
                delivery_content_digest=content_digest,
                delivery_nonce=f"preview-401-page-{offset}",
                acknowledge=lambda receipt: receipts.append(receipt),
                allow_process_local_ack=True,
            )
    finally:
        reset_preview_delivery_generation(token)

    adapter = SignalAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "account": "+37061111111",
                "http_url": "http://signal.invalid",
                "gateway_account_id": "signal-primary",
            },
        )
    )
    provider_writes: list[dict] = []
    delivery_events: list[str] = []

    class _SignalResponse:
        status_code = 200

        def __init__(self, timestamp: int) -> None:
            self._payload = {
                "jsonrpc": "2.0",
                "result": {"timestamp": timestamp},
            }
            self.text = json.dumps(self._payload)

        def json(self) -> dict:
            return dict(self._payload)

    class _SignalClient:
        async def post(self, _url, *, json, timeout):
            assert timeout == 30.0
            assert json["method"] == "send"
            provider_writes.append(dict(json["params"]))
            delivery_events.append(f"provider-{len(provider_writes)}")
            return _SignalResponse(
                1_800_000_000_000 + len(provider_writes)
            )

    adapter.client = _SignalClient()
    monkeypatch.setattr(
        "hermes_cli.planning_preview_delivery."
        "_provider_completion_timestamp",
        lambda: (
            delivery_events.append("completion")
            or "2026-07-28T12:05:00+00:00"
        ),
    )
    source = SessionSource(
        platform=Platform.SIGNAL,
        chat_id="+37060000000",
        chat_type="direct",
        gateway_account_id="signal-primary",
    )
    canonical = prepare_preview_delivery_content(
        session_key,
        generation,
        "This model wrapper is deliberately ignored.",
    )
    turn_execution_ref = "qturn_" + "7" * 64

    async def commit_turn_execution() -> None:
        assert semantic_retry_turn_execution_committed(
            turn_execution_ref
        )
        delivery_events.append("turn-execution-committed")

    flow = await deliver_preview_for_source(
        session_key,
        generation,
        delivered_content=canonical,
        source=source,
        adapter=adapter,
        metadata={"notify": True},
        delivered_at="2026-07-28T12:00:00+00:00",
        turn_execution_ref=turn_execution_ref,
        turn_execution_committed=commit_turn_execution,
    )

    assert flow.action == "delivered"
    assert flow.acknowledged is True
    assert flow.execution_committed is True
    assert len(provider_writes) > 3
    assert all(
        0 < len(write["message"]) <= 8_000
        for write in provider_writes
    )
    delivered_text = "".join(
        str(write["message"]) for write in provider_writes
    )
    assert delivered_text.count("task-") == 401
    assert "task-001" in delivered_text
    assert "task-401" in delivered_text
    assert len(receipts) == 3
    expected_ids = tuple(
        str(1_800_000_000_000 + index)
        for index in range(1, len(provider_writes) + 1)
    )
    assert all(
        receipt.provider_message_ids == expected_ids
        for receipt in receipts
    )
    assert delivery_events[-1] == "completion"
    assert delivery_events[0] == "turn-execution-committed"
    assert all(
        receipt.delivered_at == "2026-07-28T12:05:00+00:00"
        for receipt in receipts
    )


@pytest.mark.asyncio
async def test_turn_commit_failure_parks_before_provider_without_losing_outbox(
) -> None:
    session_key = "telegram:account:direct:commit-failure"
    generation = 402
    receipts: list[ProviderDeliveryReceipt] = []
    _register(session_key, generation, receipts)
    adapter = _Adapter(
        SendResult(success=True, message_id="must-not-send")
    )
    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="commit-failure",
        chat_type="direct",
        gateway_account_id="telegram-account",
    )
    content = prepare_preview_delivery_content(
        session_key,
        generation,
        "ignored wrapper",
    )
    turn_execution_ref = "qturn_" + "8" * 64

    async def fail_commit() -> None:
        raise OSError("queue ledger temporarily unavailable")

    flow = await deliver_preview_for_source(
        session_key,
        generation,
        delivered_content=content,
        source=source,
        adapter=adapter,
        delivered_at="2026-07-28T12:00:00+00:00",
        turn_execution_ref=turn_execution_ref,
        turn_execution_committed=fail_commit,
    )

    assert flow.action == "turn_execution_commit_failed"
    assert flow.execution_committed is False
    assert adapter.sent == []
    assert receipts == []
    assert has_preview_delivery_intent(session_key, generation)
    assert semantic_retry_turn_execution_committed(turn_execution_ref)
    discard_preview_delivery_intent(session_key, generation)


@pytest.mark.asyncio
async def test_base_commits_turn_execution_before_its_first_provider_write(
) -> None:
    """Exercise the actual Base response path, not only the delivery helper."""

    generation = 403
    adapter = _Adapter(
        SendResult(success=True, message_id="base-exact-message")
    )
    event = _event()
    session_key = build_session_key(event.source)
    turn_execution_ref = "qturn_" + "9" * 64
    delivery_events: list[str] = []
    handler_calls = 0

    async def handler(_event):
        nonlocal handler_calls
        handler_calls += 1
        _register(session_key, generation, [])
        return "The signed preview follows."

    async def commit_turn_execution() -> None:
        assert semantic_retry_turn_execution_committed(
            turn_execution_ref
        )
        delivery_events.append("turn-execution-committed")

    original_send = adapter.send

    async def observed_send(*args, **kwargs):
        delivery_events.append("provider-write")
        return await original_send(*args, **kwargs)

    adapter.send = observed_send
    event._hermes_turn_execution_ref = turn_execution_ref
    event._hermes_commit_turn_execution = commit_turn_execution
    adapter.set_message_handler(handler)
    active = asyncio.Event()
    setattr(active, "_hermes_run_generation", generation)
    adapter._active_sessions[session_key] = active

    await adapter._process_message_background(event, session_key)

    assert handler_calls == 1
    assert delivery_events == [
        "turn-execution-committed",
        "provider-write",
    ]
    assert len(adapter.sent) == 1
    assert adapter.exact_requests[0].provider_route == (
        ("transport", "telegram-test-v1"),
    )


@pytest.mark.asyncio
async def test_base_commit_failure_sends_no_preview_or_generic_recovery(
) -> None:
    """A queue-retirement failure parks the owned outbox without a second send."""

    generation = 404
    adapter = _Adapter(
        SendResult(success=True, message_id="must-remain-unused")
    )
    event = _event()
    session_key = build_session_key(event.source)
    turn_execution_ref = "qturn_" + "b" * 64
    handler_calls = 0

    async def handler(_event):
        nonlocal handler_calls
        handler_calls += 1
        _register(session_key, generation, [])
        return "The signed preview follows."

    async def fail_commit() -> None:
        raise OSError("durable ingress retirement unavailable")

    event._hermes_turn_execution_ref = turn_execution_ref
    event._hermes_commit_turn_execution = fail_commit
    adapter.set_message_handler(handler)
    active = asyncio.Event()
    setattr(active, "_hermes_run_generation", generation)
    adapter._active_sessions[session_key] = active

    await adapter._process_message_background(event, session_key)

    assert handler_calls == 1
    assert adapter.sent == []
    assert len(adapter.results) == 1
    assert has_preview_delivery_intent(session_key, generation)
    assert semantic_retry_turn_execution_committed(turn_execution_ref)
    discard_preview_delivery_intent(session_key, generation)


@pytest.mark.asyncio
async def test_base_failed_send_creates_zero_review_receipts() -> None:
    adapter, calls = await _run_base_delivery(
        SendResult(
            success=False,
            error="ReadTimeout: delivery outcome unknown",
        )
    )

    assert calls == []
    assert len(adapter.sent) == 1
    assert _payload()[2] in adapter.sent[0]
    assert '"threadId"' not in adapter.sent[0]


@pytest.mark.asyncio
async def test_truncated_formatting_fallback_creates_zero_receipts() -> None:
    adapter, calls = await _run_base_delivery(
        [
            SendResult(success=False, error="Bad Request: bad_format"),
            SendResult(success=True, message_id="fallback-message"),
        ],
        response_text="x" * 4000,
    )

    assert calls == []
    # Semantic exact-attempt mode never invokes the adapter's formatting
    # fallback: after one possibly-written attempt the outcome is ambiguous.
    assert len(adapter.sent) == 1
    assert _payload()[2] in adapter.sent[0]


@pytest.mark.asyncio
async def test_success_without_provider_message_id_is_ambiguous() -> None:
    _adapter, calls = await _run_base_delivery(
        SendResult(success=True)
    )

    assert calls == []


@pytest.mark.asyncio
async def test_unproven_direct_success_creates_zero_review_receipts() -> None:
    calls: list[ProviderDeliveryReceipt] = []
    _register("session-unproven", 3, calls)
    content = prepare_preview_delivery_content(
        "session-unproven",
        3,
        "Preview.",
    )

    assert not await complete_preview_delivery(
        "session-unproven",
        3,
        delivered_content=content,
        result=SendResult(success=True, message_id="message-unproven"),
        delivered_at="2026-07-27T12:30:00+00:00",
    )
    assert calls == []


@pytest.mark.asyncio
async def test_multipart_receipt_preserves_complete_send_order() -> None:
    _adapter, calls = await _run_base_delivery(
        SendResult(
            success=True,
            message_id="chunk-3",
            continuation_message_ids=("chunk-1", "chunk-2", "chunk-3"),
        )
    )

    assert calls[0].provider_message_ids == (
        "chunk-1",
        "chunk-2",
        "chunk-3",
    )


@pytest.mark.asyncio
async def test_lost_ack_replays_byte_identical_receipt(
    monkeypatch,
) -> None:
    calls: list[ProviderDeliveryReceipt] = []
    attempts: list[ProviderDeliveryReceipt] = []
    payload, digest, content, content_digest = _payload()
    token = bind_preview_delivery_generation("session-replay", 4)
    try:
        def acknowledge(receipt: ProviderDeliveryReceipt) -> None:
            attempts.append(receipt)
            if len(attempts) == 1:
                raise TimeoutError("Hub response lost after commit")
            calls.append(receipt)

        assert register_preview_delivery_intent(
            thread_id="thread-1",
            preview_result_id="preview-1",
            preview_result_hash="sha256:" + "a" * 64,
            offset=0,
            count=1,
            page_digest="sha256:" + "b" * 64,
            delivery_payload=payload,
            delivery_payload_digest=digest,
            delivery_content=content,
            delivery_content_digest=content_digest,
            delivery_nonce="preview-delivery-nonce-replay-0001",
            acknowledge=acknowledge,
            acknowledgement_request={
                "schemaVersion": "planning.preview-ack-request.v1",
                "threadId": "thread-1",
                "previewResultId": "preview-1",
                "idempotencyKey": "preview-review-replay-key",
                "expectedPreviewHash": "sha256:" + "a" * 64,
                "offset": 0,
                "count": 1,
                "pageDigest": "sha256:" + "b" * 64,
                "origin": {
                    "schemaVersion": "1.0",
                    "provider": "telegram",
                    "gatewayInstanceId": "runner-1",
                    "gatewayAccountId": "telegram-account",
                    "chatId": "test-target",
                    "threadId": None,
                    "messageId": "message-inbound",
                    "senderId": "user-1",
                    "chatType": "direct",
                    "sourceTimestamp": "2026-07-27T12:29:00Z",
                    "providerEventId": "event-inbound",
                },
                "deliveryProofBase": {
                    "schemaVersion": (
                        "planning.preview-delivery-proof.v1"
                    ),
                    "deliveryNonce": (
                        "preview-delivery-nonce-replay-0001"
                    ),
                    "provider": "telegram",
                    "gatewayInstanceId": "runner-1",
                    "gatewayAccountId": "telegram-account",
                    "chatId": "test-target",
                    "previewResultId": "preview-1",
                    "previewResultHash": "sha256:" + "a" * 64,
                    "offset": 0,
                    "count": 1,
                    "pageDigest": "sha256:" + "b" * 64,
                },
            },
        )
    finally:
        reset_preview_delivery_generation(token)
    content = prepare_preview_delivery_content(
        "session-replay",
        4,
        "Preview.",
    )
    assert _claim("session-replay", 4, content).action == "send"

    clock = [1_000.0]
    monkeypatch.setattr(
        planning_preview_ack_outbox.time,
        "time",
        lambda: clock[0],
    )
    assert await complete_preview_delivery(
        "session-replay",
        4,
        delivered_content=content,
        result=SendResult(
            success=True,
            message_id="message-1",
            delivered_content_digest=(
                "sha256:" + hashlib.sha256(content.encode()).hexdigest()
            ),
            delivered_content_complete=True,
        ),
        delivered_at="2026-07-27T12:30:00+00:00",
    )
    assert len(attempts) == 1
    clock[0] = 1_003.0

    def recover(request) -> None:
        proof = request["deliveryProof"]
        acknowledge(
            ProviderDeliveryReceipt(
                provider_message_ids=tuple(
                    proof["providerMessageIds"]
                ),
                delivered_at=proof["deliveredAt"],
                delivery_nonce=proof["deliveryNonce"],
                delivery_payload_digest=proof[
                    "deliveryPayloadDigest"
                ],
                delivery_content_digest=proof[
                    "deliveryContentDigest"
                ],
            )
        )

    recovered = planning_preview_ack_outbox.dispatch_one_preview_ack(
        deliver=recover,
    )
    assert recovered is not None
    assert recovered["state"] == "succeeded"
    assert attempts[0] == attempts[1] == calls[0]


@pytest.mark.asyncio
async def test_stale_generation_and_restart_fail_closed() -> None:
    calls: list[ProviderDeliveryReceipt] = []
    _register("session-stale", 1, calls)
    content = prepare_preview_delivery_content(
        "session-stale",
        1,
        "Preview.",
    )

    assert not await complete_preview_delivery(
        "session-stale",
        2,
        delivered_content=content,
        result=SendResult(success=True, message_id="message-stale"),
        delivered_at="2026-07-27T12:30:00+00:00",
    )
    assert calls == []

    discard_preview_delivery_intent("session-stale", 1)
    assert not await complete_preview_delivery(
        "session-stale",
        1,
        delivered_content=content,
        result=SendResult(success=True, message_id="message-after-restart"),
        delivered_at="2026-07-27T12:30:00+00:00",
    )
    assert calls == []


@pytest.mark.asyncio
async def test_three_pages_are_losslessly_delivered_and_acknowledged() -> None:
    calls: list[tuple[int, ProviderDeliveryReceipt]] = []
    token = bind_preview_delivery_generation("session-pages", 12)
    try:
        for offset, count in ((0, 50), (50, 50), (100, 37)):
            payload = {
                "schemaVersion": (
                    "planning.preview-delivery-payload.v1"
                ),
                "threadId": "thread-137",
                "previewResultId": "preview-137",
                "offset": offset,
                "count": count,
                "tasks": [
                    {"stableTaskId": f"task-{index:03d}"}
                    for index in range(offset, offset + count)
                ],
            }
            digest = "sha256:" + hashlib.sha256(
                _canonical(payload).encode()
            ).hexdigest()
            content = (
                f"## Implementation plan\n\n"
                f"### Tasks {offset + 1}–{offset + count}\n\n"
                + "\n".join(
                    f"- Task {index + 1}"
                    for index in range(offset, offset + count)
                )
            )
            content_digest = "sha256:" + hashlib.sha256(
                content.encode()
            ).hexdigest()
            assert register_preview_delivery_intent(
                thread_id="thread-137",
                preview_result_id="preview-137",
                preview_result_hash="sha256:" + "c" * 64,
                offset=offset,
                count=count,
                page_digest=(
                    "sha256:"
                    + hashlib.sha256(str(offset).encode()).hexdigest()
                ),
                delivery_payload=payload,
                delivery_payload_digest=digest,
                delivery_content=content,
                delivery_content_digest=content_digest,
                delivery_nonce=f"preview-page-{offset:03d}-nonce-stable",
                acknowledge=lambda receipt, page_offset=offset: (
                    calls.append((page_offset, receipt))
                ),
                allow_process_local_ack=True,
            )
    finally:
        reset_preview_delivery_generation(token)

    outbound = prepare_preview_delivery_content(
        "session-pages",
        12,
        "All requested preview pages follow.",
    )
    for marker in ("Tasks 1–50", "Tasks 51–100", "Tasks 101–137"):
        assert marker in outbound
    assert '"threadId"' not in outbound
    assert _claim("session-pages", 12, outbound).action == "send"

    assert await complete_preview_delivery(
        "session-pages",
        12,
        delivered_content=outbound,
        result=SendResult(
            success=True,
            message_id="all-pages-message",
            delivered_content_digest=(
                "sha256:" + hashlib.sha256(outbound.encode()).hexdigest()
            ),
            delivered_content_complete=True,
        ),
        delivered_at="2026-07-27T12:30:00+00:00",
    )
    assert [offset for offset, _receipt in calls] == [0, 50, 100]
    assert len({receipt.delivery_nonce for _, receipt in calls}) == 3


def _origin(**updates) -> dict:
    origin = {
        "schemaVersion": "1.0",
        "provider": "matrix",
        "gatewayInstanceId": "runner-1",
        "gatewayAccountId": "account-1",
        "chatId": "!room:example.org",
        "threadId": "$thread:event",
        "messageId": "$message:event",
        "senderId": "@owner:example.org",
        "chatType": "thread",
        "sourceTimestamp": "2026-07-28T08:00:00+00:00",
        "providerEventId": "$event:example.org",
    }
    origin.update(updates)
    return origin


def _nonce(origin: dict) -> str:
    return derive_preview_delivery_nonce(
        planning_thread_id="planning-thread-1",
        preview_result_id="preview-1",
        preview_result_hash="sha256:" + "a" * 64,
        offset=0,
        count=1,
        page_digest="sha256:" + "b" * 64,
        origin=origin,
    )


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("schemaVersion", "1.1"),
        ("provider", "discord"),
        ("gatewayInstanceId", "runner-2"),
        ("gatewayAccountId", "account-2"),
        ("chatId", "!other:example.org"),
        ("threadId", "$other-thread:event"),
        ("messageId", "$other-message:event"),
        ("senderId", "@other:example.org"),
        ("chatType", "channel"),
        ("sourceTimestamp", "2026-07-28T08:00:01+00:00"),
        ("providerEventId", "$other-event:example.org"),
    ],
)
def test_preview_nonce_binds_every_turn_origin_dimension(
    field: str,
    replacement: str,
) -> None:
    baseline = _origin()
    assert _nonce(baseline) == _nonce(dict(baseline))
    assert _nonce(baseline) != _nonce(
        _origin(**{field: replacement})
    )


def test_canonical_target_has_no_matrix_colon_decomposition_collision() -> None:
    room_with_colon = canonical_preview_delivery_target(
        provider="matrix",
        gateway_account_id="account-1",
        chat_id="!room:example.org:$thread",
        thread_id=None,
    )
    room_and_thread = canonical_preview_delivery_target(
        provider="matrix",
        gateway_account_id="account-1",
        chat_id="!room:example.org",
        thread_id="$thread",
    )

    assert room_with_colon.startswith("matrix:preview-target-v1:")
    assert room_with_colon != room_and_thread


def test_shared_normal_and_queued_claim_freezes_one_target_identity() -> None:
    session_key = "session-shared-paths"
    generation = 20
    calls: list[ProviderDeliveryReceipt] = []
    source = SessionSource(
        platform=Platform.MATRIX,
        chat_id="!room:example.org",
        thread_id="$thread:event",
        gateway_account_id="account-1",
    )
    normal_adapter = _exact_claim_adapter(
        Platform.MATRIX,
        account="account-1",
    )
    _register(session_key, generation, calls)
    content = prepare_preview_delivery_content(
        session_key,
        generation,
        "Preview.",
    )
    normal_claim = claim_preview_delivery_for_source(
        session_key,
        generation,
        delivered_content=content,
        source=source,
        adapter=normal_adapter,
    )
    assert normal_claim.action == "send"
    discard_preview_delivery_intent(session_key, generation)

    # Reconstruct the queued path after reconnect: the source no longer
    # carries account identity, so the adapter config supplies it.
    queued_source = SessionSource(
        platform=Platform.MATRIX,
        chat_id="!room:example.org",
        thread_id="$thread:event",
    )
    queued_adapter = _exact_claim_adapter(
        Platform.MATRIX,
        account="account-1",
    )
    _register(session_key, generation, calls)
    queued_claim = claim_preview_delivery_for_source(
        session_key,
        generation,
        delivered_content=content,
        source=queued_source,
        adapter=queued_adapter,
    )

    assert queued_claim.action == "send"
    assert queued_claim.delivery_id == normal_claim.delivery_id
    assert (
        queued_claim.metadata["semantic_delivery_target"]
        == normal_claim.metadata["semantic_delivery_target"]
    )
    discard_preview_delivery_intent(session_key, generation)


def test_chat_origin_preview_fails_closed_without_gateway_account() -> None:
    session_key = "session-missing-account"
    generation = 24
    calls: list[ProviderDeliveryReceipt] = []
    _register(session_key, generation, calls)
    content = prepare_preview_delivery_content(
        session_key,
        generation,
        "Preview.",
    )
    source = SessionSource(
        platform=Platform.SIGNAL,
        chat_id="+15557654321",
    )
    adapter = type(
        "Adapter",
        (),
        {
            "platform": Platform.SIGNAL,
            "config": PlatformConfig(enabled=True),
        },
    )()

    claim = claim_preview_delivery_for_source(
        session_key,
        generation,
        delivered_content=content,
        source=source,
        adapter=adapter,
    )

    assert claim.action == "rejected"
    discard_preview_delivery_intent(session_key, generation)


def test_chat_origin_preview_rejects_account_authority_mismatch(
    monkeypatch,
) -> None:
    session_key = "session-account-mismatch"
    generation = 27
    calls: list[ProviderDeliveryReceipt] = []
    _register(session_key, generation, calls)
    content = prepare_preview_delivery_content(
        session_key,
        generation,
        "Preview.",
    )
    source = SessionSource(
        platform=Platform.SIGNAL,
        chat_id="+15557654321",
        gateway_account_id="account-one",
    )
    adapter = type(
        "Adapter",
        (),
        {
            "platform": Platform.SIGNAL,
            "config": PlatformConfig(
                enabled=True,
                extra={"gateway_account_id": "account-two"},
            ),
        },
    )()
    monkeypatch.setattr(
        "hermes_cli.planning_preview_delivery.begin_semantic_delivery",
        lambda **_kwargs: pytest.fail(
            "account mismatch must fail before the ledger claim"
        ),
    )

    claim = claim_preview_delivery_for_source(
        session_key,
        generation,
        delivered_content=content,
        source=source,
        adapter=adapter,
    )

    assert claim.action == "rejected"
    discard_preview_delivery_intent(session_key, generation)


def test_chat_origin_preview_fails_closed_for_unknown_provider() -> None:
    session_key = "session-unknown-provider"
    generation = 25
    calls: list[ProviderDeliveryReceipt] = []
    _register(session_key, generation, calls)
    content = prepare_preview_delivery_content(
        session_key,
        generation,
        "Preview.",
    )
    source = SessionSource(
        platform="unknown-provider",
        chat_id="room-1",
        gateway_account_id="unknown-account",
    )
    adapter = type(
        "Adapter",
        (),
        {
            "platform": "unknown-provider",
            "config": PlatformConfig(enabled=True),
        },
    )()

    claim = claim_preview_delivery_for_source(
        session_key,
        generation,
        delivered_content=content,
        source=source,
        adapter=adapter,
    )

    assert claim.action == "rejected"
    discard_preview_delivery_intent(session_key, generation)


def test_preview_edge_identity_and_ledger_bind_gateway_account() -> None:
    session_key = "session-account-authority"
    generation = 26
    calls: list[ProviderDeliveryReceipt] = []
    _register(session_key, generation, calls)
    content = prepare_preview_delivery_content(
        session_key,
        generation,
        "Preview.",
    )
    first = claim_preview_delivery(
        session_key,
        generation,
        delivered_content=content,
        provider="telegram",
        target="telegram:same-target",
        gateway_account_id="account-one",
    )
    assert first.action == "send"
    discard_preview_delivery_intent(session_key, generation)

    _register(session_key, generation, calls)
    second = claim_preview_delivery(
        session_key,
        generation,
        delivered_content=content,
        provider="telegram",
        target="telegram:same-target",
        gateway_account_id="account-two",
    )

    assert second.action == "send"
    assert second.delivery_id != first.delivery_id
    from hermes_cli.semantic_delivery import delivery_ledger_path

    with sqlite3.connect(delivery_ledger_path()) as connection:
        rows = connection.execute(
            "SELECT delivery_id,gateway_account_id "
            "FROM semantic_deliveries "
            "WHERE delivery_id IN (?,?)",
            (first.delivery_id, second.delivery_id),
        ).fetchall()
    assert set(rows) == {
        (first.delivery_id, "account-one"),
        (second.delivery_id, "account-two"),
    }
    discard_preview_delivery_intent(session_key, generation)


def test_same_process_second_preview_claim_is_in_flight() -> None:
    calls: list[ProviderDeliveryReceipt] = []
    _register("session-double-claim", 21, calls)
    content = prepare_preview_delivery_content(
        "session-double-claim",
        21,
        "Preview.",
    )

    first = _claim("session-double-claim", 21, content)
    second = _claim("session-double-claim", 21, content)

    assert first.action == "send"
    assert second.action == "in_flight"
    discard_preview_delivery_intent("session-double-claim", 21)


def test_changed_wrapper_reuses_identity_and_never_reopens_send() -> None:
    calls: list[ProviderDeliveryReceipt] = []
    _register("session-wrapper", 22, calls)
    first_content = prepare_preview_delivery_content(
        "session-wrapper",
        22,
        "First generated introduction.",
    )
    first = _claim("session-wrapper", 22, first_content)
    assert first.action == "send"
    first_id = first.delivery_id

    # Simulate process loss after the provider attempt began but before an
    # exact receipt was committed. The non-native route must not be resent.
    discard_preview_delivery_intent("session-wrapper", 22)
    _register("session-wrapper", 22, calls)
    changed_content = prepare_preview_delivery_content(
        "session-wrapper",
        22,
        "Different regenerated introduction.",
    )
    second = _claim("session-wrapper", 22, changed_content)

    assert second.delivery_id == first_id
    assert second.action == "conflict"
    discard_preview_delivery_intent("session-wrapper", 22)


def test_changed_adapter_limit_reconstructs_the_same_unit_identity() -> None:
    session_key = "session-stable-unit-budget"
    generation = 32
    calls: list[ProviderDeliveryReceipt] = []
    payload, digest, _, _ = _payload()
    content = "\n".join(
        f"Task {index:03d}: preserve this canonical planning evidence."
        for index in range(90)
    )
    content_digest = (
        "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest()
    )

    def register() -> None:
        token = bind_preview_delivery_generation(session_key, generation)
        try:
            assert register_preview_delivery_intent(
                thread_id="thread-1",
                preview_result_id="preview-1",
                preview_result_hash="sha256:" + "a" * 64,
                offset=0,
                count=1,
                page_digest="sha256:" + "b" * 64,
                delivery_payload=payload,
                delivery_payload_digest=digest,
                delivery_content=content,
                delivery_content_digest=content_digest,
                delivery_nonce="preview-stable-unit-budget",
                acknowledge=lambda receipt: calls.append(receipt),
                allow_process_local_ack=True,
            )
        finally:
            reset_preview_delivery_generation(token)

    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="stable-unit-chat",
        gateway_account_id="telegram-account",
    )

    first_adapter = _exact_claim_adapter(
        Platform.TELEGRAM,
        account="telegram-account",
        max_logical_units=1800,
        segmentation_version="telegram-preview-logical-v1",
    )
    first_encoding = semantic_exact_attempt_encoding_contract(
        first_adapter.SEMANTIC_EXACT_ATTEMPT_CAPABILITY
    )
    register()
    outbound = prepare_preview_delivery_content(
        session_key,
        generation,
        "First generated wrapper.",
    )
    first = claim_preview_delivery_for_source(
        session_key,
        generation,
        delivered_content=outbound,
        source=source,
        adapter=first_adapter,
    )
    assert first.action == "send"
    assert first.unit_count > 1

    # Simulate a fresh process/deploy after the provider attempt began. The
    # reconstructed adapter now advertises a very different mutable limit.
    # The deployed segmentation budget also changed. The durable manifest,
    # rather than either runtime value, must reconstruct the original units.
    discard_preview_delivery_intent(session_key, generation)
    upgraded_adapter = _exact_claim_adapter(
        Platform.TELEGRAM,
        account="telegram-account",
        max_logical_units=80,
        segmentation_version="telegram-preview-logical-v2",
        historical=(first_encoding,),
    )
    register()
    replay = claim_preview_delivery_for_source(
        session_key,
        generation,
        delivered_content=outbound,
        source=source,
        adapter=upgraded_adapter,
    )

    assert replay.action == "conflict"
    assert replay.delivery_id == first.delivery_id
    assert replay.unit_count == first.unit_count
    discard_preview_delivery_intent(session_key, generation)


def test_restart_skips_committed_units_and_fences_only_ambiguous_unit() -> None:
    calls: list[ProviderDeliveryReceipt] = []
    session_key = "session-unit-restart"
    generation = 28
    _register(session_key, generation, calls)
    content = prepare_preview_delivery_content(
        session_key,
        generation,
        "Ignored wrapper.",
    )
    first = claim_preview_delivery(
        session_key,
        generation,
        delivered_content=content,
        provider="telegram",
        target="telegram:test-target",
        gateway_account_id="telegram-account",
        max_message_length=24,
    )
    assert first.action == "send"
    assert first.unit_index == 0
    first_result = SendResult(
        success=True,
        message_id="unit-0",
        delivered_content_digest=(
            "sha256:" + hashlib.sha256(first.content.encode()).hexdigest()
        ),
        delivered_content_complete=True,
    )
    persisted = record_preview_delivery_result(
        session_key,
        generation,
        delivered_content=content,
        result=first_result,
    )
    assert persisted["outcome"] == "delivered"

    second = claim_preview_delivery(
        session_key,
        generation,
        delivered_content=content,
        provider="telegram",
        target="telegram:test-target",
        gateway_account_id="telegram-account",
        max_message_length=24,
    )
    assert second.action == "send"
    assert second.unit_index == 1
    ambiguous_delivery_id = second.delivery_id

    # Process death after the second unit's durable prewrite but before its
    # provider receipt. The first committed unit must never be reopened.
    discard_preview_delivery_intent(session_key, generation)
    _register(session_key, generation, calls)
    replay = claim_preview_delivery(
        session_key,
        generation,
        delivered_content=content,
        provider="telegram",
        target="telegram:test-target",
        gateway_account_id="telegram-account",
        max_message_length=24,
    )
    assert replay.action == "conflict"
    assert replay.unit_index == 1
    assert replay.delivery_id == ambiguous_delivery_id
    assert replay.delivery_id != first.delivery_id
    discard_preview_delivery_intent(session_key, generation)


def test_preview_provider_rejection_evidence_survives_fresh_readback() -> None:
    calls: list[ProviderDeliveryReceipt] = []
    session_key = "session-preview-rejection-evidence"
    generation = 29
    _register(session_key, generation, calls)
    content = prepare_preview_delivery_content(
        session_key,
        generation,
        "Ignored wrapper.",
    )
    claim = _claim(session_key, generation, content)
    assert claim.action == "send"

    secret = "ghp_" + ("preview-secret-" * 40)
    rejection = provider_rejection_evidence(
        provider="Telegram",
        status=422,
        body={
            "authorization": f"Bearer {secret}",
            "detail": "preview delivery rejected permanently",
        },
    )
    persisted = record_preview_delivery_result(
        session_key,
        generation,
        delivered_content=content,
        result=SendResult(
            success=False,
            error=f"Telegram rejected credential {secret}",
            raw_response={
                "provider_write_attempted": False,
                "provider_retryable": False,
                "provider_rejection": rejection,
            },
        ),
    )
    assert persisted is not None
    assert persisted["outcome"] == "rejected"
    assert persisted["provider_rejection"] == rejection
    assert isinstance(persisted["provider_error"], dict)

    ledger = delivery_ledger_path()
    scope_id = semantic_delivery_scope_id(ledger_path=ledger)
    root = Path(__file__).resolve().parents[2]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = (
        str(root)
        + (
            os.pathsep + environment["PYTHONPATH"]
            if environment.get("PYTHONPATH")
            else ""
        )
    )
    code = (
        "import json,sys;"
        "from pathlib import Path;"
        "from hermes_cli.semantic_delivery import semantic_delivery_status;"
        "print(json.dumps(semantic_delivery_status("
        "delivery_id=sys.argv[1],"
        "contract_version=sys.argv[2],"
        "expected_scope_id=sys.argv[3],"
        "expected_provider='telegram',"
        "gateway_account_id='telegram-account',"
        "ledger_path=Path(sys.argv[4]))))"
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
            claim.delivery_id,
            SEMANTIC_DELIVERY_CONTRACT,
            scope_id,
            str(ledger),
        ],
        cwd=root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    replay = json.loads(completed.stdout)

    assert replay["outcome"] == "rejected"
    assert replay["replayed"] is True
    assert replay["provider_rejection"] == rejection
    assert replay["provider_error"] == persisted["provider_error"]
    assert secret.encode("utf-8") not in ledger.read_bytes()
    discard_preview_delivery_intent(session_key, generation)


@pytest.mark.asyncio
async def test_stream_holds_preview_bytes_until_one_semantic_send() -> None:
    adapter = _Adapter(SendResult(success=True, message_id="provider-1"))
    session_key = "session-stream-preview"
    generation = 23
    calls: list[ProviderDeliveryReceipt] = []
    consumer = GatewayStreamConsumer(
        adapter,
        "chat-1",
        StreamConsumerConfig(
            edit_interval=0,
            buffer_threshold=1,
            cursor="",
        ),
        delivery_hold=lambda: has_preview_delivery_intent(
            session_key,
            generation,
        ),
    )
    stream_task = asyncio.create_task(consumer.run())

    _register(session_key, generation, calls)
    outbound = prepare_preview_delivery_content(
        session_key,
        generation,
        "Final plan follows.",
    )
    consumer.on_delta(outbound)
    consumer.finish()
    await stream_task

    assert adapter.sent == []
    claim = _claim(session_key, generation, outbound)
    assert claim.action == "send"
    result = await adapter._send_with_retry(
        chat_id="chat-1",
        content=outbound,
        metadata=claim.metadata,
    )
    record_preview_delivery_result(
        session_key,
        generation,
        delivered_content=outbound,
        result=result,
    )
    assert await complete_preview_delivery(
        session_key,
        generation,
        delivered_content=outbound,
        result=result,
        delivered_at="2026-07-28T08:00:00+00:00",
    )
    assert adapter.sent == [outbound]
    assert len(calls) == 1
