"""Release gates for provider-confirmed Planning preview review receipts."""
from __future__ import annotations

import asyncio
import hashlib
import json

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    SendResult,
)
from gateway.session import SessionSource, build_session_key
from hermes_cli.planning_preview_delivery import (
    ProviderDeliveryReceipt,
    bind_preview_delivery_generation,
    complete_preview_delivery,
    discard_preview_delivery_intent,
    prepare_preview_delivery_content,
    register_preview_delivery_intent,
    reset_preview_delivery_generation,
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
        )
    finally:
        reset_preview_delivery_generation(token)


class _Adapter(BasePlatformAdapter):
    def __init__(
        self,
        result: SendResult | list[SendResult],
    ) -> None:
        super().__init__(PlatformConfig(enabled=True), Platform.TELEGRAM)
        self.results = (
            list(result) if isinstance(result, list) else [result]
        )
        self.sent: list[str] = []

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

    async def send_typing(self, chat_id, metadata=None) -> None:
        return None

    async def stop_typing(self, chat_id, metadata=None) -> None:
        return None


def _event() -> MessageEvent:
    return MessageEvent(
        text="show preview",
        source=SessionSource(
            platform=Platform.TELEGRAM,
            chat_id="chat-1",
            chat_type="direct",
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
    assert len(adapter.sent) == 2
    assert _payload()[2] in adapter.sent[0]
    assert _payload()[2] not in adapter.sent[1]


@pytest.mark.asyncio
async def test_success_without_provider_message_id_uses_gateway_receipt() -> None:
    _adapter, calls = await _run_base_delivery(
        SendResult(success=True)
    )

    assert len(calls) == 1
    assert calls[0].provider_message_id.startswith("gateway:")


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
async def test_lost_ack_replays_byte_identical_receipt() -> None:
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
        )
    finally:
        reset_preview_delivery_generation(token)
    content = prepare_preview_delivery_content(
        "session-replay",
        4,
        "Preview.",
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
