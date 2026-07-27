"""Provider-neutral, generation-fenced Planning preview delivery receipts.

Fetching a preview is read-only.  A review receipt is released only after the
exact canonical page payload has been included in a successful outbound
provider send.  The registry is process-local by design: a restart loses the
unproven intent, so Dev Hub re-delivers the same unreviewed page instead of
inventing a receipt.
"""
from __future__ import annotations

import asyncio
from contextvars import ContextVar, Token
from dataclasses import dataclass
import hashlib
import inspect
import json
import threading
from typing import Any, Callable


_ACTIVE_DELIVERY: ContextVar[tuple[str, int] | None] = ContextVar(
    "planning_preview_active_delivery",
    default=None,
)
_LOCK = threading.RLock()
_INTENTS: dict[tuple[str, int], list["PreviewDeliveryIntent"]] = {}


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


@dataclass(frozen=True, slots=True)
class ProviderDeliveryReceipt:
    """Evidence returned only after every provider message chunk succeeded."""

    provider_message_ids: tuple[str, ...]
    delivered_at: str
    delivery_nonce: str
    delivery_payload_digest: str
    delivery_content_digest: str

    @property
    def provider_message_id(self) -> str:
        return self.provider_message_ids[-1]


@dataclass(frozen=True, slots=True)
class PreviewDeliveryIntent:
    """One exact page waiting for real provider delivery."""

    session_key: str
    generation: int
    thread_id: str
    preview_result_id: str
    preview_result_hash: str
    offset: int
    count: int
    page_digest: str
    delivery_payload: dict[str, Any]
    delivery_payload_digest: str
    delivery_content: str
    delivery_content_digest: str
    delivery_nonce: str
    acknowledge: Callable[[ProviderDeliveryReceipt], Any]

    @property
    def canonical_payload(self) -> str:
        return _canonical_json(self.delivery_payload)

    @property
    def outbound_block(self) -> str:
        return f"\n\n---\n{self.delivery_content}"


def bind_preview_delivery_generation(
    session_key: str,
    generation: int,
) -> Token:
    """Bind the current agent/tool thread to one gateway run generation."""

    return _ACTIVE_DELIVERY.set((str(session_key), int(generation)))


def reset_preview_delivery_generation(token: Token) -> None:
    _ACTIVE_DELIVERY.reset(token)


def register_preview_delivery_intent(
    *,
    thread_id: str,
    preview_result_id: str,
    preview_result_hash: str,
    offset: int,
    count: int,
    page_digest: str,
    delivery_payload: dict[str, Any],
    delivery_payload_digest: str,
    delivery_content: str,
    delivery_content_digest: str,
    delivery_nonce: str,
    acknowledge: Callable[[ProviderDeliveryReceipt], Any],
) -> bool:
    """Register an intent only inside an authenticated gateway generation."""

    active = _ACTIVE_DELIVERY.get()
    if (
        active is None
        or not callable(acknowledge)
        or not isinstance(delivery_payload, dict)
    ):
        return False
    session_key, generation = active
    canonical_payload = _canonical_json(delivery_payload)
    expected_digest = "sha256:" + hashlib.sha256(
        canonical_payload.encode("utf-8")
    ).hexdigest()
    expected_content_digest = "sha256:" + hashlib.sha256(
        str(delivery_content).encode("utf-8")
    ).hexdigest()
    if (
        expected_digest != delivery_payload_digest
        or expected_content_digest != delivery_content_digest
        or not str(delivery_content).strip()
    ):
        return False
    intent = PreviewDeliveryIntent(
        session_key=session_key,
        generation=generation,
        thread_id=str(thread_id),
        preview_result_id=str(preview_result_id),
        preview_result_hash=str(preview_result_hash),
        offset=int(offset),
        count=int(count),
        page_digest=str(page_digest),
        delivery_payload=dict(delivery_payload),
        delivery_payload_digest=str(delivery_payload_digest),
        delivery_content=str(delivery_content),
        delivery_content_digest=str(delivery_content_digest),
        delivery_nonce=str(delivery_nonce),
        acknowledge=acknowledge,
    )
    with _LOCK:
        key = (session_key, generation)
        outstanding = _INTENTS.setdefault(key, [])
        identity = (
            intent.thread_id,
            intent.preview_result_id,
            intent.preview_result_hash,
            intent.offset,
            intent.count,
            intent.page_digest,
            intent.delivery_payload_digest,
            intent.delivery_content_digest,
        )
        for existing in outstanding:
            existing_identity = (
                existing.thread_id,
                existing.preview_result_id,
                existing.preview_result_hash,
                existing.offset,
                existing.count,
                existing.page_digest,
                existing.delivery_payload_digest,
                existing.delivery_content_digest,
            )
            if existing_identity == identity:
                return True
        outstanding.append(intent)
    return True


def has_preview_delivery_intent(
    session_key: str,
    generation: int | None,
) -> bool:
    if not session_key or generation is None:
        return False
    with _LOCK:
        return (str(session_key), int(generation)) in _INTENTS


def prepare_preview_delivery_content(
    session_key: str,
    generation: int | None,
    content: str,
) -> str:
    """Append the exact canonical page if the model did not reproduce it."""

    if not session_key or generation is None:
        return content
    with _LOCK:
        intents = tuple(
            _INTENTS.get((str(session_key), int(generation)), ())
        )
    prepared = content.rstrip()
    for intent in intents:
        if intent.delivery_content not in prepared:
            prepared = f"{prepared}{intent.outbound_block}"
    return prepared


def discard_preview_delivery_intent(
    session_key: str,
    generation: int | None,
) -> None:
    if not session_key or generation is None:
        return
    with _LOCK:
        _INTENTS.pop((str(session_key), int(generation)), None)


def _provider_message_ids(result: Any) -> tuple[str, ...]:
    ordered: list[str] = []
    for value in getattr(result, "continuation_message_ids", ()) or ():
        message_id = str(value or "").strip()
        if message_id and message_id not in ordered:
            ordered.append(message_id)
    final_id = str(getattr(result, "message_id", "") or "").strip()
    if final_id and final_id not in ordered:
        ordered.append(final_id)
    return tuple(ordered)


async def complete_preview_delivery(
    session_key: str,
    generation: int | None,
    *,
    delivered_content: str,
    result: Any,
    delivered_at: str,
) -> bool:
    """ACK once, and only once, after a complete exact provider delivery."""

    if not session_key or generation is None:
        return False
    key = (str(session_key), int(generation))
    with _LOCK:
        intents = tuple(_INTENTS.get(key, ()))
    if not intents:
        return False

    delivered_digest = "sha256:" + hashlib.sha256(
        delivered_content.encode("utf-8")
    ).hexdigest()
    transport_digest = getattr(
        result,
        "delivered_content_digest",
        None,
    )
    transport_complete = getattr(
        result,
        "delivered_content_complete",
        None,
    )
    message_ids = _provider_message_ids(result)
    if getattr(result, "success", False) and not message_ids:
        # Some real Hermes transports acknowledge a successful send without
        # exposing a provider-native message id. Create a stable, explicitly
        # gateway-namespaced transport receipt only *after* that success.
        # It binds this exact intent and outbound bytes; it never pretends to
        # be a provider id and cannot exist on a failed/ambiguous send.
        transport_identity = {
            "schemaVersion": "planning.gateway-transport-receipt.v1",
            "sessionKey": intents[0].session_key,
            "generation": intents[0].generation,
            "deliveryNonces": [
                intent.delivery_nonce for intent in intents
            ],
            "deliveredContentDigest": (
                "sha256:"
                + hashlib.sha256(
                    delivered_content.encode("utf-8")
                ).hexdigest()
            ),
        }
        message_ids = (
            "gateway:"
            + hashlib.sha256(
                _canonical_json(transport_identity).encode("utf-8")
            ).hexdigest(),
        )
    if (
        not getattr(result, "success", False)
        or transport_complete is not True
        or transport_digest != delivered_digest
        or not message_ids
        or any(
            intent.delivery_content not in delivered_content
            for intent in intents
        )
    ):
        discard_preview_delivery_intent(session_key, generation)
        return False

    for intent in intents:
        receipt = ProviderDeliveryReceipt(
            provider_message_ids=message_ids,
            delivered_at=str(delivered_at),
            delivery_nonce=intent.delivery_nonce,
            delivery_payload_digest=intent.delivery_payload_digest,
            delivery_content_digest=intent.delivery_content_digest,
        )
        # A transport response can be lost after Hub commits. Replaying the
        # same callback once preserves the exact nonce, proof and key.
        for attempt in range(2):
            try:
                outcome = await asyncio.to_thread(
                    intent.acknowledge,
                    receipt,
                )
                if inspect.isawaitable(outcome):
                    await outcome
                break
            except Exception:
                if attempt == 1:
                    discard_preview_delivery_intent(
                        session_key,
                        generation,
                    )
                    return False
    with _LOCK:
        current = _INTENTS.get(key)
        if current is not None and tuple(current) == intents:
            _INTENTS.pop(key, None)
    return True
