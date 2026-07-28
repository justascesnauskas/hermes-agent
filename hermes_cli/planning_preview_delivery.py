"""Generation-fenced, crash-safe Planning preview delivery receipts.

Fetching a preview is read-only. A review receipt is released only after the
exact canonical page payload has been included in a successful provider send.
The callback stays process-local, while the edge-send identity and provider
receipt use the durable semantic-delivery ledger. Automatic restart therefore
never blindly duplicates an ambiguous preview; a later explicit user event
mints a new, user-authorized delivery attempt.
"""
from __future__ import annotations

import asyncio
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import inspect
import json
import threading
from typing import Any, Callable, Mapping

from hermes_cli.semantic_delivery import (
    SEMANTIC_DELIVERY_CONTRACT,
    SemanticDeliveryAttempt,
    _send_result_mapping,
    begin_semantic_delivery,
    exact_provider_message_id,
    finish_semantic_delivery,
    semantic_delivery_scope_id,
    semantic_retry_turn_execution_committed,
    stage_semantic_delivery_retry,
)
from hermes_cli.planning_preview_ack_outbox import (
    PLANNING_PREVIEW_ACK_COMPLETION_CONTRACT,
    claim_preview_ack,
    complete_preview_ack,
    preview_ack_status,
    reconcile_preview_ack_bridge,
    retry_preview_ack,
    stage_preview_ack_bridge,
)
from hermes_cli.planning_preview_manifest import (
    stage_preview_delivery_manifest,
)


_ACTIVE_DELIVERY: ContextVar[tuple[str, int] | None] = ContextVar(
    "planning_preview_active_delivery",
    default=None,
)
_LOCK = threading.RLock()
_INTENTS: dict[tuple[str, int], list["PreviewDeliveryIntent"]] = {}
_SEMANTIC_ATTEMPTS: dict[
    tuple[str, int],
    "_PreviewSemanticState",
] = {}
_PREVIEW_SEGMENTATION_CONTRACT = "planning.preview-segmentation.v2"


def _provider_completion_timestamp() -> str:
    return datetime.now(UTC).isoformat()


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
    delivery_target: str
    acknowledge: Callable[[ProviderDeliveryReceipt], Any]
    acknowledgement_request: dict[str, Any] | None
    allow_process_local_ack: bool

    @property
    def canonical_payload(self) -> str:
        return _canonical_json(self.delivery_payload)

    @property
    def outbound_block(self) -> str:
        return f"\n\n---\n{self.delivery_content}"


@dataclass(frozen=True, slots=True)
class PreviewDeliveryClaim:
    """Decision made immediately before one provider send."""

    action: str
    delivery_id: str = ""
    provider_message_ids: tuple[str, ...] = ()
    metadata: dict[str, Any] | None = None
    content: str = ""
    unit_index: int = 0
    unit_count: int = 0
    execution_committed: bool = False


@dataclass(frozen=True, slots=True)
class ReplayedPreviewSendResult:
    """SendResult-compatible proof reconstructed from the durable ledger."""

    success: bool
    message_id: str | None
    continuation_message_ids: tuple[str, ...]
    delivered_content_digest: str
    delivered_content_complete: bool = True
    error: str | None = None


@dataclass(frozen=True, slots=True)
class PreviewDeliveryFlowResult:
    """Whole multi-unit edge outcome returned to the gateway."""

    result: Any | None
    acknowledged: bool
    action: str
    execution_committed: bool = False


@dataclass(slots=True)
class _PreviewSemanticState:
    delivery_base_id: str
    delivered_content: str
    delivered_content_digest: str
    provider: str
    target: str
    gateway_account_id: str
    semantic_scope_id: str
    chat_id: str
    thread_id: str | None
    reply_to: str | None
    turn_execution_ref: str
    encoding_contract: Any | None
    provider_routes: tuple[tuple[tuple[str, str], ...], ...]
    units: tuple[str, ...]
    delivery_ids: tuple[str, ...]
    bridge_id: str | None
    next_unit: int = 0
    active_attempt: SemanticDeliveryAttempt | None = None
    active_unit: int | None = None
    ledger_results: dict[int, dict[str, Any]] | None = None

    def __post_init__(self) -> None:
        if self.ledger_results is None:
            self.ledger_results = {}


def derive_preview_delivery_nonce(
    *,
    planning_thread_id: str,
    preview_result_id: str,
    preview_result_hash: str,
    offset: int,
    count: int,
    page_digest: str,
    origin: Mapping[str, Any],
) -> str:
    """Bind a page attempt to the exact inbound user/provider event.

    Replaying the same gateway event after a restart derives the same nonce.
    A later human ``show/resend`` message has a new provider event id and thus
    explicitly authorizes a new edge-delivery attempt for the same page.
    """

    identity = {
        "schemaVersion": "planning.preview-edge-attempt.v1",
        "planningThreadId": str(planning_thread_id),
        "previewResultId": str(preview_result_id),
        "previewResultHash": str(preview_result_hash),
        "offset": int(offset),
        "count": int(count),
        "pageDigest": str(page_digest),
        # Bind the complete immutable PlanningOriginPayload, not a selected
        # subset. This keeps authority exact across accounts, messaging
        # threads, senders, provider events, and gateway instances.
        "origin": {
            key: origin.get(key)
            for key in (
                "schemaVersion",
                "provider",
                "gatewayInstanceId",
                "gatewayAccountId",
                "chatId",
                "threadId",
                "messageId",
                "senderId",
                "chatType",
                "sourceTimestamp",
                "providerEventId",
            )
        },
    }
    digest = hashlib.sha256(
        _canonical_json(identity).encode("utf-8")
    ).hexdigest()
    return f"preview-delivery-{digest}"


def canonical_preview_delivery_target(
    *,
    provider: str,
    gateway_account_id: str | None,
    chat_id: str,
    thread_id: str | None,
) -> str:
    """Encode one provider destination without delimiter ambiguity.

    Provider chat and thread identifiers are opaque. Matrix room identifiers
    contain colons, so concatenating ``provider:chat:thread`` cannot preserve
    the original tuple. The provider prefix remains routable while the exact
    destination tuple is represented by a canonical hash.
    """

    clean_provider = str(provider or "").strip().lower()
    identity = {
        "schemaVersion": "planning.preview-target.v1",
        "provider": clean_provider,
        "gatewayAccountId": (
            str(gateway_account_id)
            if gateway_account_id is not None
            else None
        ),
        "chatId": str(chat_id),
        "threadId": str(thread_id) if thread_id is not None else None,
    }
    digest = hashlib.sha256(
        _canonical_json(identity).encode("utf-8")
    ).hexdigest()
    return f"{clean_provider}:preview-target-v1:{digest}"


def preview_delivery_target_for_source(
    *,
    provider: str,
    source: Any,
    gateway_account_id: str,
) -> str:
    """Build one destination from an already validated account authority."""

    return canonical_preview_delivery_target(
        provider=provider,
        gateway_account_id=gateway_account_id,
        chat_id=str(getattr(source, "chat_id", "") or ""),
        thread_id=getattr(source, "thread_id", None),
    )


def _gateway_account_id_for_source(
    *,
    source: Any,
    adapter: Any,
) -> str:
    def _validated(raw_account_id: Any) -> str:
        account_id = str(raw_account_id or "")
        if (
            not account_id
            or account_id != account_id.strip()
            or len(account_id) > 500
            or any(
                ord(character) < 32 or ord(character) == 127
                for character in account_id
            )
        ):
            return ""
        return account_id

    source_raw = getattr(source, "gateway_account_id", None)
    extra = getattr(getattr(adapter, "config", None), "extra", None)
    adapter_raw = (
        extra.get("gateway_account_id")
        if isinstance(extra, dict)
        else None
    )
    source_present = source_raw is not None and str(source_raw) != ""
    adapter_present = adapter_raw is not None and str(adapter_raw) != ""
    source_account_id = _validated(source_raw) if source_present else ""
    adapter_account_id = _validated(adapter_raw) if adapter_present else ""
    if (
        (source_present and not source_account_id)
        or (adapter_present and not adapter_account_id)
        or (
            source_account_id
            and adapter_account_id
            and source_account_id != adapter_account_id
        )
    ):
        return ""
    return source_account_id or adapter_account_id


def _segmentation_contract_for_encoding(encoding_contract: Any) -> str:
    """Bind the splitter algorithm to the exact adapter encoder contract."""

    from gateway.semantic_exact_attempt import (
        coerce_live_semantic_exact_attempt_encoding_contract,
    )

    frozen = coerce_live_semantic_exact_attempt_encoding_contract(
        encoding_contract
    )
    return _canonical_json(
        {
            "schemaVersion": _PREVIEW_SEGMENTATION_CONTRACT,
            "splitter": "ordered-codepoint-readable-boundary-v1",
            "encodingContract": frozen.as_mapping(),
        }
    )


def _logical_length(value: str, *, utf16: bool) -> int:
    if not utf16:
        return len(value)
    return len(value.encode("utf-16-le")) // 2


def _prefix_for_logical_limit(
    value: str,
    limit: int,
    *,
    utf16: bool,
) -> int:
    """Return a non-empty codepoint boundary within one provider-unit budget."""

    if not value:
        return 0
    if _logical_length(value, utf16=utf16) <= limit:
        return len(value)
    low = 1
    high = min(len(value), limit)
    best = 0
    while low <= high:
        middle = (low + high) // 2
        if _logical_length(value[:middle], utf16=utf16) <= limit:
            best = middle
            low = middle + 1
        else:
            high = middle - 1
    return max(1, best)


def _split_preview_delivery_units(
    content: str,
    *,
    max_length: int,
    utf16: bool,
) -> tuple[str, ...]:
    """Split without deleting/reordering a codepoint and without a unit cap."""

    remaining = str(content)
    units: list[str] = []
    while remaining:
        boundary = _prefix_for_logical_limit(
            remaining,
            max_length,
            utf16=utf16,
        )
        if boundary < len(remaining):
            # Prefer a readable boundary while preserving every separator in
            # exactly one unit.  Never shrink below half the provider budget,
            # which keeps the unit count bounded by content size rather than
            # adversarial early newlines.
            floor = max(1, boundary // 2)
            newline = remaining.rfind("\n", floor, boundary)
            if newline >= floor:
                boundary = newline + 1
        units.append(remaining[:boundary])
        remaining = remaining[boundary:]
    return tuple(units)


def _canonical_ordered_preview(
    intents: tuple[PreviewDeliveryIntent, ...],
) -> str | None:
    """Validate one page envelope and return its only deliverable rendering."""

    if not intents:
        return None
    first = intents[0]
    previous_end: int | None = None
    for intent in intents:
        if (
            intent.thread_id != first.thread_id
            or intent.preview_result_id != first.preview_result_id
            or intent.preview_result_hash != first.preview_result_hash
            or intent.offset < 0
            or intent.count < 1
            or (
                previous_end is not None
                and intent.offset < previous_end
            )
        ):
            return None
        previous_end = intent.offset + intent.count
    return "\n\n---\n".join(
        intent.delivery_content for intent in intents
    )


def claim_preview_delivery_for_source(
    session_key: str,
    generation: int,
    *,
    delivered_content: str,
    source: Any,
    adapter: Any,
    reply_to: str | None = None,
    turn_execution_ref: str = "",
) -> PreviewDeliveryClaim:
    """Claim through the one destination resolver used by both send paths."""

    provider = str(
        getattr(adapter.platform, "value", adapter.platform)
    ).strip().lower()
    from gateway.platform_registry import (
        planning_semantic_delivery_ineligibility,
        supports_live_semantic_exact_attempt,
    )
    from gateway.semantic_exact_attempt import (
        live_semantic_exact_attempt_capability,
        live_semantic_exact_attempt_encoding_contract,
    )

    planning_ineligible_reason = (
        planning_semantic_delivery_ineligibility(
            provider,
            adapter=adapter,
        )
    )
    if planning_ineligible_reason:
        return PreviewDeliveryClaim(
            action="planning_ineligible",
            metadata={
                "planning_ineligible_reason": planning_ineligible_reason,
            },
        )
    if not supports_live_semantic_exact_attempt(
        provider,
        adapter=adapter,
    ):
        return PreviewDeliveryClaim(action="rejected")
    capability = live_semantic_exact_attempt_capability(adapter)
    encoding_contract = live_semantic_exact_attempt_encoding_contract(adapter)
    if capability is None or encoding_contract is None:
        return PreviewDeliveryClaim(action="rejected")
    gateway_account_id = _gateway_account_id_for_source(
        source=source,
        adapter=adapter,
    )
    if not gateway_account_id:
        return PreviewDeliveryClaim(action="rejected")
    target = preview_delivery_target_for_source(
        provider=provider,
        source=source,
        gateway_account_id=gateway_account_id,
    )
    return claim_preview_delivery(
        session_key,
        generation,
        delivered_content=delivered_content,
        provider=provider,
        target=target,
        gateway_account_id=gateway_account_id,
        max_message_length=capability.max_logical_units,
        utf16_length=(
            capability.length_semantics == "utf16_code_units"
        ),
        adapter=adapter,
        encoding_contract=encoding_contract,
        chat_id=str(getattr(source, "chat_id", "") or ""),
        thread_id=getattr(source, "thread_id", None),
        reply_to=reply_to,
        turn_execution_ref=turn_execution_ref,
    )


def _semantic_delivery_id(
    intents: tuple[PreviewDeliveryIntent, ...],
    *,
    target: str,
    gateway_account_id: str,
) -> str:
    """Return the stable batch identity, independent of model wrapper prose.

    Each nonce binds the immutable inbound event and exact signed page.  Unit
    identities below additionally bind ordered canonical content chunks.
    """

    identity = {
        "schemaVersion": "planning.preview-edge-delivery.v2",
        "deliveryNonces": [intent.delivery_nonce for intent in intents],
        "target": target,
        "gatewayAccountId": gateway_account_id,
    }
    digest = hashlib.sha256(
        _canonical_json(identity).encode("utf-8")
    ).hexdigest()
    return f"preview_{digest}"


def _semantic_delivery_unit_id(
    base_id: str,
    *,
    unit_index: int,
    unit_count: int,
    content: str,
) -> str:
    identity = {
        "schemaVersion": "planning.preview-edge-unit.v1",
        "baseId": base_id,
        "unitIndex": unit_index,
        "unitCount": unit_count,
        "contentDigest": (
            "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest()
        ),
    }
    digest = hashlib.sha256(
        _canonical_json(identity).encode("utf-8")
    ).hexdigest()
    return f"{base_id}_u{unit_index}_{digest[:24]}"


def _message_ids_from_mapping(value: Any) -> tuple[str, ...]:
    if not isinstance(value, dict):
        return ()
    ordered: list[str] = []
    message_ids = value.get("message_ids")
    if not isinstance(message_ids, (list, tuple)):
        message_ids = ()
    for item in (*message_ids, value.get("message_id")):
        message_id = exact_provider_message_id(item)
        if message_id and message_id not in ordered:
            ordered.append(message_id)
    return tuple(ordered)


def claim_preview_delivery(
    session_key: str,
    generation: int | None,
    *,
    delivered_content: str,
    provider: str,
    target: str,
    gateway_account_id: str,
    max_message_length: int = 1_000_000,
    utf16_length: bool = False,
    adapter: Any | None = None,
    encoding_contract: Any | None = None,
    chat_id: str = "",
    thread_id: str | None = None,
    reply_to: str | None = None,
    turn_execution_ref: str = "",
) -> PreviewDeliveryClaim:
    """Claim the next exact provider unit, never an oversized aggregate."""

    if not session_key or generation is None:
        return PreviewDeliveryClaim(action="none")
    key = (str(session_key), int(generation))
    with _LOCK:
        intents = tuple(_INTENTS.get(key, ()))
        existing = _SEMANTIC_ATTEMPTS.get(key)
    if not intents:
        return PreviewDeliveryClaim(action="none")
    gateway_account_id = str(gateway_account_id or "")
    if (
        not gateway_account_id
        or gateway_account_id != gateway_account_id.strip()
        or len(gateway_account_id) > 500
        or any(
            ord(character) < 32 or ord(character) == 127
            for character in gateway_account_id
        )
    ):
        return PreviewDeliveryClaim(action="rejected")
    canonical_delivery = _canonical_ordered_preview(intents)
    if (
        canonical_delivery is None
        or delivered_content != canonical_delivery
    ):
        return PreviewDeliveryClaim(action="rejected")
    if str(target).partition(":")[0].lower() != str(provider).lower():
        return PreviewDeliveryClaim(action="rejected")
    frozen_targets = {
        intent.delivery_target
        for intent in intents
        if intent.delivery_target
    }
    if len(frozen_targets) > 1 or (
        frozen_targets and str(target) not in frozen_targets
    ):
        return PreviewDeliveryClaim(action="rejected")
    raw_turn_execution_ref = str(turn_execution_ref or "")
    if raw_turn_execution_ref != raw_turn_execution_ref.strip():
        return PreviewDeliveryClaim(action="rejected")
    turn_execution_ref = raw_turn_execution_ref
    from gateway.semantic_exact_attempt import (
        coerce_live_semantic_exact_attempt_encoding_contract,
        coerce_live_semantic_exact_attempt_provider_route,
        supports_live_semantic_exact_attempt_encoding_contract,
    )

    try:
        candidate_encoding_contract = (
            coerce_live_semantic_exact_attempt_encoding_contract(
                encoding_contract
                if encoding_contract is not None
                else {
                    "provider": str(provider),
                    "contract": "hermes-live-semantic-exact-attempt/1",
                    "segmentation_version": (
                        "planning-preview-process-local-v1"
                    ),
                    "max_logical_units": max_message_length,
                    "length_semantics": (
                        "utf16_code_units"
                        if utf16_length
                        else "unicode_codepoints"
                    ),
                    "wire_encoding": (
                        "planning-preview-process-local-wire-v1"
                    ),
                }
            )
        )
    except (TypeError, ValueError):
        return PreviewDeliveryClaim(action="rejected")
    if (
        candidate_encoding_contract.provider != str(provider)
        or (
            adapter is not None
            and not supports_live_semantic_exact_attempt_encoding_contract(
                adapter,
                candidate_encoding_contract,
            )
        )
    ):
        return PreviewDeliveryClaim(action="rejected")

    delivery_base_id = _semantic_delivery_id(
        intents,
        target=str(target),
        gateway_account_id=gateway_account_id,
    )
    if existing is not None:
        if (
            existing.delivery_base_id != delivery_base_id
            or existing.delivered_content != delivered_content
            or existing.provider != str(provider)
            or existing.target != str(target)
            or existing.gateway_account_id != gateway_account_id
            or (
                adapter is not None
                and (
                    existing.chat_id != str(chat_id)
                    or existing.thread_id != thread_id
                    or existing.reply_to != reply_to
                    or existing.turn_execution_ref != turn_execution_ref
                )
            )
        ):
            return PreviewDeliveryClaim(
                action="conflict",
                delivery_id=delivery_base_id,
            )
        state = existing
        if (
            adapter is not None
            and (
                state.encoding_contract is None
                or not supports_live_semantic_exact_attempt_encoding_contract(
                    adapter,
                    state.encoding_contract,
                )
            )
        ):
            return PreviewDeliveryClaim(
                action="rejected",
                delivery_id=delivery_base_id,
            )
    else:
        if (
            isinstance(max_message_length, bool)
            or not isinstance(max_message_length, int)
            or max_message_length < 1
        ):
            return PreviewDeliveryClaim(
                action="rejected",
                delivery_id=delivery_base_id,
            )
        candidate_units = _split_preview_delivery_units(
            delivered_content,
            max_length=max_message_length,
            utf16=bool(utf16_length),
        )
        if not candidate_units:
            return PreviewDeliveryClaim(
                action="rejected",
                delivery_id=delivery_base_id,
            )
        scope_id = semantic_delivery_scope_id()
        manifest = stage_preview_delivery_manifest(
            delivery_base_id=delivery_base_id,
            semantic_scope_id=scope_id,
            content=delivered_content,
            candidate_units=candidate_units,
            segmentation_contract=(
                _segmentation_contract_for_encoding(
                    candidate_encoding_contract
                )
            ),
            encoding_contract=candidate_encoding_contract.as_mapping(),
            provider=str(provider),
            gateway_account_id=gateway_account_id,
        )
        units = manifest.units
        try:
            frozen_encoding_contract = (
                coerce_live_semantic_exact_attempt_encoding_contract(
                    manifest.encoding_contract
                )
            )
        except (TypeError, ValueError):
            return PreviewDeliveryClaim(
                action="rejected",
                delivery_id=delivery_base_id,
            )
        if (
            adapter is not None
            and not supports_live_semantic_exact_attempt_encoding_contract(
                adapter,
                frozen_encoding_contract,
            )
        ):
            return PreviewDeliveryClaim(
                action="rejected",
                delivery_id=delivery_base_id,
            )
        delivery_ids = tuple(
            _semantic_delivery_unit_id(
                delivery_base_id,
                unit_index=index,
                unit_count=len(units),
                content=content,
            )
            for index, content in enumerate(units)
        )
        acknowledgement_requests = [
            {
                **dict(intent.acknowledgement_request or {}),
                "deliveryPayloadDigest": intent.delivery_payload_digest,
                "deliveryContentDigest": intent.delivery_content_digest,
            }
            for intent in intents
            if intent.acknowledgement_request is not None
        ]
        bridge_id: str | None = None
        if len(acknowledgement_requests) == len(intents):
            bridge = stage_preview_ack_bridge(
                acknowledgement_requests,
                semantic_delivery_ids=delivery_ids,
                semantic_scope_id=scope_id,
                provider=str(provider),
                gateway_account_id=gateway_account_id,
            )
            bridge_id = str(bridge["bridgeId"])
        elif acknowledgement_requests or not all(
            intent.allow_process_local_ack for intent in intents
        ):
            return PreviewDeliveryClaim(
                action="rejected",
                delivery_id=delivery_base_id,
            )
        if adapter is not None:
            completion_contract = (
                PLANNING_PREVIEW_ACK_COMPLETION_CONTRACT
                if bridge_id is not None
                else ""
            )
            completion_ref = bridge_id or ""
            staged_provider_routes: list[
                tuple[tuple[str, str], ...]
            ] = []
            for unit_index, (delivery_id, unit_content) in enumerate(
                zip(delivery_ids, units, strict=True)
            ):
                staged = stage_semantic_delivery_retry(
                    delivery_id=delivery_id,
                    contract_version=SEMANTIC_DELIVERY_CONTRACT,
                    target=str(target),
                    message=unit_content,
                    gateway_account_id=gateway_account_id,
                    adapter=adapter,
                    encoding_contract=frozen_encoding_contract,
                    chat_id=str(chat_id),
                    thread_id=thread_id,
                    reply_to=(reply_to if unit_index == 0 else None),
                    delivery_group_id=delivery_base_id,
                    unit_index=unit_index,
                    unit_count=len(units),
                    turn_execution_ref=turn_execution_ref,
                    completion_contract=completion_contract,
                    completion_ref=completion_ref,
                    expected_scope_id=scope_id,
                )
                try:
                    staged_provider_routes.append(
                        coerce_live_semantic_exact_attempt_provider_route(
                            staged.get("provider_route")
                        )
                    )
                except (TypeError, ValueError):
                    return PreviewDeliveryClaim(
                        action="rejected",
                        delivery_id=delivery_base_id,
                    )
            if (
                turn_execution_ref
                and not semantic_retry_turn_execution_committed(
                    turn_execution_ref,
                )
            ):
                return PreviewDeliveryClaim(
                    action="rejected",
                    delivery_id=delivery_base_id,
                )
            provider_routes = tuple(staged_provider_routes)
        else:
            provider_routes = tuple(() for _unit in units)
        state = _PreviewSemanticState(
            delivery_base_id=delivery_base_id,
            delivered_content=delivered_content,
            delivered_content_digest=(
                "sha256:"
                + hashlib.sha256(delivered_content.encode("utf-8")).hexdigest()
            ),
            provider=str(provider),
            target=str(target),
            gateway_account_id=gateway_account_id,
            semantic_scope_id=scope_id,
            chat_id=str(chat_id),
            thread_id=thread_id,
            reply_to=reply_to,
            turn_execution_ref=turn_execution_ref,
            encoding_contract=frozen_encoding_contract,
            provider_routes=provider_routes,
            units=units,
            delivery_ids=delivery_ids,
            bridge_id=bridge_id,
        )
        with _LOCK:
            concurrent = _SEMANTIC_ATTEMPTS.get(key)
            if concurrent is None:
                _SEMANTIC_ATTEMPTS[key] = state
            else:
                state = concurrent

    if state.active_attempt is not None:
        return PreviewDeliveryClaim(
            action="in_flight",
            delivery_id=state.delivery_ids[state.active_unit or 0],
            unit_index=state.active_unit or 0,
            unit_count=len(state.units),
            execution_committed=bool(
                adapter is not None
                and (
                    not state.turn_execution_ref
                    or semantic_retry_turn_execution_committed(
                        state.turn_execution_ref
                    )
                )
            ),
        )

    while state.next_unit < len(state.units):
        unit_index = state.next_unit
        delivery_id = state.delivery_ids[unit_index]
        unit_content = state.units[unit_index]
        attempt = begin_semantic_delivery(
            delivery_id=delivery_id,
            contract_version=SEMANTIC_DELIVERY_CONTRACT,
            target=state.target,
            message=unit_content,
            expected_scope_id=state.semantic_scope_id,
            gateway_account_id=state.gateway_account_id,
        )
        result = dict(attempt.result or {})
        if attempt.action == "delivered":
            state.ledger_results[unit_index] = result
            state.next_unit += 1
            continue
        if attempt.action == "send":
            state.active_attempt = attempt
            state.active_unit = unit_index
            return PreviewDeliveryClaim(
                action="send",
                delivery_id=delivery_id,
                metadata={
                    "semantic_delivery_contract": SEMANTIC_DELIVERY_CONTRACT,
                    "semantic_delivery_id": delivery_id,
                    "semantic_delivery_target": state.target,
                    "semantic_delivery_unit": unit_index,
                },
                content=unit_content,
                unit_index=unit_index,
                unit_count=len(state.units),
                execution_committed=bool(adapter is not None),
            )
        return PreviewDeliveryClaim(
            action=(
                "conflict"
                if attempt.action == "ambiguous"
                else attempt.action
            ),
            delivery_id=delivery_id,
            provider_message_ids=_message_ids_from_mapping(result),
            unit_index=unit_index,
            unit_count=len(state.units),
            execution_committed=bool(adapter is not None),
        )

    message_ids: list[str] = []
    for unit_index in range(len(state.units)):
        for message_id in _message_ids_from_mapping(
            state.ledger_results.get(unit_index)
        ):
            if message_id not in message_ids:
                message_ids.append(message_id)
    return PreviewDeliveryClaim(
        action="delivered",
        delivery_id=state.delivery_base_id,
        provider_message_ids=tuple(message_ids),
        unit_index=len(state.units),
        unit_count=len(state.units),
        execution_committed=bool(adapter is not None),
    )


def preview_delivery_next_action(claim: PreviewDeliveryClaim) -> str:
    """Return a plain recovery action without exposing storage internals."""

    if claim.action == "in_flight":
        return ""
    return (
        "I could not verify whether the preview reached this chat before the "
        "restart, so I did not send it twice. Send “show preview again” to "
        "create a fresh, user-authorized delivery attempt."
    )


def replayed_preview_send_result(
    claim: PreviewDeliveryClaim,
    delivered_content: str,
) -> ReplayedPreviewSendResult:
    """Rebuild transport proof without calling the provider again."""

    message_ids = claim.provider_message_ids
    return ReplayedPreviewSendResult(
        success=bool(message_ids),
        message_id=message_ids[-1] if message_ids else None,
        continuation_message_ids=message_ids,
        delivered_content_digest=(
            "sha256:"
            + hashlib.sha256(delivered_content.encode("utf-8")).hexdigest()
        ),
        delivered_content_complete=bool(message_ids),
        error=None if message_ids else "semantic_delivery_receipt_invalid",
    )


async def deliver_preview_for_source(
    session_key: str,
    generation: int,
    *,
    delivered_content: str,
    source: Any,
    adapter: Any,
    metadata: Mapping[str, Any] | None = None,
    reply_to: str | None = None,
    delivered_at: str,
    turn_execution_ref: str = "",
    turn_execution_committed: Callable[[], Any] | None = None,
) -> PreviewDeliveryFlowResult:
    """Deliver every deterministic unit in order, then release page ACKs."""

    # ``delivered_at`` remains in the compatibility signature while older
    # gateway callers roll forward. It was captured before the first unit and
    # therefore cannot be provider-delivery evidence for a multi-unit batch.
    del delivered_at
    del metadata
    last_result: Any | None = None
    execution_commit_notified = False
    while True:
        claim = await asyncio.to_thread(
            claim_preview_delivery_for_source,
            session_key,
            generation,
            delivered_content=delivered_content,
            source=source,
            adapter=adapter,
            reply_to=reply_to,
            turn_execution_ref=turn_execution_ref,
        )
        if (
            claim.execution_committed
            and turn_execution_ref
            and not execution_commit_notified
        ):
            if not callable(turn_execution_committed):
                return PreviewDeliveryFlowResult(
                    result=None,
                    acknowledged=False,
                    action="turn_execution_commit_callback_missing",
                    execution_committed=False,
                )
            try:
                committed = turn_execution_committed()
                if inspect.isawaitable(committed):
                    await committed
            except Exception:
                return PreviewDeliveryFlowResult(
                    result=None,
                    acknowledged=False,
                    action="turn_execution_commit_failed",
                    execution_committed=False,
                )
            execution_commit_notified = True
        if claim.action == "send":
            from gateway.semantic_exact_attempt import (
                LiveSemanticExactAttemptRequest,
                send_via_exact_adapter_method,
            )

            with _LOCK:
                state = _SEMANTIC_ATTEMPTS.get(
                    (str(session_key), int(generation))
                )
            if (
                state is None
                or state.encoding_contract is None
                or claim.unit_index >= len(state.units)
                or claim.unit_index >= len(state.provider_routes)
                or state.delivery_ids[claim.unit_index] != claim.delivery_id
            ):
                return PreviewDeliveryFlowResult(
                    result=None,
                    acknowledged=False,
                    action="conflict",
                    execution_committed=execution_commit_notified,
                )
            request = LiveSemanticExactAttemptRequest(
                chat_id=state.chat_id,
                content=claim.content,
                delivery_contract=SEMANTIC_DELIVERY_CONTRACT,
                delivery_id=claim.delivery_id,
                delivery_target=state.target,
                delivery_unit=claim.unit_index,
                encoding_contract=state.encoding_contract,
                provider_route=state.provider_routes[claim.unit_index],
                thread_id=state.thread_id,
                reply_to=(
                    state.reply_to if claim.unit_index == 0 else None
                ),
            )
            try:
                last_result = await send_via_exact_adapter_method(
                    adapter,
                    request,
                )
            except asyncio.CancelledError:
                with _LOCK:
                    current = _SEMANTIC_ATTEMPTS.get(
                        (str(session_key), int(generation))
                    )
                    if current is state and current.active_attempt is not None:
                        current.active_attempt.release()
                        current.active_attempt = None
                        current.active_unit = None
                raise
            except Exception as exc:
                from types import SimpleNamespace

                # An untyped exception might have happened after the provider
                # accepted the write. It is intentionally ambiguous, never an
                # automatic pre-write retry.
                last_result = SimpleNamespace(
                    success=False,
                    message_id=None,
                    continuation_message_ids=(),
                    delivered_content_digest=None,
                    delivered_content_complete=False,
                    error=str(type(exc).__name__),
                    raw_response={},
                )
            ledger_result = record_preview_delivery_result(
                session_key,
                generation,
                delivered_content=delivered_content,
                result=last_result,
                exact_dispatch=True,
            )
            if (
                not isinstance(ledger_result, dict)
                or ledger_result.get("outcome") != "delivered"
            ):
                return PreviewDeliveryFlowResult(
                    result=last_result,
                    acknowledged=False,
                    action=str(
                        (ledger_result or {}).get("outcome")
                        or "failed"
                    ),
                    execution_committed=execution_commit_notified,
                )
            continue
        if claim.action == "delivered":
            aggregate = replayed_preview_send_result(
                claim,
                delivered_content,
            )
            acknowledged = await complete_preview_delivery(
                session_key,
                generation,
                delivered_content=delivered_content,
                result=aggregate,
                delivered_at=_provider_completion_timestamp(),
            )
            return PreviewDeliveryFlowResult(
                result=aggregate,
                acknowledged=acknowledged,
                action="delivered",
                execution_committed=execution_commit_notified,
            )
        return PreviewDeliveryFlowResult(
            result=last_result,
            acknowledged=False,
            action=claim.action,
            execution_committed=execution_commit_notified,
        )


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
    delivery_target: str = "",
    acknowledgement_request: Mapping[str, Any] | None = None,
    allow_process_local_ack: bool = False,
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
    durable_request = (
        dict(acknowledgement_request)
        if isinstance(acknowledgement_request, Mapping)
        else None
    )
    proof_base = (
        durable_request.get("deliveryProofBase")
        if durable_request is not None
        else None
    )
    if durable_request is not None:
        expected_request_identity = {
            "threadId": str(thread_id),
            "previewResultId": str(preview_result_id),
            "expectedPreviewHash": str(preview_result_hash),
            "offset": int(offset),
            "count": int(count),
            "pageDigest": str(page_digest),
        }
        expected_proof_identity = {
            "deliveryNonce": str(delivery_nonce),
            "previewResultId": str(preview_result_id),
            "previewResultHash": str(preview_result_hash),
            "offset": int(offset),
            "count": int(count),
            "pageDigest": str(page_digest),
        }
        durable_identity_valid = bool(
            isinstance(proof_base, dict)
            and all(
                durable_request.get(name) == value
                for name, value in expected_request_identity.items()
            )
            and all(
                proof_base.get(name) == value
                for name, value in expected_proof_identity.items()
            )
        )
    else:
        durable_identity_valid = acknowledgement_request is None
    if (
        expected_digest != delivery_payload_digest
        or expected_content_digest != delivery_content_digest
        or not str(delivery_content).strip()
        or not durable_identity_valid
        or (
            acknowledgement_request is None
            and not allow_process_local_ack
        )
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
        delivery_target=str(delivery_target),
        acknowledge=acknowledge,
        acknowledgement_request=durable_request,
        allow_process_local_ack=bool(allow_process_local_ack),
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
            intent.delivery_nonce,
            intent.delivery_target,
            (
                _canonical_json(intent.acknowledgement_request)
                if intent.acknowledgement_request is not None
                else None
            ),
            intent.allow_process_local_ack,
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
                existing.delivery_nonce,
                existing.delivery_target,
                (
                    _canonical_json(existing.acknowledgement_request)
                    if existing.acknowledgement_request is not None
                    else None
                ),
                existing.allow_process_local_ack,
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
    """Return only the ordered canonical pages while preview proof is pending.

    Model-authored wrapper prose is intentionally excluded from the semantic
    identity.  A regenerated introduction must never mint a fresh provider
    attempt after an ambiguous send, and canonical review data must be the
    first bytes delivered even when the model produced a very long preamble.
    """

    if not session_key or generation is None:
        return content
    with _LOCK:
        intents = tuple(
            _INTENTS.get((str(session_key), int(generation)), ())
        )
    if not intents:
        return content
    canonical_delivery = _canonical_ordered_preview(intents)
    return canonical_delivery if canonical_delivery is not None else content


def discard_preview_delivery_intent(
    session_key: str,
    generation: int | None,
) -> None:
    if not session_key or generation is None:
        return
    with _LOCK:
        key = (str(session_key), int(generation))
        _INTENTS.pop(key, None)
        semantic_state = _SEMANTIC_ATTEMPTS.pop(key, None)
    if (
        semantic_state is not None
        and semantic_state.active_attempt is not None
    ):
        semantic_state.active_attempt.release()


def _provider_message_ids(result: Any) -> tuple[str, ...]:
    ordered: list[str] = []
    for value in getattr(result, "continuation_message_ids", ()) or ():
        message_id = exact_provider_message_id(value)
        if message_id and message_id not in ordered:
            ordered.append(message_id)
    final_id = exact_provider_message_id(
        getattr(result, "message_id", None)
    )
    if final_id and final_id not in ordered:
        ordered.append(final_id)
    return tuple(ordered)


def _provider_delivery_flag(result: Any, key: str) -> bool | None:
    direct = getattr(result, key, None)
    if isinstance(direct, bool):
        return direct
    raw_response = getattr(result, "raw_response", None)
    if isinstance(raw_response, dict):
        nested = raw_response.get(key)
        if isinstance(nested, bool):
            return nested
    return None


def record_preview_delivery_result(
    session_key: str,
    generation: int | None,
    *,
    delivered_content: str,
    result: Any,
    exact_dispatch: bool = False,
) -> dict[str, Any] | None:
    """Persist provider proof before any Dev Hub review callback."""

    if not session_key or generation is None:
        return None
    key = (str(session_key), int(generation))
    with _LOCK:
        state = _SEMANTIC_ATTEMPTS.get(key)
    if state is None:
        return None
    attempt = state.active_attempt
    unit_index = state.active_unit
    if attempt is None or unit_index is None:
        if state.next_unit > 0:
            return dict(
                state.ledger_results.get(state.next_unit - 1, {})
            )
        return None
    if attempt.action != "send":
        return dict(attempt.result or {})

    unit_content = state.units[unit_index]
    delivered_digest = (
        "sha256:"
        + hashlib.sha256(unit_content.encode("utf-8")).hexdigest()
    )
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
    if exact_dispatch:
        # This receipt came through the structural one-write dispatcher. No
        # formatter, chunker, fallback, or ordinary send retry can alter the
        # frozen request content on this path.
        transport_digest = delivered_digest
        transport_complete = True
    message_ids = _provider_message_ids(result)
    exact_success = bool(
        delivered_content == state.delivered_content
        and getattr(result, "success", False)
        and transport_complete is True
        and transport_digest == delivered_digest
    )
    # Preserve the same bounded adapter evidence as every other semantic
    # settlement.  Preview delivery adds an exact-content proof but must not
    # accidentally turn that extra proof boundary into an evidence scrubber.
    provider_result = _send_result_mapping(result)
    provider_result.update(
        {
            "success": exact_success,
            "message_id": message_ids[-1] if message_ids else None,
            "message_ids": list(message_ids),
        }
    )
    if not exact_success:
        write_attempted = _provider_delivery_flag(
            result,
            "provider_write_attempted",
        )
        if getattr(result, "success", False):
            # A provider accepted some bytes, but the exact preview proof failed.
            write_attempted = True
        if write_attempted is not None:
            provider_result["provider_write_attempted"] = write_attempted
        provider_retryable = _provider_delivery_flag(
            result,
            "provider_retryable",
        )
        if provider_retryable is not None:
            provider_result["provider_retryable"] = provider_retryable

    ledger_result = finish_semantic_delivery(
        attempt,
        provider_result,
    )
    with _LOCK:
        current = _SEMANTIC_ATTEMPTS.get(key)
        if current is state:
            state.ledger_results[unit_index] = dict(ledger_result)
            state.active_attempt = None
            state.active_unit = None
            if ledger_result.get("outcome") == "delivered":
                state.next_unit = max(state.next_unit, unit_index + 1)
    return ledger_result


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
        semantic_state = _SEMANTIC_ATTEMPTS.get(key)
    if not intents or semantic_state is None:
        # Provider success alone is not delivery authority. A matching claim
        # must have durably recorded the exact identity before any Hub review
        # callback is released.
        return False

    if semantic_state.active_attempt is not None:
        semantic_result = record_preview_delivery_result(
            session_key,
            generation,
            delivered_content=delivered_content,
            result=result,
        )
        if (
            not isinstance(semantic_result, dict)
            or semantic_result.get("outcome") != "delivered"
        ):
            return False
    message_ids: list[str] = []
    for unit_index in range(len(semantic_state.units)):
        unit_result = semantic_state.ledger_results.get(unit_index)
        if (
            not isinstance(unit_result, dict)
            or unit_result.get("outcome") != "delivered"
        ):
            return False
        for message_id in _message_ids_from_mapping(unit_result):
            if message_id not in message_ids:
                message_ids.append(message_id)
    if (
        delivered_content != semantic_state.delivered_content
        or semantic_state.next_unit != len(semantic_state.units)
        or not message_ids
        or delivered_content != _canonical_ordered_preview(intents)
    ):
        return False

    ack_ids: list[str] = []
    stable_delivered_at = str(delivered_at)
    if semantic_state.bridge_id is not None:
        try:
            bridge = reconcile_preview_ack_bridge(
                semantic_state.bridge_id,
                delivered_at=stable_delivered_at,
            )
        except Exception:
            return False
        if bridge is None or bridge.get("state") != "enqueued":
            return False
        ack_ids = [str(value) for value in bridge.get("ackIds", ())]
        stable_delivered_at = str(
            bridge.get("deliveredAt") or stable_delivered_at
        )
        if len(ack_ids) != len(intents):
            return False

    for intent_index, intent in enumerate(intents):
        receipt = ProviderDeliveryReceipt(
            provider_message_ids=tuple(message_ids),
            delivered_at=stable_delivered_at,
            delivery_nonce=intent.delivery_nonce,
            delivery_payload_digest=intent.delivery_payload_digest,
            delivery_content_digest=intent.delivery_content_digest,
        )
        acknowledgement = intent.acknowledgement_request
        if acknowledgement is not None:
            if not ack_ids:
                return False
            ack_id = ack_ids[intent_index]
            status = preview_ack_status(ack_id)
            if status is None or status["state"] == "rejected":
                return False
            if status["state"] == "succeeded":
                continue
            claim = claim_preview_ack(ack_id)
            if claim is None:
                # Another live worker owns it, or its retry is already durably
                # scheduled. Provider delivery can now be confirmed without
                # replaying the provider message.
                continue
            try:
                outcome = await asyncio.to_thread(
                    intent.acknowledge,
                    receipt,
                )
                if inspect.isawaitable(outcome):
                    await outcome
            except Exception as exc:
                retry_preview_ack(
                    claim.ack_id,
                    claim.lease_token,
                    error=str(
                        getattr(exc, "code", None)
                        or type(exc).__name__
                    ),
                    attempts=claim.attempts,
                )
                continue
            if not complete_preview_ack(
                claim.ack_id,
                claim.lease_token,
            ):
                # A concurrently recovered lease owns the same byte-identical
                # request. The durable row, not this process, is authoritative.
                status = preview_ack_status(claim.ack_id)
                if status is None or status["state"] == "rejected":
                    return False
            continue

        # Compatibility for non-Planning callers/tests that registered only a
        # process-local callback. Product preview delivery always supplies the
        # durable request contract above.
        try:
            outcome = await asyncio.to_thread(
                intent.acknowledge,
                receipt,
            )
            if inspect.isawaitable(outcome):
                await outcome
        except Exception:
            return False
    with _LOCK:
        current = _INTENTS.get(key)
        if current is not None and tuple(current) == intents:
            _INTENTS.pop(key, None)
        _SEMANTIC_ATTEMPTS.pop(key, None)
    return True
