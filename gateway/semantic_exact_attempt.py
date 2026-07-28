"""Provider-neutral one-write entrypoint for durable semantic delivery.

Capability is structural as well as declarative: a bound adapter must own
``send_semantic_exact_attempt`` directly on its concrete class. Ordinary
``send`` / ``_send_with_retry`` are never treated as proof because they may
chunk, retry, probe, transform, or fall back.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any, Mapping


_CAPABILITY_CONTRACT = "hermes-live-semantic-exact-attempt/1"
_PROVIDER = re.compile(r"^[a-z][a-z0-9_-]{0,119}$")
_PROVIDER_ROUTE_KEY = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_FORBIDDEN_PROVIDER_ROUTE_KEY_PARTS = (
    "auth",
    "cookie",
    "credential",
    "key",
    "password",
    "secret",
    "token",
    "url",
    "webhook",
)
_LENGTH_SEMANTICS = frozenset(
    {"unicode_codepoints", "utf16_code_units"}
)
_REJECTION_PREVIEW_CHARACTERS = 200
_REJECTION_REDACTION_WINDOW_CHARACTERS = 8192


@dataclass(frozen=True, slots=True)
class LiveSemanticExactAttemptCapability:
    """Audited immutable logical-unit contract owned by one adapter class."""

    provider: str
    contract: str
    segmentation_version: str
    max_logical_units: int
    length_semantics: str
    wire_encoding: str


@dataclass(frozen=True, slots=True)
class LiveSemanticExactAttemptEncodingContract:
    """Frozen encoder identity carried by every exact provider write.

    A queued delivery must continue with the encoder it was segmented and
    signed for. Deploying a new adapter capability therefore cannot silently
    reinterpret an older outbox row.
    """

    provider: str
    contract: str
    segmentation_version: str
    max_logical_units: int
    length_semantics: str
    wire_encoding: str

    def as_mapping(self) -> dict[str, str | int]:
        return {
            "provider": self.provider,
            "contract": self.contract,
            "segmentation_version": self.segmentation_version,
            "max_logical_units": self.max_logical_units,
            "length_semantics": self.length_semantics,
            "wire_encoding": self.wire_encoding,
        }


@dataclass(frozen=True, slots=True)
class LiveSemanticExactAttemptRequest:
    chat_id: str
    content: str
    delivery_contract: str
    delivery_id: str
    delivery_target: str
    delivery_unit: int
    encoding_contract: LiveSemanticExactAttemptEncodingContract
    provider_route: tuple[tuple[str, str], ...] = ()
    thread_id: str | None = None
    reply_to: str | None = None


def provider_rejection_evidence(
    *,
    provider: str,
    status: int,
    body: Any,
) -> dict[str, Any]:
    """Preserve bounded, redacted evidence for a provider rejection.

    Provider bodies are untrusted and can contain credentials or be
    arbitrarily large. The human-readable preview is therefore bounded after
    forced redaction. The digest and counts bind the exact representation that
    the adapter supplied: raw bytes, decoded UTF-8 text, or canonical JSON when
    an SDK exposes only a decoded value. ``truncated`` makes the diagnostic
    boundary explicit instead of silently slicing the rejection.
    """

    content = _provider_rejection_content(body)
    return {
        "schema_version": "hermes.provider-rejection-evidence/1",
        "provider": str(provider),
        "status": int(status),
        "body_representation": content["representation"],
        "body_sha256": content["sha256"],
        "body_bytes": content["bytes"],
        "body_characters": content["characters"],
        "body_preview": content["preview"],
        "preview_characters": content["preview_characters"],
        "redaction_window_characters": content[
            "redaction_window_characters"
        ],
        "truncated": content["truncated"],
        "redacted": content["redacted"],
    }


def provider_protocol_rejection_evidence(
    *,
    provider: str,
    protocol: str,
    response: Any,
) -> dict[str, Any]:
    """Preserve a bounded native-protocol rejection without faking HTTP."""

    content = _provider_rejection_content(response)
    return {
        "schema_version": (
            "hermes.provider-protocol-rejection-evidence/1"
        ),
        "provider": str(provider),
        "protocol": str(protocol),
        "response_representation": content["representation"],
        "response_sha256": content["sha256"],
        "response_bytes": content["bytes"],
        "response_characters": content["characters"],
        "response_preview": content["preview"],
        "preview_characters": content["preview_characters"],
        "redaction_window_characters": content[
            "redaction_window_characters"
        ],
        "truncated": content["truncated"],
        "redacted": content["redacted"],
    }


def _provider_rejection_content(value: Any) -> dict[str, Any]:
    if isinstance(value, bytes):
        representation = "raw_bytes"
        raw = value
        text = value.decode("utf-8", errors="replace")
    elif isinstance(value, str):
        representation = "utf8_text"
        text = value
        raw = value.encode("utf-8", errors="replace")
    else:
        representation = "canonical_json"
        try:
            text = json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            )
        except Exception:
            text = str(value)
        raw = text.encode("utf-8", errors="replace")
    redaction_window = text[
        :_REJECTION_REDACTION_WINDOW_CHARACTERS
    ]
    try:
        from agent.redact import redact_sensitive_text

        redacted = redact_sensitive_text(
            redaction_window,
            force=True,
            redact_url_credentials=True,
        )
    except Exception:
        redacted = "<provider response redacted>"
    preview = redacted[:_REJECTION_PREVIEW_CHARACTERS]
    return {
        "representation": representation,
        "sha256": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
        "characters": len(text),
        "preview": preview,
        "preview_characters": len(preview),
        "redaction_window_characters": min(
            len(text),
            _REJECTION_REDACTION_WINDOW_CHARACTERS,
        ),
        "truncated": (
            len(text) > _REJECTION_PREVIEW_CHARACTERS
            or len(redacted) > _REJECTION_PREVIEW_CHARACTERS
        ),
        "redacted": redacted != redaction_window,
    }


def provider_rejection_error(evidence: Mapping[str, Any]) -> str:
    """Return a non-secret summary that points at structured rejection data."""

    if (
        evidence.get("schema_version")
        == "hermes.provider-protocol-rejection-evidence/1"
    ):
        representation = evidence.get(
            "response_representation",
            "unknown-representation",
        )
        return (
            f"{evidence.get('provider', 'provider')} "
            f"{evidence.get('protocol', 'protocol')} rejection; response "
            f"{evidence.get('response_sha256', 'digest-unavailable')} "
            f"({evidence.get('response_bytes', 0)} bytes, {representation}, "
            "structured evidence attached)"
        )
    representation = evidence.get(
        "body_representation",
        "unknown-representation",
    )
    return (
        f"{evidence.get('provider', 'provider')} HTTP "
        f"{evidence.get('status', 'unknown')}; response "
        f"{evidence.get('body_sha256', 'digest-unavailable')} "
        f"({evidence.get('body_bytes', 0)} bytes, {representation}, "
        "structured evidence attached)"
    )


def coerce_live_semantic_exact_attempt_provider_route(
    value: Mapping[str, Any] | tuple[tuple[str, str], ...] | None,
) -> tuple[tuple[str, str], ...]:
    """Canonicalize a small, non-secret provider route discriminator.

    The durable route may identify a transport or chat kind, but it may never
    carry a webhook URL, token, credential, or other provider secret.
    """

    if value is None:
        return ()
    if isinstance(value, Mapping):
        items = tuple(value.items())
    elif isinstance(value, tuple):
        items = value
    else:
        raise ValueError("semantic exact-attempt provider route invalid")
    if len(items) > 12:
        raise ValueError("semantic exact-attempt provider route invalid")
    normalized: list[tuple[str, str]] = []
    for item in items:
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError("semantic exact-attempt provider route invalid")
        key, raw_value = item
        if not isinstance(key, str) or not isinstance(raw_value, str):
            raise ValueError("semantic exact-attempt provider route invalid")
        clean_key = key.strip().lower()
        clean_value = raw_value.strip()
        if (
            clean_key != key
            or clean_value != raw_value
            or _PROVIDER_ROUTE_KEY.fullmatch(clean_key) is None
            or any(
                part in clean_key
                for part in _FORBIDDEN_PROVIDER_ROUTE_KEY_PARTS
            )
            or not clean_value
            or len(clean_value) > 240
            or "://" in clean_value
            or any(
                ord(character) < 32 or ord(character) == 127
                for character in clean_value
            )
        ):
            raise ValueError("semantic exact-attempt provider route invalid")
        normalized.append((clean_key, clean_value))
    canonical = tuple(sorted(normalized))
    if len({key for key, _ in canonical}) != len(canonical):
        raise ValueError("semantic exact-attempt provider route invalid")
    return canonical


def live_semantic_exact_attempt_provider_route_mapping(
    value: Mapping[str, Any] | tuple[tuple[str, str], ...] | None,
) -> dict[str, str]:
    return dict(coerce_live_semantic_exact_attempt_provider_route(value))


def bind_live_semantic_exact_attempt_provider_route(
    adapter: Any,
    *,
    chat_id: str,
    thread_id: str | None = None,
    reply_to: str | None = None,
) -> tuple[tuple[str, str], ...]:
    """Freeze a concrete adapter's non-secret route before provider I/O.

    Most providers need no additional discriminator. Providers whose chat ID
    is ambiguous across transports must own a direct synchronous binder on
    their concrete adapter class. A missing live route fails before staging.
    """

    owned_method = vars(type(adapter)).get(
        "bind_semantic_exact_attempt_provider_route"
    )
    if owned_method is None:
        return ()
    method = getattr(
        adapter,
        "bind_semantic_exact_attempt_provider_route",
        None,
    )
    if not callable(owned_method) or not callable(method):
        raise ValueError("semantic exact-attempt provider route binder invalid")
    return coerce_live_semantic_exact_attempt_provider_route(
        method(
            chat_id=str(chat_id),
            thread_id=thread_id,
            reply_to=reply_to,
        )
    )


def coerce_live_semantic_exact_attempt_encoding_contract(
    value: (
        LiveSemanticExactAttemptEncodingContract | Mapping[str, Any]
    ),
) -> LiveSemanticExactAttemptEncodingContract:
    if isinstance(value, LiveSemanticExactAttemptEncodingContract):
        candidate = value
    elif isinstance(value, Mapping):
        expected = {
            "provider",
            "contract",
            "segmentation_version",
            "max_logical_units",
            "length_semantics",
            "wire_encoding",
        }
        if set(value) != expected:
            raise ValueError("semantic exact-attempt encoding contract invalid")
        budget = value["max_logical_units"]
        if isinstance(budget, bool) or not isinstance(budget, int):
            raise ValueError("semantic exact-attempt encoding contract invalid")
        candidate = LiveSemanticExactAttemptEncodingContract(
            provider=str(value["provider"] or ""),
            contract=str(value["contract"] or ""),
            segmentation_version=str(
                value["segmentation_version"] or ""
            ),
            max_logical_units=budget,
            length_semantics=str(value["length_semantics"] or ""),
            wire_encoding=str(value["wire_encoding"] or ""),
        )
    else:
        raise ValueError("semantic exact-attempt encoding contract invalid")
    string_fields = (
        candidate.provider,
        candidate.contract,
        candidate.segmentation_version,
        candidate.length_semantics,
        candidate.wire_encoding,
    )
    if (
        _PROVIDER.fullmatch(candidate.provider) is None
        or candidate.contract != _CAPABILITY_CONTRACT
        or candidate.max_logical_units < 1
        or candidate.length_semantics not in _LENGTH_SEMANTICS
        or any(
            not field
            or field != field.strip()
            or len(field) > 240
            or any(
                ord(character) < 33 or ord(character) == 127
                for character in field
            )
            for field in string_fields
        )
    ):
        raise ValueError("semantic exact-attempt encoding contract invalid")
    return candidate


def semantic_exact_attempt_encoding_contract(
    capability: LiveSemanticExactAttemptCapability,
) -> LiveSemanticExactAttemptEncodingContract:
    if not isinstance(capability, LiveSemanticExactAttemptCapability):
        raise ValueError("semantic exact-attempt capability invalid")
    return coerce_live_semantic_exact_attempt_encoding_contract(
        {
            "provider": capability.provider,
            "contract": capability.contract,
            "segmentation_version": capability.segmentation_version,
            "max_logical_units": capability.max_logical_units,
            "length_semantics": capability.length_semantics,
            "wire_encoding": capability.wire_encoding,
        }
    )


def semantic_exact_attempt_encoding_contract_digest(
    value: (
        LiveSemanticExactAttemptEncodingContract | Mapping[str, Any]
    ),
) -> str:
    contract = coerce_live_semantic_exact_attempt_encoding_contract(value)
    canonical = json.dumps(
        contract.as_mapping(),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def coerce_live_semantic_exact_attempt_request(
    request: LiveSemanticExactAttemptRequest | Mapping[str, Any],
) -> LiveSemanticExactAttemptRequest:
    if isinstance(request, LiveSemanticExactAttemptRequest):
        candidate = request
    elif not isinstance(request, Mapping):
        raise ValueError("semantic exact-attempt request invalid")
    else:
        expected = {
            "chat_id",
            "content",
            "delivery_contract",
            "delivery_id",
            "delivery_target",
            "delivery_unit",
            "encoding_contract",
            "thread_id",
            "reply_to",
        }
        if set(request) not in (expected, expected | {"provider_route"}):
            raise ValueError("semantic exact-attempt request invalid")
        unit = request["delivery_unit"]
        if isinstance(unit, bool) or not isinstance(unit, int) or unit < 0:
            raise ValueError("semantic exact-attempt unit invalid")
        required = {
            key: str(request[key] or "")
            for key in (
                "chat_id",
                "content",
                "delivery_contract",
                "delivery_id",
                "delivery_target",
            )
        }
        candidate = LiveSemanticExactAttemptRequest(
            **required,
            delivery_unit=unit,
            encoding_contract=(
                coerce_live_semantic_exact_attempt_encoding_contract(
                    request["encoding_contract"]
                )
            ),
            provider_route=(
                coerce_live_semantic_exact_attempt_provider_route(
                    request.get("provider_route")
                )
            ),
            thread_id=(
                str(request["thread_id"])
                if request["thread_id"] is not None
                else None
            ),
            reply_to=(
                str(request["reply_to"])
                if request["reply_to"] is not None
                else None
            ),
        )
    required_values = (
        candidate.chat_id,
        candidate.content,
        candidate.delivery_contract,
        candidate.delivery_id,
        candidate.delivery_target,
    )
    if (
        isinstance(candidate.delivery_unit, bool)
        or not isinstance(candidate.delivery_unit, int)
        or candidate.delivery_unit < 0
        or any(not str(value or "") for value in required_values)
    ):
        raise ValueError("semantic exact-attempt request incomplete")
    encoding_contract = (
        coerce_live_semantic_exact_attempt_encoding_contract(
            candidate.encoding_contract
        )
    )
    if encoding_contract.provider != candidate.delivery_target.partition(":")[0]:
        raise ValueError("semantic exact-attempt encoding provider mismatch")
    provider_route = coerce_live_semantic_exact_attempt_provider_route(
        candidate.provider_route
    )
    if (
        encoding_contract is candidate.encoding_contract
        and provider_route is candidate.provider_route
    ):
        return candidate
    return LiveSemanticExactAttemptRequest(
        chat_id=candidate.chat_id,
        content=candidate.content,
        delivery_contract=candidate.delivery_contract,
        delivery_id=candidate.delivery_id,
        delivery_target=candidate.delivery_target,
        delivery_unit=candidate.delivery_unit,
        encoding_contract=encoding_contract,
        provider_route=provider_route,
        thread_id=candidate.thread_id,
        reply_to=candidate.reply_to,
    )


def owns_live_semantic_exact_attempt(adapter: Any) -> bool:
    """True only for a concrete-class override, never an inherited default."""

    if adapter is None:
        return False
    method = vars(type(adapter)).get("send_semantic_exact_attempt")
    capability = vars(type(adapter)).get(
        "SEMANTIC_EXACT_ATTEMPT_CAPABILITY"
    )
    provider = str(
        getattr(
            getattr(adapter, "platform", None),
            "value",
            getattr(adapter, "platform", ""),
        )
        or ""
    ).strip().lower()
    if (
        not callable(method)
        or not callable(getattr(adapter, "send_semantic_exact_attempt", None))
        or not isinstance(capability, LiveSemanticExactAttemptCapability)
        or capability.provider != provider
    ):
        return False
    try:
        current = semantic_exact_attempt_encoding_contract(capability)
        advertised = _advertised_encoding_contracts(adapter, current=current)
    except ValueError:
        return False
    return current in advertised


def _advertised_encoding_contracts(
    adapter: Any,
    *,
    current: LiveSemanticExactAttemptEncodingContract,
) -> tuple[LiveSemanticExactAttemptEncodingContract, ...]:
    """Return current plus explicitly retained historical encoder contracts."""

    raw = vars(type(adapter)).get(
        "SEMANTIC_EXACT_ATTEMPT_ENCODING_CONTRACTS"
    )
    if raw is None:
        return (current,)
    if not isinstance(raw, tuple) or not raw:
        raise ValueError("semantic exact-attempt encoding contracts invalid")
    contracts = tuple(
        coerce_live_semantic_exact_attempt_encoding_contract(value)
        for value in raw
    )
    if (
        current not in contracts
        or any(contract.provider != current.provider for contract in contracts)
        or len(set(contracts)) != len(contracts)
    ):
        raise ValueError("semantic exact-attempt encoding contracts invalid")
    return contracts


def live_semantic_exact_attempt_capability(
    adapter: Any,
) -> LiveSemanticExactAttemptCapability | None:
    if not owns_live_semantic_exact_attempt(adapter):
        return None
    return vars(type(adapter))["SEMANTIC_EXACT_ATTEMPT_CAPABILITY"]


def live_semantic_exact_attempt_encoding_contract(
    adapter: Any,
) -> LiveSemanticExactAttemptEncodingContract | None:
    capability = live_semantic_exact_attempt_capability(adapter)
    if capability is None:
        return None
    return semantic_exact_attempt_encoding_contract(capability)


def supports_live_semantic_exact_attempt_encoding_contract(
    adapter: Any,
    value: LiveSemanticExactAttemptEncodingContract | Mapping[str, Any],
) -> bool:
    if not owns_live_semantic_exact_attempt(adapter):
        return False
    try:
        requested = coerce_live_semantic_exact_attempt_encoding_contract(value)
        current = semantic_exact_attempt_encoding_contract(
            vars(type(adapter))["SEMANTIC_EXACT_ATTEMPT_CAPABILITY"]
        )
        return requested in _advertised_encoding_contracts(
            adapter,
            current=current,
        )
    except ValueError:
        return False


async def send_via_exact_adapter_method(
    adapter: Any,
    request: LiveSemanticExactAttemptRequest | Mapping[str, Any],
) -> Any:
    """Invoke the concrete provider primitive exactly once."""

    if not owns_live_semantic_exact_attempt(adapter):
        raise ValueError("semantic exact-attempt adapter override required")
    coerced = coerce_live_semantic_exact_attempt_request(request)
    if not supports_live_semantic_exact_attempt_encoding_contract(
        adapter,
        coerced.encoding_contract,
    ):
        raise ValueError("semantic exact-attempt encoding unsupported")
    return await adapter.send_semantic_exact_attempt(coerced)


async def semantic_exact_attempt_via_send(
    adapter: Any,
    request: LiveSemanticExactAttemptRequest | Mapping[str, Any],
) -> Any:
    """Shared implementation used only by audited concrete overrides.

    It calls ``adapter.send`` exactly once. The provider's semantic metadata
    branch must itself be wire-tested as a single provider write with no
    fallback; assigning this helper without those tests is not conformance.
    """

    coerced = coerce_live_semantic_exact_attempt_request(request)
    if not supports_live_semantic_exact_attempt_encoding_contract(
        adapter,
        coerced.encoding_contract,
    ):
        raise ValueError("semantic exact-attempt encoding unsupported")
    metadata = {
        "semantic_delivery_contract": coerced.delivery_contract,
        "semantic_delivery_id": coerced.delivery_id,
        "semantic_delivery_target": coerced.delivery_target,
        "semantic_delivery_unit": coerced.delivery_unit,
        "semantic_encoding_contract": (
            coerced.encoding_contract.as_mapping()
        ),
        "semantic_encoding_contract_digest": (
            semantic_exact_attempt_encoding_contract_digest(
                coerced.encoding_contract
            )
        ),
        "semantic_provider_route": (
            live_semantic_exact_attempt_provider_route_mapping(
                coerced.provider_route
            )
        ),
    }
    if coerced.thread_id is not None:
        metadata["thread_id"] = coerced.thread_id
    return await adapter.send(
        chat_id=coerced.chat_id,
        content=coerced.content,
        reply_to=coerced.reply_to,
        metadata=metadata,
    )


__all__ = [
    "LiveSemanticExactAttemptCapability",
    "LiveSemanticExactAttemptEncodingContract",
    "LiveSemanticExactAttemptRequest",
    "bind_live_semantic_exact_attempt_provider_route",
    "coerce_live_semantic_exact_attempt_encoding_contract",
    "coerce_live_semantic_exact_attempt_provider_route",
    "coerce_live_semantic_exact_attempt_request",
    "live_semantic_exact_attempt_provider_route_mapping",
    "live_semantic_exact_attempt_capability",
    "live_semantic_exact_attempt_encoding_contract",
    "owns_live_semantic_exact_attempt",
    "provider_rejection_error",
    "provider_rejection_evidence",
    "provider_protocol_rejection_evidence",
    "semantic_exact_attempt_encoding_contract",
    "semantic_exact_attempt_encoding_contract_digest",
    "semantic_exact_attempt_via_send",
    "send_via_exact_adapter_method",
    "supports_live_semantic_exact_attempt_encoding_contract",
]
