"""Versioned, platform-neutral origin metadata for one conversation turn.

``TurnOriginV1`` is deliberately small and JSON-shaped.  It identifies the
gateway event that caused a turn without coupling the agent or plugin layers
to any concrete messaging adapter.  Gateway callers bind it for the lifetime
of ``AIAgent.run_conversation``; observer hooks and middleware can then receive
the same immutable origin even when tool dispatch moves onto worker threads.

The envelope is observability/routing context only.  It is not injected into
the model prompt or persisted into conversation history, preserving existing
prompt-cache and transcript behaviour.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, ClassVar, Iterator, Mapping, MutableMapping, Optional


TURN_ORIGIN_SCHEMA_VERSION = "hermes.turn_origin.v1"
TURN_ATTACHMENT_SCHEMA_VERSION = "hermes.turn_attachment.v1"


def _optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def serialize_source_timestamp(value: Any) -> Optional[str]:
    """Return a stable JSON-safe source timestamp.

    Providers currently supply a mix of aware/naive ``datetime`` objects,
    epoch seconds, and already-normalized strings.  Preserve a naive
    datetime's lack of timezone instead of guessing one; normalize aware
    datetimes and epoch values to UTC with an RFC3339 ``Z`` suffix.
    """

    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat()
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return (
                datetime.fromtimestamp(float(value), tz=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
        except (OverflowError, OSError, ValueError):
            return _optional_text(value)
    return _optional_text(value)


def derive_turn_event_id(
    *,
    provider: Any,
    gateway_account_id: Any = None,
    chat_id: Any = None,
    thread_id: Any = None,
    message_id: Any = None,
    sender_id: Any = None,
    source_timestamp: Any = None,
    upstream_event_id: Any = None,
) -> Optional[str]:
    """Derive a stable event id without ever hashing message text.

    A provider update id or message id is preferred as the event discriminator.
    The surrounding account/chat/thread identity is included because many
    providers scope message ids to a chat or bot account.  A source timestamp
    is the final fallback for providers that expose no event/message id.
    """

    normalized_timestamp = serialize_source_timestamp(source_timestamp)
    normalized_upstream_id = _optional_text(upstream_event_id)
    normalized_message_id = _optional_text(message_id)
    if not (normalized_upstream_id or normalized_message_id or normalized_timestamp):
        return None

    identity = {
        "schema_version": TURN_ORIGIN_SCHEMA_VERSION,
        "provider": _optional_text(provider),
        "gateway_account_id": _optional_text(gateway_account_id),
        "chat_id": _optional_text(chat_id),
        "thread_id": _optional_text(thread_id),
        "message_id": normalized_message_id,
        "sender_id": _optional_text(sender_id),
        # A provider update/message id should remain stable across retries even
        # when Hermes only has a new receipt timestamp on replay. Timestamp is
        # therefore an identity discriminator only when no durable upstream id
        # exists.
        "source_timestamp": (
            None
            if normalized_upstream_id or normalized_message_id
            else normalized_timestamp
        ),
        "upstream_event_id": normalized_upstream_id,
    }
    encoded = json.dumps(
        identity,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"evt_v1_{hashlib.sha256(encoded).hexdigest()[:32]}"


def turn_attachment_path_fingerprint(value: Any) -> Optional[str]:
    """Return an opaque gateway-local path match key.

    The fingerprint lets a tool relate the model-visible cached path back to
    the immutable ingress ordinal without carrying the absolute path in tool
    output, provider metadata, or an idempotency key.
    """

    path = _optional_text(value)
    if path is None:
        return None
    return "sha256:" + hashlib.sha256(path.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True, kw_only=True)
class TurnAttachmentOriginV1:
    """Opaque identity plus process-private source for one ingress attachment."""

    SCHEMA_VERSION: ClassVar[str] = TURN_ATTACHMENT_SCHEMA_VERSION

    attachment_id: str
    ingress_ordinal: int
    path_fingerprint: str
    # The cached path is runtime capability state, never origin wire data. It
    # lets a trusted facade durably spool every attachment without putting a
    # filesystem path in model arguments, hook payloads, logs, or Hub identity.
    local_path: Optional[str] = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        attachment_id = _optional_text(self.attachment_id)
        path_fingerprint = _optional_text(self.path_fingerprint)
        if attachment_id is None or path_fingerprint is None:
            raise ValueError("attachment identity and path fingerprint are required")
        if isinstance(self.ingress_ordinal, bool):
            raise ValueError("attachment ingress ordinal must be positive")
        ordinal = int(self.ingress_ordinal)
        if ordinal < 1:
            raise ValueError("attachment ingress ordinal must be positive")
        object.__setattr__(self, "attachment_id", attachment_id)
        object.__setattr__(self, "ingress_ordinal", ordinal)
        object.__setattr__(self, "path_fingerprint", path_fingerprint)
        local_path = _optional_text(self.local_path)
        if (
            local_path is not None
            and turn_attachment_path_fingerprint(local_path)
            != path_fingerprint
        ):
            raise ValueError(
                "attachment local path does not match its path fingerprint"
            )
        object.__setattr__(self, "local_path", local_path)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "TurnAttachmentOriginV1":
        schema_version = value.get("schema_version")
        if schema_version not in (None, "", TURN_ATTACHMENT_SCHEMA_VERSION):
            raise ValueError(
                f"Unsupported turn-attachment schema: {schema_version}"
            )
        return cls(
            attachment_id=value.get("attachment_id") or value.get("id"),
            ingress_ordinal=(
                value.get("ingress_ordinal")
                if "ingress_ordinal" in value
                else value.get("ordinal")
            ),
            path_fingerprint=(
                value.get("path_fingerprint")
                if "path_fingerprint" in value
                else value.get("pathFingerprint")
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": TURN_ATTACHMENT_SCHEMA_VERSION,
            "attachment_id": self.attachment_id,
            "ingress_ordinal": self.ingress_ordinal,
            "path_fingerprint": self.path_fingerprint,
        }


def derive_turn_attachment_origins(
    *,
    provider: Any,
    event_id: Any,
    media_paths: Any,
    provider_attachment_ids: Any = None,
) -> tuple[TurnAttachmentOriginV1, ...]:
    """Bind cached media paths to provider identity or a stable ingress ordinal."""

    if not isinstance(media_paths, (list, tuple)):
        return ()
    raw_ids = (
        list(provider_attachment_ids)
        if isinstance(provider_attachment_ids, (list, tuple))
        else []
    )
    provider_text = _optional_text(provider) or "unknown"
    event_text = _optional_text(event_id)
    attachments: list[TurnAttachmentOriginV1] = []
    for index, raw_path in enumerate(media_paths, 1):
        path_fingerprint = turn_attachment_path_fingerprint(raw_path)
        if path_fingerprint is None:
            continue
        provider_attachment_id = (
            _optional_text(raw_ids[index - 1])
            if index <= len(raw_ids)
            else None
        )
        identity = {
            "schema_version": TURN_ATTACHMENT_SCHEMA_VERSION,
            "provider": provider_text,
            "event_id": event_text,
            "provider_attachment_id": provider_attachment_id,
            # The ordinal remains part of the identity even when a provider id
            # exists. Some providers reuse a media id when one file is attached
            # more than once to the same event.
            "ingress_ordinal": index,
        }
        digest = hashlib.sha256(
            json.dumps(
                identity,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        attachments.append(
            TurnAttachmentOriginV1(
                attachment_id=f"att_v1_{digest[:32]}",
                ingress_ordinal=index,
                path_fingerprint=path_fingerprint,
                local_path=str(raw_path),
            )
        )
    return tuple(attachments)


@dataclass(frozen=True, slots=True, kw_only=True)
class TurnOriginV1:
    """Generic origin envelope attached to one user-facing turn."""

    SCHEMA_VERSION: ClassVar[str] = TURN_ORIGIN_SCHEMA_VERSION

    provider: str
    gateway_account_id: Optional[str] = None
    chat_id: Optional[str] = None
    thread_id: Optional[str] = None
    message_id: Optional[str] = None
    sender_id: Optional[str] = None
    chat_type: Optional[str] = None
    source_timestamp: Optional[str] = None
    event_id: Optional[str] = None
    attachments: tuple[TurnAttachmentOriginV1, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "provider", _optional_text(self.provider) or "unknown")
        for field_name in (
            "gateway_account_id",
            "chat_id",
            "thread_id",
            "message_id",
            "sender_id",
            "chat_type",
            "event_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _optional_text(getattr(self, field_name)),
            )
        object.__setattr__(
            self,
            "source_timestamp",
            serialize_source_timestamp(self.source_timestamp),
        )
        normalized_attachments: list[TurnAttachmentOriginV1] = []
        for item in self.attachments or ():
            if isinstance(item, TurnAttachmentOriginV1):
                normalized_attachments.append(item)
            elif isinstance(item, Mapping):
                normalized_attachments.append(
                    TurnAttachmentOriginV1.from_mapping(item)
                )
            else:
                raise ValueError("turn attachments must be mappings")
        if len(
            {attachment.ingress_ordinal for attachment in normalized_attachments}
        ) != len(normalized_attachments):
            raise ValueError("turn attachment ingress ordinals must be unique")
        object.__setattr__(self, "attachments", tuple(normalized_attachments))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "TurnOriginV1":
        """Coerce a JSON-shaped envelope, accepting legacy field aliases."""

        schema_version = value.get("schema_version")
        if schema_version not in (None, "", TURN_ORIGIN_SCHEMA_VERSION):
            raise ValueError(f"Unsupported turn-origin schema: {schema_version}")
        return cls(
            provider=value.get("provider") or value.get("platform") or "unknown",
            gateway_account_id=(
                value.get("gateway_account_id")
                if "gateway_account_id" in value
                else value.get("account_id")
            ),
            chat_id=value.get("chat_id"),
            thread_id=value.get("thread_id"),
            message_id=value.get("message_id"),
            sender_id=(
                value.get("sender_id")
                if "sender_id" in value
                else value.get("user_id")
            ),
            chat_type=value.get("chat_type"),
            source_timestamp=(
                value.get("source_timestamp")
                if "source_timestamp" in value
                else value.get("timestamp")
            ),
            event_id=value.get("event_id"),
            attachments=tuple(value.get("attachments") or ()),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the stable hook/middleware wire shape, including nulls."""

        payload = {
            "schema_version": TURN_ORIGIN_SCHEMA_VERSION,
            "provider": self.provider,
            "gateway_account_id": self.gateway_account_id,
            "chat_id": self.chat_id,
            "thread_id": self.thread_id,
            "message_id": self.message_id,
            "sender_id": self.sender_id,
            "chat_type": self.chat_type,
            "source_timestamp": self.source_timestamp,
            "event_id": self.event_id,
        }
        # Preserve the exact historical hook shape for turns without media.
        # Attachment identity is additive and contains no provider id or path.
        if self.attachments:
            payload["attachments"] = [
                attachment.to_dict() for attachment in self.attachments
            ]
        return payload


def coerce_turn_origin(value: Any) -> Optional[TurnOriginV1]:
    """Best-effort conversion used at public/backward-compatible boundaries."""

    if value is None:
        return None
    if isinstance(value, TurnOriginV1):
        return value
    if isinstance(value, Mapping):
        try:
            return TurnOriginV1.from_mapping(value)
        except (TypeError, ValueError):
            return None
    return None


_CURRENT_TURN_ORIGIN: ContextVar[Optional[TurnOriginV1]] = ContextVar(
    "hermes_current_turn_origin",
    default=None,
)
_CURRENT_TURN_USER_TEXT: ContextVar[Optional[str]] = ContextVar(
    "hermes_current_turn_user_text",
    default=None,
)
_CURRENT_TURN_DELIVERY_ADAPTER: ContextVar[Any | None] = ContextVar(
    "hermes_current_turn_delivery_adapter",
    default=None,
)


def get_current_turn_origin() -> Optional[TurnOriginV1]:
    return _CURRENT_TURN_ORIGIN.get()


def get_current_turn_origin_payload() -> Optional[dict[str, Any]]:
    origin = get_current_turn_origin()
    return origin.to_dict() if origin is not None else None


def _coerce_turn_user_text(value: Any) -> Optional[str]:
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        parts: list[str] = []
        for item in value:
            if not isinstance(item, Mapping):
                continue
            if item.get("type") in {"text", "input_text"}:
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
        if parts:
            return "\n".join(parts)
    return None


def get_current_turn_user_text() -> Optional[str]:
    """Return the immutable user text bound before model tool arguments exist."""

    return _CURRENT_TURN_USER_TEXT.get()


def get_current_turn_delivery_adapter() -> Any | None:
    """Return the live adapter bound to this gateway turn.

    The adapter is runtime-only authority used for provider capability checks.
    It is never serialized into turn origin, model input, or conversation
    history.
    """

    return _CURRENT_TURN_DELIVERY_ADAPTER.get()


@contextmanager
def scoped_turn_origin(value: Any) -> Iterator[Optional[TurnOriginV1]]:
    """Bind an origin for exactly one conversation turn."""

    origin = coerce_turn_origin(value)
    token = _CURRENT_TURN_ORIGIN.set(origin)
    try:
        yield origin
    finally:
        _CURRENT_TURN_ORIGIN.reset(token)


@contextmanager
def scoped_turn_user_text(value: Any) -> Iterator[Optional[str]]:
    """Bind exact current-turn input for gateway-sensitive side effects."""

    text = _coerce_turn_user_text(value)
    token = _CURRENT_TURN_USER_TEXT.set(text)
    try:
        yield text
    finally:
        _CURRENT_TURN_USER_TEXT.reset(token)


@contextmanager
def scoped_turn_delivery_adapter(value: Any) -> Iterator[Any | None]:
    """Bind the exact live adapter for one gateway conversation execution."""

    token = _CURRENT_TURN_DELIVERY_ADAPTER.set(value)
    try:
        yield value
    finally:
        _CURRENT_TURN_DELIVERY_ADAPTER.reset(token)


def inject_current_turn_origin(payload: MutableMapping[str, Any]) -> None:
    """Add ``turn_origin`` when a gateway turn is currently bound.

    ``setdefault`` semantics are intentional: explicit call-site context wins,
    including an explicit ``None`` used by a compatibility adapter.
    """

    if "turn_origin" in payload:
        return
    current = get_current_turn_origin_payload()
    if current is not None:
        payload["turn_origin"] = current
