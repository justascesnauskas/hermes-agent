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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, ClassVar, Iterator, Mapping, MutableMapping, Optional


TURN_ORIGIN_SCHEMA_VERSION = "hermes.turn_origin.v1"


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
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the stable hook/middleware wire shape, including nulls."""

        return {
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


def get_current_turn_origin() -> Optional[TurnOriginV1]:
    return _CURRENT_TURN_ORIGIN.get()


def get_current_turn_origin_payload() -> Optional[dict[str, Any]]:
    origin = get_current_turn_origin()
    return origin.to_dict() if origin is not None else None


@contextmanager
def scoped_turn_origin(value: Any) -> Iterator[Optional[TurnOriginV1]]:
    """Bind an origin for exactly one conversation turn."""

    origin = coerce_turn_origin(value)
    token = _CURRENT_TURN_ORIGIN.set(origin)
    try:
        yield origin
    finally:
        _CURRENT_TURN_ORIGIN.reset(token)


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
