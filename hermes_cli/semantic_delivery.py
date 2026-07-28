"""Crash-safe, provider-aware delivery for externally owned messages.

The ordinary ``hermes send`` command remains a one-shot convenience surface.
Callers with a durable outbox can opt into this versioned contract and supply
one stable delivery identity. Hermes then stores only the payload digest and
the provider receipt in a private, profile-scoped SQLite ledger.

Exactly-once delivery is not claimed for transports without a durable native
idempotency primitive. If Hermes dies while one of those sends is in flight,
the next process returns a typed ambiguous outcome instead of sending again.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import time
from typing import Any, Protocol
import unicodedata
import uuid

from hermes_constants import get_hermes_home


SEMANTIC_DELIVERY_CONTRACT = "hermes-semantic-delivery/1"
_DELIVERY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,239}$")
_PROVIDER_ID = re.compile(r"^[a-z][a-z0-9_-]{0,119}$")
_REPLAY_SAFE_PROVIDERS = frozenset({"matrix"})
_BOUNDED_NATIVE_PROVIDERS = frozenset({"discord"})
_SCHEMA = """
CREATE TABLE IF NOT EXISTS semantic_deliveries (
    delivery_id TEXT PRIMARY KEY,
    contract_version TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    provider TEXT NOT NULL,
    gateway_account_id TEXT NOT NULL DEFAULT '',
    target TEXT NOT NULL,
    state TEXT NOT NULL
        CHECK (
            state IN (
                'intent', 'sending', 'delivered', 'ambiguous', 'rejected'
            )
        ),
    replay_strategy TEXT NOT NULL
        CHECK (replay_strategy IN ('durable_native', 'bounded_native', 'none')),
    provider_receipt_json TEXT NOT NULL DEFAULT '{}',
    error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""
_METADATA_SCHEMA = """
CREATE TABLE IF NOT EXISTS semantic_delivery_metadata (
    metadata_key TEXT PRIMARY KEY,
    metadata_value TEXT NOT NULL
)
"""
_RETRY_OUTBOX_SCHEMA = """
CREATE TABLE IF NOT EXISTS semantic_delivery_retry_outbox (
    delivery_id TEXT PRIMARY KEY,
    delivery_group_id TEXT NOT NULL,
    unit_index INTEGER NOT NULL,
    unit_count INTEGER NOT NULL,
    contract_version TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    provider TEXT NOT NULL,
    gateway_account_id TEXT NOT NULL,
    target TEXT NOT NULL,
    replay_strategy TEXT NOT NULL
        CHECK (replay_strategy IN ('durable_native', 'bounded_native', 'none')),
    encoding_contract_json TEXT NOT NULL,
    route_json TEXT NOT NULL,
    payload_backend TEXT NOT NULL,
    payload_ref TEXT NOT NULL,
    state TEXT NOT NULL
        CHECK (
            state IN (
                'pending', 'leased', 'retired', 'corrupt'
            )
        ),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL,
    lease_id TEXT,
    lease_expires_at REAL,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""
_RETRY_GROUP_SCHEMA = """
CREATE TABLE IF NOT EXISTS semantic_delivery_retry_groups (
    delivery_group_id TEXT PRIMARY KEY,
    unit_count INTEGER NOT NULL,
    turn_execution_ref TEXT NOT NULL DEFAULT '',
    completion_contract TEXT NOT NULL DEFAULT '',
    completion_ref TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL
        CHECK (
            state IN (
                'staging', 'pending', 'completing', 'completed', 'blocked'
            )
        ),
    completion_attempt_count INTEGER NOT NULL DEFAULT 0,
    next_completion_at REAL NOT NULL DEFAULT 0,
    completion_lease_id TEXT,
    completion_lease_expires_at REAL,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""
_RETRY_DUE_INDEX_SCHEMA = """
CREATE INDEX IF NOT EXISTS semantic_delivery_retry_due
ON semantic_delivery_retry_outbox(state, next_attempt_at)
"""
_RETRY_TURN_EXECUTION_INDEX_SCHEMA = """
CREATE UNIQUE INDEX IF NOT EXISTS semantic_delivery_retry_turn_execution
ON semantic_delivery_retry_groups(turn_execution_ref)
WHERE turn_execution_ref <> ''
"""
_SCOPE_METADATA_KEY = "ledger_scope_id"
SEMANTIC_RETRY_ROUTE_CONTRACT = "hermes-semantic-live-route/3"
_LEGACY_SEMANTIC_RETRY_ROUTE_CONTRACT = "hermes-semantic-live-route/2"
_RETRY_INITIAL_DELAY_SECONDS = 5.0
_RETRY_LEASE_SECONDS = 120.0
_RETRY_BACKOFF_MAX_SECONDS = 300.0
_DIAGNOSTIC_PREVIEW_CHARACTERS = 200
_DIAGNOSTIC_REDACTION_WINDOW_CHARACTERS = 8_192
_PROVIDER_REJECTION_SCHEMA = "hermes.provider-rejection-evidence/1"
_PROVIDER_PROTOCOL_REJECTION_SCHEMA = (
    "hermes.provider-protocol-rejection-evidence/1"
)
_BOUNDED_DIAGNOSTIC_SCHEMA = "hermes.bounded-diagnostic/1"
_DELIVERY_FAILURE_SCHEMA = "hermes.semantic-delivery-failure/1"


def _bounded_diagnostic(value: Any) -> str | dict[str, Any] | None:
    """Keep short safe codes intact and structure every redacted/truncated value."""

    text = str(value or "")
    if not text:
        return None
    raw = text.encode("utf-8", errors="replace")
    window = text[:_DIAGNOSTIC_REDACTION_WINDOW_CHARACTERS]
    try:
        from agent.redact import redact_sensitive_text

        redacted = redact_sensitive_text(
            window,
            force=True,
            redact_url_credentials=True,
        )
    except Exception:
        redacted = "<diagnostic redacted>"
    preview = redacted[:_DIAGNOSTIC_PREVIEW_CHARACTERS]
    truncated = (
        len(text) > _DIAGNOSTIC_PREVIEW_CHARACTERS
        or len(redacted) > _DIAGNOSTIC_PREVIEW_CHARACTERS
    )
    was_redacted = redacted != window
    if (
        not truncated
        and not was_redacted
        and "\x00" not in text
        and len(text) <= 240
    ):
        return text
    return {
        "schema_version": _BOUNDED_DIAGNOSTIC_SCHEMA,
        "text_sha256": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "text_bytes": len(raw),
        "text_characters": len(text),
        "text_preview": preview,
        "preview_characters": len(preview),
        "redaction_window_characters": min(
            len(text),
            _DIAGNOSTIC_REDACTION_WINDOW_CHARACTERS,
        ),
        "truncated": truncated,
        "redacted": was_redacted,
    }


def _safe_bounded_diagnostic(
    value: Any,
) -> str | dict[str, Any] | None:
    if isinstance(value, str):
        return (
            value
            if value
            and len(value) <= 240
            and "\x00" not in value
            else None
        )
    if not isinstance(value, Mapping):
        return None
    expected = {
        "schema_version",
        "text_sha256",
        "text_bytes",
        "text_characters",
        "text_preview",
        "preview_characters",
        "redaction_window_characters",
        "truncated",
        "redacted",
    }
    if set(value) != expected:
        return None
    preview = value.get("text_preview")
    digest = value.get("text_sha256")
    byte_count = value.get("text_bytes")
    character_count = value.get("text_characters")
    preview_count = value.get("preview_characters")
    window_count = value.get("redaction_window_characters")
    if (
        value.get("schema_version") != _BOUNDED_DIAGNOSTIC_SCHEMA
        or not isinstance(digest, str)
        or re.fullmatch(r"sha256:[a-f0-9]{64}", digest) is None
        or isinstance(byte_count, bool)
        or not isinstance(byte_count, int)
        or byte_count < 0
        or isinstance(character_count, bool)
        or not isinstance(character_count, int)
        or character_count < 0
        or not isinstance(preview, str)
        or len(preview) > _DIAGNOSTIC_PREVIEW_CHARACTERS
        or "\x00" in preview
        or isinstance(preview_count, bool)
        or not isinstance(preview_count, int)
        or preview_count != len(preview)
        or isinstance(window_count, bool)
        or not isinstance(window_count, int)
        or window_count != min(
            character_count,
            _DIAGNOSTIC_REDACTION_WINDOW_CHARACTERS,
        )
        or type(value.get("truncated")) is not bool
        or type(value.get("redacted")) is not bool
    ):
        return None
    return dict(value)


def _safe_provider_rejection_evidence(
    value: Any,
) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    schema = value.get("schema_version")
    if schema == _PROVIDER_PROTOCOL_REJECTION_SCHEMA:
        expected = {
            "schema_version",
            "provider",
            "protocol",
            "response_representation",
            "response_sha256",
            "response_bytes",
            "response_characters",
            "response_preview",
            "preview_characters",
            "redaction_window_characters",
            "truncated",
            "redacted",
        }
        if set(value) != expected:
            return None
        provider = value.get("provider")
        protocol = value.get("protocol")
        representation = value.get("response_representation")
        digest = value.get("response_sha256")
        byte_count = value.get("response_bytes")
        character_count = value.get("response_characters")
        preview = value.get("response_preview")
        preview_count = value.get("preview_characters")
        window_count = value.get("redaction_window_characters")
        if (
            not isinstance(provider, str)
            or not provider
            or len(provider) > 120
            or any(
                ord(character) < 32 or ord(character) == 127
                for character in provider
            )
            or not isinstance(protocol, str)
            or re.fullmatch(r"[a-z][a-z0-9._-]{0,119}", protocol)
            is None
            or representation
            not in {"raw_bytes", "utf8_text", "canonical_json"}
            or not isinstance(digest, str)
            or re.fullmatch(r"sha256:[a-f0-9]{64}", digest) is None
            or isinstance(byte_count, bool)
            or not isinstance(byte_count, int)
            or byte_count < 0
            or isinstance(character_count, bool)
            or not isinstance(character_count, int)
            or character_count < 0
            or not isinstance(preview, str)
            or len(preview) > _DIAGNOSTIC_PREVIEW_CHARACTERS
            or "\x00" in preview
            or isinstance(preview_count, bool)
            or not isinstance(preview_count, int)
            or preview_count != len(preview)
            or isinstance(window_count, bool)
            or not isinstance(window_count, int)
            or window_count
            != min(
                character_count,
                _DIAGNOSTIC_REDACTION_WINDOW_CHARACTERS,
            )
            or type(value.get("truncated")) is not bool
            or type(value.get("redacted")) is not bool
        ):
            return None
        return dict(value)
    expected = {
        "schema_version",
        "provider",
        "status",
        "body_representation",
        "body_sha256",
        "body_bytes",
        "body_characters",
        "body_preview",
        "preview_characters",
        "redaction_window_characters",
        "truncated",
        "redacted",
    }
    if set(value) != expected:
        return None
    provider = value.get("provider")
    status = value.get("status")
    representation = value.get("body_representation")
    digest = value.get("body_sha256")
    byte_count = value.get("body_bytes")
    character_count = value.get("body_characters")
    preview = value.get("body_preview")
    preview_count = value.get("preview_characters")
    window_count = value.get("redaction_window_characters")
    if (
        schema != _PROVIDER_REJECTION_SCHEMA
        or not isinstance(provider, str)
        or not provider
        or len(provider) > 120
        or any(
            ord(character) < 32 or ord(character) == 127
            for character in provider
        )
        or isinstance(status, bool)
        or not isinstance(status, int)
        or not 100 <= status <= 599
        or representation
        not in {"raw_bytes", "utf8_text", "canonical_json"}
        or not isinstance(digest, str)
        or re.fullmatch(r"sha256:[a-f0-9]{64}", digest) is None
        or isinstance(byte_count, bool)
        or not isinstance(byte_count, int)
        or byte_count < 0
        or isinstance(character_count, bool)
        or not isinstance(character_count, int)
        or character_count < 0
        or not isinstance(preview, str)
        or len(preview) > _DIAGNOSTIC_PREVIEW_CHARACTERS
        or "\x00" in preview
        or isinstance(preview_count, bool)
        or not isinstance(preview_count, int)
        or preview_count != len(preview)
        or isinstance(window_count, bool)
        or not isinstance(window_count, int)
        or window_count != min(
            character_count,
            _DIAGNOSTIC_REDACTION_WINDOW_CHARACTERS,
        )
        or type(value.get("truncated")) is not bool
        or type(value.get("redacted")) is not bool
    ):
        return None
    return dict(value)


def _delivery_failure_text(
    *,
    error: str,
    provider_error: str | Mapping[str, Any] | None = None,
    provider_rejection: Mapping[str, Any] | None = None,
) -> str:
    safe_error = _safe_bounded_diagnostic(provider_error)
    safe_rejection = _safe_provider_rejection_evidence(
        provider_rejection
    )
    if safe_error is None and safe_rejection is None:
        return error
    evidence: dict[str, Any] = {
        "schema_version": _DELIVERY_FAILURE_SCHEMA,
        "error": error,
    }
    if safe_error is not None:
        evidence["provider_error"] = safe_error
    if safe_rejection is not None:
        evidence["provider_rejection"] = safe_rejection
    return json.dumps(
        evidence,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _bounded_diagnostic_text(value: Any) -> str:
    diagnostic = _bounded_diagnostic(value)
    if isinstance(diagnostic, str):
        return diagnostic
    if isinstance(diagnostic, Mapping):
        return json.dumps(
            diagnostic,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    return ""


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        path.chmod(0o700)
    except OSError:
        pass


def _private_file(path: Path) -> None:
    try:
        path.chmod(0o600)
    except OSError:
        pass


def delivery_ledger_path() -> Path:
    """Return the private, profile-scoped semantic delivery ledger."""

    return (
        get_hermes_home()
        / "state"
        / "semantic-delivery"
        / "ledger.sqlite3"
    )


class SemanticRetryPayloadBackend(Protocol):
    """Credential-free durable storage for exact retry payload bytes.

    A future R2 integration implements this protocol and registers itself by
    name. Backend credentials stay in the integration's own secret scope;
    neither the outbox row nor ``payload_ref`` may contain them.
    """

    name: str

    def put(
        self,
        *,
        delivery_id: str,
        payload_digest: str,
        payload: bytes,
        ledger_path: Path | None,
    ) -> str: ...

    def read(
        self,
        *,
        payload_ref: str,
        ledger_path: Path | None,
    ) -> bytes: ...

    def delete(
        self,
        *,
        payload_ref: str,
        ledger_path: Path | None,
    ) -> None: ...


class _LocalSemanticRetryPayloadBackend:
    """Private profile-local fallback; no credential or arbitrary path input."""

    name = "local"
    _REF = re.compile(r"^[a-f0-9]{64}-[a-f0-9]{64}\.payload$")

    @staticmethod
    def _directory(ledger_path: Path | None) -> Path:
        ledger = ledger_path or delivery_ledger_path()
        directory = ledger.parent / "retry-payloads"
        _private_directory(directory)
        return directory

    def put(
        self,
        *,
        delivery_id: str,
        payload_digest: str,
        payload: bytes,
        ledger_path: Path | None,
    ) -> str:
        identity = hashlib.sha256(delivery_id.encode("utf-8")).hexdigest()
        payload_hash = hashlib.sha256(payload).hexdigest()
        if not re.fullmatch(r"[a-f0-9]{64}", payload_digest):
            raise ValueError("semantic retry canonical digest invalid")
        payload_ref = f"{identity}-{payload_hash}.payload"
        destination = self._directory(ledger_path) / payload_ref
        if destination.exists():
            existing = destination.read_bytes()
            if existing != payload:
                raise RuntimeError("semantic retry payload authority conflict")
            _private_file(destination)
            return payload_ref
        temporary = destination.with_name(
            f".{destination.name}.{uuid.uuid4().hex}.tmp"
        )
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, destination)
            _private_file(destination)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        return payload_ref

    def read(
        self,
        *,
        payload_ref: str,
        ledger_path: Path | None,
    ) -> bytes:
        if self._REF.fullmatch(str(payload_ref or "")) is None:
            raise ValueError("semantic retry payload reference invalid")
        path = self._directory(ledger_path) / payload_ref
        payload = path.read_bytes()
        _private_file(path)
        return payload

    def delete(
        self,
        *,
        payload_ref: str,
        ledger_path: Path | None,
    ) -> None:
        if self._REF.fullmatch(str(payload_ref or "")) is None:
            raise ValueError("semantic retry payload reference invalid")
        try:
            (self._directory(ledger_path) / payload_ref).unlink()
        except FileNotFoundError:
            pass


_semantic_retry_payload_backends: dict[
    str, SemanticRetryPayloadBackend
] = {"local": _LocalSemanticRetryPayloadBackend()}
_semantic_retry_completion_handlers: dict[str, Callable[..., Any]] = {}


def register_semantic_retry_payload_backend(
    backend: SemanticRetryPayloadBackend,
) -> None:
    """Register an immutable payload backend contract by stable name."""

    name = str(getattr(backend, "name", "") or "").strip().lower()
    if (
        _PROVIDER_ID.fullmatch(name) is None
        or not callable(getattr(backend, "put", None))
        or not callable(getattr(backend, "read", None))
        or not callable(getattr(backend, "delete", None))
    ):
        raise ValueError("invalid semantic retry payload backend")
    existing = _semantic_retry_payload_backends.get(name)
    if existing is not None and existing is not backend:
        raise ValueError(
            f"semantic retry payload backend {name!r} already registered"
        )
    _semantic_retry_payload_backends[name] = backend


def register_semantic_retry_completion_handler(
    contract: str,
    handler: Callable[..., Any],
) -> None:
    """Register one restart-recoverable group completion action.

    The outbox persists only ``contract`` and an opaque non-secret reference.
    A product integration (Planning ACK bridge) owns the handler and its
    durable state; retries remain unbounded when that integration is offline.
    """

    clean = str(contract or "").strip()
    if (
        not clean
        or len(clean) > 240
        or any(ord(character) < 33 or ord(character) == 127 for character in clean)
        or not callable(handler)
    ):
        raise ValueError("invalid semantic retry completion handler")
    existing = _semantic_retry_completion_handlers.get(clean)
    if existing is not None and existing is not handler:
        raise ValueError(
            f"semantic retry completion handler {clean!r} already registered"
        )
    _semantic_retry_completion_handlers[clean] = handler


def unregister_semantic_retry_completion_handler(
    contract: str,
    *,
    handler: Callable[..., Any] | None = None,
) -> bool:
    clean = str(contract or "").strip()
    existing = _semantic_retry_completion_handlers.get(clean)
    if existing is None or (handler is not None and existing is not handler):
        return False
    del _semantic_retry_completion_handlers[clean]
    return True


def _connection(path: Path | None = None) -> sqlite3.Connection:
    target = path or delivery_ledger_path()
    _private_directory(target.parent)
    connection = sqlite3.connect(target, timeout=30)
    _private_file(target)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA journal_mode=WAL")
    delivery_table_existed = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        ("semantic_deliveries",),
    ).fetchone() is not None
    connection.execute(_SCHEMA)
    authority_changed = not delivery_table_existed
    columns = {
        str(row["name"])
        for row in connection.execute(
            "PRAGMA table_info(semantic_deliveries)"
        ).fetchall()
    }
    if "gateway_account_id" not in columns:
        connection.execute(
            "ALTER TABLE semantic_deliveries "
            "ADD COLUMN gateway_account_id TEXT NOT NULL DEFAULT ''"
        )
        authority_changed = True
    connection.execute(_METADATA_SCHEMA)
    connection.execute(_RETRY_OUTBOX_SCHEMA)
    retry_columns = {
        str(row["name"])
        for row in connection.execute(
            "PRAGMA table_info(semantic_delivery_retry_outbox)"
        ).fetchall()
    }
    if "replay_strategy" not in retry_columns:
        connection.execute(
            "ALTER TABLE semantic_delivery_retry_outbox "
            "ADD COLUMN replay_strategy TEXT NOT NULL DEFAULT 'none'"
        )
        authority_changed = True
    for column, definition in (
        ("delivery_group_id", "TEXT NOT NULL DEFAULT ''"),
        ("unit_index", "INTEGER NOT NULL DEFAULT 0"),
        ("unit_count", "INTEGER NOT NULL DEFAULT 1"),
        ("encoding_contract_json", "TEXT NOT NULL DEFAULT ''"),
    ):
        if column not in retry_columns:
            connection.execute(
                f"ALTER TABLE semantic_delivery_retry_outbox "
                f"ADD COLUMN {column} {definition}"
            )
            authority_changed = True
    connection.execute(_RETRY_GROUP_SCHEMA)
    group_columns = {
        str(row["name"])
        for row in connection.execute(
            "PRAGMA table_info(semantic_delivery_retry_groups)"
        ).fetchall()
    }
    if "turn_execution_ref" not in group_columns:
        connection.execute(
            "ALTER TABLE semantic_delivery_retry_groups "
            "ADD COLUMN turn_execution_ref TEXT NOT NULL DEFAULT ''"
        )
        authority_changed = True
    connection.execute(_RETRY_TURN_EXECUTION_INDEX_SCHEMA)
    connection.execute(_RETRY_DUE_INDEX_SCHEMA)
    scope_insert = connection.execute(
        "INSERT OR IGNORE INTO semantic_delivery_metadata "
        "(metadata_key,metadata_value) VALUES (?,?)",
        (_SCOPE_METADATA_KEY, f"scope_{secrets.token_hex(24)}"),
    )
    authority_changed = authority_changed or scope_insert.rowcount == 1
    connection.commit()
    if authority_changed:
        # Keep newly-created/migrated authority in the main database image as
        # well as WAL so copying the closed ledger preserves its identity.
        # A FULL checkpoint on every status/send connection would serialize
        # otherwise unrelated delivery writers.
        connection.execute("PRAGMA wal_checkpoint(FULL)")
    for suffix in ("-wal", "-shm"):
        _private_file(Path(f"{target}{suffix}"))
    return connection


def semantic_delivery_scope_id(
    *,
    ledger_path: Path | None = None,
) -> str:
    """Return the ledger's persistent opaque scope identity.

    The random value lives inside the ledger itself, so moving/copying the DB
    preserves authority while deleting/recreating it produces a different
    scope.  Hostnames and filesystem paths are intentionally absent.
    """

    with _connection(ledger_path) as connection:
        row = connection.execute(
            "SELECT metadata_value FROM semantic_delivery_metadata "
            "WHERE metadata_key=?",
            (_SCOPE_METADATA_KEY,),
        ).fetchone()
    if row is None:
        raise RuntimeError("semantic delivery scope metadata is unavailable")
    return str(row["metadata_value"])


def semantic_delivery_status(
    *,
    delivery_id: str,
    contract_version: str,
    expected_scope_id: str,
    expected_provider: str = "",
    gateway_account_id: str = "",
    ledger_path: Path | None = None,
) -> dict[str, Any]:
    """Read one durable outcome without ever opening a provider send path."""

    delivery_id = str(delivery_id or "").strip()
    contract_version = str(contract_version or "").strip()
    expected_scope_id = str(expected_scope_id or "").strip()
    expected_provider = str(expected_provider or "").strip().lower()
    gateway_account_id = str(gateway_account_id or "")
    if (
        contract_version != SEMANTIC_DELIVERY_CONTRACT
        or _DELIVERY_ID.fullmatch(delivery_id) is None
        or not expected_scope_id
        or (
            expected_provider
            and _PROVIDER_ID.fullmatch(expected_provider) is None
        )
        or (
            gateway_account_id
            and (
                gateway_account_id != gateway_account_id.strip()
                or len(gateway_account_id) > 500
                or any(
                    ord(character) < 32 or ord(character) == 127
                    for character in gateway_account_id
                )
            )
        )
    ):
        return {
            "delivery_contract": contract_version,
            "delivery_id": delivery_id,
            "error": "semantic_delivery_status_invalid",
            "gateway_account_id": gateway_account_id,
            "outcome": "rejected",
            "provider": expected_provider,
        }
    status_lock = _DeliveryFileLock(_lock_path(delivery_id, ledger_path))
    if not status_lock.acquire():
        # A live sender owns the only delivery lock. Do not reinterpret its
        # committed ``sending`` row as a crash residue while provider I/O is
        # still in progress.
        return _typed(
            delivery_id=delivery_id,
            provider=expected_provider,
            target="",
            outcome="in_flight",
            replay_strategy_value="none",
            error="semantic_delivery_in_flight",
            replayed=True,
            gateway_account_id=gateway_account_id,
            delivery_scope_id=expected_scope_id,
        )
    try:
        with _connection(ledger_path) as connection:
            connection.execute("BEGIN")
            scope_row = connection.execute(
                "SELECT metadata_value FROM semantic_delivery_metadata "
                "WHERE metadata_key=?",
                (_SCOPE_METADATA_KEY,),
            ).fetchone()
            current_scope_id = str(scope_row["metadata_value"])
            if current_scope_id != expected_scope_id:
                connection.rollback()
                return {
                    "delivery_contract": SEMANTIC_DELIVERY_CONTRACT,
                    "delivery_id": delivery_id,
                    "delivery_scope_id": current_scope_id,
                    "error": "semantic_delivery_scope_conflict",
                    "gateway_account_id": gateway_account_id,
                    "outcome": "conflict",
                    "provider": expected_provider,
                }
            row = connection.execute(
                "SELECT * FROM semantic_deliveries WHERE delivery_id=?",
                (delivery_id,),
            ).fetchone()
            connection.rollback()
        if row is None:
            # Under the same ledger scope/account/provider this is a proven
            # pre-write boundary: begin_semantic_delivery commits ``sending``
            # before the provider call.
            return _typed(
                delivery_id=delivery_id,
                provider=expected_provider,
                target="",
                outcome="retryable",
                replay_strategy_value="none",
                error="semantic_delivery_missing_prewrite",
                replayed=True,
                gateway_account_id=gateway_account_id,
                delivery_scope_id=current_scope_id,
                provider_write_attempted=False,
                provider_write_started=False,
            )
        row_provider = str(row["provider"])
        row_account_id = str(row["gateway_account_id"])
        strategy = str(row["replay_strategy"])
        target = str(row["target"])
        if (
            (expected_provider and row_provider != expected_provider)
            or (gateway_account_id and row_account_id != gateway_account_id)
        ):
            return _typed(
                delivery_id=delivery_id,
                provider=expected_provider or row_provider,
                target=target,
                outcome="conflict",
                replay_strategy_value=strategy,
                error="semantic_delivery_identity_conflict",
                replayed=True,
                gateway_account_id=gateway_account_id,
                delivery_scope_id=current_scope_id,
            )
        state = str(row["state"])
        if state in {"delivered", "rejected"}:
            return _stored_result(
                row,
                delivery_scope_id=current_scope_id,
            )
        if state == "intent":
            return _stored_result(
                row,
                delivery_scope_id=current_scope_id,
            )
        if state == "sending" and strategy == "durable_native":
            return _typed(
                delivery_id=delivery_id,
                provider=row_provider,
                target=target,
                outcome="retryable",
                replay_strategy_value=strategy,
                error="semantic_delivery_provider_failed",
                replayed=True,
                gateway_account_id=row_account_id,
                delivery_scope_id=current_scope_id,
            )
        return _typed(
            delivery_id=delivery_id,
            provider=row_provider,
            target=target,
            outcome="ambiguous",
            replay_strategy_value=strategy,
            error="semantic_delivery_outcome_ambiguous",
            replayed=True,
            gateway_account_id=row_account_id,
            delivery_scope_id=current_scope_id,
        )
    finally:
        status_lock.release()


def _lock_path(delivery_id: str, path: Path | None) -> Path:
    ledger = path or delivery_ledger_path()
    lock_dir = ledger.parent / "locks"
    _private_directory(lock_dir)
    digest = hashlib.sha256(delivery_id.encode("utf-8")).hexdigest()
    return lock_dir / f"{digest}.lock"


class _DeliveryFileLock:
    """One blocking, cross-platform, cross-process delivery lock."""

    def __init__(self, path: Path):
        self.path = path
        self._file: Any = None

    def acquire(self) -> bool:
        _private_directory(self.path.parent)
        self._file = open(self.path, "a+b")
        _private_file(self.path)
        try:
            if os.name == "nt":
                import msvcrt

                # Windows byte-range locks cannot reliably lock byte zero of a
                # newly-created empty file. Materialize and flush that byte
                # before requesting the non-blocking one-byte lock.
                self._file.seek(0, 2)
                if self._file.tell() == 0:
                    self._file.write(b"\0")
                    self._file.flush()
                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(
                    self._file.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
            return True
        except BlockingIOError:
            self._file.close()
            self._file = None
            return False
        except OSError as exc:
            # Windows reports a held byte-range lock as EACCES/EAGAIN.
            if os.name == "nt" and exc.errno in {11, 13, 35}:
                self._file.close()
                self._file = None
                return False
            self._file.close()
            self._file = None
            raise
        except Exception:
            self._file.close()
            self._file = None
            raise

    def release(self) -> None:
        if self._file is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        finally:
            self._file.close()
            self._file = None


def _canonical_digest(
    *,
    contract_version: str,
    gateway_account_id: str,
    target: str,
    message: str,
) -> str:
    payload = json.dumps(
        {
            "contractVersion": contract_version,
            "gatewayAccountId": gateway_account_id,
            "message": message,
            "target": target,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _provider(target: str) -> str:
    return target.partition(":")[0].strip().lower()


def replay_strategy(provider: str, message: str) -> str:
    """Return only the replay guarantee Hermes can actually prove."""

    clean = provider.strip().lower()
    if clean in _REPLAY_SAFE_PROVIDERS and "MEDIA:" not in message:
        return "durable_native"
    if clean in _BOUNDED_NATIVE_PROVIDERS and "MEDIA:" not in message:
        return "bounded_native"
    return "none"


def provider_delivery_token(
    delivery_id: str,
    *,
    provider: str,
    target: str = "",
    unit: str | int = 0,
) -> str:
    """Derive a stable provider-safe token without exposing caller identity."""

    digest = hashlib.sha256(
        (
            f"{SEMANTIC_DELIVERY_CONTRACT}\x1f{provider.lower()}\x1f"
            f"{target}\x1f{delivery_id}\x1f{unit}"
        ).encode("utf-8")
    ).hexdigest()
    if provider.strip().lower() == "discord":
        # Discord Create Message accepts at most 25 nonce characters.
        return f"dh_{digest[:22]}"
    return f"dh_{digest}"


@dataclass(frozen=True, slots=True)
class SemanticRetryRoute:
    """Credential-free provider route needed to replay one exact write."""

    provider: str
    chat_id: str
    encoding_contract: Any
    provider_route: tuple[tuple[str, str], ...] = ()
    thread_id: str | None = None
    reply_to: str | None = None
    delivery_unit: int = 0
    contract: str = SEMANTIC_RETRY_ROUTE_CONTRACT

    def as_json(self) -> str:
        payload = {
            "contract": self.contract,
            "provider": self.provider,
            "chatId": self.chat_id,
            "encodingContract": (
                self.encoding_contract.as_mapping()
            ),
            "threadId": self.thread_id,
            "replyTo": self.reply_to,
            "deliveryUnit": self.delivery_unit,
        }
        if self.contract == SEMANTIC_RETRY_ROUTE_CONTRACT:
            payload["providerRoute"] = dict(self.provider_route)
        return json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass(frozen=True, slots=True)
class SemanticRetryWork:
    """One leased outbox item; lease identity fences every transition."""

    delivery_id: str
    delivery_group_id: str
    unit_index: int
    unit_count: int
    contract_version: str
    payload_digest: str
    provider: str
    gateway_account_id: str
    target: str
    route: SemanticRetryRoute
    payload_backend: str
    payload_ref: str
    attempt_count: int
    lease_id: str
    delivery_scope_id: str


@dataclass(frozen=True, slots=True)
class SemanticRetryCompletionWork:
    delivery_group_id: str
    completion_contract: str
    completion_ref: str
    completion_attempt_count: int
    completion_lease_id: str
    delivery_ids: tuple[str, ...]


def _safe_route_identifier(
    value: Any,
    *,
    required: bool,
) -> str | None:
    if value is None and not required:
        return None
    clean = str(value or "")
    if (
        (required and not clean)
        or clean != clean.strip()
        or len(clean) > 4096
        or any(ord(character) < 32 or ord(character) == 127 for character in clean)
    ):
        raise ValueError("semantic retry route identifier invalid")
    return clean or None


def _decode_retry_route(raw: str, *, provider: str) -> SemanticRetryRoute:
    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("semantic retry route invalid") from exc
    common_keys = {
        "contract",
        "provider",
        "chatId",
        "encodingContract",
        "threadId",
        "replyTo",
        "deliveryUnit",
    }
    if not isinstance(payload, dict):
        raise ValueError("semantic retry route invalid")
    route_contract = payload.get("contract")
    if route_contract == SEMANTIC_RETRY_ROUTE_CONTRACT:
        expected_keys = common_keys | {"providerRoute"}
    elif route_contract == _LEGACY_SEMANTIC_RETRY_ROUTE_CONTRACT:
        expected_keys = common_keys
    else:
        raise ValueError("semantic retry route invalid")
    if set(payload) != expected_keys:
        raise ValueError("semantic retry route invalid")
    route_provider = str(payload["provider"] or "").strip().lower()
    delivery_unit = payload["deliveryUnit"]
    if (
        route_provider != provider
        or isinstance(delivery_unit, bool)
        or not isinstance(delivery_unit, int)
        or delivery_unit < 0
    ):
        raise ValueError("semantic retry route invalid")
    from gateway.semantic_exact_attempt import (
        coerce_live_semantic_exact_attempt_encoding_contract,
        coerce_live_semantic_exact_attempt_provider_route,
    )

    encoding_contract = (
        coerce_live_semantic_exact_attempt_encoding_contract(
            payload["encodingContract"]
        )
    )
    if encoding_contract.provider != route_provider:
        raise ValueError("semantic retry route invalid")
    return SemanticRetryRoute(
        provider=route_provider,
        chat_id=_safe_route_identifier(
            payload["chatId"],
            required=True,
        )
        or "",
        encoding_contract=encoding_contract,
        provider_route=(
            coerce_live_semantic_exact_attempt_provider_route(
                payload["providerRoute"]
            )
            if route_contract == SEMANTIC_RETRY_ROUTE_CONTRACT
            else ()
        ),
        thread_id=_safe_route_identifier(
            payload["threadId"],
            required=False,
        ),
        reply_to=_safe_route_identifier(
            payload["replyTo"],
            required=False,
        ),
        delivery_unit=delivery_unit,
        contract=str(route_contract),
    )


def stage_semantic_delivery_retry(
    *,
    delivery_id: str,
    contract_version: str,
    target: str,
    message: str,
    gateway_account_id: str,
    adapter: Any,
    encoding_contract: Any,
    chat_id: str,
    thread_id: str | None = None,
    reply_to: str | None = None,
    delivery_group_id: str | None = None,
    unit_index: int = 0,
    unit_count: int = 1,
    turn_execution_ref: str = "",
    completion_contract: str = "",
    completion_ref: str = "",
    expected_scope_id: str | None = None,
    payload_backend: str = "local",
    initial_delay_seconds: float = _RETRY_INITIAL_DELAY_SECONDS,
    ledger_path: Path | None = None,
) -> dict[str, Any]:
    """Durably authorize automatic exact retry before the first provider call.

    The API intentionally accepts no credential/token/headers field. Payload
    bytes go to a private pluggable backend; the SQLite outbox stores only its
    opaque reference, canonical digest, and credential-free route.
    """

    delivery_id = str(delivery_id or "").strip()
    contract_version = str(contract_version or "").strip()
    target = str(target or "").strip()
    message = str(message or "")
    gateway_account_id = str(gateway_account_id or "")
    provider = target.partition(":")[0].strip().lower()
    delivery_group_id = str(
        delivery_group_id or delivery_id
    ).strip()
    completion_contract = str(completion_contract or "").strip()
    completion_ref = str(completion_ref or "")
    turn_execution_ref = str(turn_execution_ref or "").strip()
    backend_name = str(payload_backend or "").strip().lower()
    if (
        contract_version != SEMANTIC_DELIVERY_CONTRACT
        or _DELIVERY_ID.fullmatch(delivery_id) is None
        or _PROVIDER_ID.fullmatch(provider) is None
        or target.partition(":")[1] != ":"
        or not target.partition(":")[2].strip()
        or not message.strip()
        or not gateway_account_id
        or gateway_account_id != gateway_account_id.strip()
        or len(gateway_account_id) > 500
        or any(
            ord(character) < 32 or ord(character) == 127
            for character in gateway_account_id
        )
        or _DELIVERY_ID.fullmatch(delivery_group_id) is None
        or isinstance(unit_index, bool)
        or not isinstance(unit_index, int)
        or unit_index < 0
        or isinstance(unit_count, bool)
        or not isinstance(unit_count, int)
        or unit_count < 1
        or unit_index >= unit_count
        or (
            turn_execution_ref
            and _DELIVERY_ID.fullmatch(turn_execution_ref) is None
        )
        or bool(completion_contract) != bool(completion_ref)
        or len(completion_contract) > 240
        or len(completion_ref) > 2048
        or any(
            ord(character) < 33 or ord(character) == 127
            for character in completion_contract
        )
        or any(
            ord(character) < 32 or ord(character) == 127
            for character in completion_ref
        )
        or isinstance(initial_delay_seconds, bool)
    ):
        raise ValueError("semantic retry identity invalid")
    try:
        initial_delay = max(0.0, float(initial_delay_seconds))
    except (TypeError, ValueError) as exc:
        raise ValueError("semantic retry delay invalid") from exc
    if not initial_delay < float("inf"):
        raise ValueError("semantic retry delay invalid")
    from gateway.platform_registry import (
        supports_live_semantic_exact_attempt,
    )
    from gateway.semantic_exact_attempt import (
        bind_live_semantic_exact_attempt_provider_route,
        coerce_live_semantic_exact_attempt_encoding_contract,
        supports_live_semantic_exact_attempt_encoding_contract,
    )

    if (
        not supports_live_semantic_exact_attempt(
            provider,
            adapter=adapter,
        )
        or _adapter_account_id(adapter) != gateway_account_id
    ):
        raise ValueError("semantic retry live provider capability required")
    frozen_encoding_contract = (
        coerce_live_semantic_exact_attempt_encoding_contract(
            encoding_contract
        )
    )
    if (
        frozen_encoding_contract.provider != provider
        or not supports_live_semantic_exact_attempt_encoding_contract(
            adapter,
            frozen_encoding_contract,
        )
    ):
        raise ValueError("semantic retry live provider capability required")
    frozen_provider_route = (
        bind_live_semantic_exact_attempt_provider_route(
            adapter,
            chat_id=chat_id,
            thread_id=thread_id,
            reply_to=reply_to,
        )
    )
    route = SemanticRetryRoute(
        provider=provider,
        chat_id=_safe_route_identifier(chat_id, required=True) or "",
        encoding_contract=frozen_encoding_contract,
        provider_route=frozen_provider_route,
        thread_id=_safe_route_identifier(thread_id, required=False),
        reply_to=_safe_route_identifier(reply_to, required=False),
        delivery_unit=unit_index,
    )
    route_json = route.as_json()
    encoding_contract_json = json.dumps(
        frozen_encoding_contract.as_mapping(),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = _canonical_digest(
        contract_version=contract_version,
        gateway_account_id=gateway_account_id,
        target=target,
        message=message,
    )
    strategy = replay_strategy(provider, message)
    current_scope_id = semantic_delivery_scope_id(ledger_path=ledger_path)
    if (
        expected_scope_id is not None
        and str(expected_scope_id) != current_scope_id
    ):
        raise ValueError("semantic retry delivery scope conflict")
    backend = _semantic_retry_payload_backends.get(backend_name)
    if backend is None:
        raise ValueError("semantic retry payload backend unavailable")
    payload_ref = backend.put(
        delivery_id=delivery_id,
        payload_digest=digest,
        payload=message.encode("utf-8"),
        ledger_path=ledger_path,
    )
    now_text = _now()
    next_attempt_at = time.time() + initial_delay
    keep_payload = False
    inserted_new = False
    existing_retired = False
    group_state = "staging"
    group_fully_staged = False
    try:
        with _connection(ledger_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM semantic_delivery_retry_outbox "
                "WHERE delivery_id=?",
                (delivery_id,),
            ).fetchone()
            group_row = connection.execute(
                "SELECT * FROM semantic_delivery_retry_groups "
                "WHERE delivery_group_id=?",
                (delivery_group_id,),
            ).fetchone()
            turn_owner = (
                connection.execute(
                    "SELECT delivery_group_id "
                    "FROM semantic_delivery_retry_groups "
                    "WHERE turn_execution_ref=?",
                    (turn_execution_ref,),
                ).fetchone()
                if turn_execution_ref
                else None
            )
            if (
                turn_owner is not None
                and str(turn_owner["delivery_group_id"])
                != delivery_group_id
            ):
                connection.rollback()
                raise ValueError(
                    "semantic retry turn execution identity conflict"
                )
            if group_row is None:
                connection.execute(
                    "INSERT INTO semantic_delivery_retry_groups "
                    "(delivery_group_id,unit_count,turn_execution_ref,"
                    "completion_contract,"
                    "completion_ref,state,completion_attempt_count,"
                    "next_completion_at,completion_lease_id,"
                    "completion_lease_expires_at,last_error,created_at,"
                    "updated_at) "
                    "VALUES (?,?,?,?,?,'staging',0,0,NULL,NULL,NULL,?,?)",
                    (
                        delivery_group_id,
                        unit_count,
                        turn_execution_ref,
                        completion_contract,
                        completion_ref,
                        now_text,
                        now_text,
                    ),
                )
            elif (
                int(group_row["unit_count"]) != unit_count
                or str(group_row["turn_execution_ref"])
                != turn_execution_ref
                or str(group_row["completion_contract"])
                != completion_contract
                or str(group_row["completion_ref"]) != completion_ref
            ):
                connection.rollback()
                raise ValueError("semantic retry group identity conflict")
            unit_row = connection.execute(
                "SELECT delivery_id FROM semantic_delivery_retry_outbox "
                "WHERE delivery_group_id=? AND unit_index=?",
                (delivery_group_id, unit_index),
            ).fetchone()
            if (
                unit_row is not None
                and str(unit_row["delivery_id"]) != delivery_id
            ):
                connection.rollback()
                raise ValueError("semantic retry group unit conflict")
            if row is None:
                connection.execute(
                    "INSERT INTO semantic_delivery_retry_outbox "
                    "(delivery_id,delivery_group_id,unit_index,unit_count,"
                    "contract_version,payload_digest,provider,"
                    "gateway_account_id,target,replay_strategy,"
                    "encoding_contract_json,route_json,payload_backend,"
                    "payload_ref,state,attempt_count,next_attempt_at,lease_id,"
                    "lease_expires_at,last_error,created_at,updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',0,?,NULL,NULL,NULL,?,?)",
                    (
                        delivery_id,
                        delivery_group_id,
                        unit_index,
                        unit_count,
                        contract_version,
                        digest,
                        provider,
                        gateway_account_id,
                        target,
                        strategy,
                        encoding_contract_json,
                        route_json,
                        backend_name,
                        payload_ref,
                        next_attempt_at,
                        now_text,
                        now_text,
                    ),
                )
                inserted_new = True
            else:
                # ``put`` is idempotent and may have returned the payload
                # already owned by this row. A conflicting re-stage must
                # never delete bytes that authorize the winning identity.
                keep_payload = bool(
                    str(row["payload_backend"]) == backend_name
                    and str(row["payload_ref"]) == payload_ref
                )
                identity = (
                    str(row["delivery_group_id"]) == delivery_group_id
                    and int(row["unit_index"]) == unit_index
                    and int(row["unit_count"]) == unit_count
                    and str(row["contract_version"]) == contract_version
                    and str(row["payload_digest"]) == digest
                    and str(row["provider"]) == provider
                    and str(row["gateway_account_id"]) == gateway_account_id
                    and str(row["target"]) == target
                    and str(row["replay_strategy"]) == strategy
                    and str(row["encoding_contract_json"])
                    == encoding_contract_json
                    and str(row["route_json"]) == route_json
                    and str(row["payload_backend"]) == backend_name
                    and str(row["payload_ref"]) == payload_ref
                )
                if not identity or str(row["state"]) == "corrupt":
                    connection.rollback()
                    raise ValueError(
                        "semantic retry outbox identity conflict"
                    )
                existing_retired = str(row["state"]) == "retired"
                if existing_retired:
                    keep_payload = False
            staged_count = int(
                connection.execute(
                    "SELECT COUNT(*) AS n "
                    "FROM semantic_delivery_retry_outbox "
                    "WHERE delivery_group_id=?",
                    (delivery_group_id,),
                ).fetchone()["n"]
            )
            if staged_count == unit_count:
                connection.execute(
                    "UPDATE semantic_delivery_retry_groups "
                    "SET state=CASE WHEN state='staging' THEN 'pending' "
                    "ELSE state END,updated_at=? "
                    "WHERE delivery_group_id=?",
                    (_now(), delivery_group_id),
                )
            group_state_row = connection.execute(
                "SELECT state FROM semantic_delivery_retry_groups "
                "WHERE delivery_group_id=?",
                (delivery_group_id,),
            ).fetchone()
            group_state = str(group_state_row["state"])
            group_fully_staged = staged_count == unit_count
            connection.commit()
            if inserted_new:
                keep_payload = True
    except BaseException:
        if not keep_payload:
            backend.delete(
                payload_ref=payload_ref,
                ledger_path=ledger_path,
            )
        raise
    if existing_retired:
        backend.delete(
            payload_ref=payload_ref,
            ledger_path=ledger_path,
        )
    return {
        "delivery_contract": contract_version,
        "delivery_id": delivery_id,
        "delivery_group_id": delivery_group_id,
        "delivery_scope_id": current_scope_id,
        "gateway_account_id": gateway_account_id,
        "encoding_contract": frozen_encoding_contract.as_mapping(),
        "group_fully_staged": group_fully_staged,
        "group_state": group_state,
        "outcome": "retired" if existing_retired else "staged",
        "payload_backend": backend_name,
        "provider": provider,
        "provider_route": dict(frozen_provider_route),
        "retry_automatic": True,
        "retry_limit": None,
        "target": target,
        "turn_execution_ref": turn_execution_ref or None,
        "unit_count": unit_count,
        "unit_index": unit_index,
    }


def _retry_delay(
    delivery_id: str,
    attempt_count: int,
    retry_after: Any,
) -> float:
    if not isinstance(retry_after, bool):
        try:
            server_delay = float(retry_after)
        except (TypeError, ValueError):
            server_delay = -1.0
        if 0.0 <= server_delay < float("inf"):
            return server_delay
    exponent = min(max(attempt_count - 1, 0), 8)
    base = min(_RETRY_BACKOFF_MAX_SECONDS, 2.0 ** exponent)
    jitter_seed = hashlib.sha256(
        f"{delivery_id}:{attempt_count}".encode("utf-8")
    ).digest()
    jitter = int.from_bytes(jitter_seed[:2], "big") / 65535.0
    return min(_RETRY_BACKOFF_MAX_SECONDS, base + jitter)


def _schedule_retry_outbox(
    attempt: "SemanticDeliveryAttempt",
    *,
    retry_after: Any = None,
    last_error: str = "semantic_delivery_prewrite_failed",
) -> bool:
    """Make one proven pre-write failure due again; attempts are unbounded."""

    now = time.time()
    with _connection(attempt._ledger_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT attempt_count FROM semantic_delivery_retry_outbox "
            f"WHERE {_IDENTITY_WHERE}",
            _identity_update_where(attempt),
        ).fetchone()
        if row is None:
            connection.rollback()
            return False
        attempt_count = int(row["attempt_count"]) + 1
        delay = _retry_delay(
            attempt.delivery_id,
            attempt_count,
            retry_after,
        )
        cursor = connection.execute(
            "UPDATE semantic_delivery_retry_outbox "
            "SET state='pending',attempt_count=?,next_attempt_at=?,"
            "lease_id=NULL,lease_expires_at=NULL,"
            "last_error=?,updated_at=? "
            f"WHERE {_IDENTITY_WHERE}",
            (
                attempt_count,
                now + delay,
                last_error,
                _now(),
                *_identity_update_where(attempt),
            ),
        )
        connection.commit()
    return cursor.rowcount == 1


def _retire_retry_outbox(
    attempt: "SemanticDeliveryAttempt",
    *,
    reason: str,
) -> bool:
    payload: tuple[str, str] | None = None
    delivery_group_id = ""
    with _connection(attempt._ledger_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT payload_backend,payload_ref,delivery_group_id "
            "FROM semantic_delivery_retry_outbox "
            f"WHERE {_IDENTITY_WHERE}",
            _identity_update_where(attempt),
        ).fetchone()
        if row is None:
            connection.rollback()
            return False
        payload = (
            str(row["payload_backend"]),
            str(row["payload_ref"]),
        )
        delivery_group_id = str(row["delivery_group_id"])
        cursor = connection.execute(
            "UPDATE semantic_delivery_retry_outbox "
            "SET state='retired',lease_id=NULL,lease_expires_at=NULL,"
            "last_error=?,updated_at=? "
            f"WHERE {_IDENTITY_WHERE}",
            (
                _bounded_diagnostic_text(reason),
                _now(),
                *_identity_update_where(attempt),
            ),
        )
        if reason == "delivered":
            incomplete = connection.execute(
                "SELECT 1 FROM semantic_delivery_retry_outbox "
                "WHERE delivery_group_id=? AND NOT ("
                "state='retired' AND last_error='delivered') LIMIT 1",
                (delivery_group_id,),
            ).fetchone()
            if incomplete is None:
                connection.execute(
                    "UPDATE semantic_delivery_retry_groups "
                    "SET next_completion_at=?,updated_at=? "
                    "WHERE delivery_group_id=? AND state='pending'",
                    (time.time(), _now(), delivery_group_id),
                )
        else:
            connection.execute(
                "UPDATE semantic_delivery_retry_groups "
                "SET state='blocked',last_error=?,updated_at=? "
                "WHERE delivery_group_id=? AND state IN "
                "('staging','pending','completing')",
                (
                    _bounded_diagnostic_text(f"unit_terminal:{reason}"),
                    _now(),
                    delivery_group_id,
                ),
            )
        connection.commit()
    backend = _semantic_retry_payload_backends.get(payload[0])
    if backend is not None:
        try:
            backend.delete(
                payload_ref=payload[1],
                ledger_path=attempt._ledger_path,
            )
        except Exception:
            # Terminal delivery authority is already in the semantic ledger.
            # A later profile cleanup may remove an orphaned payload object.
            pass
    return cursor.rowcount == 1


def _typed(
    *,
    delivery_id: str,
    provider: str,
    target: str,
    outcome: str,
    replay_strategy_value: str,
    error: str | None = None,
    replayed: bool = False,
    gateway_account_id: str = "",
    message_id: str | None = None,
    message_ids: tuple[str, ...] = (),
    delivery_scope_id: str | None = None,
    provider_write_attempted: bool | None = None,
    provider_write_started: bool | None = None,
    provider_error: str | Mapping[str, Any] | None = None,
    provider_rejection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "delivery_contract": SEMANTIC_DELIVERY_CONTRACT,
        "delivery_id": delivery_id,
        "gateway_account_id": gateway_account_id,
        "outcome": outcome,
        "provider": provider,
        "replay_strategy": replay_strategy_value,
        "replayed": replayed,
        "target": target,
    }
    if delivery_scope_id:
        payload["delivery_scope_id"] = delivery_scope_id
    if error:
        payload["error"] = error
    if message_id:
        payload["message_id"] = message_id
    if message_ids:
        payload["message_ids"] = list(message_ids)
    if provider_write_attempted is not None:
        payload["provider_write_attempted"] = provider_write_attempted
    if provider_write_started is not None:
        payload["provider_write_started"] = provider_write_started
    safe_provider_error = _safe_bounded_diagnostic(provider_error)
    if safe_provider_error is not None:
        payload["provider_error"] = safe_provider_error
    safe_provider_rejection = _safe_provider_rejection_evidence(
        provider_rejection
    )
    if safe_provider_rejection is not None:
        payload["provider_rejection"] = safe_provider_rejection
    if outcome == "delivered":
        payload["success"] = True
    return payload


def exact_provider_message_id(value: Any) -> str | None:
    """Return one opaque native receipt unchanged, or reject its structure."""

    if not isinstance(value, str):
        return None
    message_id = value
    if (
        not message_id
        or any(
            unicodedata.category(character) == "Cc"
            or 0xD800 <= ord(character) <= 0xDFFF
            for character in message_id
        )
    ):
        return None
    return message_id


def is_exact_provider_message_id(value: Any) -> bool:
    return exact_provider_message_id(value) is not None


def _safe_message_id(value: Any) -> str | None:
    return exact_provider_message_id(value)


def _safe_message_ids(provider_result: dict[str, Any]) -> tuple[str, ...]:
    ordered: list[str] = []
    values = provider_result.get("message_ids")
    if not isinstance(values, (list, tuple)):
        values = ()
    for value in (*values, provider_result.get("message_id")):
        message_id = _safe_message_id(value)
        if message_id and message_id not in ordered:
            ordered.append(message_id)
    return tuple(ordered)


def _stored_result(
    row: sqlite3.Row,
    *,
    delivery_scope_id: str | None = None,
) -> dict[str, Any]:
    delivery_id = str(row["delivery_id"])
    provider = str(row["provider"])
    gateway_account_id = str(row["gateway_account_id"])
    target = str(row["target"])
    strategy = str(row["replay_strategy"])
    state = str(row["state"])
    if state in {"delivered", "rejected", "intent"}:
        try:
            payload = json.loads(row["provider_receipt_json"])
        except (TypeError, json.JSONDecodeError):
            payload = None
        expected_outcome = (
            "retryable" if state == "intent" else state
        )
        provider_error = (
            _safe_bounded_diagnostic(payload.get("provider_error"))
            if isinstance(payload, dict)
            else None
        )
        provider_rejection = (
            _safe_provider_rejection_evidence(
                payload.get("provider_rejection")
            )
            if isinstance(payload, dict)
            else None
        )
        identity_valid = (
            isinstance(payload, dict)
            and payload.get("delivery_contract") == row["contract_version"]
            and payload.get("delivery_id") == delivery_id
            and payload.get("gateway_account_id", "") == gateway_account_id
            and payload.get("provider") == provider
            and payload.get("target") == target
            and payload.get("replay_strategy") == strategy
            and payload.get("outcome") == expected_outcome
            and (
                "provider_error" not in payload
                or provider_error is not None
            )
            and (
                "provider_rejection" not in payload
                or provider_rejection is not None
            )
            and (
                not delivery_scope_id
                or payload.get("delivery_scope_id") == delivery_scope_id
            )
        )
        if identity_valid and state == "delivered":
            message_ids = _safe_message_ids(payload)
            if payload.get("success") is True and message_ids:
                return _typed(
                    delivery_id=delivery_id,
                    provider=provider,
                    gateway_account_id=gateway_account_id,
                    target=target,
                    outcome="delivered",
                    replay_strategy_value=strategy,
                    replayed=True,
                    message_id=message_ids[-1],
                    message_ids=message_ids,
                    delivery_scope_id=delivery_scope_id,
                    provider_write_attempted=True,
                    provider_write_started=True,
                )
        elif (
            identity_valid
            and state == "rejected"
            and payload.get("error") == "semantic_delivery_provider_rejected"
        ):
            return _typed(
                delivery_id=delivery_id,
                provider=provider,
                gateway_account_id=gateway_account_id,
                target=target,
                outcome="rejected",
                replay_strategy_value=strategy,
                error="semantic_delivery_provider_rejected",
                replayed=True,
                delivery_scope_id=delivery_scope_id,
                provider_write_attempted=False,
                provider_write_started=False,
                provider_error=provider_error,
                provider_rejection=provider_rejection,
            )
        elif (
            identity_valid
            and state == "intent"
            and payload.get("error")
            == "semantic_delivery_prewrite_failed"
        ):
            return _typed(
                delivery_id=delivery_id,
                provider=provider,
                gateway_account_id=gateway_account_id,
                target=target,
                outcome="retryable",
                replay_strategy_value=strategy,
                error="semantic_delivery_prewrite_failed",
                replayed=True,
                delivery_scope_id=delivery_scope_id,
                provider_write_attempted=False,
                provider_write_started=False,
                provider_error=provider_error,
                provider_rejection=provider_rejection,
            )
        return _typed(
            delivery_id=delivery_id,
            provider=provider,
            gateway_account_id=gateway_account_id,
            target=target,
            outcome="ambiguous",
            replay_strategy_value=strategy,
            error="semantic_delivery_receipt_invalid",
            replayed=True,
            delivery_scope_id=delivery_scope_id,
        )
    return _typed(
        delivery_id=delivery_id,
        provider=provider,
        gateway_account_id=gateway_account_id,
        target=target,
        outcome="ambiguous",
        replay_strategy_value=strategy,
        error="semantic_delivery_outcome_ambiguous",
        replayed=True,
        delivery_scope_id=delivery_scope_id,
    )


@dataclass(slots=True)
class SemanticDeliveryAttempt:
    """A claimed send whose file lock remains held through provider I/O."""

    delivery_id: str
    contract_version: str
    payload_digest: str
    provider: str
    gateway_account_id: str
    target: str
    replay_strategy: str
    action: str
    delivery_scope_id: str | None = None
    result: dict[str, Any] | None = None
    recovery_replay: bool = False
    _ledger_path: Path | None = field(default=None, repr=False)
    _lock: _DeliveryFileLock | None = field(default=None, repr=False)

    def release(self) -> None:
        if self._lock is not None:
            self._lock.release()
            self._lock = None


def _rejected(
    *,
    delivery_id: str,
    contract_version: str,
    error: str,
    provider: str = "",
    gateway_account_id: str = "",
    target: str = "",
) -> SemanticDeliveryAttempt:
    return SemanticDeliveryAttempt(
        delivery_id=delivery_id,
        contract_version=contract_version,
        payload_digest="",
        provider=provider,
        gateway_account_id=gateway_account_id,
        target=target,
        replay_strategy="none",
        action="rejected",
        result={
            "delivery_contract": contract_version,
            "delivery_id": delivery_id,
            "error": error,
            "gateway_account_id": gateway_account_id,
            "outcome": "rejected",
            "provider": provider,
            "replay_strategy": "none",
            "replayed": False,
            "target": target,
        },
    )


def begin_semantic_delivery(
    *,
    delivery_id: str,
    contract_version: str,
    target: str,
    message: str,
    ledger_path: Path | None = None,
    expected_scope_id: str | None = None,
    gateway_account_id: str = "",
) -> SemanticDeliveryAttempt:
    """Claim one delivery and hold its cross-process lock until completion."""

    delivery_id = str(delivery_id or "").strip()
    contract_version = str(contract_version or "").strip()
    target = str(target or "").strip()
    message = str(message or "")
    expected_scope_id = str(expected_scope_id or "").strip() or None
    gateway_account_id = str(gateway_account_id or "")
    provider = target.partition(":")[0].strip().lower()
    if contract_version != SEMANTIC_DELIVERY_CONTRACT:
        return _rejected(
            delivery_id=delivery_id,
            contract_version=contract_version,
            error="semantic_delivery_contract_unsupported",
            provider=provider,
            gateway_account_id=gateway_account_id,
            target=target,
        )
    if _DELIVERY_ID.fullmatch(delivery_id) is None:
        return _rejected(
            delivery_id=delivery_id,
            contract_version=contract_version,
            error="semantic_delivery_id_invalid",
            provider=provider,
            gateway_account_id=gateway_account_id,
            target=target,
        )
    if (
        gateway_account_id
        and (
            gateway_account_id != gateway_account_id.strip()
            or len(gateway_account_id) > 500
            or any(
                ord(character) < 32 or ord(character) == 127
                for character in gateway_account_id
            )
        )
    ):
        return _rejected(
            delivery_id=delivery_id,
            contract_version=contract_version,
            error="semantic_delivery_account_invalid",
            provider=provider,
            gateway_account_id=gateway_account_id,
            target=target,
        )
    _, separator, address = target.partition(":")
    if (
        not separator
        or _PROVIDER_ID.fullmatch(provider) is None
        or not address.strip()
    ):
        return _rejected(
            delivery_id=delivery_id,
            contract_version=contract_version,
            error="semantic_delivery_target_invalid",
            provider=provider,
            gateway_account_id=gateway_account_id,
            target=target,
        )
    if not message.strip():
        return _rejected(
            delivery_id=delivery_id,
            contract_version=contract_version,
            error="semantic_delivery_message_empty",
            provider=provider,
            gateway_account_id=gateway_account_id,
            target=target,
        )

    current_scope_id = semantic_delivery_scope_id(
        ledger_path=ledger_path,
    )
    digest = _canonical_digest(
        contract_version=contract_version,
        gateway_account_id=gateway_account_id,
        target=target,
        message=message,
    )
    strategy = replay_strategy(provider, message)
    lock = _DeliveryFileLock(_lock_path(delivery_id, ledger_path))
    if not lock.acquire():
        return SemanticDeliveryAttempt(
            delivery_id=delivery_id,
            contract_version=contract_version,
            payload_digest=digest,
            provider=provider,
            gateway_account_id=gateway_account_id,
            target=target,
            replay_strategy=strategy,
            action="in_flight",
            delivery_scope_id=current_scope_id,
            result=_typed(
                delivery_id=delivery_id,
                provider=provider,
                target=target,
                outcome="in_flight",
                replay_strategy_value=strategy,
                error="semantic_delivery_in_flight",
                replayed=True,
                gateway_account_id=gateway_account_id,
                delivery_scope_id=current_scope_id,
            ),
        )
    try:
        now = _now()
        with _connection(ledger_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            scope_row = connection.execute(
                "SELECT metadata_value FROM semantic_delivery_metadata "
                "WHERE metadata_key=?",
                (_SCOPE_METADATA_KEY,),
            ).fetchone()
            current_scope_id = str(scope_row["metadata_value"])
            if (
                expected_scope_id is not None
                and expected_scope_id != current_scope_id
            ):
                connection.rollback()
                lock.release()
                return SemanticDeliveryAttempt(
                    delivery_id=delivery_id,
                    contract_version=contract_version,
                    payload_digest=digest,
                    provider=provider,
                    gateway_account_id=gateway_account_id,
                    target=target,
                    replay_strategy=strategy,
                    action="conflict",
                    delivery_scope_id=current_scope_id,
                    result=_typed(
                        delivery_id=delivery_id,
                        provider=provider,
                        target=target,
                        outcome="conflict",
                        replay_strategy_value=strategy,
                        error="semantic_delivery_scope_conflict",
                        gateway_account_id=gateway_account_id,
                        delivery_scope_id=current_scope_id,
                    ),
                )
            row = connection.execute(
                "SELECT * FROM semantic_deliveries WHERE delivery_id=?",
                (delivery_id,),
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO semantic_deliveries "
                    "(delivery_id,contract_version,payload_digest,provider,"
                    "gateway_account_id,target,"
                    "state,replay_strategy,provider_receipt_json,error_code,"
                    "created_at,updated_at) "
                    "VALUES (?,?,?,?,?,?,'intent',?,'{}',NULL,?,?)",
                    (
                        delivery_id,
                        contract_version,
                        digest,
                        provider,
                        gateway_account_id,
                        target,
                        strategy,
                        now,
                        now,
                    ),
                )
                state = "intent"
            else:
                if (
                    row["contract_version"] != contract_version
                    or row["payload_digest"] != digest
                    or row["provider"] != provider
                    or row["gateway_account_id"] != gateway_account_id
                    or row["target"] != target
                    or row["replay_strategy"] != strategy
                ):
                    connection.rollback()
                    lock.release()
                    return SemanticDeliveryAttempt(
                        delivery_id=delivery_id,
                        contract_version=contract_version,
                        payload_digest=digest,
                        provider=provider,
                        gateway_account_id=gateway_account_id,
                        target=target,
                        replay_strategy=strategy,
                        action="conflict",
                        delivery_scope_id=current_scope_id,
                        result=_typed(
                            delivery_id=delivery_id,
                            provider=provider,
                            target=target,
                            outcome="conflict",
                            replay_strategy_value=strategy,
                            error="semantic_delivery_identity_conflict",
                            gateway_account_id=gateway_account_id,
                            delivery_scope_id=current_scope_id,
                        ),
                    )
                state = str(row["state"])
                if state in {"delivered", "ambiguous", "rejected"}:
                    result = _stored_result(
                        row,
                        delivery_scope_id=current_scope_id,
                    )
                    connection.rollback()
                    lock.release()
                    return SemanticDeliveryAttempt(
                        delivery_id=delivery_id,
                        contract_version=contract_version,
                        payload_digest=digest,
                        provider=provider,
                        gateway_account_id=gateway_account_id,
                        target=target,
                        replay_strategy=strategy,
                        action=state,
                        delivery_scope_id=current_scope_id,
                        result=result,
                    )

            recovery_replay = state == "sending"
            if recovery_replay and strategy != "durable_native":
                cursor = connection.execute(
                    "UPDATE semantic_deliveries SET state='ambiguous',"
                    "error_code='semantic_delivery_outcome_ambiguous',"
                    f"updated_at=? WHERE {_IDENTITY_WHERE} "
                    "AND state='sending'",
                    (
                        now,
                        delivery_id,
                        contract_version,
                        digest,
                        provider,
                        gateway_account_id,
                        target,
                        strategy,
                    ),
                )
                if cursor.rowcount != 1:
                    connection.rollback()
                    lock.release()
                    return SemanticDeliveryAttempt(
                        delivery_id=delivery_id,
                        contract_version=contract_version,
                        payload_digest=digest,
                        provider=provider,
                        gateway_account_id=gateway_account_id,
                        target=target,
                        replay_strategy=strategy,
                        action="ambiguous",
                        delivery_scope_id=current_scope_id,
                        result=_typed(
                            delivery_id=delivery_id,
                            provider=provider,
                            gateway_account_id=gateway_account_id,
                            target=target,
                            outcome="ambiguous",
                            replay_strategy_value=strategy,
                            error="semantic_delivery_settlement_ambiguous",
                            replayed=True,
                            delivery_scope_id=current_scope_id,
                        ),
                    )
                connection.commit()
                lock.release()
                return SemanticDeliveryAttempt(
                    delivery_id=delivery_id,
                    contract_version=contract_version,
                    payload_digest=digest,
                    provider=provider,
                    gateway_account_id=gateway_account_id,
                    target=target,
                    replay_strategy=strategy,
                    action="ambiguous",
                    delivery_scope_id=current_scope_id,
                    result=_typed(
                        delivery_id=delivery_id,
                        provider=provider,
                        target=target,
                        outcome="ambiguous",
                        replay_strategy_value=strategy,
                        error="semantic_delivery_outcome_ambiguous",
                        replayed=True,
                        gateway_account_id=gateway_account_id,
                        delivery_scope_id=current_scope_id,
                    ),
                )
            if state == "intent":
                cursor = connection.execute(
                    "UPDATE semantic_deliveries SET state='sending',updated_at=? "
                    f"WHERE {_IDENTITY_WHERE} AND state='intent'",
                    (
                        now,
                        delivery_id,
                        contract_version,
                        digest,
                        provider,
                        gateway_account_id,
                        target,
                        strategy,
                    ),
                )
                if cursor.rowcount != 1:
                    connection.rollback()
                    lock.release()
                    return SemanticDeliveryAttempt(
                        delivery_id=delivery_id,
                        contract_version=contract_version,
                        payload_digest=digest,
                        provider=provider,
                        gateway_account_id=gateway_account_id,
                        target=target,
                        replay_strategy=strategy,
                        action="ambiguous",
                        delivery_scope_id=current_scope_id,
                        result=_typed(
                            delivery_id=delivery_id,
                            provider=provider,
                            gateway_account_id=gateway_account_id,
                            target=target,
                            outcome="ambiguous",
                            replay_strategy_value=strategy,
                            error="semantic_delivery_settlement_ambiguous",
                            delivery_scope_id=current_scope_id,
                        ),
                    )
            connection.commit()
        return SemanticDeliveryAttempt(
            delivery_id=delivery_id,
            contract_version=contract_version,
            payload_digest=digest,
            provider=provider,
            gateway_account_id=gateway_account_id,
            target=target,
            replay_strategy=strategy,
            action="send",
            delivery_scope_id=current_scope_id,
            recovery_replay=recovery_replay,
            _ledger_path=ledger_path,
            _lock=lock,
        )
    except BaseException:
        lock.release()
        raise


def _settlement_ambiguous(
    attempt: SemanticDeliveryAttempt,
    *,
    error: str = "semantic_delivery_settlement_ambiguous",
) -> dict[str, Any]:
    return _typed(
        delivery_id=attempt.delivery_id,
        provider=attempt.provider,
        gateway_account_id=attempt.gateway_account_id,
        target=attempt.target,
        outcome="ambiguous",
        replay_strategy_value=attempt.replay_strategy,
        error=error,
        delivery_scope_id=attempt.delivery_scope_id,
    )


def _row_matches_attempt(
    row: sqlite3.Row | None,
    attempt: SemanticDeliveryAttempt,
) -> bool:
    return bool(
        row is not None
        and row["delivery_id"] == attempt.delivery_id
        and row["contract_version"] == attempt.contract_version
        and row["payload_digest"] == attempt.payload_digest
        and row["provider"] == attempt.provider
        and row["gateway_account_id"] == attempt.gateway_account_id
        and row["target"] == attempt.target
        and row["replay_strategy"] == attempt.replay_strategy
    )


def _settlement_row(
    connection: sqlite3.Connection,
    attempt: SemanticDeliveryAttempt,
) -> sqlite3.Row | None:
    scope_row = connection.execute(
        "SELECT metadata_value FROM semantic_delivery_metadata "
        "WHERE metadata_key=?",
        (_SCOPE_METADATA_KEY,),
    ).fetchone()
    if (
        scope_row is None
        or str(scope_row["metadata_value"]) != attempt.delivery_scope_id
    ):
        return None
    row = connection.execute(
        "SELECT * FROM semantic_deliveries WHERE delivery_id=?",
        (attempt.delivery_id,),
    ).fetchone()
    return row if _row_matches_attempt(row, attempt) else None


def _identity_update_where(attempt: SemanticDeliveryAttempt) -> tuple[Any, ...]:
    return (
        attempt.delivery_id,
        attempt.contract_version,
        attempt.payload_digest,
        attempt.provider,
        attempt.gateway_account_id,
        attempt.target,
        attempt.replay_strategy,
    )


_IDENTITY_WHERE = (
    "delivery_id=? AND contract_version=? AND payload_digest=? "
    "AND provider=? AND gateway_account_id=? AND target=? "
    "AND replay_strategy=?"
)


def _mark_ambiguous(attempt: SemanticDeliveryAttempt) -> dict[str, Any]:
    now = _now()
    with _connection(attempt._ledger_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = _settlement_row(connection, attempt)
        if row is None:
            connection.rollback()
            return _settlement_ambiguous(attempt)
        if row["state"] == "delivered":
            connection.rollback()
            return _stored_result(
                row,
                delivery_scope_id=attempt.delivery_scope_id,
            )
        if row["state"] == "ambiguous":
            connection.rollback()
            return _stored_result(
                row,
                delivery_scope_id=attempt.delivery_scope_id,
            )
        if row["state"] != "sending":
            connection.rollback()
            return _settlement_ambiguous(attempt)
        cursor = connection.execute(
            "UPDATE semantic_deliveries SET state='ambiguous',"
            "error_code='semantic_delivery_outcome_ambiguous',updated_at=? "
            f"WHERE {_IDENTITY_WHERE} AND state='sending'",
            (now, *_identity_update_where(attempt)),
        )
        if cursor.rowcount != 1:
            connection.rollback()
            return _settlement_ambiguous(attempt)
        connection.commit()
    return _typed(
        delivery_id=attempt.delivery_id,
        provider=attempt.provider,
        gateway_account_id=attempt.gateway_account_id,
        target=attempt.target,
        outcome="ambiguous",
        replay_strategy_value=attempt.replay_strategy,
        error="semantic_delivery_outcome_ambiguous",
        delivery_scope_id=attempt.delivery_scope_id,
    )


def _mark_retryable_prewrite(
    attempt: SemanticDeliveryAttempt,
    *,
    retry_after: Any = None,
    provider_error: str | Mapping[str, Any] | None = None,
    provider_rejection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a proven pre-write rejection to ``intent`` for safe retry."""

    receipt = _typed(
        delivery_id=attempt.delivery_id,
        provider=attempt.provider,
        gateway_account_id=attempt.gateway_account_id,
        target=attempt.target,
        outcome="retryable",
        replay_strategy_value=attempt.replay_strategy,
        error="semantic_delivery_prewrite_failed",
        delivery_scope_id=attempt.delivery_scope_id,
        provider_write_attempted=False,
        provider_write_started=False,
        provider_error=provider_error,
        provider_rejection=provider_rejection,
    )
    receipt_json = json.dumps(
        receipt,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    with _connection(attempt._ledger_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = _settlement_row(connection, attempt)
        if row is None or row["state"] != "sending":
            connection.rollback()
            return _settlement_ambiguous(attempt)
        cursor = connection.execute(
            "UPDATE semantic_deliveries SET state='intent',"
            "provider_receipt_json=?,"
            "error_code='semantic_delivery_prewrite_failed',updated_at=? "
            f"WHERE {_IDENTITY_WHERE} AND state='sending'",
            (
                receipt_json,
                _now(),
                *_identity_update_where(attempt),
            ),
        )
        if cursor.rowcount != 1:
            connection.rollback()
            return _settlement_ambiguous(attempt)
        connection.commit()
    _schedule_retry_outbox(
        attempt,
        retry_after=retry_after,
        last_error=_delivery_failure_text(
            error="semantic_delivery_prewrite_failed",
            provider_error=provider_error,
            provider_rejection=provider_rejection,
        ),
    )
    return receipt


def _mark_rejected_prewrite(
    attempt: SemanticDeliveryAttempt,
    *,
    provider_error: str | Mapping[str, Any] | None = None,
    provider_rejection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist a safe but permanently unsupported provider route."""

    receipt = _typed(
        delivery_id=attempt.delivery_id,
        provider=attempt.provider,
        gateway_account_id=attempt.gateway_account_id,
        target=attempt.target,
        outcome="rejected",
        replay_strategy_value=attempt.replay_strategy,
        error="semantic_delivery_provider_rejected",
        delivery_scope_id=attempt.delivery_scope_id,
        provider_write_attempted=False,
        provider_write_started=False,
        provider_error=provider_error,
        provider_rejection=provider_rejection,
    )
    with _connection(attempt._ledger_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = _settlement_row(connection, attempt)
        if row is None:
            connection.rollback()
            return _settlement_ambiguous(attempt)
        if row["state"] == "rejected":
            connection.rollback()
            return _stored_result(
                row,
                delivery_scope_id=attempt.delivery_scope_id,
            )
        if row["state"] != "sending":
            connection.rollback()
            return _settlement_ambiguous(attempt)
        cursor = connection.execute(
            "UPDATE semantic_deliveries SET state='rejected',"
            "provider_receipt_json=?,"
            "error_code='semantic_delivery_provider_rejected',updated_at=? "
            f"WHERE {_IDENTITY_WHERE} AND state='sending'",
            (
                json.dumps(
                    receipt,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                _now(),
                *_identity_update_where(attempt),
            ),
        )
        if cursor.rowcount != 1:
            connection.rollback()
            return _settlement_ambiguous(attempt)
        connection.commit()
    return receipt


def _store_delivered(
    attempt: SemanticDeliveryAttempt,
    provider_result: dict[str, Any],
) -> dict[str, Any]:
    message_ids = _safe_message_ids(provider_result)
    if not message_ids:
        return _mark_ambiguous(attempt)
    message_id = message_ids[-1]
    receipt = _typed(
        delivery_id=attempt.delivery_id,
        provider=attempt.provider,
        gateway_account_id=attempt.gateway_account_id,
        target=attempt.target,
        outcome="delivered",
        replay_strategy_value=attempt.replay_strategy,
        message_id=message_id,
        message_ids=message_ids,
        replayed=attempt.recovery_replay,
        delivery_scope_id=attempt.delivery_scope_id,
        provider_write_attempted=True,
        provider_write_started=True,
    )
    receipt_json = json.dumps(
        receipt,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    now = _now()
    with _connection(attempt._ledger_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT metadata_value FROM semantic_delivery_metadata "
            "WHERE metadata_key=?",
            (_SCOPE_METADATA_KEY,),
        ).fetchone()
        if (
            row is None
            or str(row["metadata_value"]) != attempt.delivery_scope_id
        ):
            connection.rollback()
            return _settlement_ambiguous(attempt)
        delivery_row = _settlement_row(connection, attempt)
        if delivery_row is None:
            connection.rollback()
            return _settlement_ambiguous(attempt)
        if delivery_row["state"] == "delivered":
            connection.rollback()
            return _stored_result(
                delivery_row,
                delivery_scope_id=attempt.delivery_scope_id,
            )
        if delivery_row["state"] != "sending":
            connection.rollback()
            return _settlement_ambiguous(attempt)
        cursor = connection.execute(
            "UPDATE semantic_deliveries SET state='delivered',"
            "provider_receipt_json=?,error_code=NULL,updated_at=? "
            f"WHERE {_IDENTITY_WHERE} AND state='sending'",
            (
                receipt_json,
                now,
                *_identity_update_where(attempt),
            ),
        )
        if cursor.rowcount != 1:
            connection.rollback()
            return _settlement_ambiguous(attempt)
        connection.commit()
    return receipt


def _durable_native_retryable(
    attempt: SemanticDeliveryAttempt,
) -> dict[str, Any]:
    """Confirm the exact sending row still exists before promising a retry."""

    with _connection(attempt._ledger_path) as connection:
        connection.execute("BEGIN")
        row = _settlement_row(connection, attempt)
        connection.rollback()
    if row is None or row["state"] != "sending":
        return _settlement_ambiguous(attempt)
    return _typed(
        delivery_id=attempt.delivery_id,
        provider=attempt.provider,
        gateway_account_id=attempt.gateway_account_id,
        target=attempt.target,
        outcome="retryable",
        replay_strategy_value=attempt.replay_strategy,
        error="semantic_delivery_provider_failed",
        delivery_scope_id=attempt.delivery_scope_id,
    )


def finish_semantic_delivery(
    attempt: SemanticDeliveryAttempt,
    provider_result: dict[str, Any],
) -> dict[str, Any]:
    """Persist the provider outcome and release the attempt's file lock."""

    if attempt.action != "send":
        return attempt.result or {}
    try:
        provider_error = _safe_bounded_diagnostic(
            provider_result.get("provider_error")
        )
        if provider_error is None:
            provider_error = _bounded_diagnostic(
                provider_result.get("provider_error")
                or provider_result.get("error")
            )
        provider_rejection = _safe_provider_rejection_evidence(
            provider_result.get("provider_rejection")
        )
        result: dict[str, Any]
        if provider_result.get("skipped") is True:
            result = _mark_rejected_prewrite(
                attempt,
                provider_error=provider_error,
                provider_rejection=provider_rejection,
            )
        elif provider_result.get("success") is True:
            if not _safe_message_ids(provider_result):
                result = _mark_ambiguous(attempt)
            else:
                result = _store_delivered(attempt, provider_result)
        elif provider_result.get("provider_write_attempted") is False:
            if provider_result.get("provider_retryable") is True:
                result = _mark_retryable_prewrite(
                    attempt,
                    retry_after=provider_result.get("retry_after"),
                    provider_error=provider_error,
                    provider_rejection=provider_rejection,
                )
            elif provider_result.get("provider_retryable") is False:
                result = _mark_rejected_prewrite(
                    attempt,
                    provider_error=provider_error,
                    provider_rejection=provider_rejection,
                )
            else:
                result = _mark_ambiguous(attempt)
        elif attempt.replay_strategy == "durable_native":
            # The row deliberately remains ``sending``. Another process can
            # safely retry the exact same Matrix transaction identity.
            result = _durable_native_retryable(attempt)
        else:
            result = _mark_ambiguous(attempt)
        if result.get("outcome") != "retryable":
            _retire_retry_outbox(
                attempt,
                reason=str(result.get("outcome") or "unknown"),
            )
        return result
    finally:
        attempt.release()


def semantic_send(
    *,
    delivery_id: str,
    contract_version: str,
    target: str,
    message: str,
    send: Callable[[dict[str, Any]], str | dict[str, Any]],
    ledger_path: Path | None = None,
    expected_scope_id: str | None = None,
    gateway_account_id: str = "",
    after_provider_result: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Send once or replay the exact result saved for this delivery identity."""

    attempt = begin_semantic_delivery(
        delivery_id=delivery_id,
        contract_version=contract_version,
        target=target,
        message=message,
        ledger_path=ledger_path,
        expected_scope_id=expected_scope_id,
        gateway_account_id=gateway_account_id,
    )
    if attempt.action != "send":
        return attempt.result or {}

    try:
        try:
            raw_result = send(
                {
                    "action": "send",
                    "target": target,
                    "message": message,
                    "delivery_contract": contract_version,
                    "delivery_id": delivery_id,
                    "gateway_account_id": attempt.gateway_account_id,
                    "delivery_target": target,
                    "delivery_scope_id": attempt.delivery_scope_id,
                }
            )
            decoded = (
                json.loads(raw_result)
                if isinstance(raw_result, str)
                else raw_result
            )
            result = (
                decoded
                if isinstance(decoded, dict)
                else {"error": "semantic_delivery_provider_receipt_invalid"}
            )
        except Exception:
            result = {"error": "semantic_delivery_provider_transport_failed"}

        if after_provider_result is not None:
            after_provider_result(dict(result))
        return finish_semantic_delivery(attempt, result)
    finally:
        # ``finish_semantic_delivery`` normally releases it. This also covers a
        # test/process crash seam implemented as a raised BaseException.
        attempt.release()


def _claim_next_semantic_retry(
    *,
    due_before: float,
    ledger_path: Path | None,
) -> SemanticRetryWork | None:
    """Lease one due item with a cross-process compare-and-set."""

    with _connection(ledger_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        scope_row = connection.execute(
            "SELECT metadata_value FROM semantic_delivery_metadata "
            "WHERE metadata_key=?",
            (_SCOPE_METADATA_KEY,),
        ).fetchone()
        row = connection.execute(
            "SELECT item.* FROM semantic_delivery_retry_outbox AS item "
            "JOIN semantic_delivery_retry_groups AS group_state "
            "ON group_state.delivery_group_id=item.delivery_group_id "
            "WHERE group_state.state='pending' AND ("
            "(item.state='pending' AND item.next_attempt_at<=?) "
            "OR (item.state='leased' AND item.lease_expires_at<=?)) "
            "AND NOT EXISTS ("
            "SELECT 1 FROM semantic_delivery_retry_outbox AS predecessor "
            "WHERE predecessor.delivery_group_id=item.delivery_group_id "
            "AND predecessor.unit_index<item.unit_index "
            "AND NOT (predecessor.state='retired' "
            "AND predecessor.last_error='delivered')) "
            "ORDER BY item.created_at,item.delivery_group_id,item.unit_index "
            "LIMIT 1",
            (due_before, time.time()),
        ).fetchone()
        if row is None:
            connection.rollback()
            return None
        try:
            route = _decode_retry_route(
                str(row["route_json"]),
                provider=str(row["provider"]),
            )
            canonical_encoding_contract = json.dumps(
                route.encoding_contract.as_mapping(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            if (
                route.as_json() != str(row["route_json"])
                or canonical_encoding_contract
                != str(row["encoding_contract_json"])
            ):
                raise ValueError("semantic retry route invalid")
        except ValueError:
            connection.execute(
                "UPDATE semantic_delivery_retry_outbox "
                "SET state='corrupt',lease_id=NULL,lease_expires_at=NULL,"
                "last_error='semantic_retry_route_integrity_failed',"
                "updated_at=? WHERE delivery_id=?",
                (_now(), str(row["delivery_id"])),
            )
            connection.execute(
                "UPDATE semantic_delivery_retry_groups "
                "SET state='blocked',"
                "last_error='semantic_retry_route_integrity_failed',"
                "updated_at=? WHERE delivery_group_id=? AND state IN "
                "('staging','pending','completing')",
                (_now(), str(row["delivery_group_id"])),
            )
            connection.commit()
            return None
        lease_id = f"lease_{uuid.uuid4().hex}"
        now = time.time()
        cursor = connection.execute(
            "UPDATE semantic_delivery_retry_outbox "
            "SET state='leased',lease_id=?,lease_expires_at=?,updated_at=? "
            "WHERE delivery_id=? AND ("
            "(state='pending' AND next_attempt_at<=?) OR "
            "(state='leased' AND lease_expires_at<=?))",
            (
                lease_id,
                now + _RETRY_LEASE_SECONDS,
                _now(),
                str(row["delivery_id"]),
                due_before,
                now,
            ),
        )
        if cursor.rowcount != 1:
            connection.rollback()
            return None
        connection.commit()
    return SemanticRetryWork(
        delivery_id=str(row["delivery_id"]),
        delivery_group_id=str(row["delivery_group_id"]),
        unit_index=int(row["unit_index"]),
        unit_count=int(row["unit_count"]),
        contract_version=str(row["contract_version"]),
        payload_digest=str(row["payload_digest"]),
        provider=str(row["provider"]),
        gateway_account_id=str(row["gateway_account_id"]),
        target=str(row["target"]),
        route=route,
        payload_backend=str(row["payload_backend"]),
        payload_ref=str(row["payload_ref"]),
        attempt_count=int(row["attempt_count"]),
        lease_id=lease_id,
        delivery_scope_id=str(scope_row["metadata_value"]),
    )


def _claim_next_semantic_retry_completion(
    *,
    due_before: float,
    ledger_path: Path | None,
) -> SemanticRetryCompletionWork | None:
    with _connection(ledger_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT group_state.* FROM semantic_delivery_retry_groups "
            "AS group_state "
            "WHERE (("
            "group_state.state='pending' "
            "AND group_state.next_completion_at>0 "
            "AND group_state.next_completion_at<=?) OR ("
            "group_state.state='completing' "
            "AND group_state.completion_lease_expires_at<=?)) "
            "AND NOT EXISTS ("
            "SELECT 1 FROM semantic_delivery_retry_outbox AS item "
            "WHERE item.delivery_group_id=group_state.delivery_group_id "
            "AND NOT (item.state='retired' AND item.last_error='delivered')) "
            "ORDER BY group_state.next_completion_at,"
            "group_state.created_at,group_state.delivery_group_id LIMIT 1",
            (due_before, time.time()),
        ).fetchone()
        if row is None:
            connection.rollback()
            return None
        delivery_group_id = str(row["delivery_group_id"])
        delivery_rows = connection.execute(
            "SELECT delivery_id FROM semantic_delivery_retry_outbox "
            "WHERE delivery_group_id=? ORDER BY unit_index",
            (delivery_group_id,),
        ).fetchall()
        if len(delivery_rows) != int(row["unit_count"]):
            connection.execute(
                "UPDATE semantic_delivery_retry_groups "
                "SET state='blocked',last_error="
                "'semantic_retry_group_unit_set_incomplete',updated_at=? "
                "WHERE delivery_group_id=?",
                (_now(), delivery_group_id),
            )
            connection.commit()
            return None
        lease_id = f"completion_{uuid.uuid4().hex}"
        cursor = connection.execute(
            "UPDATE semantic_delivery_retry_groups "
            "SET state='completing',completion_lease_id=?,"
            "completion_lease_expires_at=?,updated_at=? "
            "WHERE (delivery_group_id=? AND state='pending' "
            "AND next_completion_at<=?) OR (delivery_group_id=? "
            "AND state='completing' "
            "AND completion_lease_expires_at<=?)",
            (
                lease_id,
                time.time() + _RETRY_LEASE_SECONDS,
                _now(),
                delivery_group_id,
                due_before,
                delivery_group_id,
                time.time(),
            ),
        )
        if cursor.rowcount != 1:
            connection.rollback()
            return None
        connection.commit()
    return SemanticRetryCompletionWork(
        delivery_group_id=delivery_group_id,
        completion_contract=str(row["completion_contract"]),
        completion_ref=str(row["completion_ref"]),
        completion_attempt_count=int(row["completion_attempt_count"]),
        completion_lease_id=lease_id,
        delivery_ids=tuple(
            str(delivery_row["delivery_id"])
            for delivery_row in delivery_rows
        ),
    )


def _settle_semantic_retry_completion(
    work: SemanticRetryCompletionWork,
    *,
    completed: bool,
    error: str = "",
    ledger_path: Path | None,
) -> bool:
    with _connection(ledger_path) as connection:
        if completed:
            cursor = connection.execute(
                "UPDATE semantic_delivery_retry_groups "
                "SET state='completed',completion_lease_id=NULL,"
                "completion_lease_expires_at=NULL,last_error=NULL,"
                "updated_at=? WHERE delivery_group_id=? "
                "AND state='completing' AND completion_lease_id=?",
                (
                    _now(),
                    work.delivery_group_id,
                    work.completion_lease_id,
                ),
            )
        else:
            attempt_count = work.completion_attempt_count + 1
            cursor = connection.execute(
                "UPDATE semantic_delivery_retry_groups "
                "SET state='pending',completion_attempt_count=?,"
                "next_completion_at=?,completion_lease_id=NULL,"
                "completion_lease_expires_at=NULL,last_error=?,updated_at=? "
                "WHERE delivery_group_id=? AND state='completing' "
                "AND completion_lease_id=?",
                (
                    attempt_count,
                    time.time()
                    + _retry_delay(
                        work.delivery_group_id,
                        attempt_count,
                        None,
                    ),
                    _bounded_diagnostic_text(
                        error or "semantic_retry_completion_failed"
                    ),
                    _now(),
                    work.delivery_group_id,
                    work.completion_lease_id,
                ),
            )
        connection.commit()
    return cursor.rowcount == 1


async def _dispatch_semantic_retry_completions(
    *,
    ledger_path: Path | None,
) -> tuple[int, int]:
    completed = 0
    deferred = 0
    snapshot = time.time()
    while True:
        work = await asyncio.to_thread(
            _claim_next_semantic_retry_completion,
            due_before=snapshot,
            ledger_path=ledger_path,
        )
        if work is None:
            break
        if not work.completion_contract:
            ok = True
        else:
            handler = _semantic_retry_completion_handlers.get(
                work.completion_contract
            )
            if handler is None:
                await asyncio.to_thread(
                    _settle_semantic_retry_completion,
                    work,
                    completed=False,
                    error="semantic_retry_completion_handler_unavailable",
                    ledger_path=ledger_path,
                )
                deferred += 1
                continue
            try:
                value = handler(
                    completion_ref=work.completion_ref,
                    delivery_group_id=work.delivery_group_id,
                    delivery_ids=work.delivery_ids,
                    ledger_path=ledger_path,
                )
                if asyncio.iscoroutine(value):
                    value = await value
                ok = bool(
                    value is True
                    or (
                        isinstance(value, Mapping)
                        and (
                            value.get("completed") is True
                            or value.get("acknowledged") is True
                            or value.get("state") == "succeeded"
                        )
                    )
                )
            except Exception:
                ok = False
        await asyncio.to_thread(
            _settle_semantic_retry_completion,
            work,
            completed=ok,
            error=(
                ""
                if ok
                else "semantic_retry_completion_handler_failed"
            ),
            ledger_path=ledger_path,
        )
        if ok:
            completed += 1
        else:
            deferred += 1
    return completed, deferred


def _defer_semantic_retry(
    work: SemanticRetryWork,
    *,
    reason: str,
    delay: float = 15.0,
    ledger_path: Path | None,
) -> bool:
    next_attempt_at = time.time() + max(0.25, float(delay))
    with _connection(ledger_path) as connection:
        cursor = connection.execute(
            "UPDATE semantic_delivery_retry_outbox "
            "SET state='pending',next_attempt_at=?,lease_id=NULL,"
            "lease_expires_at=NULL,last_error=?,updated_at=? "
            "WHERE delivery_id=? AND state='leased' AND lease_id=?",
            (
                next_attempt_at,
                _bounded_diagnostic_text(reason),
                _now(),
                work.delivery_id,
                work.lease_id,
            ),
        )
        connection.commit()
    return cursor.rowcount == 1


def _mark_semantic_retry_corrupt(
    work: SemanticRetryWork,
    *,
    reason: str,
    ledger_path: Path | None,
) -> bool:
    with _connection(ledger_path) as connection:
        cursor = connection.execute(
            "UPDATE semantic_delivery_retry_outbox "
            "SET state='corrupt',lease_id=NULL,lease_expires_at=NULL,"
            "last_error=?,updated_at=? "
            "WHERE delivery_id=? AND state='leased' AND lease_id=?",
            (
                _bounded_diagnostic_text(reason),
                _now(),
                work.delivery_id,
                work.lease_id,
            ),
        )
        connection.execute(
            "UPDATE semantic_delivery_retry_groups "
            "SET state='blocked',last_error=?,updated_at=? "
            "WHERE delivery_group_id=? AND state IN "
            "('staging','pending','completing')",
            (
                _bounded_diagnostic_text(reason),
                _now(),
                work.delivery_group_id,
            ),
        )
        connection.commit()
    return cursor.rowcount == 1


def _iter_bound_adapters(bound_adapters: Any) -> tuple[Any, ...]:
    """Flatten primary/secondary adapter mappings without inferring a route."""

    if isinstance(bound_adapters, Mapping):
        ordered: list[Any] = []
        for value in bound_adapters.values():
            if isinstance(value, Mapping):
                ordered.extend(_iter_bound_adapters(value))
            elif value is not None:
                ordered.append(value)
        return tuple(ordered)
    if isinstance(bound_adapters, (list, tuple, set, frozenset)):
        return tuple(value for value in bound_adapters if value is not None)
    return (bound_adapters,) if bound_adapters is not None else ()


def _adapter_account_id(adapter: Any) -> str:
    direct = getattr(adapter, "gateway_account_id", None)
    extra = getattr(getattr(adapter, "config", None), "extra", None)
    configured = (
        extra.get("gateway_account_id")
        if isinstance(extra, dict)
        else None
    )
    values = [
        str(value)
        for value in (direct, configured)
        if value is not None and str(value) != ""
    ]
    if not values or any(value != values[0] for value in values[1:]):
        return ""
    value = values[0]
    if (
        value != value.strip()
        or len(value) > 500
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        return ""
    return value


def _resolve_retry_adapter(
    work: SemanticRetryWork,
    bound_adapters: Any,
) -> Any | None:
    from gateway.platform_registry import (
        supports_live_semantic_exact_attempt,
    )
    from gateway.semantic_exact_attempt import (
        owns_live_semantic_exact_attempt,
        supports_live_semantic_exact_attempt_encoding_contract,
    )

    if not supports_live_semantic_exact_attempt(work.provider):
        return None
    matches: list[Any] = []
    for adapter in _iter_bound_adapters(bound_adapters):
        provider = str(
            getattr(
                getattr(adapter, "platform", None),
                "value",
                getattr(adapter, "platform", ""),
            )
            or ""
        ).strip().lower()
        if (
            provider == work.provider
            and _adapter_account_id(adapter) == work.gateway_account_id
            and supports_live_semantic_exact_attempt(
                work.provider,
                adapter=adapter,
            )
            and owns_live_semantic_exact_attempt(adapter)
            and supports_live_semantic_exact_attempt_encoding_contract(
                adapter,
                work.route.encoding_contract,
            )
        ):
            matches.append(adapter)
    # Zero is disconnected/unavailable; >1 is ambiguous authority. Neither is
    # allowed to silently choose a credential or report fake success.
    return matches[0] if len(matches) == 1 else None


def _load_retry_message(
    work: SemanticRetryWork,
    *,
    ledger_path: Path | None,
) -> str:
    backend = _semantic_retry_payload_backends.get(work.payload_backend)
    if backend is None:
        raise LookupError("semantic retry payload backend unavailable")
    payload = backend.read(
        payload_ref=work.payload_ref,
        ledger_path=ledger_path,
    )
    try:
        message = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("semantic retry payload encoding invalid") from exc
    digest = _canonical_digest(
        contract_version=work.contract_version,
        gateway_account_id=work.gateway_account_id,
        target=work.target,
        message=message,
    )
    if digest != work.payload_digest or not message.strip():
        raise ValueError("semantic retry payload digest mismatch")
    return message


def _send_result_mapping(result: Any) -> dict[str, Any]:
    """Normalize a live adapter result without inventing provider proof."""

    raw_response = getattr(result, "raw_response", None)
    raw = raw_response if isinstance(raw_response, dict) else {}
    message_ids: list[str] = []
    continuation = getattr(result, "continuation_message_ids", None)
    if isinstance(continuation, (list, tuple)):
        for value in continuation:
            clean = _safe_message_id(value)
            if clean and clean not in message_ids:
                message_ids.append(clean)
    message_id = _safe_message_id(getattr(result, "message_id", None))
    if message_id and message_id not in message_ids:
        message_ids.append(message_id)
    if getattr(result, "success", False) is True:
        return {
            "success": True,
            "message_id": message_ids[-1] if message_ids else None,
            "message_ids": message_ids,
        }
    raw_error = str(
        getattr(result, "error", None)
        or raw.get("error")
        or "semantic_delivery_provider_failed"
    )
    mapped: dict[str, Any] = {
        "error": "semantic_delivery_provider_failed",
        "provider_error": _bounded_diagnostic(raw_error),
    }
    provider_rejection = _safe_provider_rejection_evidence(
        raw.get("provider_rejection")
    )
    if provider_rejection is not None:
        mapped["provider_rejection"] = provider_rejection
    write_attempted = raw.get("provider_write_attempted")
    if isinstance(write_attempted, bool):
        mapped["provider_write_attempted"] = write_attempted
    provider_retryable = raw.get("provider_retryable")
    if not isinstance(provider_retryable, bool):
        direct_retryable = getattr(result, "retryable", None)
        provider_retryable = (
            direct_retryable
            if isinstance(direct_retryable, bool)
            else None
        )
    if isinstance(provider_retryable, bool):
        mapped["provider_retryable"] = provider_retryable
    retry_after = getattr(result, "retry_after", None)
    if retry_after is None:
        retry_after = raw.get("retry_after")
    if not isinstance(retry_after, bool):
        try:
            retry_after_value = float(retry_after)
        except (TypeError, ValueError):
            retry_after_value = -1.0
        if 0.0 <= retry_after_value < float("inf"):
            mapped["retry_after"] = retry_after_value
    return mapped


async def dispatch_due_semantic_delivery_retries(
    *,
    bound_adapters: Any,
    ledger_path: Path | None = None,
    due_before: float | None = None,
) -> dict[str, int]:
    """Drain the due snapshot through real bound adapters, one write each.

    Rows rescheduled during this pump are newer than ``due_before`` and wait
    for a later scheduler tick, preventing a zero-delay 429 busy loop. There
    is no terminal attempt count and no user/admin/restart gate.
    """

    snapshot = time.time() if due_before is None else float(due_before)
    counts = {
        "claimed": 0,
        "delivered": 0,
        "retryable": 0,
        "deferred": 0,
        "ambiguous": 0,
        "rejected": 0,
        "corrupt": 0,
        "completion_completed": 0,
        "completion_deferred": 0,
    }
    while True:
        work = await asyncio.to_thread(
            _claim_next_semantic_retry,
            due_before=snapshot,
            ledger_path=ledger_path,
        )
        if work is None:
            break
        counts["claimed"] += 1
        adapter = _resolve_retry_adapter(work, bound_adapters)
        if adapter is None:
            await asyncio.to_thread(
                _defer_semantic_retry,
                work,
                reason="semantic_retry_exact_adapter_unavailable",
                delay=15.0,
                ledger_path=ledger_path,
            )
            counts["deferred"] += 1
            continue
        try:
            message = await asyncio.to_thread(
                _load_retry_message,
                work,
                ledger_path=ledger_path,
            )
        except LookupError:
            await asyncio.to_thread(
                _defer_semantic_retry,
                work,
                reason="semantic_retry_payload_backend_unavailable",
                delay=30.0,
                ledger_path=ledger_path,
            )
            counts["deferred"] += 1
            continue
        except (OSError, ValueError):
            await asyncio.to_thread(
                _mark_semantic_retry_corrupt,
                work,
                reason="semantic_retry_payload_integrity_failed",
                ledger_path=ledger_path,
            )
            counts["corrupt"] += 1
            continue
        attempt = await asyncio.to_thread(
            begin_semantic_delivery,
            delivery_id=work.delivery_id,
            contract_version=work.contract_version,
            target=work.target,
            message=message,
            ledger_path=ledger_path,
            expected_scope_id=work.delivery_scope_id,
            gateway_account_id=work.gateway_account_id,
        )
        if attempt.action != "send":
            outcome = str((attempt.result or {}).get("outcome") or attempt.action)
            if outcome == "in_flight":
                await asyncio.to_thread(
                    _defer_semantic_retry,
                    work,
                    reason="semantic_retry_delivery_in_flight",
                    delay=2.0,
                    ledger_path=ledger_path,
                )
                counts["deferred"] += 1
            elif outcome in {"delivered", "rejected", "ambiguous"}:
                await asyncio.to_thread(
                    _retire_retry_outbox,
                    attempt,
                    reason=outcome,
                )
                counts[outcome] += 1
            else:
                await asyncio.to_thread(
                    _mark_semantic_retry_corrupt,
                    work,
                    reason="semantic_retry_delivery_identity_conflict",
                    ledger_path=ledger_path,
                )
                counts["corrupt"] += 1
            continue
        from gateway.semantic_exact_attempt import (
            LiveSemanticExactAttemptRequest,
            send_via_exact_adapter_method,
        )

        exact_request = LiveSemanticExactAttemptRequest(
            chat_id=work.route.chat_id,
            content=message,
            delivery_contract=work.contract_version,
            delivery_id=work.delivery_id,
            delivery_target=work.target,
            delivery_unit=work.route.delivery_unit,
            encoding_contract=work.route.encoding_contract,
            provider_route=work.route.provider_route,
            thread_id=work.route.thread_id,
            reply_to=work.route.reply_to,
        )
        try:
            provider_result = _send_result_mapping(
                await send_via_exact_adapter_method(
                    adapter,
                    exact_request,
                )
            )
        except asyncio.CancelledError:
            attempt.release()
            raise
        except Exception:
            # An untyped exception might be after provider acceptance. Never
            # turn it into a pre-write retry.
            provider_result = {
                "error": "semantic_delivery_provider_transport_failed"
            }
        result = await asyncio.to_thread(
            finish_semantic_delivery,
            attempt,
            provider_result,
        )
        outcome = str(result.get("outcome") or "ambiguous")
        if outcome in counts:
            counts[outcome] += 1
        else:
            counts["ambiguous"] += 1
    completion_completed, completion_deferred = (
        await _dispatch_semantic_retry_completions(
            ledger_path=ledger_path,
        )
    )
    counts["completion_completed"] = completion_completed
    counts["completion_deferred"] = completion_deferred
    return counts


def pump_semantic_delivery_retries(
    *,
    bound_adapters: Any,
    loop: asyncio.AbstractEventLoop,
    ledger_path: Path | None = None,
    timeout: float = 180.0,
) -> dict[str, int]:
    """Thread-safe scheduler bridge into the gateway's live event loop."""

    if loop is None or not loop.is_running():
        return {
            "claimed": 0,
            "delivered": 0,
            "retryable": 0,
            "deferred": 0,
            "ambiguous": 0,
            "rejected": 0,
            "corrupt": 0,
            "completion_completed": 0,
            "completion_deferred": 0,
        }
    future = asyncio.run_coroutine_threadsafe(
        dispatch_due_semantic_delivery_retries(
            bound_adapters=bound_adapters,
            ledger_path=ledger_path,
        ),
        loop,
    )
    return future.result(timeout=max(1.0, float(timeout)))


def semantic_delivery_retry_status(
    *,
    delivery_id: str,
    ledger_path: Path | None = None,
) -> dict[str, Any] | None:
    """Read scheduler state without exposing payload bytes or provider route."""

    with _connection(ledger_path) as connection:
        row = connection.execute(
            "SELECT item.state,item.attempt_count,item.next_attempt_at,"
            "item.last_error,item.payload_backend,item.delivery_group_id,"
            "item.unit_index,item.unit_count,group_state.state AS group_state "
            "FROM semantic_delivery_retry_outbox AS item "
            "JOIN semantic_delivery_retry_groups AS group_state "
            "ON group_state.delivery_group_id=item.delivery_group_id "
            "WHERE item.delivery_id=?",
            (str(delivery_id),),
        ).fetchone()
    if row is None:
        return None
    return {
        "delivery_id": str(delivery_id),
        "delivery_group_id": str(row["delivery_group_id"]),
        "group_state": str(row["group_state"]),
        "state": str(row["state"]),
        "attempt_count": int(row["attempt_count"]),
        "next_attempt_at": float(row["next_attempt_at"]),
        "last_error": (
            str(row["last_error"])
            if row["last_error"] is not None
            else None
        ),
        "payload_backend": str(row["payload_backend"]),
        "retry_automatic": True,
        "retry_limit": None,
        "unit_count": int(row["unit_count"]),
        "unit_index": int(row["unit_index"]),
    }


def semantic_retry_turn_execution_committed(
    turn_execution_ref: str,
    *,
    ledger_path: Path | None = None,
) -> bool:
    """Prove a queued inbound turn transferred ownership to the outbox.

    The proof is deliberately derived from durable rows, never a process-local
    callback: exactly ``N`` distinct ordered units must exist and the owning
    group must have crossed its atomic ``staging`` barrier. A malformed,
    unknown, duplicated, partial, or tampered reference fails closed.
    """

    clean = str(turn_execution_ref or "").strip()
    if _DELIVERY_ID.fullmatch(clean) is None:
        return False
    with _connection(ledger_path) as connection:
        row = connection.execute(
            "SELECT group_state.state,group_state.unit_count,"
            "COUNT(item.delivery_id) AS item_count,"
            "COUNT(DISTINCT item.unit_index) AS distinct_units,"
            "MIN(item.unit_index) AS minimum_unit,"
            "MAX(item.unit_index) AS maximum_unit,"
            "SUM(CASE WHEN item.unit_count=group_state.unit_count "
            "THEN 0 ELSE 1 END) AS invalid_unit_count "
            "FROM semantic_delivery_retry_groups AS group_state "
            "LEFT JOIN semantic_delivery_retry_outbox AS item "
            "ON item.delivery_group_id=group_state.delivery_group_id "
            "WHERE group_state.turn_execution_ref=? "
            "GROUP BY group_state.delivery_group_id",
            (clean,),
        ).fetchone()
    if row is None:
        return False
    try:
        unit_count = int(row["unit_count"])
        item_count = int(row["item_count"])
        distinct_units = int(row["distinct_units"])
        minimum_unit = int(row["minimum_unit"])
        maximum_unit = int(row["maximum_unit"])
        invalid_unit_count = int(row["invalid_unit_count"])
    except (TypeError, ValueError):
        return False
    return bool(
        str(row["state"]) != "staging"
        and unit_count > 0
        and item_count == unit_count
        and distinct_units == unit_count
        and minimum_unit == 0
        and maximum_unit == unit_count - 1
        and invalid_unit_count == 0
    )


__all__ = [
    "SEMANTIC_DELIVERY_CONTRACT",
    "SEMANTIC_RETRY_ROUTE_CONTRACT",
    "SemanticDeliveryAttempt",
    "SemanticRetryPayloadBackend",
    "SemanticRetryCompletionWork",
    "SemanticRetryRoute",
    "SemanticRetryWork",
    "begin_semantic_delivery",
    "delivery_ledger_path",
    "dispatch_due_semantic_delivery_retries",
    "finish_semantic_delivery",
    "exact_provider_message_id",
    "is_exact_provider_message_id",
    "pump_semantic_delivery_retries",
    "provider_delivery_token",
    "register_semantic_retry_payload_backend",
    "register_semantic_retry_completion_handler",
    "replay_strategy",
    "semantic_delivery_scope_id",
    "semantic_delivery_retry_status",
    "semantic_delivery_status",
    "semantic_retry_turn_execution_committed",
    "semantic_send",
    "stage_semantic_delivery_retry",
    "unregister_semantic_retry_completion_handler",
]
