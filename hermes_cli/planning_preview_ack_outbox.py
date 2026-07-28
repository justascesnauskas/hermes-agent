"""Durable continuation for provider-proven Planning preview acknowledgements.

The provider send and the Dev Hub review acknowledgement cannot share one
transaction.  This profile-private outbox closes that gap: once the exact
provider proof is queued, a gateway worker keeps replaying the same
idempotency-bound Hub request until it succeeds.  A process death after Hub
commit is harmless because the next worker repeats the byte-identical request.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import secrets
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping

from hermes_constants import get_hermes_home
from hermes_cli.semantic_delivery import (
    exact_provider_message_id,
    is_exact_provider_message_id,
)


ACK_REQUEST_SCHEMA = "planning.preview-ack-request.v1"
PLANNING_PREVIEW_ACK_COMPLETION_CONTRACT = (
    "planning.preview-ack-bridge-completion.v1"
)
_LEASE_SECONDS = 45.0
_MAX_BACKOFF_SECONDS = 60.0
_SCHEMA = """
CREATE TABLE IF NOT EXISTS planning_preview_ack_outbox (
    ack_id TEXT PRIMARY KEY,
    request_digest TEXT NOT NULL,
    request_json TEXT NOT NULL,
    state TEXT NOT NULL
        CHECK (
            state IN (
                'pending', 'dispatching', 'retry_scheduled',
                'succeeded', 'rejected'
            )
        ),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at REAL NOT NULL,
    lease_token TEXT,
    lease_expires_at REAL,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    CHECK (
        (state = 'dispatching'
            AND lease_token IS NOT NULL
            AND lease_expires_at IS NOT NULL)
        OR
        (state <> 'dispatching'
            AND lease_token IS NULL
            AND lease_expires_at IS NULL)
    )
)
"""
_DUE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_planning_preview_ack_outbox_due
ON planning_preview_ack_outbox(state, available_at, created_at)
"""
_BRIDGE_SCHEMA = """
CREATE TABLE IF NOT EXISTS planning_preview_ack_bridge (
    bridge_id TEXT PRIMARY KEY,
    bridge_digest TEXT NOT NULL,
    requests_json TEXT NOT NULL,
    semantic_delivery_ids_json TEXT NOT NULL,
    semantic_scope_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    gateway_account_id TEXT NOT NULL,
    state TEXT NOT NULL
        CHECK (state IN ('pending', 'enqueued', 'ambiguous', 'rejected')),
    delivered_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
)
"""
_BRIDGE_DUE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_planning_preview_ack_bridge_due
ON planning_preview_ack_bridge(state, created_at, bridge_id)
"""


class PreviewAckOutboxError(RuntimeError):
    """A durable acknowledgement contract is invalid or conflicting."""


@dataclass(frozen=True, slots=True)
class PreviewAckClaim:
    ack_id: str
    request: dict[str, Any]
    lease_token: str
    attempts: int


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def preview_ack_outbox_path() -> Path:
    return (
        get_hermes_home()
        / "state"
        / "planning-preview-ack"
        / "outbox.sqlite3"
    )


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


def _connection(path: Path | None = None) -> sqlite3.Connection:
    target = path or preview_ack_outbox_path()
    _private_directory(target.parent)
    connection = sqlite3.connect(target, timeout=30)
    _private_file(target)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(_SCHEMA)
    connection.execute(_DUE_INDEX)
    connection.execute(_BRIDGE_SCHEMA)
    connection.execute(_BRIDGE_DUE_INDEX)
    connection.commit()
    for suffix in ("-wal", "-shm"):
        _private_file(Path(f"{target}{suffix}"))
    return connection


def _strict_text(value: Any, name: str) -> str:
    clean = str(value or "")
    if (
        not clean
        or clean != clean.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in clean)
    ):
        raise PreviewAckOutboxError(f"{name} is invalid")
    return clean


def _strict_sha256(value: Any, name: str) -> str:
    clean = _strict_text(value, name)
    digest = clean.removeprefix("sha256:")
    if (
        not clean.startswith("sha256:")
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise PreviewAckOutboxError(f"{name} is invalid")
    return clean


def _validated_request(raw: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise PreviewAckOutboxError("preview acknowledgement request is invalid")
    request = dict(raw)
    if request.get("schemaVersion") != ACK_REQUEST_SCHEMA:
        raise PreviewAckOutboxError(
            "preview acknowledgement request schema is invalid"
        )
    for name in (
        "threadId",
        "previewResultId",
        "idempotencyKey",
        "expectedPreviewHash",
        "pageDigest",
    ):
        request[name] = _strict_text(request.get(name), name)
    for name in ("offset", "count"):
        value = request.get(name)
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < (0 if name == "offset" else 1)
        ):
            raise PreviewAckOutboxError(f"{name} is invalid")
    origin = request.get("origin")
    proof = request.get("deliveryProof")
    if not isinstance(origin, dict) or not isinstance(proof, dict):
        raise PreviewAckOutboxError(
            "origin and deliveryProof must be exact objects"
        )
    for name in (
        "provider",
        "gatewayInstanceId",
        "gatewayAccountId",
        "chatId",
        "messageId",
        "senderId",
        "providerEventId",
    ):
        _strict_text(origin.get(name), f"origin.{name}")
    if origin.get("schemaVersion") != "1.0":
        raise PreviewAckOutboxError("origin.schemaVersion is invalid")
    required_proof = (
        "deliveryNonce",
        "provider",
        "gatewayInstanceId",
        "gatewayAccountId",
        "chatId",
        "deliveredAt",
        "previewResultId",
        "previewResultHash",
        "pageDigest",
        "deliveryPayloadDigest",
        "deliveryContentDigest",
    )
    if proof.get("schemaVersion") != "planning.preview-delivery-proof.v1":
        raise PreviewAckOutboxError("deliveryProof.schemaVersion is invalid")
    for name in required_proof:
        _strict_text(proof.get(name), f"deliveryProof.{name}")
    if not is_exact_provider_message_id(proof.get("providerMessageId")):
        raise PreviewAckOutboxError(
            "deliveryProof.providerMessageId is invalid"
        )
    message_ids = proof.get("providerMessageIds")
    if (
        not isinstance(message_ids, list)
        or not message_ids
        or any(
            not is_exact_provider_message_id(item)
            for item in message_ids
        )
        or len(set(message_ids)) != len(message_ids)
        or proof["providerMessageId"] != message_ids[-1]
    ):
        raise PreviewAckOutboxError(
            "deliveryProof.providerMessageIds is invalid"
        )
    for name in ("offset", "count"):
        if proof.get(name) != request[name]:
            raise PreviewAckOutboxError(
                f"deliveryProof.{name} does not match the request"
            )
    exact_pairs = {
        "previewResultId": request["previewResultId"],
        "previewResultHash": request["expectedPreviewHash"],
        "pageDigest": request["pageDigest"],
        "provider": origin["provider"],
        "gatewayInstanceId": origin["gatewayInstanceId"],
        "gatewayAccountId": origin["gatewayAccountId"],
        "chatId": origin["chatId"],
    }
    mismatches = {
        name
        for name, expected in exact_pairs.items()
        if proof.get(name) != expected
    }
    if mismatches:
        raise PreviewAckOutboxError(
            "deliveryProof identity does not match the exact request"
        )
    return request


_BRIDGE_PROOF_BASE_FIELDS = frozenset(
    {
        "schemaVersion",
        "deliveryNonce",
        "provider",
        "gatewayInstanceId",
        "gatewayAccountId",
        "chatId",
        "previewResultId",
        "previewResultHash",
        "offset",
        "count",
        "pageDigest",
    }
)


def _validated_bridge_request_base(
    raw: Mapping[str, Any],
    *,
    provider: str,
    gateway_account_id: str,
) -> dict[str, Any]:
    """Prove a bridge can become a valid ACK before provider bytes move."""

    if not isinstance(raw, Mapping):
        raise PreviewAckOutboxError(
            "preview acknowledgement bridge request is invalid"
        )
    request = dict(raw)
    proof_base = request.get("deliveryProofBase")
    if (
        request.get("schemaVersion") != ACK_REQUEST_SCHEMA
        or "deliveryProof" in request
        or not isinstance(proof_base, dict)
        or set(proof_base) != _BRIDGE_PROOF_BASE_FIELDS
    ):
        raise PreviewAckOutboxError(
            "preview acknowledgement bridge proof is invalid"
        )
    payload_digest = _strict_sha256(
        request.get("deliveryPayloadDigest"),
        "deliveryPayloadDigest",
    )
    content_digest = _strict_sha256(
        request.get("deliveryContentDigest"),
        "deliveryContentDigest",
    )
    _strict_sha256(
        request.get("expectedPreviewHash"),
        "expectedPreviewHash",
    )
    _strict_sha256(request.get("pageDigest"), "pageDigest")
    _strict_sha256(
        proof_base.get("previewResultHash"),
        "deliveryProofBase.previewResultHash",
    )
    _strict_sha256(
        proof_base.get("pageDigest"),
        "deliveryProofBase.pageDigest",
    )

    # Reuse the final ACK validator with deterministic placeholders for the
    # three facts that can exist only after provider acceptance. This proves
    # every stored base can be reconciled before the first external write.
    final_request = dict(request)
    final_request.pop("deliveryProofBase", None)
    final_request.pop("deliveryPayloadDigest", None)
    final_request.pop("deliveryContentDigest", None)
    final_request["deliveryProof"] = {
        **proof_base,
        "providerMessageId": "bridge-prewrite-validation",
        "providerMessageIds": ["bridge-prewrite-validation"],
        "deliveredAt": "1970-01-01T00:00:00+00:00",
        "deliveryPayloadDigest": payload_digest,
        "deliveryContentDigest": content_digest,
    }
    validated = _validated_request(final_request)
    origin = validated["origin"]
    if (
        str(origin["provider"]).lower() != provider
        or str(origin["gatewayAccountId"]) != gateway_account_id
    ):
        raise PreviewAckOutboxError(
            "preview acknowledgement bridge authority does not match "
            "the provider route"
        )
    return request


def _identity(request: Mapping[str, Any]) -> tuple[str, str, str]:
    request_json = _canonical_json(request)
    digest = "sha256:" + hashlib.sha256(request_json.encode("utf-8")).hexdigest()
    idempotency_key = str(request["idempotencyKey"])
    ack_id = (
        "pack_"
        + hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:48]
    )
    return ack_id, digest, request_json


def enqueue_preview_ack(
    raw_request: Mapping[str, Any],
    *,
    path: Path | None = None,
) -> dict[str, Any]:
    """Persist one exact Hub acknowledgement before the live turn retires."""

    request = _validated_request(raw_request)
    ack_id, digest, request_json = _identity(request)
    now = time.time()
    now_iso = _now_iso()
    with _connection(path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT * FROM planning_preview_ack_outbox WHERE ack_id=?",
            (ack_id,),
        ).fetchone()
        if row is None:
            connection.execute(
                "INSERT INTO planning_preview_ack_outbox "
                "(ack_id,request_digest,request_json,state,attempts,"
                "available_at,lease_token,lease_expires_at,last_error,"
                "created_at,updated_at,completed_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    ack_id,
                    digest,
                    request_json,
                    "pending",
                    0,
                    now,
                    None,
                    None,
                    None,
                    now_iso,
                    now_iso,
                    None,
                ),
            )
            state = "pending"
            replayed = False
        else:
            if (
                str(row["request_digest"]) != digest
                or str(row["request_json"]) != request_json
            ):
                connection.rollback()
                raise PreviewAckOutboxError(
                    "preview acknowledgement idempotency conflict"
                )
            state = str(row["state"])
            replayed = True
        connection.commit()
    return {
        "ackId": ack_id,
        "state": state,
        "replayed": replayed,
    }


def stage_preview_ack_bridge(
    raw_requests: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
    *,
    semantic_delivery_ids: list[str] | tuple[str, ...],
    semantic_scope_id: str,
    provider: str,
    gateway_account_id: str,
    path: Path | None = None,
) -> dict[str, Any]:
    """Persist ACK reconstruction authority before the first provider write.

    The bridge intentionally stores no preview/message bytes and no
    credentials.  It binds the immutable Hub request bases to the ordered
    semantic-delivery identities whose durable receipts will later prove that
    every provider unit landed.
    """

    delivery_ids = [str(value or "").strip() for value in semantic_delivery_ids]
    scope_id = _strict_text(semantic_scope_id, "semanticScopeId")
    clean_provider = _strict_text(provider, "provider").lower()
    account_id = _strict_text(gateway_account_id, "gatewayAccountId")
    requests = [
        _validated_bridge_request_base(
            request,
            provider=clean_provider,
            gateway_account_id=account_id,
        )
        for request in raw_requests
    ]
    if (
        not requests
        or not delivery_ids
        or any(not value for value in delivery_ids)
        or len(set(delivery_ids)) != len(delivery_ids)
    ):
        raise PreviewAckOutboxError("preview acknowledgement bridge is invalid")

    identity = {
        "schemaVersion": "planning.preview-ack-bridge.v1",
        "requests": requests,
        "semanticDeliveryIds": delivery_ids,
        "semanticScopeId": scope_id,
        "provider": clean_provider,
        "gatewayAccountId": account_id,
    }
    identity_json = _canonical_json(identity)
    digest = "sha256:" + hashlib.sha256(
        identity_json.encode("utf-8")
    ).hexdigest()
    bridge_id = "pbridge_" + digest.removeprefix("sha256:")[:48]
    requests_json = _canonical_json(requests)
    delivery_ids_json = _canonical_json(delivery_ids)
    now = _now_iso()
    with _connection(path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT bridge_digest,state FROM planning_preview_ack_bridge "
            "WHERE bridge_id=?",
            (bridge_id,),
        ).fetchone()
        if row is None:
            connection.execute(
                "INSERT INTO planning_preview_ack_bridge "
                "(bridge_id,bridge_digest,requests_json,"
                "semantic_delivery_ids_json,semantic_scope_id,provider,"
                "gateway_account_id,state,delivered_at,last_error,created_at,"
                "updated_at,completed_at) "
                "VALUES (?,?,?,?,?,?,?,'pending',NULL,NULL,?,?,NULL)",
                (
                    bridge_id,
                    digest,
                    requests_json,
                    delivery_ids_json,
                    scope_id,
                    clean_provider,
                    account_id,
                    now,
                    now,
                ),
            )
            state = "pending"
            replayed = False
        else:
            if str(row["bridge_digest"]) != digest:
                connection.rollback()
                raise PreviewAckOutboxError(
                    "preview acknowledgement bridge identity conflict"
                )
            state = str(row["state"])
            replayed = True
        connection.commit()
    return {
        "bridgeId": bridge_id,
        "state": state,
        "replayed": replayed,
    }


def _bridge_provider_message_ids(statuses: list[dict[str, Any]]) -> list[str]:
    ordered: list[str] = []
    for status in statuses:
        values = status.get("message_ids")
        if not isinstance(values, (list, tuple)):
            values = ()
        for value in (*values, status.get("message_id")):
            message_id = exact_provider_message_id(value)
            if message_id and message_id not in ordered:
                ordered.append(message_id)
    return ordered


def reconcile_preview_ack_bridge(
    bridge_id: str | None = None,
    *,
    path: Path | None = None,
    semantic_ledger_path: Path | None = None,
    status_reader: Callable[..., Mapping[str, Any]] | None = None,
    delivered_at: str | None = None,
) -> dict[str, Any] | None:
    """Turn durable provider receipts into byte-stable ACK outbox rows.

    This is deliberately safe to run in a fresh process.  Missing/pre-write
    semantic rows remain pending, live writes remain pending, and any
    ambiguous provider unit fences only this bridge instead of replaying
    already committed units.
    """

    from hermes_cli.semantic_delivery import (
        SEMANTIC_DELIVERY_CONTRACT,
        semantic_delivery_status,
    )

    if status_reader is None:
        status_reader = semantic_delivery_status

    with _connection(path) as connection:
        params: tuple[Any, ...] = ()
        query = "SELECT * FROM planning_preview_ack_bridge"
        if bridge_id is not None:
            query += " WHERE bridge_id=? AND state IN ('pending','enqueued')"
            params = (str(bridge_id),)
        else:
            query += " WHERE state='pending'"
        query += " ORDER BY created_at,bridge_id LIMIT 1"
        row = connection.execute(query, params).fetchone()
    if row is None:
        return None

    try:
        requests = json.loads(str(row["requests_json"]))
        delivery_ids = json.loads(str(row["semantic_delivery_ids_json"]))
    except (TypeError, json.JSONDecodeError):
        requests = None
        delivery_ids = None
    if (
        not isinstance(requests, list)
        or not requests
        or not all(isinstance(request, dict) for request in requests)
        or not isinstance(delivery_ids, list)
        or not delivery_ids
        or not all(isinstance(value, str) and value for value in delivery_ids)
    ):
        now = _now_iso()
        with _connection(path) as connection:
            connection.execute(
                "UPDATE planning_preview_ack_bridge SET state='rejected',"
                "last_error='planning_preview_ack_bridge_corrupt',"
                "updated_at=?,completed_at=? "
                "WHERE bridge_id=? AND state='pending'",
                (now, now, str(row["bridge_id"])),
            )
            connection.commit()
        return {
            "bridgeId": str(row["bridge_id"]),
            "state": "rejected",
            "ackIds": [],
        }

    status_kwargs = (
        {"ledger_path": semantic_ledger_path}
        if semantic_ledger_path is not None
        else {}
    )
    statuses = [
        dict(
            status_reader(
                delivery_id=delivery_id,
                contract_version=SEMANTIC_DELIVERY_CONTRACT,
                expected_scope_id=str(row["semantic_scope_id"]),
                expected_provider=str(row["provider"]),
                gateway_account_id=str(row["gateway_account_id"]),
                **status_kwargs,
            )
        )
        for delivery_id in delivery_ids
    ]
    outcomes = [str(status.get("outcome") or "") for status in statuses]
    terminal_failure = next(
        (
            outcome
            for outcome in outcomes
            if outcome in {"ambiguous", "conflict", "rejected"}
        ),
        None,
    )
    if terminal_failure is not None:
        state = "rejected" if terminal_failure == "rejected" else "ambiguous"
        now = _now_iso()
        with _connection(path) as connection:
            connection.execute(
                "UPDATE planning_preview_ack_bridge SET state=?,last_error=?,"
                "updated_at=?,completed_at=? "
                "WHERE bridge_id=? AND state='pending'",
                (
                    state,
                    f"semantic_delivery_{terminal_failure}",
                    now,
                    now,
                    str(row["bridge_id"]),
                ),
            )
            connection.commit()
        return {
            "bridgeId": str(row["bridge_id"]),
            "state": state,
            "ackIds": [],
        }
    if any(outcome != "delivered" for outcome in outcomes):
        return {
            "bridgeId": str(row["bridge_id"]),
            "state": "pending",
            "ackIds": [],
        }

    message_ids = _bridge_provider_message_ids(statuses)
    if not message_ids:
        now = _now_iso()
        with _connection(path) as connection:
            connection.execute(
                "UPDATE planning_preview_ack_bridge SET state='ambiguous',"
                "last_error='semantic_delivery_receipt_invalid',"
                "updated_at=?,completed_at=? "
                "WHERE bridge_id=? AND state='pending'",
                (now, now, str(row["bridge_id"])),
            )
            connection.commit()
        return {
            "bridgeId": str(row["bridge_id"]),
            "state": "ambiguous",
            "ackIds": [],
        }

    stable_delivered_at = str(delivered_at or "").strip() or _now_iso()
    if str(row["state"]) == "pending":
        with _connection(path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE planning_preview_ack_bridge "
                "SET delivered_at=COALESCE(delivered_at,?),updated_at=? "
                "WHERE bridge_id=? AND state='pending'",
                (stable_delivered_at, _now_iso(), str(row["bridge_id"])),
            )
            stable_row = connection.execute(
                "SELECT state,delivered_at FROM planning_preview_ack_bridge "
                "WHERE bridge_id=?",
                (str(row["bridge_id"]),),
            ).fetchone()
            connection.commit()
    else:
        stable_row = row
    if stable_row is None:
        return None
    if str(stable_row["state"]) not in {"pending", "enqueued"}:
        return {
            "bridgeId": str(row["bridge_id"]),
            "state": str(stable_row["state"]),
            "ackIds": [],
        }
    stable_delivered_at = str(stable_row["delivered_at"])

    ack_ids: list[str] = []
    for raw_request in requests:
        request = dict(raw_request)
        proof_base = request.pop("deliveryProofBase", None)
        if not isinstance(proof_base, dict):
            raise PreviewAckOutboxError(
                "preview acknowledgement bridge proof is invalid"
            )
        request["schemaVersion"] = ACK_REQUEST_SCHEMA
        request["deliveryProof"] = {
            **proof_base,
            "providerMessageId": message_ids[-1],
            "providerMessageIds": list(message_ids),
            "deliveredAt": stable_delivered_at,
            "deliveryPayloadDigest": _strict_text(
                raw_request.get("deliveryPayloadDigest"),
                "deliveryPayloadDigest",
            ),
            "deliveryContentDigest": _strict_text(
                raw_request.get("deliveryContentDigest"),
                "deliveryContentDigest",
            ),
        }
        request.pop("deliveryPayloadDigest", None)
        request.pop("deliveryContentDigest", None)
        queued = enqueue_preview_ack(request, path=path)
        ack_ids.append(str(queued["ackId"]))

    if str(stable_row["state"]) == "pending":
        now = _now_iso()
        with _connection(path) as connection:
            connection.execute(
                "UPDATE planning_preview_ack_bridge SET state='enqueued',"
                "last_error=NULL,updated_at=?,completed_at=? "
                "WHERE bridge_id=? AND state='pending'",
                (now, now, str(row["bridge_id"])),
            )
            connection.commit()
    return {
        "bridgeId": str(row["bridge_id"]),
        "state": "enqueued",
        "ackIds": ack_ids,
        "providerMessageIds": message_ids,
        "deliveredAt": stable_delivered_at,
    }


def _complete_semantic_retry_preview_ack(
    *,
    completion_ref: str,
    delivery_group_id: str,
    delivery_ids: tuple[str, ...],
    ledger_path: Path | None = None,
) -> dict[str, Any]:
    """Release ACK reconstruction after the ordered exact group lands."""

    del delivery_group_id, delivery_ids
    ack_path: Path | None = None
    if ledger_path is not None:
        semantic_path = Path(ledger_path)
        if (
            semantic_path.name != "ledger.sqlite3"
            or semantic_path.parent.name != "semantic-delivery"
            or semantic_path.parent.parent.name != "state"
        ):
            raise PreviewAckOutboxError(
                "semantic delivery ledger is not profile-scoped"
            )
        ack_path = (
            semantic_path.parent.parent
            / "planning-preview-ack"
            / "outbox.sqlite3"
        )
    outcome = reconcile_preview_ack_bridge(
        str(completion_ref),
        path=ack_path,
        semantic_ledger_path=ledger_path,
    )
    return {
        "completed": bool(
            outcome is not None and outcome.get("state") == "enqueued"
        )
    }


def claim_preview_ack(
    ack_id: str | None = None,
    *,
    path: Path | None = None,
    now: float | None = None,
) -> PreviewAckClaim | None:
    """Lease one due request; expired process claims converge automatically."""

    current = time.time() if now is None else float(now)
    with _connection(path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        params: list[Any] = [current, current]
        query = (
            "SELECT * FROM planning_preview_ack_outbox "
            "WHERE ((state IN ('pending','retry_scheduled') "
            "AND available_at<=?) OR "
            "(state='dispatching' AND lease_expires_at<=?))"
        )
        if ack_id is not None:
            query += " AND ack_id=?"
            params.append(str(ack_id))
        query += " ORDER BY available_at,created_at,ack_id LIMIT 1"
        row = connection.execute(query, tuple(params)).fetchone()
        if row is None:
            connection.rollback()
            return None
        token = f"packlease_{secrets.token_hex(24)}"
        attempts = int(row["attempts"]) + 1
        updated = connection.execute(
            "UPDATE planning_preview_ack_outbox "
            "SET state='dispatching',attempts=?,lease_token=?,"
            "lease_expires_at=?,updated_at=? "
            "WHERE ack_id=? AND state=? AND "
            "COALESCE(lease_token,'')=COALESCE(?,'')",
            (
                attempts,
                token,
                current + _LEASE_SECONDS,
                _now_iso(),
                row["ack_id"],
                row["state"],
                row["lease_token"],
            ),
        )
        if updated.rowcount != 1:
            connection.rollback()
            return None
        connection.commit()
    try:
        request = json.loads(str(row["request_json"]))
    except (TypeError, json.JSONDecodeError) as exc:
        reject_preview_ack(
            str(row["ack_id"]),
            token,
            error="planning_preview_ack_request_corrupt",
            path=path,
        )
        raise PreviewAckOutboxError(
            "persisted preview acknowledgement request is corrupt"
        ) from exc
    if not isinstance(request, dict):
        reject_preview_ack(
            str(row["ack_id"]),
            token,
            error="planning_preview_ack_request_corrupt",
            path=path,
        )
        raise PreviewAckOutboxError(
            "persisted preview acknowledgement request is corrupt"
        )
    return PreviewAckClaim(
        ack_id=str(row["ack_id"]),
        request=request,
        lease_token=token,
        attempts=attempts,
    )


def complete_preview_ack(
    ack_id: str,
    lease_token: str,
    *,
    path: Path | None = None,
) -> bool:
    now = _now_iso()
    with _connection(path) as connection:
        result = connection.execute(
            "UPDATE planning_preview_ack_outbox "
            "SET state='succeeded',lease_token=NULL,lease_expires_at=NULL,"
            "last_error=NULL,updated_at=?,completed_at=? "
            "WHERE ack_id=? AND state='dispatching' AND lease_token=?",
            (now, now, str(ack_id), str(lease_token)),
        )
        connection.commit()
    return result.rowcount == 1


def retry_preview_ack(
    ack_id: str,
    lease_token: str,
    *,
    error: str,
    attempts: int,
    path: Path | None = None,
) -> bool:
    # The number of attempts is observability, never a product stop condition.
    delay = min(_MAX_BACKOFF_SECONDS, float(2 ** min(max(attempts, 1), 6)))
    with _connection(path) as connection:
        result = connection.execute(
            "UPDATE planning_preview_ack_outbox "
            "SET state='retry_scheduled',available_at=?,lease_token=NULL,"
            "lease_expires_at=NULL,last_error=?,updated_at=? "
            "WHERE ack_id=? AND state='dispatching' AND lease_token=?",
            (
                time.time() + delay,
                str(error or "planning_preview_ack_retryable")[:1_000],
                _now_iso(),
                str(ack_id),
                str(lease_token),
            ),
        )
        connection.commit()
    return result.rowcount == 1


def reject_preview_ack(
    ack_id: str,
    lease_token: str,
    *,
    error: str,
    path: Path | None = None,
) -> bool:
    now = _now_iso()
    with _connection(path) as connection:
        result = connection.execute(
            "UPDATE planning_preview_ack_outbox "
            "SET state='rejected',lease_token=NULL,lease_expires_at=NULL,"
            "last_error=?,updated_at=?,completed_at=? "
            "WHERE ack_id=? AND state='dispatching' AND lease_token=?",
            (
                str(error or "planning_preview_ack_rejected")[:1_000],
                now,
                now,
                str(ack_id),
                str(lease_token),
            ),
        )
        connection.commit()
    return result.rowcount == 1


def preview_ack_status(
    ack_id: str,
    *,
    path: Path | None = None,
) -> dict[str, Any] | None:
    with _connection(path) as connection:
        row = connection.execute(
            "SELECT ack_id,state,attempts,available_at,last_error,"
            "created_at,updated_at,completed_at "
            "FROM planning_preview_ack_outbox WHERE ack_id=?",
            (str(ack_id),),
        ).fetchone()
    if row is None:
        return None
    return {
        "ackId": str(row["ack_id"]),
        "state": str(row["state"]),
        "attempts": int(row["attempts"]),
        "availableAt": float(row["available_at"]),
        "lastError": row["last_error"],
        "createdAt": str(row["created_at"]),
        "updatedAt": str(row["updated_at"]),
        "completedAt": row["completed_at"],
    }


def deliver_preview_ack_request(request: Mapping[str, Any]) -> Any:
    """Reconstruct the authenticated Hub call without persisting credentials."""

    from hermes_cli.dev_hub_planning_v2 import PlanningV2Client

    client = PlanningV2Client()
    return client.acknowledge_preview_page(
        str(request["threadId"]),
        str(request["previewResultId"]),
        idempotency_key=str(request["idempotencyKey"]),
        expected_preview_hash=str(request["expectedPreviewHash"]),
        offset=int(request["offset"]),
        count=int(request["count"]),
        page_digest=str(request["pageDigest"]),
        origin=dict(request["origin"]),
        delivery_proof=dict(request["deliveryProof"]),
    )


def dispatch_one_preview_ack(
    *,
    deliver: Callable[[Mapping[str, Any]], Any] = deliver_preview_ack_request,
    path: Path | None = None,
    ack_id: str | None = None,
) -> dict[str, Any] | None:
    """Run one leased request and settle it without a fixed retry count."""

    claim = claim_preview_ack(ack_id, path=path)
    if claim is None:
        return None
    try:
        deliver(claim.request)
    except Exception as exc:
        retryable = bool(
            getattr(exc, "retryable", False)
            or getattr(exc, "ambiguous", False)
        )
        # Unknown exceptions are transport/config availability failures unless
        # the typed client explicitly proves a permanent response.
        permanent = getattr(exc, "http_status", None)
        if permanent is None:
            permanent = getattr(exc, "status", None)
        if not retryable and isinstance(permanent, int) and 400 <= permanent < 500:
            reject_preview_ack(
                claim.ack_id,
                claim.lease_token,
                error=str(getattr(exc, "code", None) or type(exc).__name__),
                path=path,
            )
            state = "rejected"
        else:
            retry_preview_ack(
                claim.ack_id,
                claim.lease_token,
                error=str(getattr(exc, "code", None) or type(exc).__name__),
                attempts=claim.attempts,
                path=path,
            )
            state = "retry_scheduled"
        return {
            "ackId": claim.ack_id,
            "state": state,
            "attempts": claim.attempts,
        }
    if not complete_preview_ack(
        claim.ack_id,
        claim.lease_token,
        path=path,
    ):
        raise PreviewAckOutboxError(
            "preview acknowledgement lease was lost before settlement"
        )
    return {
        "ackId": claim.ack_id,
        "state": "succeeded",
        "attempts": claim.attempts,
    }


def run_preview_ack_worker(
    stop_event: threading.Event,
    *,
    deliver: Callable[[Mapping[str, Any]], Any] = deliver_preview_ack_request,
    path: Path | None = None,
    poll_seconds: float = 1.0,
) -> None:
    """Drain across the gateway lifetime and every later process restart."""

    while not stop_event.is_set():
        try:
            bridge_outcome = reconcile_preview_ack_bridge(path=path)
            outcome = dispatch_one_preview_ack(
                deliver=deliver,
                path=path,
            )
        except Exception:
            bridge_outcome = None
            outcome = None
        if outcome is None and (
            bridge_outcome is None
            or bridge_outcome.get("state") == "pending"
        ):
            stop_event.wait(max(0.05, float(poll_seconds)))


from hermes_cli.semantic_delivery import (  # noqa: E402
    register_semantic_retry_completion_handler,
)

register_semantic_retry_completion_handler(
    PLANNING_PREVIEW_ACK_COMPLETION_CONTRACT,
    _complete_semantic_retry_preview_ack,
)


__all__ = [
    "ACK_REQUEST_SCHEMA",
    "PLANNING_PREVIEW_ACK_COMPLETION_CONTRACT",
    "PreviewAckClaim",
    "PreviewAckOutboxError",
    "claim_preview_ack",
    "complete_preview_ack",
    "deliver_preview_ack_request",
    "dispatch_one_preview_ack",
    "enqueue_preview_ack",
    "preview_ack_outbox_path",
    "preview_ack_status",
    "reconcile_preview_ack_bridge",
    "reject_preview_ack",
    "retry_preview_ack",
    "run_preview_ack_worker",
    "stage_preview_ack_bridge",
]
