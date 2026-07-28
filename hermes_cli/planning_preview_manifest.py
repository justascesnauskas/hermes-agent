"""Upgrade-safe segmentation authority for Planning preview delivery.

The manifest lives in the semantic-delivery ledger so its lifecycle cannot
drift from the provider receipts it names. It stores only content digests and
codepoint boundaries, never preview bytes or provider credentials.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import sqlite3
from typing import Any, Mapping

from hermes_cli.semantic_delivery import (
    delivery_ledger_path,
    semantic_delivery_scope_id,
)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS planning_preview_delivery_manifests (
    delivery_base_id TEXT PRIMARY KEY,
    manifest_digest TEXT NOT NULL,
    semantic_scope_id TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    provider TEXT NOT NULL,
    gateway_account_id TEXT NOT NULL,
    segmentation_contract TEXT NOT NULL,
    encoding_contract_json TEXT NOT NULL,
    boundaries_json TEXT NOT NULL,
    unit_digests_json TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""


class PreviewDeliveryManifestError(RuntimeError):
    """A persisted segmentation manifest is invalid or conflicts."""


@dataclass(frozen=True, slots=True)
class StagedPreviewDeliveryManifest:
    """The winning persisted segmentation contract and reconstructed units."""

    units: tuple[str, ...]
    segmentation_contract: str
    encoding_contract: dict[str, Any]


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _digest(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _strict(value: Any, name: str) -> str:
    clean = str(value or "")
    if (
        not clean
        or clean != clean.strip()
        or len(clean) > 500
        or any(ord(character) < 32 or ord(character) == 127 for character in clean)
    ):
        raise PreviewDeliveryManifestError(f"{name} is invalid")
    return clean


def _connection() -> sqlite3.Connection:
    # This call creates/migrates the owning semantic ledger and its opaque
    # scope before a second connection adds the colocated manifest table.
    semantic_delivery_scope_id()
    connection = sqlite3.connect(delivery_ledger_path(), timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(_SCHEMA)
    columns = {
        str(row["name"])
        for row in connection.execute(
            "PRAGMA table_info(planning_preview_delivery_manifests)"
        ).fetchall()
    }
    if "encoding_contract_json" not in columns:
        # A pre-contract experimental row cannot safely be re-encoded after a
        # deploy.  The empty value intentionally fails closed when read.
        connection.execute(
            "ALTER TABLE planning_preview_delivery_manifests "
            "ADD COLUMN encoding_contract_json TEXT NOT NULL DEFAULT ''"
        )
    connection.commit()
    return connection


def _manifest_identity(
    *,
    delivery_base_id: str,
    semantic_scope_id: str,
    content_digest: str,
    provider: str,
    gateway_account_id: str,
    segmentation_contract: str,
    encoding_contract: Mapping[str, Any],
    boundaries: list[int],
    unit_digests: list[str],
) -> dict[str, Any]:
    return {
        "schemaVersion": "planning.preview-delivery-manifest.v1",
        "deliveryBaseId": delivery_base_id,
        "semanticScopeId": semantic_scope_id,
        "contentDigest": content_digest,
        "provider": provider,
        "gatewayAccountId": gateway_account_id,
        "segmentationContract": segmentation_contract,
        "encodingContract": dict(encoding_contract),
        "boundaries": boundaries,
        "unitDigests": unit_digests,
    }


def _units_from_row(
    row: sqlite3.Row,
    *,
    content: str,
    delivery_base_id: str,
    semantic_scope_id: str,
    content_digest: str,
    provider: str,
    gateway_account_id: str,
) -> StagedPreviewDeliveryManifest:
    if (
        str(row["delivery_base_id"]) != delivery_base_id
        or str(row["semantic_scope_id"]) != semantic_scope_id
        or str(row["content_digest"]) != content_digest
        or str(row["provider"]) != provider
        or str(row["gateway_account_id"]) != gateway_account_id
    ):
        raise PreviewDeliveryManifestError(
            "preview delivery manifest authority conflict"
        )
    try:
        boundaries = json.loads(str(row["boundaries_json"]))
        unit_digests = json.loads(str(row["unit_digests_json"]))
        raw_encoding_contract = json.loads(
            str(row["encoding_contract_json"])
        )
    except (TypeError, json.JSONDecodeError) as exc:
        raise PreviewDeliveryManifestError(
            "preview delivery manifest is corrupt"
        ) from exc
    try:
        from gateway.semantic_exact_attempt import (
            coerce_live_semantic_exact_attempt_encoding_contract,
        )

        encoding_contract = (
            coerce_live_semantic_exact_attempt_encoding_contract(
                raw_encoding_contract
            )
        )
    except (TypeError, ValueError) as exc:
        raise PreviewDeliveryManifestError(
            "preview delivery manifest encoding contract is invalid"
        ) from exc
    if encoding_contract.provider != provider:
        raise PreviewDeliveryManifestError(
            "preview delivery manifest encoding authority conflict"
        )
    if (
        not isinstance(boundaries, list)
        or not isinstance(unit_digests, list)
        or not boundaries
        or len(boundaries) != len(unit_digests)
        or any(
            isinstance(boundary, bool) or not isinstance(boundary, int)
            for boundary in boundaries
        )
        or boundaries[-1] != len(content)
    ):
        raise PreviewDeliveryManifestError(
            "preview delivery manifest boundaries are invalid"
        )
    previous = 0
    units: list[str] = []
    for boundary, expected_digest in zip(
        boundaries,
        unit_digests,
        strict=True,
    ):
        if boundary <= previous:
            raise PreviewDeliveryManifestError(
                "preview delivery manifest boundaries are invalid"
            )
        unit = content[previous:boundary]
        if _digest(unit) != expected_digest:
            raise PreviewDeliveryManifestError(
                "preview delivery manifest unit digest is invalid"
            )
        units.append(unit)
        previous = boundary
    identity = _manifest_identity(
        delivery_base_id=delivery_base_id,
        semantic_scope_id=semantic_scope_id,
        content_digest=content_digest,
        provider=provider,
        gateway_account_id=gateway_account_id,
        segmentation_contract=str(row["segmentation_contract"]),
        encoding_contract=encoding_contract.as_mapping(),
        boundaries=boundaries,
        unit_digests=unit_digests,
    )
    if str(row["manifest_digest"]) != _digest(_canonical_json(identity)):
        raise PreviewDeliveryManifestError(
            "preview delivery manifest digest is invalid"
        )
    return StagedPreviewDeliveryManifest(
        units=tuple(units),
        segmentation_contract=str(row["segmentation_contract"]),
        encoding_contract=encoding_contract.as_mapping(),
    )


def stage_preview_delivery_manifest(
    *,
    delivery_base_id: str,
    semantic_scope_id: str,
    content: str,
    candidate_units: tuple[str, ...],
    segmentation_contract: str,
    encoding_contract: Mapping[str, Any],
    provider: str,
    gateway_account_id: str,
) -> StagedPreviewDeliveryManifest:
    """Persist once; later deploys reconstruct the winning exact boundaries."""

    base_id = _strict(delivery_base_id, "deliveryBaseId")
    scope_id = _strict(semantic_scope_id, "semanticScopeId")
    clean_provider = _strict(provider, "provider").lower()
    account_id = _strict(gateway_account_id, "gatewayAccountId")
    contract = _strict(segmentation_contract, "segmentationContract")
    try:
        from gateway.semantic_exact_attempt import (
            coerce_live_semantic_exact_attempt_encoding_contract,
        )

        frozen_encoding_contract = (
            coerce_live_semantic_exact_attempt_encoding_contract(
                encoding_contract
            )
        )
    except (TypeError, ValueError) as exc:
        raise PreviewDeliveryManifestError(
            "preview delivery encoding contract is invalid"
        ) from exc
    if frozen_encoding_contract.provider != clean_provider:
        raise PreviewDeliveryManifestError(
            "preview delivery encoding authority conflict"
        )
    encoding_contract_mapping = frozen_encoding_contract.as_mapping()
    encoding_contract_json = _canonical_json(encoding_contract_mapping)
    exact_content = str(content)
    units = tuple(str(unit) for unit in candidate_units)
    if (
        not exact_content
        or not units
        or any(not unit for unit in units)
        or "".join(units) != exact_content
    ):
        raise PreviewDeliveryManifestError(
            "preview delivery candidate segmentation is invalid"
        )
    content_digest = _digest(exact_content)
    boundaries: list[int] = []
    unit_digests: list[str] = []
    boundary = 0
    for unit in units:
        boundary += len(unit)
        boundaries.append(boundary)
        unit_digests.append(_digest(unit))
    identity = _manifest_identity(
        delivery_base_id=base_id,
        semantic_scope_id=scope_id,
        content_digest=content_digest,
        provider=clean_provider,
        gateway_account_id=account_id,
        segmentation_contract=contract,
        encoding_contract=encoding_contract_mapping,
        boundaries=boundaries,
        unit_digests=unit_digests,
    )
    manifest_digest = _digest(_canonical_json(identity))

    with _connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        scope_row = connection.execute(
            "SELECT metadata_value FROM semantic_delivery_metadata "
            "WHERE metadata_key='ledger_scope_id'"
        ).fetchone()
        if scope_row is None or str(scope_row["metadata_value"]) != scope_id:
            connection.rollback()
            raise PreviewDeliveryManifestError(
                "preview delivery manifest scope conflict"
            )
        row = connection.execute(
            "SELECT * FROM planning_preview_delivery_manifests "
            "WHERE delivery_base_id=?",
            (base_id,),
        ).fetchone()
        if row is None:
            connection.execute(
                "INSERT INTO planning_preview_delivery_manifests "
                "(delivery_base_id,manifest_digest,semantic_scope_id,"
                "content_digest,provider,gateway_account_id,"
                "segmentation_contract,encoding_contract_json,boundaries_json,"
                "unit_digests_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    base_id,
                    manifest_digest,
                    scope_id,
                    content_digest,
                    clean_provider,
                    account_id,
                    contract,
                    encoding_contract_json,
                    _canonical_json(boundaries),
                    _canonical_json(unit_digests),
                    datetime.now(UTC).isoformat(),
                ),
            )
            row = connection.execute(
                "SELECT * FROM planning_preview_delivery_manifests "
                "WHERE delivery_base_id=?",
                (base_id,),
            ).fetchone()
        connection.commit()
    if row is None:
        raise PreviewDeliveryManifestError(
            "preview delivery manifest was not persisted"
        )
    return _units_from_row(
        row,
        content=exact_content,
        delivery_base_id=base_id,
        semantic_scope_id=scope_id,
        content_digest=content_digest,
        provider=clean_provider,
        gateway_account_id=account_id,
    )
