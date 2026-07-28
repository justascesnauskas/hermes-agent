"""Durable, byte-free segmentation authority for Planning previews."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import sqlite3
import threading

import pytest

from hermes_cli.planning_preview_manifest import (
    PreviewDeliveryManifestError,
    stage_preview_delivery_manifest,
)
from hermes_cli.semantic_delivery import (
    delivery_ledger_path,
    semantic_delivery_scope_id,
)

ENCODING_CONTRACT = {
    "provider": "telegram",
    "contract": "hermes-live-semantic-exact-attempt/1",
    "segmentation_version": "telegram-preview-logical-v1",
    "max_logical_units": 1800,
    "length_semantics": "utf16_code_units",
    "wire_encoding": "telegram-text-entities-v1",
}


@pytest.fixture(autouse=True)
def _private_home(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))


def _stage(
    content: str,
    units: tuple[str, ...],
    *,
    account: str = "telegram-account",
) -> tuple[str, ...]:
    return stage_preview_delivery_manifest(
        delivery_base_id="preview_manifest_upgrade_safe",
        semantic_scope_id=semantic_delivery_scope_id(),
        content=content,
        candidate_units=units,
        segmentation_contract="planning.preview-segmentation.v1",
        encoding_contract=ENCODING_CONTRACT,
        provider="telegram",
        gateway_account_id=account,
    ).units


def test_existing_manifest_wins_after_runtime_budget_upgrade() -> None:
    content = "alpha\nbeta\ngamma\ndelta"
    version_one = ("alpha\nbeta\n", "gamma\ndelta")
    version_two = tuple(content[index : index + 3] for index in range(0, len(content), 3))

    assert _stage(content, version_one) == version_one
    assert _stage(content, version_two) == version_one

    with sqlite3.connect(delivery_ledger_path()) as connection:
        row = connection.execute(
            "SELECT segmentation_contract,boundaries_json,"
            "unit_digests_json FROM planning_preview_delivery_manifests"
        ).fetchone()
    assert row is not None
    assert row[0] == "planning.preview-segmentation.v1"
    assert content not in row[1]
    assert content not in row[2]


def test_manifest_route_authority_conflict_fails_closed() -> None:
    content = "one exact preview"
    assert _stage(content, (content,)) == (content,)

    with pytest.raises(
        PreviewDeliveryManifestError,
        match="authority conflict",
    ):
        _stage(content, (content,), account="another-account")


def test_concurrent_segmentation_candidates_converge_on_one_manifest() -> None:
    content = "abcdefghijklmnopqrstuvwxyz"
    candidates = (
        ("abcdefghijklm", "nopqrstuvwxyz"),
        ("abc", "defghijkl", "mnopqrstuvwxyz"),
    )
    scope_id = semantic_delivery_scope_id()
    barrier = threading.Barrier(2)

    def stage(units: tuple[str, ...]) -> tuple[str, ...]:
        barrier.wait()
        return stage_preview_delivery_manifest(
            delivery_base_id="preview_manifest_concurrent",
            semantic_scope_id=scope_id,
            content=content,
            candidate_units=units,
            segmentation_contract="planning.preview-segmentation.v1",
            encoding_contract=ENCODING_CONTRACT,
            provider="telegram",
            gateway_account_id="telegram-account",
        ).units

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(stage, candidates))

    assert results[0] == results[1]
    assert results[0] in candidates


def test_corrupt_manifest_never_resegments_or_reopens_delivery() -> None:
    content = "one exact preview"
    assert _stage(content, (content,)) == (content,)
    with sqlite3.connect(delivery_ledger_path()) as connection:
        connection.execute(
            "UPDATE planning_preview_delivery_manifests "
            "SET unit_digests_json='[\"sha256:broken\"]'"
        )
        connection.commit()

    with pytest.raises(
        PreviewDeliveryManifestError,
        match="unit digest",
    ):
        _stage(content, ("one ", "exact preview"))
