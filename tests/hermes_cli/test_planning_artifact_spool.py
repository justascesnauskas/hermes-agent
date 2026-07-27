"""Behavior tests for the restart-safe Planning V2 artifact ingress spool."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from hermes_cli.dev_hub_planning_v2 import PlanningV2ConfigError
from hermes_cli.planning_artifact_spool import (
    acknowledge_artifact_recovery,
    list_artifact_recoveries,
    load_artifact_recovery,
    register_artifact_recovery,
)


def _origin(*, sender_id: str = "discord-user") -> dict[str, object]:
    return {
        "schemaVersion": "1.0",
        "provider": "discord",
        "gatewayInstanceId": "runner-1",
        "gatewayAccountId": "discord-production",
        "chatId": "discord-chat",
        "threadId": "discord-thread",
        "messageId": "discord-message",
        "senderId": sender_id,
        "chatType": "direct",
        "sourceTimestamp": "2026-07-28T08:00:00Z",
        "providerEventId": "discord-event",
    }


def _register(
    *,
    home: Path,
    source: Path,
    idempotency_key: str = "hermes-planning-artifact-v1:stable",
    position: int = 1,
) -> str:
    return register_artifact_recovery(
        thread_id="planning-thread-1",
        local_path=str(source),
        origin=_origin(),
        role="design_reference",
        position=position,
        required=True,
        idempotency_key=idempotency_key,
        content_type="image/png",
        retain_until=None,
        attachment_identity=f"attachment-{position}",
        ingress_ordinal=position,
        hermes_home=home,
    )


def test_recovery_survives_fresh_process_without_original_source(
    tmp_path,
) -> None:
    home = tmp_path / "hermes-home"
    source = tmp_path / "dashboard.png"
    exact_bytes = b"\x89PNG\r\nrestart-safe artifact bytes"
    source.write_bytes(exact_bytes)

    token = _register(home=home, source=source)
    first = load_artifact_recovery(
        token,
        current_origin=_origin(),
        hermes_home=home,
    )
    assert Path(first.snapshot_path).read_bytes() == exact_bytes

    metadata_text = (
        Path(first.snapshot_path).parent / "recovery.json"
    ).read_text(encoding="utf-8")
    assert str(source) not in metadata_text
    assert first.snapshot_path not in metadata_text

    source.unlink()
    assert _register(home=home, source=source) == token

    repository = Path(__file__).resolve().parents[2]
    script = """
import hashlib
import json
from pathlib import Path
from hermes_cli.planning_artifact_spool import load_artifact_recovery

origin = json.loads(__import__("os").environ["TEST_RECOVERY_ORIGIN"])
record = load_artifact_recovery(
    __import__("os").environ["TEST_RECOVERY_TOKEN"],
    current_origin=origin,
)
payload = Path(record.snapshot_path).read_bytes()
print(json.dumps({
    "checksum": "sha256:" + hashlib.sha256(payload).hexdigest(),
    "size": len(payload),
    "idempotencyKey": record.idempotency_key,
}))
"""
    environment = dict(os.environ)
    environment.update(
        {
            "HERMES_HOME": str(home),
            "PYTHONPATH": str(repository),
            "TEST_RECOVERY_TOKEN": token,
            "TEST_RECOVERY_ORIGIN": json.dumps(_origin()),
        }
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=repository,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert completed.returncode == 0, completed.stderr
    process_record = json.loads(completed.stdout)
    assert process_record == {
        "checksum": first.checksum,
        "size": len(exact_bytes),
        "idempotencyKey": first.idempotency_key,
    }

    acknowledge_artifact_recovery(token, hermes_home=home)
    assert not Path(first.snapshot_path).exists()
    with pytest.raises(PlanningV2ConfigError) as unavailable:
        load_artifact_recovery(
            token,
            current_origin=_origin(),
            hermes_home=home,
        )
    assert unavailable.value.code == "planning.artifact_recovery_unavailable"


def test_recovery_remains_bound_to_exact_conversation_and_user(
    tmp_path,
) -> None:
    home = tmp_path / "hermes-home"
    source = tmp_path / "schema.pdf"
    source.write_bytes(b"%PDF-1.7 private schema")
    token = _register(home=home, source=source)

    with pytest.raises(PlanningV2ConfigError) as mismatch:
        load_artifact_recovery(
            token,
            current_origin=_origin(sender_id="different-user"),
            hermes_home=home,
        )

    assert mismatch.value.code == "planning.artifact_recovery_scope_mismatch"
    recovered = load_artifact_recovery(
        token,
        current_origin=_origin(),
        hermes_home=home,
    )
    assert Path(recovered.snapshot_path).read_bytes() == source.read_bytes()


def test_live_recoveries_have_no_arbitrary_count_or_ttl_eviction(
    tmp_path,
) -> None:
    home = tmp_path / "hermes-home"
    source = tmp_path / "reference.png"
    source.write_bytes(b"shared immutable test payload")

    tokens = [
        _register(
            home=home,
            source=source,
            idempotency_key=f"hermes-planning-artifact-v1:item-{position}",
            position=position,
        )
        for position in range(1, 138)
    ]

    assert len(set(tokens)) == 137
    spool_root = home / "planning-v2" / "artifact-ingress"
    assert len([path for path in spool_root.iterdir() if not path.name.startswith(".")]) == 137
    for token in (tokens[0], tokens[68], tokens[-1]):
        record = load_artifact_recovery(
            token,
            current_origin=_origin(),
            hermes_home=home,
        )
        assert Path(record.snapshot_path).read_bytes() == source.read_bytes()

    discovered = list_artifact_recoveries(
        current_origin={
            **_origin(),
            # A later retry turn keeps the same conversation/user scope while
            # receiving a fresh immutable message and provider event.
            "messageId": "discord-retry-message",
            "providerEventId": "discord-retry-event",
        },
        thread_id="planning-thread-1",
        hermes_home=home,
    )
    assert len(discovered) == 137
    assert [record.position for record in discovered] == list(range(1, 138))
    assert {record.token for record in discovered} == set(tokens)


def test_pending_recovery_enumeration_is_thread_and_user_scoped(
    tmp_path,
) -> None:
    home = tmp_path / "hermes-home"
    source = tmp_path / "reference.pdf"
    source.write_bytes(b"%PDF exact bytes")
    _register(home=home, source=source)

    assert list_artifact_recoveries(
        current_origin=_origin(),
        thread_id="another-thread",
        hermes_home=home,
    ) == ()
    assert list_artifact_recoveries(
        current_origin=_origin(sender_id="another-user"),
        thread_id="planning-thread-1",
        hermes_home=home,
    ) == ()
    exact = list_artifact_recoveries(
        current_origin=_origin(),
        thread_id="planning-thread-1",
        hermes_home=home,
    )
    assert len(exact) == 1


def test_corrupt_live_snapshot_is_never_silently_replaced(
    tmp_path,
) -> None:
    home = tmp_path / "hermes-home"
    source = tmp_path / "reference.png"
    source.write_bytes(b"authoritative bytes")
    token = _register(home=home, source=source)
    record = load_artifact_recovery(
        token,
        current_origin=_origin(),
        hermes_home=home,
    )
    Path(record.snapshot_path).write_bytes(b"tampered bytes")

    with pytest.raises(PlanningV2ConfigError) as corrupt:
        _register(home=home, source=source)

    assert corrupt.value.code == "planning.artifact_recovery_corrupt"
    assert Path(record.snapshot_path).read_bytes() == b"tampered bytes"
