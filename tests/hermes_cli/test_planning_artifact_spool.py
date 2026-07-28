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
    load_artifact_recovery_completion,
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


def _completion() -> dict[str, object]:
    artifact = {
        "schemaVersion": "1.0",
        "artifactId": "blob-artifact-1",
        "artifactRef": "planning-artifact-v1:blob-artifact-1",
        "sourceReference": "planning-artifact-v1:blob-artifact-1",
        "referenceId": "reference-artifact-1",
        "checksum": "sha256:" + ("a" * 64),
        "sizeBytes": 2048,
        "contentType": "image/png",
        "role": "design_reference",
        "position": 1,
        "required": True,
        "filename": "dashboard.png",
    }
    return {
        "ok": True,
        "action": "upload_artifact",
        "threadId": "planning-thread-1",
        "artifact": artifact,
        "storageMode": "local",
        "uploadDisposition": "committed",
        "uploadReplayed": False,
        "inputStored": True,
        "inputReplayed": False,
        "previewInvalidated": False,
    }


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


def test_ack_tombstone_prevents_identical_source_resurrection(
    tmp_path,
) -> None:
    home = tmp_path / "hermes-home"
    source = tmp_path / "reference.png"
    source.write_bytes(b"still-present provider cache bytes")

    token = _register(home=home, source=source)
    record = load_artifact_recovery(
        token,
        current_origin=_origin(),
        hermes_home=home,
    )
    digest = token.removeprefix("artrec_v2_")
    spool_root = home / "planning-v2" / "artifact-ingress"

    assert acknowledge_artifact_recovery(token, hermes_home=home) is True
    assert source.exists()
    assert not Path(record.snapshot_path).exists()
    assert not (spool_root / digest).exists()
    assert (
        spool_root / ".acknowledged-tombstones" / digest
    ).is_dir()

    # A provider redelivery can retain the exact same cached local source.
    # Its immutable idempotency identity has already converged, so registration
    # returns the stable token without reopening or copying that source.
    assert _register(home=home, source=source) == token
    assert not (spool_root / digest).exists()
    assert list_artifact_recoveries(
        current_origin=_origin(),
        thread_id="planning-thread-1",
        hermes_home=home,
    ) == ()


def test_fresh_process_converges_crash_after_ack_rename_without_resurrection(
    tmp_path,
) -> None:
    home = tmp_path / "hermes-home"
    source = tmp_path / "reference.pdf"
    source.write_bytes(b"%PDF bytes retained by provider cache")
    token = _register(home=home, source=source)
    digest = token.removeprefix("artrec_v2_")
    spool_root = home / "planning-v2" / "artifact-ingress"
    live_path = spool_root / digest
    interrupted_ack_path = (
        spool_root / f".acknowledged-{digest}-4242-deadbeef"
    )

    # Crash seam: the live journal was durably retired, but the process died
    # before the ACK tombstone and private byte cleanup were materialized.
    os.rename(live_path, interrupted_ack_path)

    repository = Path(__file__).resolve().parents[2]
    script = """
import json
import os
from hermes_cli.planning_artifact_spool import register_artifact_recovery

token = register_artifact_recovery(
    thread_id="planning-thread-1",
    local_path=os.environ["TEST_RECOVERY_SOURCE"],
    origin=json.loads(os.environ["TEST_RECOVERY_ORIGIN"]),
    role="design_reference",
    position=1,
    required=True,
    idempotency_key="hermes-planning-artifact-v1:stable",
    content_type="image/png",
    retain_until=None,
    attachment_identity="attachment-1",
    ingress_ordinal=1,
)
print(token)
"""
    environment = dict(os.environ)
    environment.update(
        {
            "HERMES_HOME": str(home),
            "PYTHONPATH": str(repository),
            "TEST_RECOVERY_SOURCE": str(source),
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
    assert completed.stdout.strip() == token
    assert not interrupted_ack_path.exists()
    assert not live_path.exists()
    assert (
        spool_root / ".acknowledged-tombstones" / digest
    ).is_dir()
    assert list_artifact_recoveries(
        current_origin=_origin(),
        thread_id="planning-thread-1",
        hermes_home=home,
    ) == ()


def test_ack_completion_replays_in_fresh_process_without_bytes_or_path(
    tmp_path,
) -> None:
    home = tmp_path / "hermes-home"
    source = tmp_path / "private-dashboard.png"
    private_bytes = b"private bytes must be retired"
    source.write_bytes(private_bytes)
    token = _register(home=home, source=source)
    digest = token.removeprefix("artrec_v2_")
    spool_root = home / "planning-v2" / "artifact-ingress"

    assert acknowledge_artifact_recovery(
        token,
        completion=_completion(),
        hermes_home=home,
    )
    receipt_path = (
        spool_root
        / ".acknowledged-tombstones"
        / digest
        / "completion.json"
    )
    receipt_bytes = receipt_path.read_bytes()
    assert str(source).encode() not in receipt_bytes
    assert private_bytes not in receipt_bytes
    assert token.encode() not in receipt_bytes
    assert not (spool_root / digest).exists()

    repository = Path(__file__).resolve().parents[2]
    script = """
import json
import os
from hermes_cli.planning_artifact_spool import (
    load_artifact_recovery_completion,
)

completion = load_artifact_recovery_completion(
    os.environ["TEST_RECOVERY_TOKEN"],
    current_origin=json.loads(os.environ["TEST_RECOVERY_ORIGIN"]),
)
print(json.dumps(completion, sort_keys=True))
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
    assert json.loads(completed.stdout) == _completion()
    with pytest.raises(PlanningV2ConfigError) as scoped:
        load_artifact_recovery_completion(
            token,
            current_origin=_origin(sender_id="another-user"),
            hermes_home=home,
        )
    assert scoped.value.code == "planning.artifact_recovery_scope_mismatch"
