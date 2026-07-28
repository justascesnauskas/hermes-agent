"""Process restart proof for public-facade artifact upload convergence."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROCESS_HARNESS = Path(__file__).with_name(
    "process_harness_agent_ops_artifact_replay.py"
)


def _run(
    profile_home: Path,
    state_path: Path,
    mode: str,
    source: Path | None,
    *,
    recovery_token: str | None = None,
) -> dict[str, Any]:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            str(REPOSITORY_ROOT),
            environment.get("PYTHONPATH", ""),
        )
        if value
    )
    if recovery_token is not None:
        environment["CLOSURE_RECOVERY_TOKEN"] = recovery_token
    completed = subprocess.run(
        [
            sys.executable,
            str(PROCESS_HARNESS),
            str(profile_home),
            str(state_path),
            mode,
            str(source) if source is not None else "-",
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def test_public_facade_replays_lost_upload_and_acks_spool_after_restart(
    tmp_path: Path,
) -> None:
    profile_home = tmp_path / "hermes-home"
    profile_home.mkdir()
    (profile_home / "config.yaml").write_text(
        "toolsets:\n  - planning_v2\n",
        encoding="utf-8",
    )
    state_path = tmp_path / "hub-state.json"
    source = tmp_path / "architecture.pdf"
    source_bytes = b"%PDF-1.7 process restart artifact evidence"
    source.write_bytes(source_bytes)

    lost = _run(profile_home, state_path, "lose", source)

    assert lost["result"]["ok"] is False
    assert lost["result"]["code"] == "planning.facade_artifacts_pending"
    assert lost["result"]["pending"] == [
        {
            "code": "planning.hub_timeout",
            "outcomeAmbiguous": True,
            "position": 1,
            "retryable": True,
        }
    ]
    assert lost["state"]["uploadWrites"] == 1
    assert len(lost["state"]["uploadCalls"]) == 1
    assert len(lost["live"]) == 1
    recovery_token = lost["live"][0]["token"]
    snapshot_path = Path(lost["live"][0]["snapshotPath"])
    assert snapshot_path.read_bytes() == source_bytes
    assert str(source) not in json.dumps(lost["result"])

    source.unlink()
    recovered = _run(
        profile_home,
        state_path,
        "recover",
        None,
        recovery_token=recovery_token,
    )

    assert recovered["result"]["ok"] is True
    assert recovered["result"]["route"] == "planning_v2"
    assert recovered["result"]["intent"] == "retry"
    assert recovered["result"]["recoveredArtifacts"] == 1
    assert recovered["state"]["uploadWrites"] == 1
    assert recovered["state"]["runWrites"] == 1
    assert len(recovered["state"]["uploadCalls"]) == 2
    first, second = recovered["state"]["uploadCalls"]
    assert first["idempotencyKey"] == second["idempotencyKey"]
    assert first["checksum"] == second["checksum"]
    assert first["sizeBytes"] == second["sizeBytes"] == len(source_bytes)
    assert first["pid"] != second["pid"]
    assert recovered["live"] == []
    assert recovered["completion"]["ok"] is True
    assert recovered["completion"]["uploadReplayed"] is True
    assert recovered["completion"]["inputReplayed"] is True
    assert recovered["completion"]["artifact"]["checksum"] == first[
        "checksum"
    ]
    assert not snapshot_path.exists()
