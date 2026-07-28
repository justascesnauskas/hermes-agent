"""Fresh-process crash matrix for multipart preview supervision."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROCESS_HARNESS = Path(__file__).with_name(
    "process_harness_multipart_preview_supervisor.py"
)
CRASH_EXIT = 86


def _run(
    profile_home: Path,
    state_root: Path,
    command: str,
    *,
    expected: int = 0,
) -> dict[str, Any] | None:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            str(REPOSITORY_ROOT),
            environment.get("PYTHONPATH", ""),
        )
        if value
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(PROCESS_HARNESS),
            str(profile_home),
            str(state_root),
            command,
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == expected, completed.stderr
    if expected != 0 or not completed.stdout.strip():
        return None
    return json.loads(completed.stdout)


def _inspect(
    profile_home: Path,
    state_root: Path,
) -> dict[str, Any]:
    value = _run(profile_home, state_root, "inspect")
    assert value is not None
    return value


def test_crash_after_each_provider_unit_acks_only_after_full_group(
    tmp_path: Path,
) -> None:
    profile_home = tmp_path / "hermes-home"
    state_root = tmp_path / "external-state"

    _run(
        profile_home,
        state_root,
        "stage-crash",
        expected=CRASH_EXIT,
    )
    staged = _inspect(profile_home, state_root)
    group = staged["semantic"]["group"]
    assert group is not None
    unit_count = group["unit_count"]
    assert unit_count > 1
    assert len(staged["semantic"]["rows"]) == unit_count
    assert staged["provider"]["businessEffects"] == 1
    assert len(staged["provider"]["calls"]) == 1
    assert staged["provider"]["calls"][0]["unit"] == 0
    assert staged["ack"]["bridge"]["state"] == "pending"
    assert staged["ack"]["rows"] == []
    assert staged["ackCalls"] == []

    # The number of process restarts is derived from the winning durable
    # manifest; the test has no configured provider-unit or retry cap.
    for expected_effects in range(2, unit_count + 1):
        _run(
            profile_home,
            state_root,
            "recover-crash",
            expected=CRASH_EXIT,
        )
        interim = _inspect(profile_home, state_root)
        assert interim["provider"]["businessEffects"] == expected_effects
        assert len(interim["provider"]["calls"]) == expected_effects
        assert interim["ack"]["bridge"]["state"] == "pending"
        assert interim["ack"]["rows"] == []
        assert interim["ackCalls"] == []

    final = _run(profile_home, state_root, "converge")
    assert final is not None
    provider = final["provider"]
    calls = provider["calls"]
    rows = final["semantic"]["rows"]

    assert provider["businessEffects"] == unit_count
    assert len(calls) == unit_count
    assert [call["unit"] for call in calls] == list(range(unit_count))
    assert len({call["deliveryId"] for call in calls}) == unit_count
    assert len(provider["receipts"]) == unit_count
    assert all(
        call["encoding"]["wire_encoding"]
        == "closure-preview-wire-v1"
        and call["encoding"]["segmentation_version"]
        == "closure-preview-segmentation-v1"
        and call["providerRoute"]
        == {"transport": "closure-preview-route-v1"}
        for call in calls
    )
    assert [row["state"] for row in rows] == ["retired"] * unit_count
    assert [row["last_error"] for row in rows] == [
        "delivered"
    ] * unit_count
    assert all(
        json.loads(row["encoding_contract_json"])
        == calls[0]["encoding"]
        for row in rows
    )
    assert all(
        json.loads(row["route_json"])["providerRoute"]
        == {"transport": "closure-preview-route-v1"}
        for row in rows
    )
    assert final["semantic"]["group"]["state"] == "completed"
    assert final["ack"]["bridge"]["state"] == "enqueued"
    assert final["ack"]["rows"][0]["state"] == "succeeded"
    assert final["ack"]["rows"][0]["attempts"] == 1
    assert len(final["ackCalls"]) == 1
    expected_message_ids = [
        f"closure-preview-message-{index:03d}"
        for index in range(unit_count)
    ]
    assert final["ackCalls"][0]["deliveryProof"][
        "providerMessageIds"
    ] == expected_message_ids
