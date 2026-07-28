"""Fresh-process convergence at semantic provider and preview ACK seams."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[2]
CHILD = Path(__file__).with_name(
    "process_harness_semantic_delivery_child.py"
)
CRASH_EXIT = 86
PREVIEW_PROVIDER_MESSAGE_ID = (
    "  matrix-" + ("訊" * 2_048) + "-event  "
)


def _environment() -> dict[str, str]:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (str(ROOT), environment.get("PYTHONPATH", ""))
        if value
    )
    return environment


def _run(
    state_root: Path,
    command: str,
    *,
    expected: int = 0,
) -> dict[str, Any] | None:
    completed = subprocess.run(
        [sys.executable, str(CHILD), str(state_root), command],
        cwd=ROOT,
        env=_environment(),
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == expected, {
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
    if expected != 0:
        assert completed.stdout == ""
        return None
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    assert len(lines) == 1, completed.stdout
    payload = json.loads(lines[0])
    assert isinstance(payload, dict)
    return payload


@pytest.mark.parametrize("provider", ["discord", "slack"])
def test_non_native_acceptance_crash_never_blindly_resends(
    tmp_path: Path,
    provider: str,
) -> None:
    state_root = tmp_path / provider
    _run(
        state_root,
        f"{provider}-accept-crash",
        expected=CRASH_EXIT,
    )
    replay = _run(state_root, f"{provider}-replay")
    provider_state = json.loads(
        (state_root / f"{provider}-provider.json").read_text(
            encoding="utf-8"
        )
    )

    assert replay is not None
    assert replay["outcome"] == "ambiguous"
    assert replay["replayed"] is True
    assert provider_state["businessEffects"] == 1
    assert len(provider_state["calls"]) == 1


def test_matrix_acceptance_crash_replays_same_transaction_once(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "matrix"
    _run(
        state_root,
        "matrix-accept-crash",
        expected=CRASH_EXIT,
    )
    replay = _run(state_root, "matrix-replay")
    provider_state = json.loads(
        (state_root / "matrix-provider.json").read_text(encoding="utf-8")
    )

    assert replay is not None
    assert replay["outcome"] == "delivered"
    assert replay["replayed"] is True
    assert provider_state["businessEffects"] == 1
    assert len(provider_state["calls"]) == 2
    assert len({call["token"] for call in provider_state["calls"]}) == 1


def test_concurrent_process_claim_is_in_flight_then_replays_receipt(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "cross-process-lock"
    owner = subprocess.Popen(
        [
            sys.executable,
            str(CHILD),
            str(state_root),
            "lock-owner",
        ],
        cwd=ROOT,
        env=_environment(),
        text=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert owner.stdout is not None
        claimed_line = owner.stdout.readline()
        assert json.loads(claimed_line)["action"] == "send"

        contender = _run(state_root, "lock-contender")
        assert contender is not None
        assert contender["outcome"] == "in_flight"
        assert contender["replayed"] is True
        assert not (state_root / "slack-provider.json").exists()

        status = _run(state_root, "lock-status")
        assert status is not None
        assert status["outcome"] == "in_flight"
        assert status["error"] == "semantic_delivery_in_flight"
        assert not (state_root / "slack-provider.json").exists()

        assert owner.stdin is not None
        owner.stdin.write("release\n")
        owner.stdin.flush()
        owner_stdout, owner_stderr = owner.communicate(timeout=30)
        assert owner.returncode == 0, {
            "stdout": owner_stdout,
            "stderr": owner_stderr,
        }
        owner_result = json.loads(owner_stdout.strip())
        assert owner_result["outcome"] == "delivered"

        replay = _run(state_root, "lock-replay")
        provider_state = json.loads(
            (state_root / "slack-provider.json").read_text(
                encoding="utf-8"
            )
        )
        assert replay is not None
        assert replay["outcome"] == "delivered"
        assert replay["replayed"] is True
        assert provider_state["businessEffects"] == 1
        assert len(provider_state["calls"]) == 1
    finally:
        if owner.poll() is None:
            owner.kill()
            owner.communicate(timeout=30)


@pytest.mark.parametrize(
    ("kind", "expected_outcome", "write_attempted", "strategy"),
    [
        ("retryable", "retryable", False, "none"),
        ("rejected", "rejected", False, "none"),
        ("delivered", "delivered", True, "none"),
        ("native-sending", "retryable", None, "durable_native"),
    ],
)
def test_status_survives_death_after_local_boundary_before_hub_ack(
    tmp_path: Path,
    kind: str,
    expected_outcome: str,
    write_attempted: bool | None,
    strategy: str,
) -> None:
    state_root = tmp_path / f"status-{kind}"
    _run(
        state_root,
        f"status-{kind}-settle-crash",
        expected=CRASH_EXIT,
    )

    status = _run(state_root, f"status-{kind}-read")

    assert status is not None
    assert status["outcome"] == expected_outcome
    if write_attempted is None:
        assert "provider_write_attempted" not in status
        assert "provider_write_started" not in status
    else:
        assert status["provider_write_attempted"] is write_attempted
        assert status["provider_write_started"] is write_attempted
    assert status["replay_strategy"] == strategy
    if kind == "delivered":
        assert status["message_id"] == "status-delivered-message"
        assert status["replayed"] is True


def test_status_missing_after_death_before_row_is_retryable_prewrite(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "status-missing"
    _run(
        state_root,
        "status-scope-only-crash",
        expected=CRASH_EXIT,
    )

    status = _run(state_root, "status-missing-read")

    assert status is not None
    assert status["outcome"] == "retryable"
    assert status["error"] == "semantic_delivery_missing_prewrite"
    assert status["provider_write_attempted"] is False
    assert status["provider_write_started"] is False


def test_preview_receipt_survives_crash_before_hub_ack(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "preview"
    _run(
        state_root,
        "preview-record-crash",
        expected=CRASH_EXIT,
    )
    replay = _run(state_root, "preview-ack-replay")
    provider_state = json.loads(
        (state_root / "preview-provider.json").read_text(encoding="utf-8")
    )
    acknowledgements = json.loads(
        (state_root / "preview-acks.json").read_text(encoding="utf-8")
    )

    assert replay == {
        "acknowledged": True,
        "claimAction": "delivered",
        "providerMessageIds": [PREVIEW_PROVIDER_MESSAGE_ID],
    }
    assert provider_state == {"businessEffects": 1, "calls": 1}
    assert len(acknowledgements) == 1
    assert acknowledgements[0]["deliveryNonce"] == (
        "preview-delivery-process-stable"
    )
    assert acknowledgements[0]["providerMessageIds"] == [
        PREVIEW_PROVIDER_MESSAGE_ID
    ]


def test_fresh_process_reconstructs_ack_after_receipt_commit_crash(
    tmp_path: Path,
) -> None:
    """No tool/model rerun is needed after the exact receipt is durable."""

    state_root = tmp_path / "preview-bridge"
    _run(
        state_root,
        "preview-record-crash",
        expected=CRASH_EXIT,
    )
    recovery = _run(state_root, "preview-bridge-recover")
    assert recovery is not None
    assert recovery["reconciled"]["state"] == "enqueued"
    assert recovery["reconciled"]["providerMessageIds"] == [
        PREVIEW_PROVIDER_MESSAGE_ID
    ]
    assert recovery["dispatched"]["state"] == "succeeded"
    assert len(recovery["requests"]) == 1
    proof = recovery["requests"][0]["deliveryProof"]
    assert proof["providerMessageId"] == PREVIEW_PROVIDER_MESSAGE_ID
    assert proof["providerMessageIds"] == [PREVIEW_PROVIDER_MESSAGE_ID]
    assert proof["deliveryNonce"] == "preview-delivery-process-stable"

    provider_state = json.loads(
        (state_root / "preview-provider.json").read_text(
            encoding="utf-8"
        )
    )
    assert provider_state == {"businessEffects": 1, "calls": 1}
