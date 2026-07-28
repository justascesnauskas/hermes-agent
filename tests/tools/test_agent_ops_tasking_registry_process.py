"""Production discovery/dispatch proof for public Agent Ops tasking names."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROCESS_HARNESS = Path(__file__).with_name(
    "process_harness_agent_ops_registry.py"
)
PUBLIC_NAMES = [
    "agent_ops_task_plan",
    "agent_ops_task_approve_apply",
]


def test_fresh_process_exposes_only_public_agent_ops_tasking_names(
    tmp_path: Path,
) -> None:
    profile_home = tmp_path / "hermes-home"
    profile_home.mkdir()
    (profile_home / "config.yaml").write_text(
        "toolsets:\n  - planning_v2\n",
        encoding="utf-8",
    )
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
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["definitions"] == sorted(PUBLIC_NAMES)
    assert result["threadChoiceCursorDefinitions"] == sorted(PUBLIC_NAMES)
    assert result["registeredPlanningTools"] == sorted(PUBLIC_NAMES)
    assert result["legacyEntry"] is False
    assert result["legacyDispatch"] == {
        "error": "Unknown tool: agent_ops_planning_v2"
    }
    assert result["publicDispatch"]["code"] == (
        "planning.facade_intent_invalid"
    )
    assert result["publicDispatch"]["route"] == "planning_v2"
    recovery = result["publicRecoveryDispatch"]["recovery"]["nextAction"]
    assert recovery == {
        "tool": "agent_ops_task_plan",
        "arguments": {
            "intent": "retry",
            "threadId": "thread-1",
        },
    }
    assert "agent_ops_planning_v2" not in json.dumps(
        result["publicRecoveryDispatch"]
    )
