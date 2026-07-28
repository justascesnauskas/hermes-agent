"""Real GatewayRunner fresh-process proof for the public Agent Ops journey."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys


HARNESS = Path(__file__).with_name(
    "process_harness_agent_ops_gateway_lifecycle.py"
)
COMMANDS = (
    "new",
    "revise",
    "show",
    "recover-ack",
    "approve",
    "status",
)
TURN_COMMANDS = tuple(
    command for command in COMMANDS if command != "recover-ack"
)
UNAUTHORIZED_DIMENSIONS = ("provider", "account", "chat", "thread")
UNAUTHORIZED_ACTIONS = ("revise", "show", "approve")
UNAUTHORIZED_COMMANDS = tuple(
    f"unauthorized-{dimension}-{action}"
    for dimension in UNAUTHORIZED_DIMENSIONS
    for action in UNAUTHORIZED_ACTIONS
)
PLANNING_AUTHORITY_FIELDS = (
    "thread",
    "threadOrigin",
    "inputs",
    "runs",
    "previews",
    "reviewReceipts",
    "apply",
    "providerReceipts",
    "semanticCalls",
    "threadWrites",
    "inputWrites",
    "runWrites",
    "ackWrites",
    "ackTransportAttempts",
    "ackLossesRemaining",
    "applyWrites",
    "providerWrites",
)


def _run(root: Path, command: str) -> dict:
    repository_root = Path(__file__).parents[2]
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            str(repository_root),
            env.get("PYTHONPATH", ""),
        )
        if value
    )
    completed = subprocess.run(
        [sys.executable, str(HARNESS), str(root), command],
        cwd=repository_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert completed.returncode == 0, (
        command,
        completed.stdout,
        completed.stderr,
    )
    return json.loads(completed.stdout.strip().splitlines()[-1])


def _rows(path: Path, query: str):
    with sqlite3.connect(path) as connection:
        return connection.execute(query).fetchall()


def _state(root: Path) -> dict:
    return json.loads(
        (root / "remote-state.json").read_text(encoding="utf-8")
    )


def _planning_authority_projection(state: dict) -> dict:
    return {
        field: state[field]
        for field in PLANNING_AUTHORITY_FIELDS
    }


def _nested_keys(value) -> set[str]:
    if isinstance(value, dict):
        return set(value).union(
            *(_nested_keys(child) for child in value.values()),
            set(),
        )
    if isinstance(value, list):
        return set().union(
            *(_nested_keys(child) for child in value),
            set(),
        )
    return set()


def _owner_sensitive_values(state: dict) -> set[str]:
    thread = state["thread"]
    values = {
        thread["threadId"],
        thread["title"],
        *state["runs"],
        *state["previews"],
    }
    for preview in state["previews"].values():
        values.update(
            {
                preview["runId"],
                preview["previewResultId"],
                preview["previewResultHash"],
                preview["planHash"],
                preview["title"],
                preview["objective"],
                preview["summary"],
            }
        )
        values.update(task["summary"] for task in preview["tasks"])
    return values


def test_public_agent_ops_journey_crosses_real_gateway_and_restart_boundaries(
    tmp_path,
) -> None:
    receipts = [_run(tmp_path, command) for command in COMMANDS]
    state = json.loads(
        (tmp_path / "remote-state.json").read_text(encoding="utf-8")
    )
    home = tmp_path / "hermes-home"

    # Every logical human turn crossed both production normalization guards
    # and retired its exact first-idle durable queue row.  Distinct PIDs prove
    # that no process-local registry, session, preview, or client closure was
    # relied on across the journey.
    turn_receipts = [
        receipt
        for receipt in receipts
        if receipt["command"] != "recover-ack"
    ]
    assert [item["command"] for item in turn_receipts] == list(TURN_COMMANDS)
    assert all(
        item["guardCallers"][:2] == ["handle_message", "_handle_message"]
        for item in turn_receipts
    )
    assert all(item["queueDepth"] == 0 for item in turn_receipts)
    assert len({item["pid"] for item in receipts}) == len(receipts)

    queue_rows = _rows(
        home / "gateway-turn-queue" / "queue.sqlite3",
        "SELECT event_id, disposition FROM completed_turns "
        "ORDER BY completed_at",
    )
    assert queue_rows == [
        (f"gateway-lifecycle-{command}", "delivered")
        for command in TURN_COMMANDS
    ]
    assert _rows(
        home / "gateway-turn-queue" / "queue.sqlite3",
        "SELECT COUNT(*) FROM queued_turns",
    ) == [(0,)]

    # One outer MessageEvent produced exactly one model dispatch.  This is the
    # process-level regression for the claimed-copy/outer-event duplicate bug.
    model_calls = state["modelCalls"]
    assert [item["command"] for item in model_calls] == list(TURN_COMMANDS)
    assert [item["tool"] for item in model_calls] == [
        "agent_ops_task_plan",
        "agent_ops_task_plan",
        "agent_ops_task_plan",
        "agent_ops_task_approve_apply",
        "agent_ops_task_plan",
    ]
    assert all(item["ok"] is True for item in model_calls)
    assert len({item["eventId"] for item in model_calls}) == len(TURN_COMMANDS)
    assert [item["command"] for item in state["gatewayTurns"]] == list(
        TURN_COMMANDS
    )

    # The public facade created one thread, appended one exact revision, and
    # admitted one immutable run per input head.  It never fell back to the
    # removed legacy public tool.
    assert state["threadWrites"] == 1
    assert state["inputWrites"] == 2
    assert state["runWrites"] == 2
    assert len(state["runs"]) == 2
    assert all(
        item["tool"] != "agent_ops_planning_v2" for item in model_calls
    )

    # Show crossed the real semantic outbox once.  The remote ACK committed
    # once, both client retry attempts lost their response, and the restarted
    # production supervisor replayed the same request into one successful
    # local outbox receipt without another provider write.
    assert state["providerWrites"] == 1
    assert len(state["providerReceipts"]) == 1
    assert len(state["semanticCalls"]) == 1
    assert state["ackWrites"] == 1
    assert state["ackTransportAttempts"] == 3
    assert state["ackLossesRemaining"] == 0
    assert receipts[3]["bindingCount"] == 1
    assert receipts[3]["ackSucceeded"] == 1
    assert _rows(
        home / "state" / "planning-preview-ack" / "outbox.sqlite3",
        "SELECT state, COUNT(*) FROM planning_preview_ack_outbox "
        "GROUP BY state",
    ) == [("succeeded", 1)]

    active_preview = state["previews"][
        state["thread"]["activePreviewVersionId"]
    ]
    assert active_preview["reviewed"] is True
    review = next(iter(state["reviewReceipts"].values()))
    provider_message_id = next(iter(state["providerReceipts"].values()))
    assert review["deliveryProof"]["providerMessageId"] == provider_message_id
    assert review["deliveryProof"]["providerMessageIds"] == [
        provider_message_id
    ]

    # Approval is a later, distinct provider event and the remote Jira
    # boundary persisted the completed operation once.  Final public status
    # readback reports that actual persisted operation, not a gateway-created
    # success string.
    assert state["applyWrites"] == 1
    assert state["apply"]["providerEventId"] == (
        "gateway-lifecycle-approve"
    )
    assert state["apply"]["providerEventId"] != "gateway-lifecycle-show"
    assert state["apply"]["approvalMessage"] == (
        "I approve this exact current preview for Jira apply."
    )
    assert state["apply"]["operation"] == {
        "jiraKeys": ["SEO-4242"],
        "status": "completed",
    }
    approve_result = model_calls[3]["result"]
    assert approve_result["operationStatus"] == "completed"
    assert approve_result["exactLaterTurnApproval"] is True
    assert approve_result["fullyReviewedPreview"] is True
    status_result = model_calls[4]["result"]
    assert status_result["applyStatus"]["status"] == "clear"
    assert status_result["applyStatus"]["items"][0]["state"] == "completed"
    assert status_result["applyStatus"]["items"][0]["nextAction"] == "none"

    # Ordinary final responses exist only for non-preview turns.  The preview
    # itself went through the exact semantic provider rail.
    assert len(state["outbound"]) == 4


def test_fresh_process_cross_origin_task_actions_neither_reveal_nor_mutate(
    tmp_path,
) -> None:
    seed_receipt = _run(tmp_path, "new")
    seeded = _state(tmp_path)
    authority_before = _planning_authority_projection(seeded)
    sensitive_values = _owner_sensitive_values(seeded)
    remote_call_count = len(seeded["calls"])
    attack_receipts = []

    forbidden_result_keys = {
        "threadId",
        "runId",
        "previewResultId",
        "previewResultHash",
        "planHash",
        "planId",
        "operationId",
        "tasks",
    }

    # Exercise the complete 4 × 3 authority matrix through a newly spawned
    # Hermes process for every attempt.  Each attacker even supplies the real
    # owner thread handle, so a pass proves the public facade resolved the
    # exact current origin before any read, preview delivery, or Jira apply.
    for command in UNAUTHORIZED_COMMANDS:
        receipt = _run(tmp_path, command)
        attack_receipts.append(receipt)
        current = _state(tmp_path)
        model_call = current["modelCalls"][-1]
        result = model_call["result"]

        assert receipt["command"] == command
        assert receipt["unauthorizedDimension"] in (
            UNAUTHORIZED_DIMENSIONS
        )
        assert receipt["action"] in UNAUTHORIZED_ACTIONS
        assert receipt["guardCallers"][:2] == [
            "handle_message",
            "_handle_message",
        ]
        assert receipt["queueDepth"] == 0

        assert model_call["command"] == command
        assert model_call["action"] == receipt["action"]
        assert model_call["unauthorizedDimension"] == (
            receipt["unauthorizedDimension"]
        )
        owner_thread_id = seeded["thread"]["threadId"]
        if receipt["action"] == "approve":
            assert model_call["arguments"]["selectedOption"] == (
                owner_thread_id
            )
        else:
            assert model_call["arguments"]["threadId"] == owner_thread_id
        assert result["ok"] is False
        if receipt["unauthorizedDimension"] == "provider":
            assert result["code"] == (
                "planning.bound_gateway_ineligible"
            )
            assert result["reason"] == (
                "bound_gateway_adapter_unavailable"
            )
        else:
            assert result["code"] == {
                "revise": "planning.facade_thread_not_in_conversation",
                "show": "planning.facade_thread_not_in_conversation",
                "approve": "planning.facade_current_thread_not_found",
            }[receipt["action"]]
        assert result["stateChanged"] is False
        assert forbidden_result_keys.isdisjoint(_nested_keys(result))
        serialized = json.dumps(
            result,
            ensure_ascii=False,
            sort_keys=True,
        )
        assert all(value not in serialized for value in sensitive_values)

        # The exact public response delivered to the foreign origin is the
        # non-disclosing typed failure inspected above.
        delivered = current["outbound"][-1]
        assert json.loads(delivered["content"]) == result

        # A provider without a bound exact adapter stops before any Hub call.
        # Other foreign dimensions reach only the set-valued, origin-filtered
        # lookup, never a thread read, revision, preview, or apply call.
        new_calls = current["calls"][remote_call_count:]
        if receipt["unauthorizedDimension"] == "provider":
            assert new_calls == []
            remote_call_count = len(current["calls"])
            assert (
                _planning_authority_projection(current)
                == authority_before
            )
            continue
        assert [
            (call["method"], call["path"])
            for call in new_calls
        ] == [
            (
                "POST",
                "/api/runner/planning/v2/threads/resolve-current",
            )
        ]
        expected_origin = dict(seeded["threadOrigin"])
        origin_field, foreign_value = {
            "provider": ("provider", "discord"),
            "account": (
                "gatewayAccountId",
                "gateway-lifecycle-foreign-account",
            ),
            "chat": ("chatId", "gateway-lifecycle-foreign-chat"),
            "thread": (
                "threadId",
                "gateway-lifecycle-foreign-thread",
            ),
        }[receipt["unauthorizedDimension"]]
        expected_origin[origin_field] = foreign_value
        assert new_calls[0]["origin"] == expected_origin
        assert {
            field
            for field, value in expected_origin.items()
            if value != seeded["threadOrigin"][field]
        } == {origin_field}
        remote_call_count = len(current["calls"])
        assert _planning_authority_projection(current) == authority_before

    home = tmp_path / "hermes-home"
    assert not (
        home
        / "state"
        / "planning-preview-ack"
        / "outbox.sqlite3"
    ).exists()

    # The exact owner origin remains usable after every denied attempt:
    # revision, complete preview delivery/ACK recovery, and later exact
    # approval all cross their normal production paths successfully.
    owner_receipts = [
        _run(tmp_path, command)
        for command in ("revise", "show", "recover-ack", "approve")
    ]
    final = _state(tmp_path)
    owner_results = {
        item["command"]: item["result"]
        for item in final["modelCalls"]
        if item["command"] in {"revise", "show", "approve"}
    }
    assert owner_results["revise"]["ok"] is True
    assert owner_results["show"]["ok"] is True
    assert owner_results["approve"]["ok"] is True
    assert final["threadWrites"] == 1
    assert final["inputWrites"] == 2
    assert final["runWrites"] == 2
    assert final["providerWrites"] == 1
    assert final["ackWrites"] == 1
    assert final["applyWrites"] == 1
    assert final["apply"]["operation"] == {
        "jiraKeys": ["SEO-4242"],
        "status": "completed",
    }
    assert len(
        {
            receipt["pid"]
            for receipt in (
                seed_receipt,
                *attack_receipts,
                *owner_receipts,
            )
        }
    ) == 1 + len(attack_receipts) + len(owner_receipts)


def test_nonidle_queued_event_runs_and_delivers_exactly_once(tmp_path) -> None:
    receipt = _run(tmp_path, "queued")
    state = json.loads(
        (tmp_path / "remote-state.json").read_text(encoding="utf-8")
    )
    home = tmp_path / "hermes-home"

    assert receipt["eventIds"] == [
        "gateway-lifecycle-queued-first",
        "gateway-lifecycle-queued-second",
    ]
    assert receipt["queuedDepthWhileBusy"] == 2
    assert receipt["queueDepth"] == 0

    assert [
        (item["command"], item["eventId"], item["tool"])
        for item in state["modelCalls"]
    ] == [
        (
            "new",
            "gateway-lifecycle-queued-first",
            "agent_ops_task_plan",
        ),
        (
            "revise",
            "gateway-lifecycle-queued-second",
            "agent_ops_task_plan",
        ),
    ]
    assert all(item["ok"] is True for item in state["modelCalls"])
    assert state["threadWrites"] == 1
    assert state["inputWrites"] == 2
    assert state["runWrites"] == 2

    assert _rows(
        home / "gateway-turn-queue" / "queue.sqlite3",
        "SELECT event_id, disposition FROM completed_turns "
        "ORDER BY completed_at",
    ) == [
        ("gateway-lifecycle-queued-first", "delivered"),
        ("gateway-lifecycle-queued-second", "delivered"),
    ]
    assert _rows(
        home / "gateway-turn-queue" / "queue.sqlite3",
        "SELECT COUNT(*) FROM queued_turns",
    ) == [(0,)]

    # The first response is sent by GatewayRunner before it recurses into the
    # hydrated queue head.  The second returns to BasePlatformAdapter for its
    # normal final send.  Each successful provider send retires exactly the
    # corresponding durable ingress row.
    assert [item["messageId"] for item in state["outbound"]] == [
        "ordinary-1",
        "ordinary-2",
    ]
    assert len(state["outbound"]) == len(state["modelCalls"]) == 2
