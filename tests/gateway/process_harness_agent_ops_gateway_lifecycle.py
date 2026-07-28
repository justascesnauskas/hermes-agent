"""Fresh-process public Agent Ops journey through the real gateway runner.

The only doubles in this harness are the two external boundaries:

* ``ScriptedModelAgent`` stands in for the model transport.  It invokes the
  production ``model_tools`` dispatcher, public registry entries, facade, and
  private Planning V2 handler.
* ``StateTransport`` stands in for the remote Hub/Jira HTTP service.  Every
  request still crosses the production ``PlanningV2Client`` request encoder,
  retry state machine, and response validators.

Everything between those boundaries is production code, including
``BasePlatformAdapter.handle_message(MessageEvent)``, ``GatewayRunner``,
session persistence, durable turn admission/retirement, semantic preview
delivery, the ACK outbox, and ``ProfileDeliverySupervisor``.
"""

from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import threading
import time
from types import ModuleType
from typing import Any
from urllib.parse import parse_qs, urlparse


REPOSITORY_ROOT = Path(__file__).parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

PROVIDER = "telegram"
ACCOUNT = "gateway-lifecycle-account"
CHAT = "gateway-lifecycle-chat"
USER = "gateway-lifecycle-owner"
UNAUTHORIZED_DIMENSIONS = ("provider", "account", "chat", "thread")
UNAUTHORIZED_ACTIONS = ("revise", "show", "approve")
UNAUTHORIZED_COMMANDS = tuple(
    f"unauthorized-{dimension}-{action}"
    for dimension in UNAUTHORIZED_DIMENSIONS
    for action in UNAUTHORIZED_ACTIONS
)
MODEL = "openai/gpt-4o-mini"
BASE_URL = "https://planning-hub.external.invalid"
RUNNER_ID = "gateway-lifecycle-runner"
RUNNER_TOKEN = "gateway-lifecycle-runner-token"
APPROVAL_TEXT = "I approve this exact current preview for Jira apply."
APPROVAL_QUOTE = "approve this exact current preview"
TIMESTAMP = "2026-07-28T12:00:00Z"
PREFIX = "/api/runner/planning/v2"


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _digest(value: Any) -> str:
    raw = value if isinstance(value, bytes) else _canonical(value).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _read(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "schemaVersion": "gateway-lifecycle-state.v1",
            "thread": None,
            "threadOrigin": None,
            "inputs": [],
            "runs": {},
            "previews": {},
            "reviewReceipts": {},
            "apply": None,
            "providerReceipts": {},
            "calls": [],
            "gatewayTurns": [],
            "modelCalls": [],
            "outbound": [],
            "semanticCalls": [],
            "threadWrites": 0,
            "inputWrites": 0,
            "runWrites": 0,
            "ackWrites": 0,
            "ackTransportAttempts": 0,
            "ackLossesRemaining": 0,
            "applyWrites": 0,
            "providerWrites": 0,
        }
    return json.loads(path.read_text(encoding="utf-8"))


def _write(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(_canonical(state), encoding="utf-8")
    os.replace(temporary, path)


def _mutate(path: Path, callback) -> Any:
    """Serialize state updates across the gateway executor and supervisor."""

    import fcntl

    lock_path = path.with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        state = _read(path)
        result = callback(state)
        _write(path, state)
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    return result


class _Response:
    def __init__(self, status: int, payload: Any = None) -> None:
        self.status = status
        self._raw = (
            b"" if payload is None else _canonical(payload).encode("utf-8")
        )

    def read(self) -> bytes:
        return self._raw

    def close(self) -> None:
        return None


def _thread_projection(state: dict[str, Any], *, duplicate: bool | None = None):
    thread = dict(state["thread"])
    payload: dict[str, Any] = {
        "thread": thread,
        "bindings": [],
        "inputEventCount": len(state["inputs"]),
        "semanticEventHead": len(state["inputs"]),
    }
    if duplicate is not None:
        payload["duplicate"] = duplicate
        payload["inputEvent"] = {
            "eventId": f"input-{len(state['inputs'])}",
        }
    return payload


def _conversation_identity(origin: dict[str, Any]) -> dict[str, Any]:
    """Project the exact Hub channel/user authority used by resolve-current."""

    return {
        "provider": origin["provider"],
        "gatewayAccountId": origin["gatewayAccountId"],
        "chatId": origin["chatId"],
        "threadId": origin.get("threadId"),
        "senderId": origin["senderId"],
    }


def _thread_origin(state: dict[str, Any]) -> dict[str, Any] | None:
    origin = state.get("threadOrigin")
    if isinstance(origin, dict):
        return origin
    if state.get("inputs"):
        legacy = state["inputs"][0].get("origin")
        if isinstance(legacy, dict):
            return _conversation_identity(legacy)
    return None


def _origin_can_resolve_thread(
    state: dict[str, Any],
    origin: dict[str, Any],
) -> bool:
    owner = _thread_origin(state)
    return owner is not None and owner == _conversation_identity(origin)


def _input_digest(state: dict[str, Any]) -> str:
    return _digest(
        [
            {
                "sequence": item["sequence"],
                "contentHash": item["contentHash"],
            }
            for item in state["inputs"]
        ]
    )


def _add_input(state: dict[str, Any], body: dict[str, Any]) -> None:
    sequence = len(state["inputs"]) + 1
    state["inputs"].append(
        {
            "sequence": sequence,
            "contentHash": _digest(body["payload"]),
            "inputKind": body["inputKind"],
            "payload": body["payload"],
            "origin": body["origin"],
        }
    )
    state["thread"]["headInputSequence"] = sequence
    state["inputWrites"] += 1


def _review_status(preview: dict[str, Any]) -> dict[str, Any]:
    reviewed = bool(preview["reviewed"])
    count = len(preview["tasks"])
    return {
        "taskCount": count,
        "coveredTaskCount": count if reviewed else 0,
        "coveredRanges": [{"offset": 0, "count": count}] if reviewed else [],
        "missingRanges": [] if reviewed else [{"offset": 0, "count": count}],
        "complete": reviewed,
    }


def _preview_page(
    preview: dict[str, Any],
    *,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    tasks = preview["tasks"][offset : offset + limit]
    task_count = len(preview["tasks"])
    returned = len(tasks)
    next_offset = offset + returned
    has_more = next_offset < task_count
    page_document = {
        "schemaVersion": "planning.preview-page.v1",
        "threadId": preview["threadId"],
        "previewResultId": preview["previewResultId"],
        "previewResultHash": preview["previewResultHash"],
        "taskCount": task_count,
        "offset": offset,
        "count": returned,
        "tasks": tasks,
    }
    page_digest = _digest(page_document)
    delivery_payload = {
        "schemaVersion": "planning.preview-delivery-payload.v1",
        "threadId": preview["threadId"],
        "runId": preview["runId"],
        "previewResultId": preview["previewResultId"],
        "previewResultHash": preview["previewResultHash"],
        "planHash": preview["planHash"],
        "basisInputSequence": preview["basisInputSequence"],
        "title": preview["title"],
        "objective": preview["objective"],
        "summary": preview["summary"],
        "decisions": preview["decisions"],
        "coverage": preview["coverage"],
        "taskCount": task_count,
        "offset": offset,
        "count": returned,
        "tasks": tasks,
        "pageDigest": page_digest,
        "hasMore": has_more,
        "nextOffset": next_offset if has_more else None,
    }
    delivery_content = "\n\n".join(
        (
            f"## {preview['title']}",
            preview["objective"],
            *(
                f"### {offset + index + 1}. {task['summary']}"
                for index, task in enumerate(tasks)
            ),
        )
    )
    return {
        "threadId": preview["threadId"],
        "runId": preview["runId"],
        "previewResultId": preview["previewResultId"],
        "previewResultHash": preview["previewResultHash"],
        "planHash": preview["planHash"],
        "basisInputSequence": preview["basisInputSequence"],
        "taskCount": task_count,
        "offset": offset,
        "limit": limit,
        "returned": returned,
        "tasks": tasks,
        "pageDigest": page_digest,
        "reviewStatus": _review_status(preview),
        "hasMore": has_more,
        "nextOffset": next_offset if has_more else None,
        "title": preview["title"],
        "objective": preview["objective"],
        "summary": preview["summary"],
        "decisions": preview["decisions"],
        "coverage": preview["coverage"],
        "acceptedAt": preview["acceptedAt"],
        "approvalEligible": bool(preview["reviewed"]),
        "deliveryPayload": delivery_payload,
        "deliveryPayloadDigest": _digest(delivery_payload),
        "deliveryContent": delivery_content,
        "deliveryContentDigest": _digest(delivery_content.encode()),
    }


def _delivery_attention(thread_id: str, origin: dict[str, Any]):
    return {
        "ok": True,
        "threadId": thread_id,
        "deliveryAttention": {
            "status": "clear",
            "count": 0,
            "returnedCount": 0,
            "hasMore": False,
            "nextAfterResolutionToken": None,
            "requiresExplicitToken": False,
            "items": [],
        },
        "authorizedBinding": {
            "provider": origin["provider"],
            "gatewayAccountId": origin["gatewayAccountId"],
        },
    }


def _apply_status(state: dict[str, Any], thread_id: str):
    apply = state["apply"]
    items = []
    if apply is not None:
        items.append(
            {
                "recoveryToken": "par_" + ("a" * 64),
                "state": apply["operation"]["status"],
                "progress": {"total": 1, "published": 1},
                "nextAction": "none",
                "message": "The Jira operation completed durably.",
                "recoveryReason": None,
                "requiresFreshPreviewApproval": False,
                "reusesOriginalOperation": True,
                "automaticWhenJiraPreflightPasses": False,
                "requestedAt": apply["createdAt"],
                "updatedAt": apply["updatedAt"],
            }
        )
    return {
        "ok": True,
        "threadId": thread_id,
        "applyStatus": {
            "count": len(items),
            "actionableCount": 0,
            "automaticResumeCount": 0,
            "revisionRequiredCount": 0,
            "returnedCount": len(items),
            "hasMore": False,
            "nextAfterRecoveryToken": None,
            "items": items,
        },
    }


class StateTransport:
    """File-backed remote boundary for the real PlanningV2Client."""

    def __init__(self, state_path: Path):
        self.state_path = state_path

    def __call__(self, request, *, timeout: float):
        del timeout
        method = request.get_method()
        parsed = urlparse(request.full_url)
        path = parsed.path
        query = parse_qs(parsed.query)
        body = (
            json.loads(request.data.decode("utf-8"))
            if request.data is not None
            else None
        )

        def handle(state: dict[str, Any]):
            call = {
                "method": method,
                "path": path,
                "pid": os.getpid(),
                "bodyDigest": _digest(body) if body is not None else None,
            }
            if (
                path == f"{PREFIX}/threads/resolve-current"
                and isinstance(body, dict)
                and isinstance(body.get("origin"), dict)
            ):
                call["origin"] = _conversation_identity(body["origin"])
            state["calls"].append(call)

            if method == "POST" and path == f"{PREFIX}/threads":
                from hermes_cli.dev_hub_planning_v2 import (
                    derive_thread_id_from_origin,
                )

                duplicate = state["thread"] is not None
                if not duplicate:
                    thread_id = derive_thread_id_from_origin(body["origin"])
                    state["thread"] = {
                        "threadId": thread_id,
                        "title": body["title"],
                        "status": "active",
                        "headInputSequence": 0,
                        "activeRunId": None,
                        "activePreviewVersionId": None,
                        "updatedAt": TIMESTAMP,
                    }
                    state["threadOrigin"] = _conversation_identity(
                        body["origin"]
                    )
                    _add_input(state, body)
                    state["threadWrites"] += 1
                return _Response(
                    200 if duplicate else 201,
                    _thread_projection(state, duplicate=duplicate),
                )

            if method == "POST" and path == f"{PREFIX}/threads/resolve-current":
                authorized = (
                    state["thread"] is not None
                    and _origin_can_resolve_thread(state, body["origin"])
                )
                threads = [dict(state["thread"])] if authorized else []
                return _Response(
                    200,
                    {
                        "match": "none" if not threads else "one",
                        "matchCount": len(threads),
                        "threads": threads,
                    },
                )

            if not path.startswith(f"{PREFIX}/threads/") and not path.startswith(
                f"{PREFIX}/runs/"
            ):
                raise AssertionError(f"unexpected Planning route: {method} {path}")

            relative = path.removeprefix(PREFIX + "/")
            parts = relative.split("/")

            if parts[0] == "runs" and method == "GET":
                return _Response(200, dict(state["runs"][parts[1]]))

            thread_id = parts[1]
            if state["thread"]["threadId"] != thread_id:
                raise AssertionError("thread identity changed")

            if method == "GET" and len(parts) == 2:
                return _Response(200, _thread_projection(state))

            if method == "GET" and parts[2] == "inputs":
                after = int(query["afterSequence"][0])
                inputs = [
                    dict(item)
                    for item in state["inputs"]
                    if item["sequence"] > after
                ]
                return _Response(
                    200,
                    {
                        "threadId": thread_id,
                        "basisInputSequence": len(state["inputs"]),
                        "inputs": inputs,
                        "hasMore": False,
                        "nextAfterSequence": (
                            inputs[-1]["sequence"] if inputs else after
                        ),
                    },
                )

            if method == "POST" and parts[2] == "inputs":
                _add_input(state, body)
                state["thread"]["activePreviewVersionId"] = None
                state["thread"]["updatedAt"] = TIMESTAMP
                return _Response(
                    201,
                    {
                        **_thread_projection(state, duplicate=False),
                        "previewInvalidated": True,
                    },
                )

            if method == "POST" and parts[2] == "runs":
                if body["expectedBasisInputSequence"] != len(state["inputs"]):
                    raise AssertionError("run basis changed")
                if body["expectedInputDigest"] != _input_digest(state):
                    raise AssertionError("run input digest changed")
                run_no = len(state["runs"]) + 1
                run_id = f"run-gateway-lifecycle-{run_no}"
                preview_id = f"preview-gateway-lifecycle-{run_no}"
                run = {
                    "runId": run_id,
                    "threadId": thread_id,
                    "status": "succeeded",
                    "basisInputSequence": len(state["inputs"]),
                    "workItems": [
                        {
                            "workItemId": f"work-preview-{run_no}",
                            "workKind": "preview",
                            "scopeKey": "preview",
                            "status": "succeeded",
                            "acceptedResultId": preview_id,
                        }
                    ],
                    "progress": {
                        "total": 1,
                        "completed": 1,
                        "states": {"succeeded": 1},
                    },
                }
                state["runs"][run_id] = run
                state["previews"][preview_id] = {
                    "threadId": thread_id,
                    "runId": run_id,
                    "previewResultId": preview_id,
                    "previewResultHash": _digest(
                        {"preview": preview_id, "basis": len(state["inputs"])}
                    ),
                    "planHash": _digest({"plan": run_id}),
                    "basisInputSequence": len(state["inputs"]),
                    "title": "Gateway lifecycle implementation plan",
                    "objective": "Apply the reviewed implementation through Jira.",
                    "summary": "One immutable task proves the complete public path.",
                    "decisions": [{"code": "ready_for_approval"}],
                    "coverage": {"ready": True, "findings": []},
                    "tasks": [
                        {
                            "stableTaskId": f"task-{run_no}",
                            "summary": (
                                "Execute the provider-confirmed Jira lifecycle"
                            ),
                        }
                    ],
                    "acceptedAt": TIMESTAMP,
                    "reviewed": False,
                }
                state["thread"]["activeRunId"] = run_id
                state["thread"]["activePreviewVersionId"] = preview_id
                state["thread"]["updatedAt"] = TIMESTAMP
                state["runWrites"] += 1
                return _Response(201, run)

            if method == "POST" and parts[2] == "delivery-attention":
                return _Response(
                    200,
                    _delivery_attention(thread_id, body["origin"]),
                )

            if method == "POST" and parts[2] == "apply-status":
                return _Response(200, _apply_status(state, thread_id))

            if method == "GET" and parts[2] == "previews":
                preview = state["previews"][parts[3]]
                return _Response(
                    200,
                    _preview_page(
                        preview,
                        offset=int(query["offset"][0]),
                        limit=int(query["limit"][0]),
                    ),
                )

            if (
                method == "POST"
                and parts[2] == "previews"
                and parts[4] == "review-receipts"
            ):
                preview = state["previews"][parts[3]]
                state["ackTransportAttempts"] += 1
                key = request.get_header("Idempotency-key")
                replayed = key in state["reviewReceipts"]
                page = _preview_page(
                    preview,
                    offset=body["offset"],
                    limit=body["count"],
                )
                if not replayed:
                    state["reviewReceipts"][key] = {
                        "pageDigest": body["pageDigest"],
                        "deliveryProof": body["deliveryProof"],
                    }
                    preview["reviewed"] = True
                    state["ackWrites"] += 1
                response = {
                    "ok": True,
                    "replayed": replayed,
                    "receipt": {
                        "reviewReceiptId": "review-receipt-gateway-lifecycle",
                        "threadId": thread_id,
                        "previewResultId": parts[3],
                        "previewResultHash": preview["previewResultHash"],
                        "offset": body["offset"],
                        "count": body["count"],
                        "pageDigest": body["pageDigest"],
                    },
                    "reviewStatus": _review_status(preview),
                    "approvalEligible": True,
                }
                if state["ackLossesRemaining"] > 0:
                    state["ackLossesRemaining"] -= 1
                    return TimeoutError(
                        "remote ACK response lost after durable commit"
                    )
                return _Response(200, response)

            if (
                method == "POST"
                and parts[2] == "previews"
                and parts[4] == "approve-apply"
            ):
                preview = state["previews"][parts[3]]
                if not preview["reviewed"]:
                    raise AssertionError("unreviewed preview reached apply")
                if body["expectedPreviewHash"] != preview["previewResultHash"]:
                    raise AssertionError("preview hash changed")
                if body["expectedPlanHash"] != preview["planHash"]:
                    raise AssertionError("plan hash changed")
                replayed = state["apply"] is not None
                if not replayed:
                    state["apply"] = {
                        "approvalMessage": body["approvalMessage"],
                        "approvalEvidence": body["approvalEvidence"],
                        "providerEventId": body["origin"]["providerEventId"],
                        "operation": {
                            "status": "completed",
                            "jiraKeys": ["SEO-4242"],
                        },
                        "createdAt": TIMESTAMP,
                        "updatedAt": TIMESTAMP,
                    }
                    state["applyWrites"] += 1
                return _Response(
                    200,
                    {
                        "ok": True,
                        "replayed": replayed,
                        "applyBindingId": "apply-binding-gateway-lifecycle",
                        "threadId": thread_id,
                        "runId": preview["runId"],
                        "previewResultId": parts[3],
                        "previewResultHash": preview["previewResultHash"],
                        "planHash": preview["planHash"],
                        "basisInputSequence": preview["basisInputSequence"],
                        "planId": "plan-gateway-lifecycle",
                        "approvalId": "approval-gateway-lifecycle",
                        "operationId": "jira-operation-gateway-lifecycle",
                        "status": "completed",
                        "operation": dict(state["apply"]["operation"]),
                        "error": None,
                        "createdAt": state["apply"]["createdAt"],
                        "updatedAt": state["apply"]["updatedAt"],
                    },
                )

            raise AssertionError(f"unexpected Planning route: {method} {path}")

        result = _mutate(self.state_path, handle)
        if isinstance(result, BaseException):
            raise result
        return result


def _install_real_client_transport(state_path: Path):
    import hermes_cli.dev_hub_planning_v2 as client_module

    real_client = client_module.PlanningV2Client

    def factory(*_args, **_kwargs):
        return real_client(
            base_url=BASE_URL,
            token=RUNNER_TOKEN,
            runner_id=RUNNER_ID,
            transport=StateTransport(state_path),
            sleep=lambda _seconds: None,
        )

    client_module.PlanningV2Client = factory
    import hermes_cli.dev_hub_planning_facade as facade
    import tools.agent_ops_tasking_tool as public_tool
    import tools.dev_hub_planning_tool as private_tool

    facade.PlanningV2Client = factory
    public_tool.PlanningV2Client = factory
    private_tool.PlanningV2Client = factory
    return factory


def _configure(root: Path) -> tuple[Path, Path]:
    home = root / "hermes-home"
    home.mkdir(parents=True, exist_ok=True)
    state_path = root / "remote-state.json"
    os.environ["HERMES_HOME"] = str(home)
    os.environ["TELEGRAM_ALLOW_ALL_USERS"] = "true"
    os.environ["DISCORD_ALLOW_ALL_USERS"] = "true"
    os.environ["OPENROUTER_API_KEY"] = "sk-or-v1-" + ("a" * 64)
    os.environ["AGENT_OPS_API_URL"] = BASE_URL
    os.environ["AGENT_OPS_RUNNER_ID"] = RUNNER_ID
    os.environ["AGENT_OPS_RUNNER_TOKEN"] = RUNNER_TOKEN
    os.environ["HERMES_AGENT_NOTIFY_INTERVAL"] = "0"
    os.environ["HERMES_HUMAN_DELAY_MODE"] = "off"
    (home / "config.yaml").write_text(
        "\n".join(
            (
                "model:",
                f"  default: {MODEL}",
                "  provider: openrouter",
                "toolsets:",
                "  - planning_v2",
                "display:",
                "  tool_progress: false",
                "  thinking_progress: false",
                "streaming:",
                "  enabled: false",
                "",
            )
        ),
        encoding="utf-8",
    )
    return home, state_path


def _tool_call_for(command: str, text: str) -> tuple[str, dict[str, Any]]:
    if command == "new":
        return "agent_ops_task_plan", {"intent": "new", "mode": "quick"}
    if command == "revise":
        return "agent_ops_task_plan", {"intent": "revise", "mode": "quick"}
    if command == "show":
        return "agent_ops_task_plan", {"intent": "show", "detail": "all"}
    if command == "approve":
        return (
            "agent_ops_task_approve_apply",
            {
                "approvalMessage": text,
                "approvalEvidence": {
                    "meaning": "approve_current_preview_exactly",
                    "exactQuote": APPROVAL_QUOTE,
                },
            },
        )
    if command == "status":
        return "agent_ops_task_plan", {"intent": "status"}
    raise AssertionError(command)


def _install_scripted_model(
    command: str,
    state_path: Path,
    *,
    first_call_started: threading.Event | None = None,
    first_call_release: threading.Event | None = None,
) -> None:
    class ScriptedModelAgent:
        def __init__(self, *_args, **kwargs):
            self.session_id = kwargs.get("session_id")
            self.model = kwargs.get("model") or MODEL
            self.provider = kwargs.get("provider") or "openrouter"
            self.api_mode = kwargs.get("api_mode") or "chat_completions"
            self.base_url = kwargs.get("base_url")
            self.api_key = kwargs.get("api_key")
            self.tools = []
            self._session_messages = []
            self.session_prompt_tokens = 0
            self.session_completion_tokens = 0

        def run_conversation(
            self,
            message,
            *,
            conversation_history=None,
            task_id=None,
            turn_origin=None,
            **_kwargs,
        ):
            import model_tools
            from agent.auxiliary_client import scoped_runtime_main
            from hermes_cli.turn_origin import (
                scoped_turn_origin,
                scoped_turn_user_text,
            )

            if not isinstance(message, str):
                raise AssertionError("scripted lifecycle expects a text turn")
            model_command = command
            if command == "queued":
                model_command = (
                    "new"
                    if turn_origin.event_id == "gateway-lifecycle-queued-first"
                    else "revise"
                )
                if model_command == "new":
                    if first_call_started is None or first_call_release is None:
                        raise AssertionError(
                            "queued lifecycle model barriers are missing"
                        )
                    first_call_started.set()
                    if not first_call_release.wait(timeout=10):
                        raise TimeoutError(
                            "queued lifecycle first model was not released"
                        )
            unauthorized_dimension, model_action = _command_parts(
                model_command
            )
            tool_name, arguments = _tool_call_for(model_action, message)
            if unauthorized_dimension is not None:
                owner_state = _read(state_path)
                owner_thread = owner_state.get("thread")
                if not isinstance(owner_thread, dict):
                    raise AssertionError(
                        "unauthorized lifecycle probe requires an owner thread"
                    )
                owner_thread_id = owner_thread["threadId"]
                if model_action in {"revise", "show"}:
                    arguments["threadId"] = owner_thread_id
                elif model_action == "approve":
                    arguments["selectedOption"] = owner_thread_id
            with (
                scoped_turn_origin(turn_origin),
                scoped_turn_user_text(message),
                scoped_runtime_main(
                    {
                        "provider": "openrouter",
                        "model": MODEL,
                        "api_mode": "chat_completions",
                        "base_url": "https://model.external.invalid/v1",
                        "api_key": "external-model-boundary",
                        "auth_mode": "bearer",
                    }
                ),
            ):
                raw = model_tools.handle_function_call(
                    tool_name,
                    arguments,
                    session_id=task_id,
                    user_task=message,
                    enabled_toolsets=["planning_v2"],
                    skip_pre_tool_call_hook=True,
                    skip_tool_request_middleware=True,
                )
            decoded = json.loads(raw)

            def record(state):
                state["modelCalls"].append(
                    {
                        "command": model_command,
                        "action": model_action,
                        "unauthorizedDimension": unauthorized_dimension,
                        "tool": tool_name,
                        "arguments": arguments,
                        "pid": os.getpid(),
                        "eventId": turn_origin.event_id,
                        "resultDigest": _digest(decoded),
                        "ok": decoded.get("ok"),
                        "result": decoded,
                    }
                )

            _mutate(state_path, record)
            messages = list(conversation_history or [])
            messages.extend(
                (
                    {"role": "user", "content": message},
                    {"role": "assistant", "content": raw},
                )
            )
            self._session_messages = messages
            return {
                "final_response": raw,
                "messages": messages,
                "api_calls": 1,
                "completed": True,
                "agent_persisted": False,
            }

        def interrupt(self, *_args, **_kwargs):
            return None

    module = ModuleType("run_agent")
    module.AIAgent = ScriptedModelAgent
    sys.modules["run_agent"] = module


def _declare_provider() -> None:
    from gateway.platform_registry import declare_semantic_exact_attempt

    declare_semantic_exact_attempt(
        PROVIDER,
        standalone=False,
        live=True,
        owner="agent-ops-gateway-lifecycle-process-test",
    )


def _build_runner(home: Path, state_path: Path):
    from gateway.config import (
        GatewayConfig,
        HomeChannel,
        Platform,
        PlatformConfig,
    )
    from gateway.platforms.base import (
        BasePlatformAdapter,
        SendResult,
    )
    from gateway.semantic_exact_attempt import (
        LiveSemanticExactAttemptCapability,
    )
    from gateway.run import GatewayRunner

    _declare_provider()

    class LifecycleAdapter(BasePlatformAdapter):
        SEMANTIC_EXACT_ATTEMPT_CAPABILITY = (
            LiveSemanticExactAttemptCapability(
                provider=PROVIDER,
                contract="hermes-live-semantic-exact-attempt/1",
                segmentation_version="gateway-lifecycle-unicode-v1",
                max_logical_units=4096,
                length_semantics="unicode_codepoints",
                wire_encoding="gateway-lifecycle-json-v1",
            )
        )

        async def connect(self, *, is_reconnect: bool = False) -> bool:
            del is_reconnect
            self._running = True
            return True

        async def disconnect(self) -> None:
            self._running = False

        async def send(
            self,
            chat_id,
            content,
            reply_to=None,
            metadata=None,
        ):
            del reply_to, metadata

            def record(state):
                message_id = f"ordinary-{len(state['outbound']) + 1}"
                state["outbound"].append(
                    {
                        "chatId": chat_id,
                        "content": content,
                        "contentDigest": _digest(content.encode()),
                        "messageId": message_id,
                        "pid": os.getpid(),
                    }
                )
                return message_id

            message_id = _mutate(state_path, record)
            return SendResult(
                success=True,
                message_id=message_id,
                delivered_content_digest=_digest(content.encode()),
                delivered_content_complete=True,
            )

        async def get_chat_info(self, chat_id):
            return {"id": chat_id, "name": "Lifecycle", "type": "dm"}

        async def send_typing(self, chat_id, metadata=None):
            del chat_id, metadata
            return None

        def bind_semantic_exact_attempt_provider_route(
            self,
            *,
            chat_id,
            thread_id=None,
            reply_to=None,
        ):
            del self, chat_id, thread_id, reply_to
            return {"transport": "gateway_lifecycle"}

        async def send_semantic_exact_attempt(self, request):
            def settle(state):
                prior = state["providerReceipts"].get(request.delivery_id)
                if prior is None:
                    message_id = (
                        f"semantic-{len(state['providerReceipts']) + 1}"
                    )
                    state["providerReceipts"][request.delivery_id] = message_id
                    state["providerWrites"] += 1
                else:
                    message_id = prior
                state["semanticCalls"].append(
                    {
                        "deliveryId": request.delivery_id,
                        "messageId": message_id,
                        "contentDigest": _digest(request.content.encode()),
                        "pid": os.getpid(),
                    }
                )
                return message_id

            message_id = _mutate(state_path, settle)
            return SendResult(
                success=True,
                message_id=message_id,
                delivered_content_digest=_digest(request.content.encode()),
                delivered_content_complete=True,
            )

    platform_config = PlatformConfig(
        enabled=True,
        token="external-provider-boundary",
        typing_indicator=False,
        home_channel=HomeChannel(
            platform=Platform.TELEGRAM,
            chat_id=CHAT,
            name="Lifecycle",
        ),
        extra={"gateway_account_id": ACCOUNT},
    )
    config = GatewayConfig(
        platforms={Platform.TELEGRAM: platform_config},
        sessions_dir=home / "sessions",
    )
    runner = GatewayRunner(config)
    adapter = LifecycleAdapter(platform_config, Platform.TELEGRAM)
    adapter.gateway_runner = runner
    runner.adapters = {Platform.TELEGRAM: adapter}
    adapter.set_session_store(runner.session_store)
    adapter.set_message_handler(runner._handle_message)
    adapter.set_busy_session_handler(
        runner._handle_active_session_busy_message
    )
    adapter.set_authorization_check(
        runner._make_adapter_auth_check(Platform.TELEGRAM)
    )
    return runner, adapter


async def _stop_runner_without_provider_notice(runner) -> None:
    """Close process resources without adding a synthetic shutdown message."""

    async def no_notice() -> None:
        return None

    runner._notify_active_sessions_of_shutdown = no_notice
    await runner.stop()


def _command_parts(command: str) -> tuple[str | None, str]:
    if not command.startswith("unauthorized-"):
        return None, command
    prefix, dimension, action = command.split("-", 2)
    if (
        prefix != "unauthorized"
        or dimension not in UNAUTHORIZED_DIMENSIONS
        or action not in UNAUTHORIZED_ACTIONS
    ):
        raise AssertionError(f"invalid unauthorized command: {command}")
    return dimension, action


def _text(command: str) -> str:
    _dimension, action = _command_parts(command)
    return {
        "new": "Create a durable implementation plan for the SEO rollout.",
        "revise": "Revise it to make the Jira rollout dependency explicit.",
        "show": "Show me the complete current preview.",
        "approve": APPROVAL_TEXT,
        "status": "Read back the durable Jira apply status.",
    }[action]


async def _run_gateway_turn(
    root: Path,
    command: str,
) -> dict[str, Any]:
    home, state_path = _configure(root)
    _install_real_client_transport(state_path)
    _install_scripted_model(command, state_path)
    runner, adapter = _build_runner(home, state_path)

    from gateway.config import Platform
    from gateway.platforms.base import MessageEvent, MessageType
    from gateway.session import SessionSource

    event_id = f"gateway-lifecycle-{command}"
    unauthorized_dimension, action = _command_parts(command)
    source = SessionSource(
        platform=(
            Platform.DISCORD
            if unauthorized_dimension == "provider"
            else Platform.TELEGRAM
        ),
        chat_id=(
            "gateway-lifecycle-foreign-chat"
            if unauthorized_dimension == "chat"
            else CHAT
        ),
        chat_type="dm",
        user_id=USER,
        user_name="Lifecycle Owner",
        thread_id=(
            "gateway-lifecycle-foreign-thread"
            if unauthorized_dimension == "thread"
            else None
        ),
        message_id=f"message-{command}",
        gateway_account_id=(
            "gateway-lifecycle-foreign-account"
            if unauthorized_dimension == "account"
            else ACCOUNT
        ),
    )
    event = MessageEvent(
        text=_text(command),
        message_type=MessageType.TEXT,
        source=source,
        message_id=f"message-{command}",
        event_id=event_id,
    )

    # Observe, but do not replace, the two production normalization guards.
    original_ensure = MessageEvent.ensure_turn_origin
    guard_callers: list[str] = []

    def observed_ensure(self, *args, **kwargs):
        import inspect

        guard_callers.append(inspect.currentframe().f_back.f_code.co_name)
        return original_ensure(self, *args, **kwargs)

    MessageEvent.ensure_turn_origin = observed_ensure
    try:
        await adapter.handle_message(event)
        while adapter._background_tasks:
            await asyncio.gather(
                *tuple(adapter._background_tasks),
                return_exceptions=False,
            )
    finally:
        MessageEvent.ensure_turn_origin = original_ensure

    from hermes_cli.gateway_turn_queue import queue_depth

    session_key = runner._session_key_for_source(source)
    result = {
        "command": command,
        "action": action,
        "unauthorizedDimension": unauthorized_dimension,
        "pid": os.getpid(),
        "eventId": event.turn_origin.event_id,
        "guardCallers": guard_callers,
        "queueDepth": queue_depth(session_key, profile_home=home),
    }

    def record(state):
        state["gatewayTurns"].append(result)

    _mutate(state_path, record)
    await _stop_runner_without_provider_notice(runner)
    return result


async def _run_nonidle_queued_turns(root: Path) -> dict[str, Any]:
    """Admit a second provider event while the first model turn is running."""

    home, state_path = _configure(root)
    _install_real_client_transport(state_path)
    first_call_started = threading.Event()
    first_call_release = threading.Event()
    _install_scripted_model(
        "queued",
        state_path,
        first_call_started=first_call_started,
        first_call_release=first_call_release,
    )
    runner, adapter = _build_runner(home, state_path)
    runner._busy_input_mode = "queue"
    runner._busy_text_mode = "queue"
    adapter._busy_text_mode = "queue"

    from gateway.config import Platform
    from gateway.platforms.base import MessageEvent, MessageType
    from gateway.session import SessionSource
    from hermes_cli.gateway_turn_queue import queue_depth

    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id=CHAT,
        chat_type="dm",
        user_id=USER,
        user_name="Lifecycle Owner",
        gateway_account_id=ACCOUNT,
    )

    def event(label: str, text: str) -> MessageEvent:
        return MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=f"message-queued-{label}",
            event_id=f"gateway-lifecycle-queued-{label}",
        )

    first = event("first", _text("new"))
    second = event("second", _text("revise"))
    session_key = runner._session_key_for_source(source)
    queued_depth_while_busy = 0
    try:
        await adapter.handle_message(first)
        started = await asyncio.to_thread(first_call_started.wait, 10)
        if not started:
            raise TimeoutError("first queued lifecycle model did not start")
        if session_key not in adapter._active_sessions:
            raise AssertionError("adapter session was not active")

        await adapter.handle_message(second)
        queued_depth_while_busy = queue_depth(
            session_key,
            profile_home=home,
        )
    finally:
        first_call_release.set()

    while adapter._background_tasks:
        await asyncio.gather(
            *tuple(adapter._background_tasks),
            return_exceptions=False,
        )

    result = {
        "command": "queued",
        "pid": os.getpid(),
        "eventIds": [
            first.turn_origin.event_id,
            second.turn_origin.event_id,
        ],
        "queuedDepthWhileBusy": queued_depth_while_busy,
        "queueDepth": queue_depth(session_key, profile_home=home),
    }
    await _stop_runner_without_provider_notice(runner)
    return result


def _ack_succeeded(home: Path) -> int:
    path = (
        home
        / "state"
        / "planning-preview-ack"
        / "outbox.sqlite3"
    )
    if not path.exists():
        return 0
    with sqlite3.connect(path) as connection:
        return int(
            connection.execute(
                "SELECT COUNT(*) FROM planning_preview_ack_outbox "
                "WHERE state='succeeded'"
            ).fetchone()[0]
        )


async def _recover_ack(root: Path) -> dict[str, Any]:
    home, state_path = _configure(root)
    _install_real_client_transport(state_path)
    runner, _adapter = _build_runner(home, state_path)
    from gateway.profile_delivery_supervisor import (
        ProfileDeliverySupervisor,
    )

    supervisor = ProfileDeliverySupervisor(runner, poll_seconds=0.02)
    if not supervisor.start():
        raise RuntimeError("preview supervisor did not start")
    deadline = time.monotonic() + 10
    try:
        while time.monotonic() < deadline:
            state = _read(state_path)
            if _ack_succeeded(home) == 1 and state["ackLossesRemaining"] == 0:
                break
            await asyncio.sleep(0.02)
        else:
            raise RuntimeError("preview ACK supervisor did not converge")
    finally:
        await supervisor.stop(timeout=2)
    result = {
        "command": "recover-ack",
        "pid": os.getpid(),
        "activeProfileCount": supervisor.active_profile_count,
        "bindingCount": len(supervisor.bindings),
        "ackSucceeded": _ack_succeeded(home),
    }
    await _stop_runner_without_provider_notice(runner)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument(
        "command",
        choices=(
            "new",
            "revise",
            "show",
            "recover-ack",
            "approve",
            "status",
            "queued",
            *UNAUTHORIZED_COMMANDS,
        ),
    )
    args = parser.parse_args()
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    if args.command == "show":
        _, state_path = _configure(root)

        def arm_loss(state):
            state["ackLossesRemaining"] = 2

        _mutate(state_path, arm_loss)
    if args.command == "recover-ack":
        result = asyncio.run(_recover_ack(root))
    elif args.command == "queued":
        result = asyncio.run(_run_nonidle_queued_turns(root))
    else:
        result = asyncio.run(_run_gateway_turn(root, args.command))
    # The real runner has already completed its bounded production shutdown.
    # Flush the one process receipt and exit directly so unrelated optional
    # native libraries cannot race Python's interpreter-finalization phase and
    # turn a completed gateway proof into ``FATAL: exception not rethrown``.
    sys.stdout.write(_canonical(result) + "\n")
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
