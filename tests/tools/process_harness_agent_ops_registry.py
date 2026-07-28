"""Fresh-process model registry proof for the Agent Ops Planning V2 facade."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys


def main() -> None:
    profile_home = Path(sys.argv[1]).resolve()
    os.environ["HERMES_HOME"] = str(profile_home)
    os.environ["AGENT_OPS_API_URL"] = "https://hub.invalid"
    os.environ["AGENT_OPS_RUNNER_ID"] = "registry-process-runner"
    os.environ["AGENT_OPS_RUNNER_TOKEN"] = "registry-process-token"

    import model_tools
    from hermes_cli import dev_hub_planning_facade as facade
    from tools.registry import registry

    definitions = model_tools.get_tool_definitions(
        enabled_toolsets=["planning_v2"],
        quiet_mode=True,
        skip_tool_search_assembly=True,
    )
    names = [
        str(definition["function"]["name"])
        for definition in definitions
    ]
    cursor_schema_names = sorted(
        str(definition["function"]["name"])
        for definition in definitions
        if (
            definition["function"]["parameters"]["properties"]
            .get("threadAfterCursor", {})
            .get("type")
            == "string"
        )
    )
    legacy = model_tools.handle_function_call(
        "agent_ops_planning_v2",
        {"action": "status"},
        enabled_toolsets=["planning_v2"],
        skip_pre_tool_call_hook=True,
        skip_tool_request_middleware=True,
    )
    public = model_tools.handle_function_call(
        "agent_ops_task_plan",
        {"intent": "not-a-public-intent"},
        enabled_toolsets=["planning_v2"],
        skip_pre_tool_call_hook=True,
        skip_tool_request_middleware=True,
    )
    facade.current_planning_gateway_admission = lambda: type(
        "Admission",
        (),
        {
            "eligible": True,
            "provider": "discord",
            "reason": "live_exact_delivery_conformant",
        },
    )()
    facade.PlanningV2Client = lambda: object()
    facade._resolve_thread = lambda *_args, **_kwargs: (
        {
            "threadId": "thread-1",
            "title": "Recover public planning",
        },
        None,
    )
    facade._status_with_delivery_attention = lambda **_kwargs: {
        "ok": False,
        "code": "planning.retryable_test",
        "retryable": True,
        "recovery": {
            "nextAction": {
                "tool": "agent_ops_planning_v2",
                "arguments": {
                    "action": "start_run",
                    "thread_id": "thread-1",
                    "recovery_token": "artrec_v2_" + "a" * 64,
                },
            }
        },
    }
    public_recovery = model_tools.handle_function_call(
        "agent_ops_task_plan",
        {"intent": "status"},
        enabled_toolsets=["planning_v2"],
        skip_pre_tool_call_hook=True,
        skip_tool_request_middleware=True,
    )
    print(
        json.dumps(
            {
                "definitions": names,
                "threadChoiceCursorDefinitions": cursor_schema_names,
                "legacyEntry": (
                    registry.get_entry("agent_ops_planning_v2") is not None
                ),
                "legacyDispatch": json.loads(legacy),
                "publicDispatch": json.loads(public),
                "publicRecoveryDispatch": json.loads(public_recovery),
                "registeredPlanningTools": (
                    registry.get_tool_names_for_toolset("planning_v2")
                ),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
