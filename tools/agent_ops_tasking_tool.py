"""Public Agent Ops tasking facade backed only by Planning V2.

These are the stable model-visible names used by Agent Ops installations.
The detailed Planning V2 engine remains process-private: callers cannot select
the legacy low-level tool or fall back to another planner.
"""

from __future__ import annotations

import json
from typing import Any

from hermes_cli.dev_hub_planning_facade import invoke_public_tasking_tool
from hermes_cli.dev_hub_planning_v2 import (
    PlanningV2Client,
    PlanningV2ClientError,
)
from tools.dev_hub_planning_tool import (
    PLANNING_V2_TOOLSET,
    _profile_opted_in,
)
from tools.registry import registry


AGENT_OPS_TASK_PLAN_TOOL_NAME = "agent_ops_task_plan"
AGENT_OPS_TASK_APPROVE_APPLY_TOOL_NAME = (
    "agent_ops_task_approve_apply"
)


def _check_agent_ops_tasking_requirements() -> bool:
    """Expose the public facade only for an opted-in, configured profile."""

    if not _profile_opted_in():
        return False
    try:
        PlanningV2Client()
    except PlanningV2ClientError:
        return False
    return True


def _invoke_public(
    tool_name: str,
    arguments: dict[str, Any],
    **runtime_kwargs: Any,
) -> str:
    result = invoke_public_tasking_tool(
        tool_name=tool_name,
        arguments=arguments,
        runtime_kwargs=runtime_kwargs,
    )
    if isinstance(result, str):
        return result
    return json.dumps(
        result,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _handle_agent_ops_task_plan(
    arguments: dict[str, Any],
    **runtime_kwargs: Any,
) -> str:
    return _invoke_public(
        AGENT_OPS_TASK_PLAN_TOOL_NAME,
        arguments,
        **runtime_kwargs,
    )


def _handle_agent_ops_task_approve_apply(
    arguments: dict[str, Any],
    **runtime_kwargs: Any,
) -> str:
    return _invoke_public(
        AGENT_OPS_TASK_APPROVE_APPLY_TOOL_NAME,
        arguments,
        **runtime_kwargs,
    )


AGENT_OPS_TASK_PLAN_SCHEMA = {
    "name": AGENT_OPS_TASK_PLAN_TOOL_NAME,
    "description": (
        "Create, revise, resume, inspect, or cancel the current conversation's "
        "durable Agent Ops plan. The exact human turn and gateway attachment "
        "identities are authoritative. Use show to schedule every immutable "
        "preview page for provider-confirmed delivery before asking for a "
        "later approval."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "intent": {
                "type": "string",
                "enum": [
                    "new",
                    "revise",
                    "retry",
                    "resume",
                    "status",
                    "show",
                    "cancel",
                    "resolve_delivery",
                ],
            },
            "mode": {
                "type": "string",
                "enum": ["auto", "quick", "deep"],
            },
            "detail": {
                "type": "string",
                "enum": ["human", "technical", "all"],
            },
            "taskPosition": {
                "type": "integer",
                "minimum": 1,
                "description": (
                    "Optional one-based task selector. Omit it to deliver the "
                    "complete preview; there is no total task-count cap."
                ),
            },
            "threadId": {"type": "string"},
            "selectedOption": {"type": "string"},
            "threadAfterCursor": {
                "type": "string",
                "description": (
                    "Opaque model-private continuation returned as "
                    "nextChoiceCursor when active-thread choices span more "
                    "than one bounded page. Never invent or alter it."
                ),
            },
            "laneHint": {"type": "string"},
            "visualRole": {
                "type": "string",
                "enum": ["reference", "implementation_target"],
            },
            "deliveryAction": {
                "type": "string",
                "enum": [
                    "mark_delivered",
                    "resend_acknowledged",
                ],
            },
            "resolutionToken": {"type": "string"},
            "applyRecoveryToken": {"type": "string"},
            "deliveryAfterResolutionToken": {"type": "string"},
            "applyAfterRecoveryToken": {"type": "string"},
        },
        "required": ["intent"],
        "additionalProperties": True,
    },
}


AGENT_OPS_TASK_APPROVE_APPLY_SCHEMA = {
    "name": AGENT_OPS_TASK_APPROVE_APPLY_TOOL_NAME,
    "description": (
        "Apply exactly one immutable Agent Ops preview only after the current "
        "human turn explicitly approves it and every preview task has a "
        "provider-confirmed review receipt. Never infer or pre-authorize "
        "approval from an earlier turn."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "approvalMessage": {"type": "string"},
            "approvalEvidence": {
                "type": "object",
                "properties": {
                    "meaning": {
                        "type": "string",
                        "enum": [
                            "approve_current_preview_exactly",
                        ],
                    },
                    "exactQuote": {"type": "string"},
                },
                "required": ["meaning", "exactQuote"],
                "additionalProperties": True,
            },
            "selectedOption": {"type": "string"},
            "threadAfterCursor": {
                "type": "string",
                "description": (
                    "Opaque model-private continuation returned as "
                    "nextChoiceCursor when reviewed preview choices span "
                    "more than one bounded page. Never invent or alter it."
                ),
            },
        },
        "required": ["approvalMessage", "approvalEvidence"],
        "additionalProperties": True,
    },
}


registry.register(
    name=AGENT_OPS_TASK_PLAN_TOOL_NAME,
    toolset=PLANNING_V2_TOOLSET,
    schema=AGENT_OPS_TASK_PLAN_SCHEMA,
    handler=_handle_agent_ops_task_plan,
    check_fn=_check_agent_ops_tasking_requirements,
    emoji="🧭",
)
registry.register(
    name=AGENT_OPS_TASK_APPROVE_APPLY_TOOL_NAME,
    toolset=PLANNING_V2_TOOLSET,
    schema=AGENT_OPS_TASK_APPROVE_APPLY_SCHEMA,
    handler=_handle_agent_ops_task_approve_apply,
    check_fn=_check_agent_ops_tasking_requirements,
    emoji="✅",
)


__all__ = [
    "AGENT_OPS_TASK_APPROVE_APPLY_SCHEMA",
    "AGENT_OPS_TASK_APPROVE_APPLY_TOOL_NAME",
    "AGENT_OPS_TASK_PLAN_SCHEMA",
    "AGENT_OPS_TASK_PLAN_TOOL_NAME",
]
