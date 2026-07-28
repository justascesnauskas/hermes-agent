"""Public Planning admission uses the exact live bound adapter."""

from __future__ import annotations

import json
from typing import Any

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.api_server import APIServerAdapter
from gateway.platforms.msgraph_webhook import MSGraphWebhookAdapter
from gateway.platforms.signal import SignalAdapter
from gateway.platforms.webhook import WebhookAdapter
from gateway.relay.adapter import RelayAdapter
from hermes_cli import dev_hub_planning_facade as facade
from hermes_cli.planning_gateway_admission import (
    current_planning_gateway_admission,
)
from hermes_cli.turn_origin import (
    TurnOriginV1,
    get_current_turn_delivery_adapter,
    scoped_turn_delivery_adapter,
    scoped_turn_origin,
)
from plugins.platforms.raft.adapter import RaftAdapter
from tools import agent_ops_tasking_tool


def _adapter(adapter_type: type, platform: Any):
    value = object.__new__(adapter_type)
    value.platform = platform
    value.config = PlatformConfig(
        enabled=True,
        extra={"gateway_account_id": f"{str(platform)}-account"},
    )
    return value


def _origin(provider: str) -> TurnOriginV1:
    return TurnOriginV1(
        provider=provider,
        gateway_account_id=f"{provider}-account",
        chat_id="planning-chat",
        message_id="planning-message",
        sender_id="planning-user",
        event_id=f"{provider}-event",
    )


_INELIGIBLE = (
    (
        "api_server",
        APIServerAdapter,
        Platform.API_SERVER,
        "response_plane_without_push_receipt",
    ),
    (
        "msgraph_webhook",
        MSGraphWebhookAdapter,
        Platform.MSGRAPH_WEBHOOK,
        "ingress_only_without_outbound_receipt",
    ),
    (
        "webhook",
        WebhookAdapter,
        Platform.WEBHOOK,
        "delegated_destination_not_frozen",
    ),
    (
        "raft",
        RaftAdapter,
        "raft",
        "external_cli_without_response_receipt",
    ),
    (
        "relay",
        RelayAdapter,
        Platform.RELAY,
        "connector_true_provider_receipt_not_negotiated",
    ),
)


@pytest.mark.parametrize(
    ("provider", "adapter_type", "platform", "reason"),
    _INELIGIBLE,
)
@pytest.mark.parametrize("public_tool", ["plan", "approve"])
def test_public_tools_reject_ineligible_gateway_before_client_construction(
    monkeypatch,
    provider: str,
    adapter_type: type,
    platform: Any,
    reason: str,
    public_tool: str,
) -> None:
    client_constructions = 0

    def forbidden_client():
        nonlocal client_constructions
        client_constructions += 1
        raise AssertionError("Planning client crossed gateway admission")

    monkeypatch.setattr(facade, "PlanningV2Client", forbidden_client)
    with (
        scoped_turn_origin(_origin(provider)),
        scoped_turn_delivery_adapter(_adapter(adapter_type, platform)),
    ):
        if public_tool == "plan":
            raw = agent_ops_tasking_tool._handle_agent_ops_task_plan(
                {"intent": "new"},
                user_task="Plan safely",
            )
        else:
            raw = (
                agent_ops_tasking_tool
                ._handle_agent_ops_task_approve_apply(
                    {
                        "approvalMessage": "Approve",
                        "approvalEvidence": {
                            "meaning": "approve_current_preview_exactly",
                            "exactQuote": "Approve",
                        },
                    },
                    user_task="Approve",
                )
            )

    result = json.loads(raw)
    assert result == {
        "code": "planning.bound_gateway_ineligible",
        "detail": (
            "Planning is unavailable from this bound gateway because it "
            "cannot produce a provider-confirmed exact delivery receipt. No "
            "Planning state was changed."
        ),
        "nextAction": (
            "Continue from a directly bound gateway that supports "
            "provider-confirmed exact Planning delivery."
        ),
        "ok": False,
        "outcomeAmbiguous": False,
        "provider": provider,
        "reason": reason,
        "retryable": False,
        "route": "planning_v2",
        "stateChanged": False,
    }
    assert client_constructions == 0


def test_facade_eligible_control_reaches_client_only_with_concrete_exact_owner(
    monkeypatch,
) -> None:
    adapter = _adapter(SignalAdapter, Platform.SIGNAL)
    client_constructions = 0

    def reached_client():
        nonlocal client_constructions
        client_constructions += 1
        raise RuntimeError("eligible-control-reached-client")

    monkeypatch.setattr(facade, "PlanningV2Client", reached_client)
    with (
        scoped_turn_origin(_origin("signal")),
        scoped_turn_delivery_adapter(adapter),
    ):
        admission = current_planning_gateway_admission()
        raw = facade.invoke_public_tasking_tool(
            tool_name="agent_ops_task_plan",
            arguments={"intent": "status"},
            runtime_kwargs={"user_task": "Show status"},
        )

    assert admission.eligible is True
    assert admission.reason == "live_exact_delivery_conformant"
    assert json.loads(raw)["code"] == "planning.facade_internal_error"
    assert client_constructions == 1


def test_positive_declaration_without_concrete_exact_method_fails_closed(
    monkeypatch,
) -> None:
    class BrokenSignalAdapter:
        platform = Platform.SIGNAL

    client_constructions = 0

    def forbidden_client():
        nonlocal client_constructions
        client_constructions += 1
        raise AssertionError("declaration-only adapter crossed admission")

    monkeypatch.setattr(facade, "PlanningV2Client", forbidden_client)
    with (
        scoped_turn_origin(_origin("signal")),
        scoped_turn_delivery_adapter(BrokenSignalAdapter()),
    ):
        raw = facade.invoke_public_tasking_tool(
            tool_name="agent_ops_task_plan",
            arguments={"intent": "new"},
            runtime_kwargs={"user_task": "Plan safely"},
        )

    result = json.loads(raw)
    assert result["code"] == "planning.bound_gateway_ineligible"
    assert result["reason"] == "bound_gateway_exact_delivery_unavailable"
    assert client_constructions == 0


def test_runtime_adapter_scope_is_nested_and_resets() -> None:
    outer = object()
    inner = object()

    assert get_current_turn_delivery_adapter() is None
    with scoped_turn_delivery_adapter(outer):
        assert get_current_turn_delivery_adapter() is outer
        with scoped_turn_delivery_adapter(inner):
            assert get_current_turn_delivery_adapter() is inner
        assert get_current_turn_delivery_adapter() is outer
    assert get_current_turn_delivery_adapter() is None
