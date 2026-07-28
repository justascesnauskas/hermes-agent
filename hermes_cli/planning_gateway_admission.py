"""Fail-closed Planning admission for the current live gateway adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from gateway.platform_registry import (
    planning_semantic_delivery_ineligibility,
    supports_live_semantic_exact_attempt,
)
from gateway.semantic_exact_attempt import owns_live_semantic_exact_attempt
from hermes_cli.turn_origin import (
    get_current_turn_delivery_adapter,
    get_current_turn_origin,
)


@dataclass(frozen=True, slots=True)
class PlanningGatewayAdmission:
    """One runtime verdict for the exact adapter bound to the current turn."""

    provider: str
    eligible: bool
    reason: str


def _provider_name(value: Any) -> str:
    return str(getattr(value, "value", value) or "").strip().lower()


def current_planning_gateway_admission() -> PlanningGatewayAdmission:
    """Verify provider declaration *and* concrete live adapter ownership.

    A declaration alone is insufficient: the adapter selected for this exact
    account/profile turn must own the audited one-attempt delivery primitive.
    There is deliberately no legacy sender or standalone-delivery fallback.
    """

    origin = get_current_turn_origin()
    if origin is None:
        return PlanningGatewayAdmission(
            provider="",
            eligible=False,
            reason="bound_turn_origin_unavailable",
        )

    provider = _provider_name(origin.provider)
    adapter = get_current_turn_delivery_adapter()
    if adapter is None:
        return PlanningGatewayAdmission(
            provider=provider,
            eligible=False,
            reason="bound_gateway_adapter_unavailable",
        )

    adapter_provider = _provider_name(getattr(adapter, "platform", None))
    if not provider or adapter_provider != provider:
        return PlanningGatewayAdmission(
            provider=provider,
            eligible=False,
            reason="bound_gateway_provider_mismatch",
        )

    explicit_ineligibility = planning_semantic_delivery_ineligibility(
        provider,
        adapter=adapter,
    )
    if explicit_ineligibility:
        return PlanningGatewayAdmission(
            provider=provider,
            eligible=False,
            reason=explicit_ineligibility,
        )

    owns_exact_attempt = owns_live_semantic_exact_attempt(adapter)
    runtime_supported = supports_live_semantic_exact_attempt(
        provider,
        adapter=adapter,
    )
    if not owns_exact_attempt or not runtime_supported:
        return PlanningGatewayAdmission(
            provider=provider,
            eligible=False,
            reason="bound_gateway_exact_delivery_unavailable",
        )

    return PlanningGatewayAdmission(
        provider=provider,
        eligible=True,
        reason="live_exact_delivery_conformant",
    )


__all__ = [
    "PlanningGatewayAdmission",
    "current_planning_gateway_admission",
]
