"""Planning preflight closure for indirect and response-plane adapters."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import MessageEvent
from gateway.platforms.api_server import APIServerAdapter
from gateway.platforms.msgraph_webhook import MSGraphWebhookAdapter
from gateway.platforms.webhook import WebhookAdapter
from gateway.relay.adapter import RelayAdapter
from gateway.session import SessionSource, build_session_key
from hermes_cli.planning_preview_delivery import (
    bind_preview_delivery_generation,
    claim_preview_delivery_for_source,
    discard_preview_delivery_intent,
    prepare_preview_delivery_content,
    register_preview_delivery_intent,
    reset_preview_delivery_generation,
)
from hermes_cli.semantic_delivery import delivery_ledger_path
from plugins.platforms.raft.adapter import RaftAdapter


def _canonical(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _register(session_key: str, generation: int) -> str:
    content = "Task 1: preserve the exact signed Planning preview."
    payload = {
        "schemaVersion": "planning.preview-delivery-payload.v1",
        "threadId": "response-plane-thread",
        "previewResultId": "response-plane-preview",
        "offset": 0,
        "count": 1,
        "tasks": [{"stableTaskId": "task-1"}],
    }
    token = bind_preview_delivery_generation(session_key, generation)
    try:
        assert register_preview_delivery_intent(
            thread_id="response-plane-thread",
            preview_result_id="response-plane-preview",
            preview_result_hash="sha256:" + "a" * 64,
            offset=0,
            count=1,
            page_digest="sha256:" + "b" * 64,
            delivery_payload=payload,
            delivery_payload_digest=(
                "sha256:"
                + hashlib.sha256(
                    _canonical(payload).encode("utf-8")
                ).hexdigest()
            ),
            delivery_content=content,
            delivery_content_digest=(
                "sha256:"
                + hashlib.sha256(content.encode("utf-8")).hexdigest()
            ),
            delivery_nonce="response-plane-preview-stable-nonce",
            acknowledge=lambda _receipt: None,
            allow_process_local_ack=True,
        )
    finally:
        reset_preview_delivery_generation(token)
    return prepare_preview_delivery_content(
        session_key,
        generation,
        "The signed preview follows.",
    )


def _uninitialized_adapter(adapter_type, platform):
    adapter = object.__new__(adapter_type)
    adapter.platform = platform
    adapter.config = PlatformConfig(
        enabled=True,
        extra={"gateway_account_id": f"{platform}-account"},
    )
    return adapter


_SURFACES = (
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
    ("provider", "adapter_type", "platform", "expected_reason"),
    _SURFACES,
)
def test_response_plane_fails_before_manifest_ledger_or_provider_mutation(
    monkeypatch,
    tmp_path,
    provider: str,
    adapter_type,
    platform,
    expected_reason: str,
) -> None:
    profile_home = tmp_path / provider
    monkeypatch.setenv("HERMES_HOME", str(profile_home))
    session_key = f"response-plane:{provider}"
    generation = 1
    content = _register(session_key, generation)
    adapter = _uninitialized_adapter(adapter_type, platform)
    source = SessionSource(
        platform=platform,
        chat_id=f"{provider}-target",
        gateway_account_id=f"{platform}-account",
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError(
            "Planning-ineligible surface crossed its preflight boundary"
        )

    async def forbidden_send(*_args, **_kwargs):
        forbidden()

    monkeypatch.setattr(
        "hermes_cli.planning_preview_delivery."
        "stage_preview_delivery_manifest",
        forbidden,
    )
    monkeypatch.setattr(
        "hermes_cli.planning_preview_delivery."
        "stage_semantic_delivery_retry",
        forbidden,
    )
    monkeypatch.setattr(
        "hermes_cli.planning_preview_delivery.begin_semantic_delivery",
        forbidden,
    )
    monkeypatch.setattr(adapter_type, "send", forbidden_send)

    claim = claim_preview_delivery_for_source(
        session_key,
        generation,
        delivered_content=content,
        source=source,
        adapter=adapter,
        turn_execution_ref="qturn_" + "c" * 64,
    )

    assert claim.action == "planning_ineligible"
    assert claim.metadata == {
        "planning_ineligible_reason": expected_reason,
    }
    assert not delivery_ledger_path().exists()
    discard_preview_delivery_intent(session_key, generation)


def test_runtime_enumeration_classifies_all_response_planes_as_safe_preflight(
) -> None:
    from gateway.platform_registry import (
        enumerate_live_semantic_exact_attempt_conformance,
    )

    adapters = {
        provider: _uninitialized_adapter(adapter_type, platform)
        for provider, adapter_type, platform, _reason in _SURFACES
    }
    rows = {
        row.provider: row
        for row in enumerate_live_semantic_exact_attempt_conformance(
            adapters
        )
    }

    for provider, _adapter_type, _platform, expected_reason in _SURFACES:
        row = rows[provider]
        assert row.bound is True
        assert row.outbound_send is True
        assert row.exact_attempt_method is False
        assert row.declaration is False
        assert row.supported is False
        assert row.conformant is True
        assert row.reason == "planning_ineligible"
        assert row.planning_ineligible_reason == expected_reason


@pytest.mark.asyncio
async def test_base_never_converts_planning_ineligible_into_generic_send(
    monkeypatch,
    tmp_path,
) -> None:
    profile_home = tmp_path / "base-response-plane"
    monkeypatch.setenv("HERMES_HOME", str(profile_home))
    adapter = MSGraphWebhookAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "host": "127.0.0.1",
                "client_state": "test-client-state",
            },
        )
    )
    event = MessageEvent(
        text="Create the signed plan.",
        source=SessionSource(
            platform=Platform.MSGRAPH_WEBHOOK,
            chat_id="subscription-1",
            gateway_account_id="msgraph-webhook-account",
        ),
        message_id="notification-1",
    )
    session_key = build_session_key(event.source)
    generation = 9
    send = AsyncMock(
        side_effect=AssertionError(
            "Planning-ineligible response plane must not send a fallback"
        )
    )
    monkeypatch.setattr(adapter, "send", send)

    async def handler(_event):
        _register(session_key, generation)
        return "The signed preview follows."

    adapter.set_message_handler(handler)
    active = asyncio.Event()
    setattr(active, "_hermes_run_generation", generation)
    adapter._active_sessions[session_key] = active

    await adapter._process_message_background(event, session_key)

    send.assert_not_awaited()
    assert not delivery_ledger_path().exists()


def test_ineligible_reason_cannot_mask_a_live_exact_declaration() -> None:
    from gateway.platform_registry import declare_semantic_exact_attempt

    with pytest.raises(
        ValueError,
        match="invalid semantic exact-attempt declaration",
    ):
        declare_semantic_exact_attempt(
            "lying-response-plane",
            standalone=False,
            live=True,
            owner=__name__,
            planning_ineligible_reason="must_not_mask_live_contract",
        )
