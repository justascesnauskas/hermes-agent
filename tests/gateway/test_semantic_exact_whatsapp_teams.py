"""Cross-boundary exact delivery proofs for Baileys WhatsApp and Teams."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from gateway.config import Platform
from gateway.semantic_exact_attempt import (
    LiveSemanticExactAttemptRequest,
    coerce_live_semantic_exact_attempt_provider_route,
    live_semantic_exact_attempt_encoding_contract,
    owns_live_semantic_exact_attempt,
    send_via_exact_adapter_method,
)
from hermes_cli.semantic_delivery import SEMANTIC_DELIVERY_CONTRACT
from plugins.platforms.teams.adapter import (
    TeamsAdapter,
    register as register_teams,
)
from plugins.platforms.whatsapp.adapter import (
    WhatsAppAdapter,
    _whatsapp_semantic_exact_message_id,
    register as register_whatsapp,
)


ROOT = Path(__file__).resolve().parents[2]


class _AioResponse:
    def __init__(self, status: int, payload=None):
        self.status = status
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self.payload


class _AioSession:
    def __init__(self, response):
        self.closed = False
        self.response = response
        self.post = MagicMock(side_effect=self._post)

    def _post(self, *args, **kwargs):
        return self.response


def _request(
    adapter,
    *,
    chat_id: str,
    content: str = "Frozen Planning preview",
    route: dict[str, str],
    thread_id: str | None = None,
    reply_to: str | None = None,
) -> LiveSemanticExactAttemptRequest:
    provider = adapter.SEMANTIC_EXACT_ATTEMPT_CAPABILITY.provider
    return LiveSemanticExactAttemptRequest(
        chat_id=chat_id,
        content=content,
        delivery_contract=SEMANTIC_DELIVERY_CONTRACT,
        delivery_id=f"{provider}-delivery-1",
        delivery_target=f"{provider}:account-1",
        delivery_unit=0,
        encoding_contract=live_semantic_exact_attempt_encoding_contract(adapter),
        provider_route=coerce_live_semantic_exact_attempt_provider_route(route),
        thread_id=thread_id,
        reply_to=reply_to,
    )


def _whatsapp_adapter(response) -> WhatsAppAdapter:
    adapter = object.__new__(WhatsAppAdapter)
    adapter.platform = Platform.WHATSAPP
    adapter._running = True
    adapter._bridge_port = 3000
    adapter._http_session = _AioSession(response)
    return adapter


def _teams_adapter(app) -> TeamsAdapter:
    adapter = object.__new__(TeamsAdapter)
    adapter.platform = SimpleNamespace(value="teams")
    adapter._app = app
    return adapter


def _teams_app(*, response=None, error: Exception | None = None):
    if response is None:
        response = SimpleNamespace(
            json=lambda: {"id": "teams-activity-001"},
        )
    post = AsyncMock(
        side_effect=error,
        return_value=response,
    )
    return SimpleNamespace(
        activity_sender=SimpleNamespace(
            _client=SimpleNamespace(post=post),
        ),
        api=SimpleNamespace(
            service_url="https://smba.trafficmanager.net/teams",
        ),
    )


def test_cross_boundary_adapters_own_concrete_exact_methods() -> None:
    whatsapp = _whatsapp_adapter(_AioResponse(200, {}))
    teams = _teams_adapter(_teams_app())
    assert owns_live_semantic_exact_attempt(whatsapp) is True
    assert owns_live_semantic_exact_attempt(teams) is True


def test_cross_boundary_plugin_declarations_enable_only_live_exact() -> None:
    whatsapp_ctx = SimpleNamespace(
        register_platform=MagicMock(),
    )
    teams_ctx = SimpleNamespace(
        register_platform=MagicMock(),
    )
    register_whatsapp(whatsapp_ctx)
    register_teams(teams_ctx)
    for context in (whatsapp_ctx, teams_ctx):
        declaration = context.register_platform.call_args.kwargs
        assert declaration["semantic_exact_attempt"] is False
        assert declaration["live_semantic_exact_attempt"] is True


def test_whatsapp_python_and_bridge_share_the_frozen_message_key() -> None:
    assert (
        _whatsapp_semantic_exact_message_id(
            chat_id="15551234567@s.whatsapp.net",
            content=("Frozen planning preview\nhttps://example.test/review"),
            delivery_id="preview_delivery_01",
        )
        == "3EB0511011225520AE1D6A"
    )


def test_cross_boundary_route_binders_freeze_transport_and_mode() -> None:
    whatsapp = _whatsapp_adapter(_AioResponse(200, {}))
    assert whatsapp.bind_semantic_exact_attempt_provider_route(
        chat_id="+1 (555) 123-4567",
        reply_to="ignored-for-flat-exact",
    ) == {
        "message_mode": "flat",
        "transport": "baileys_send_exact",
    }
    teams = _teams_adapter(_teams_app())
    assert teams.bind_semantic_exact_attempt_provider_route(
        chat_id="19:conversation_01@thread.tacv2",
        reply_to="1740000000000",
    ) == {
        "message_mode": "thread_reply",
        "transport": "activity_sender",
    }


@pytest.mark.asyncio
async def test_whatsapp_exact_calls_one_frozen_bridge_endpoint() -> None:
    content = "No prefix, no split 🚀\nhttps://example.test/review"
    expected_id = _whatsapp_semantic_exact_message_id(
        chat_id="15551234567@s.whatsapp.net",
        content=content,
        delivery_id="whatsapp-delivery-1",
    )
    adapter = _whatsapp_adapter(
        _AioResponse(
            200,
            {"success": True, "messageId": expected_id},
        )
    )
    request = _request(
        adapter,
        chat_id="+1 (555) 123-4567",
        content=content,
        route={
            "message_mode": "flat",
            "transport": "baileys_send_exact",
        },
        reply_to="quoted-message-is-intentionally-not-used",
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id == expected_id
    adapter._http_session.post.assert_called_once()
    call = adapter._http_session.post.call_args
    assert call.args[0] == "http://127.0.0.1:3000/send-exact"
    assert call.kwargs["allow_redirects"] is False
    assert call.kwargs["json"] == {
        "chatId": "15551234567@s.whatsapp.net",
        "deliveryId": "whatsapp-delivery-1",
        "encodingContract": "whatsapp-baileys-text-json-v1",
        "message": content,
    }
    assert "replyTo" not in call.kwargs["json"]


@pytest.mark.asyncio
async def test_whatsapp_rejects_space_mutated_deterministic_bridge_receipt() -> None:
    content = "Deterministic bridge receipt"
    expected_id = _whatsapp_semantic_exact_message_id(
        chat_id="15551234567@s.whatsapp.net",
        content=content,
        delivery_id="whatsapp-delivery-1",
    )
    adapter = _whatsapp_adapter(
        _AioResponse(
            200,
            {
                "success": True,
                "messageId": f" {expected_id} ",
            },
        )
    )

    result = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="+1 (555) 123-4567",
            content=content,
            route={
                "message_mode": "flat",
                "transport": "baileys_send_exact",
            },
        ),
    )

    assert result.success is False
    rejection = result.raw_response["provider_rejection"]
    assert rejection["provider"] == "WhatsApp bridge"
    assert rejection["status"] == 200
    assert rejection["body_sha256"] in result.error
    adapter._http_session.post.assert_called_once()


@pytest.mark.asyncio
async def test_whatsapp_exact_oversize_rejects_before_bridge() -> None:
    adapter = _whatsapp_adapter(_AioResponse(200, {}))
    result = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="15551234567@s.whatsapp.net",
            content="🚀" * 2049,
            route={
                "message_mode": "flat",
                "transport": "baileys_send_exact",
            },
        ),
    )

    assert result.success is False
    assert result.raw_response["provider_write_attempted"] is False
    adapter._http_session.post.assert_not_called()


@pytest.mark.asyncio
async def test_whatsapp_bridge_zero_write_and_ambiguous_receipt_are_distinct() -> None:
    unavailable = _whatsapp_adapter(
        _AioResponse(
            503,
            {
                "error": "Not connected to WhatsApp",
                "providerWriteAttempted": False,
                "retryable": True,
            },
        )
    )
    request = _request(
        unavailable,
        chat_id="15551234567@s.whatsapp.net",
        route={
            "message_mode": "flat",
            "transport": "baileys_send_exact",
        },
    )
    zero_write = await send_via_exact_adapter_method(
        unavailable,
        request,
    )
    assert zero_write.success is False
    assert zero_write.retryable is True
    assert zero_write.raw_response["provider_write_attempted"] is False
    unavailable._http_session.post.assert_called_once()

    invalid_receipt = _whatsapp_adapter(
        _AioResponse(
            200,
            {"success": True, "messageId": "wrong-key"},
        )
    )
    ambiguous = await send_via_exact_adapter_method(
        invalid_receipt,
        _request(
            invalid_receipt,
            chat_id="15551234567@s.whatsapp.net",
            route={
                "message_mode": "flat",
                "transport": "baileys_send_exact",
            },
        ),
    )
    assert ambiguous.success is False
    assert not (
        ambiguous.raw_response
        and ambiguous.raw_response.get("provider_write_attempted") is False
    )
    invalid_receipt._http_session.post.assert_called_once()


def test_whatsapp_pinned_bridge_boundary_suite() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for the WhatsApp bridge")
    test_file = ROOT / "scripts" / "whatsapp-bridge" / "semantic_exact.test.mjs"
    completed = subprocess.run(
        [node, "--test", str(test_file)],
        cwd=test_file.parent,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr


@pytest.mark.asyncio
async def test_teams_exact_flat_route_uses_one_activity_sender_write() -> None:
    app = _teams_app()
    adapter = _teams_adapter(app)
    request = _request(
        adapter,
        chat_id="19:conversation_01@thread.tacv2",
        content="One exact Teams activity",
        route={
            "message_mode": "conversation",
            "transport": "activity_sender",
        },
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id == "teams-activity-001"
    app.activity_sender._client.post.assert_awaited_once_with(
        "https://smba.trafficmanager.net/teams/v3/conversations/"
        "19%3Aconversation_01%40thread.tacv2/activities",
        json={
            "type": "message",
            "text": "One exact Teams activity",
            "textFormat": "markdown",
        },
        follow_redirects=False,
    )


@pytest.mark.parametrize(
    ("receipt_id", "accepted"),
    [
        ("  teams-" + ("訊" * 2_048) + "-receipt  ", True),
        (7, False),
        ("teams\tcontrol", False),
        ("\ud800", False),
    ],
)
@pytest.mark.asyncio
async def test_teams_receipt_preserves_safe_unicode_and_rejects_invalid(
    receipt_id,
    accepted: bool,
) -> None:
    app = _teams_app(
        response=SimpleNamespace(
            json=lambda: {"id": receipt_id},
        )
    )
    adapter = _teams_adapter(app)

    result = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="19:conversation_01@thread.tacv2",
            route={
                "message_mode": "conversation",
                "transport": "activity_sender",
            },
        ),
    )

    assert result.success is accepted
    if accepted:
        assert result.message_id == receipt_id
    else:
        rejection = result.raw_response["provider_rejection"]
        assert rejection["provider"] == "Teams"
        assert rejection["status"] == 200
        assert rejection["body_sha256"] in result.error
    app.activity_sender._client.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_teams_exact_reply_never_falls_back_to_flat_send() -> None:
    app = _teams_app(error=httpx.ReadTimeout("response lost"))
    adapter = _teams_adapter(app)
    result = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="19:conversation_01@thread.tacv2",
            route={
                "message_mode": "thread_reply",
                "transport": "activity_sender",
            },
            reply_to="1740000000000",
        ),
    )

    assert result.success is False
    app.activity_sender._client.post.assert_awaited_once_with(
        "https://smba.trafficmanager.net/teams/v3/conversations/"
        "19%3Aconversation_01%40thread.tacv2/activities/1740000000000",
        json={
            "type": "message",
            "text": "Frozen Planning preview",
            "textFormat": "markdown",
            "replyToId": "1740000000000",
        },
        follow_redirects=False,
    )
    assert not (
        result.raw_response
        and result.raw_response.get("provider_write_attempted") is False
    )


@pytest.mark.asyncio
async def test_teams_exact_rate_limit_is_proven_zero_write_retry() -> None:
    request = httpx.Request(
        "POST",
        "https://smba.trafficmanager.net/teams/v3/conversations/x/activities",
    )
    response = httpx.Response(
        429,
        request=request,
        headers={"Retry-After": "3"},
    )
    error = httpx.HTTPStatusError(
        "rate limited",
        request=request,
        response=response,
    )
    app = _teams_app(error=error)
    adapter = _teams_adapter(app)
    result = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="19:conversation_01@thread.tacv2",
            route={
                "message_mode": "conversation",
                "transport": "activity_sender",
            },
        ),
    )

    assert result.success is False
    assert result.retryable is True
    assert result.retry_after == 3
    assert result.raw_response["provider_write_attempted"] is False
    app.activity_sender._client.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_teams_placeholder_receipt_is_post_write_ambiguous() -> None:
    app = _teams_app(
        response=SimpleNamespace(
            json=lambda: {"id": "DO_NOT_USE_PLACEHOLDER_ID"},
        )
    )
    adapter = _teams_adapter(app)
    result = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="19:conversation_01@thread.tacv2",
            route={
                "message_mode": "conversation",
                "transport": "activity_sender",
            },
        ),
    )

    assert result.success is False
    app.activity_sender._client.post.assert_awaited_once()
    assert not (
        result.raw_response
        and result.raw_response.get("provider_write_attempted") is False
    )


@pytest.mark.asyncio
async def test_teams_oversize_rejects_before_sdk() -> None:
    app = _teams_app()
    adapter = _teams_adapter(app)
    result = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="19:conversation_01@thread.tacv2",
            content="x" * 28001,
            route={
                "message_mode": "conversation",
                "transport": "activity_sender",
            },
        ),
    )

    assert result.success is False
    assert result.raw_response["provider_write_attempted"] is False
    app.activity_sender._client.post.assert_not_awaited()
