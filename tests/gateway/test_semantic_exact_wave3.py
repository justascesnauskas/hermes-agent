"""One-write semantic delivery proofs for QQ, Weixin, Yuanbao, and DingTalk."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig
from gateway.platform_registry import PlatformEntry, platform_registry
from gateway.semantic_exact_attempt import (
    LiveSemanticExactAttemptRequest,
    coerce_live_semantic_exact_attempt_provider_route,
    live_semantic_exact_attempt_encoding_contract,
    owns_live_semantic_exact_attempt,
    send_via_exact_adapter_method,
)
from hermes_cli.semantic_delivery import (
    SEMANTIC_DELIVERY_CONTRACT,
    dispatch_due_semantic_delivery_retries,
    semantic_delivery_retry_status,
    stage_semantic_delivery_retry,
)

ROOT = Path(__file__).resolve().parents[2]
PROCESS_HARNESS = Path(__file__).with_name(
    "process_harness_wave3_dingtalk.py"
)


def _request(
    adapter,
    *,
    chat_id: str,
    content: str,
    route: dict[str, str],
    reply_to: str | None = None,
) -> LiveSemanticExactAttemptRequest:
    provider = adapter.platform.value
    return LiveSemanticExactAttemptRequest(
        chat_id=chat_id,
        content=content,
        delivery_contract=SEMANTIC_DELIVERY_CONTRACT,
        delivery_id=f"{provider}-delivery-1",
        delivery_target=f"{provider}:account-1",
        delivery_unit=0,
        encoding_contract=live_semantic_exact_attempt_encoding_contract(
            adapter
        ),
        provider_route=coerce_live_semantic_exact_attempt_provider_route(
            route
        ),
        reply_to=reply_to,
    )


def test_wave3_adapters_own_concrete_exact_attempts() -> None:
    from gateway.platforms.qqbot import QQAdapter
    from gateway.platforms.weixin import WeixinAdapter
    from gateway.platforms.yuanbao import YuanbaoAdapter
    from plugins.platforms.dingtalk.adapter import DingTalkAdapter

    adapters = (
        QQAdapter(
            PlatformConfig(
                enabled=True,
                extra={"app_id": "qq-app", "client_secret": "qq-secret"},
            )
        ),
        WeixinAdapter(
            PlatformConfig(
                enabled=True,
                token="weixin-token",
                extra={"account_id": "weixin-account"},
            )
        ),
        YuanbaoAdapter(
            PlatformConfig(
                enabled=True,
                extra={
                    "app_id": "yuanbao-app",
                    "app_secret": "yuanbao-secret",
                    "bot_id": "yuanbao-bot",
                },
            )
        ),
        DingTalkAdapter(
            PlatformConfig(
                enabled=True,
                extra={
                    "client_id": "dt-app",
                    "client_secret": "dt-secret",
                },
            )
        ),
    )
    for adapter in adapters:
        assert owns_live_semantic_exact_attempt(adapter) is True
        assert (
            vars(type(adapter)).get("send_semantic_exact_attempt")
            is not None
        )


@pytest.mark.asyncio
async def test_qq_exact_group_route_makes_one_frozen_rest_write() -> None:
    from gateway.platforms.qqbot import QQAdapter
    from gateway.platforms.qqbot.constants import (
        API_BASE,
        MSG_TYPE_MARKDOWN,
    )

    adapter = QQAdapter(
        PlatformConfig(
            enabled=True,
            extra={"app_id": "qq-app", "client_secret": "qq-secret"},
        )
    )
    adapter._chat_type_map["group-open-id"] = "group"
    adapter._access_token = "cached-access-token"
    adapter._token_expires_at = time.time() + 3_600
    response = MagicMock(
        status_code=200,
        headers={},
    )
    response.json.return_value = {"id": "qq-message-123"}
    adapter._http_client = SimpleNamespace(
        request=AsyncMock(return_value=response)
    )
    adapter._ensure_token = AsyncMock(
        side_effect=AssertionError("exact attempt must not refresh credentials")
    )
    route = adapter.bind_semantic_exact_attempt_provider_route(
        chat_id="group-open-id"
    )

    result = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="group-open-id",
            content="**Release approved**",
            route=route,
            reply_to="inbound-message-7",
        ),
    )

    assert result.success is True
    assert result.message_id == "qq-message-123"
    adapter._ensure_token.assert_not_awaited()
    adapter._http_client.request.assert_awaited_once()
    call = adapter._http_client.request.await_args
    assert call.args[:2] == (
        "POST",
        f"{API_BASE}/v2/groups/group-open-id/messages",
    )
    assert call.kwargs["follow_redirects"] is False
    assert call.kwargs["json"]["markdown"]["content"] == (
        "**Release approved**"
    )
    assert call.kwargs["json"]["msg_type"] == MSG_TYPE_MARKDOWN
    assert call.kwargs["json"]["msg_id"] == "inbound-message-7"


@pytest.mark.parametrize(
    ("receipt_id", "accepted"),
    [
        ("  qq-" + ("訊" * 2_048) + "-receipt  ", True),
        (7, False),
        ("qq\tcontrol", False),
        ("\ud800", False),
    ],
)
@pytest.mark.asyncio
async def test_qq_receipt_id_preserves_exact_safe_unicode_and_rejects_invalid(
    receipt_id,
    accepted: bool,
) -> None:
    from gateway.platforms.qqbot import QQAdapter

    adapter = QQAdapter(
        PlatformConfig(
            enabled=True,
            extra={"app_id": "qq-app", "client_secret": "qq-secret"},
        )
    )
    adapter._chat_type_map["group-open-id"] = "group"
    adapter._access_token = "cached-access-token"
    adapter._token_expires_at = time.time() + 3_600
    response = MagicMock(
        status_code=200,
        headers={},
        content=json.dumps(
            {"id": receipt_id},
            ensure_ascii=True,
        ).encode("utf-8"),
    )
    response.json.return_value = {"id": receipt_id}
    adapter._http_client = SimpleNamespace(
        request=AsyncMock(return_value=response)
    )
    route = adapter.bind_semantic_exact_attempt_provider_route(
        chat_id="group-open-id"
    )

    result = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="group-open-id",
            content="Exact receipt boundary",
            route=route,
        ),
    )

    assert result.success is accepted
    if accepted:
        assert result.message_id == receipt_id
    else:
        rejection = result.raw_response["provider_rejection"]
        assert rejection["provider"] == "QQ Bot"
        assert rejection["status"] == 200
        assert rejection["body_sha256"] in result.error
    adapter._http_client.request.assert_awaited_once()


@pytest.mark.asyncio
async def test_qq_malformed_success_preserves_redacted_body_evidence() -> None:
    from gateway.platforms.qqbot import QQAdapter

    adapter = QQAdapter(
        PlatformConfig(
            enabled=True,
            extra={"app_id": "qq-app", "client_secret": "qq-secret"},
        )
    )
    adapter._chat_type_map["group-open-id"] = "group"
    adapter._access_token = "cached-access-token"
    adapter._token_expires_at = time.time() + 3_600
    secret = "super-secret-provider-token-" + ("x" * 5_000)
    payload = {"id": 7, "access_token": secret}
    response = MagicMock(
        status_code=200,
        headers={},
        content=json.dumps(payload).encode("utf-8"),
    )
    response.json.return_value = payload
    adapter._http_client = SimpleNamespace(
        request=AsyncMock(return_value=response)
    )
    route = adapter.bind_semantic_exact_attempt_provider_route(
        chat_id="group-open-id"
    )

    result = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="group-open-id",
            content="Exact preview",
            route=route,
        ),
    )

    assert result.success is False
    rejection = result.raw_response["provider_rejection"]
    assert rejection["provider"] == "QQ Bot"
    assert rejection["status"] == 200
    assert rejection["body_sha256"] in result.error
    assert rejection["truncated"] is True
    assert rejection["redacted"] is True
    assert secret not in rejection["body_preview"]
    assert secret not in result.error
    adapter._http_client.request.assert_awaited_once()


@pytest.mark.asyncio
async def test_qq_oversize_and_wrong_route_are_zero_write() -> None:
    from gateway.platforms.qqbot import QQAdapter

    adapter = QQAdapter(
        PlatformConfig(
            enabled=True,
            extra={"app_id": "qq-app", "client_secret": "qq-secret"},
        )
    )
    adapter._access_token = "cached-access-token"
    adapter._token_expires_at = time.time() + 3_600
    adapter._http_client = SimpleNamespace(request=AsyncMock())
    request = _request(
        adapter,
        chat_id="group-open-id",
        content="x" * 4_001,
        route={"chat_type": "c2c", "message_mode": "markdown"},
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.raw_response["provider_write_attempted"] is False
    adapter._http_client.request.assert_not_awaited()


class _AiohttpResponse:
    def __init__(
        self,
        *,
        status: int,
        body: dict | str,
        headers: dict[str, str] | None = None,
    ):
        self.status = status
        self.headers = headers or {}
        self._body = (
            json.dumps(body, ensure_ascii=False)
            if isinstance(body, dict)
            else body
        )

    async def text(self) -> str:
        return self._body


class _AiohttpContext:
    def __init__(self, response: _AiohttpResponse):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _AiohttpSession:
    def __init__(self, response: _AiohttpResponse):
        self.closed = False
        self.response = response
        self.calls: list[tuple[str, dict]] = []

    def post(self, url: str, **kwargs):
        self.calls.append((url, kwargs))
        return _AiohttpContext(self.response)


@pytest.mark.asyncio
async def test_weixin_exact_send_preserves_text_and_stable_client_id() -> None:
    from gateway.platforms.weixin import EP_SEND_MESSAGE, WeixinAdapter

    adapter = WeixinAdapter(
        PlatformConfig(
            enabled=True,
            token="weixin-token",
            extra={"account_id": "weixin-account"},
        )
    )
    session = _AiohttpSession(
        _AiohttpResponse(status=200, body={"ret": 0, "errcode": 0})
    )
    adapter._send_session = session
    adapter._token_store.get = MagicMock(return_value="live-context-token")
    route = adapter.bind_semantic_exact_attempt_provider_route(
        chat_id="weixin-user-1"
    )
    request = _request(
        adapter,
        chat_id="weixin-user-1",
        content="Deployment\n\nis ready.",
        route=route,
    )

    first = await send_via_exact_adapter_method(adapter, request)

    assert first.success is True
    assert len(session.calls) == 1
    url, kwargs = session.calls[0]
    assert url.endswith(f"/{EP_SEND_MESSAGE}")
    assert kwargs["allow_redirects"] is False
    body = json.loads(kwargs["data"])
    assert body["msg"]["to_user_id"] == "weixin-user-1"
    assert body["msg"]["context_token"] == "live-context-token"
    assert body["msg"]["item_list"][0]["text_item"]["text"] == (
        "Deployment\n\nis ready."
    )
    assert body["msg"]["client_id"] == first.message_id


@pytest.mark.asyncio
async def test_weixin_malformed_success_preserves_redacted_body_evidence() -> None:
    from gateway.platforms.weixin import WeixinAdapter

    adapter = WeixinAdapter(
        PlatformConfig(
            enabled=True,
            token="weixin-token",
            extra={"account_id": "weixin-account"},
        )
    )
    secret = "super-secret-provider-token-" + ("x" * 5_000)
    session = _AiohttpSession(
        _AiohttpResponse(
            status=200,
            body=f"Authorization: Bearer {secret}",
        )
    )
    adapter._send_session = session
    request = _request(
        adapter,
        chat_id="weixin-user-1",
        content="Exact preview",
        route={"transport": "ilink_text"},
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    rejection = result.raw_response["provider_rejection"]
    assert rejection["provider"] == "Weixin"
    assert rejection["status"] == 200
    assert rejection["body_sha256"] in result.error
    assert rejection["truncated"] is True
    assert rejection["redacted"] is True
    assert secret not in rejection["body_preview"]
    assert secret not in result.error
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_weixin_429_is_proven_prewrite_and_never_retries_inside_adapter() -> None:
    from gateway.platforms.weixin import WeixinAdapter

    adapter = WeixinAdapter(
        PlatformConfig(
            enabled=True,
            token="weixin-token",
            extra={"account_id": "weixin-account"},
        )
    )
    session = _AiohttpSession(
        _AiohttpResponse(
            status=429,
            body={"error": "rate limited"},
            headers={"Retry-After": "9"},
        )
    )
    adapter._send_session = session
    request = _request(
        adapter,
        chat_id="weixin-user-1",
        content="Retry later",
        route={"transport": "ilink_text"},
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.retryable is True
    assert result.retry_after == 9
    assert result.raw_response["provider_write_attempted"] is False
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_yuanbao_exact_group_send_is_one_text_protobuf_and_exact_ack() -> None:
    from gateway.platforms.yuanbao import YuanbaoAdapter
    from gateway.platforms.yuanbao_proto import decode_conn_msg

    adapter = YuanbaoAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "app_id": "yuanbao-app",
                "app_secret": "yuanbao-secret",
                "bot_id": "yuanbao-bot",
            },
        )
    )
    adapter._connection._ws = object()
    captured: list[tuple[bytes, str]] = []

    async def _send(encoded: bytes, req_id: str):
        captured.append((encoded, req_id))
        return {
            "head": {
                "status": 0,
                "msg_id": req_id,
                "cmd": "send_group_message",
            }
        }

    adapter._connection.send_biz_request = AsyncMock(side_effect=_send)
    request = _request(
        adapter,
        chat_id="group:engineering",
        content="Build 214 is green ✓",
        route={"route_kind": "group"},
        reply_to="yuanbao-inbound-9",
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    adapter._connection.send_biz_request.assert_awaited_once()
    encoded, req_id = captured[0]
    decoded = decode_conn_msg(encoded)
    assert decoded["head"]["cmd"] == "send_group_message"
    assert decoded["head"]["msg_id"] == req_id == result.message_id
    assert "Build 214 is green ✓".encode("utf-8") in encoded


@pytest.mark.asyncio
async def test_yuanbao_rejection_and_oversize_never_hide_an_internal_retry() -> None:
    from gateway.platforms.yuanbao import YuanbaoAdapter

    adapter = YuanbaoAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "app_id": "yuanbao-app",
                "app_secret": "yuanbao-secret",
                "bot_id": "yuanbao-bot",
            },
        )
    )
    adapter._connection._ws = object()
    adapter._connection.send_biz_request = AsyncMock(
        return_value={"head": {"status": 403, "msg_id": "rejected"}}
    )
    rejected = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="direct:user-1",
            content="One attempt",
            route={"route_kind": "direct"},
        ),
    )
    oversized = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="direct:user-1",
            content="x" * 4_001,
            route={"route_kind": "direct"},
        ),
    )

    assert rejected.raw_response["provider_write_attempted"] is False
    rejection = rejected.raw_response["provider_rejection"]
    assert rejection["schema_version"] == (
        "hermes.provider-protocol-rejection-evidence/1"
    )
    assert rejection["provider"] == "Yuanbao"
    assert rejection["protocol"] == "yuanbao-tim-websocket"
    assert '"msg_id":"rejected"' in rejection["response_preview"]
    assert rejection["response_sha256"] in rejected.error
    assert oversized.raw_response["provider_write_attempted"] is False
    assert adapter._connection.send_biz_request.await_count == 1


@pytest.mark.asyncio
async def test_yuanbao_rejects_a_space_mutated_deterministic_receipt() -> None:
    from gateway.platforms.yuanbao import YuanbaoAdapter

    adapter = YuanbaoAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "app_id": "yuanbao-app",
                "app_secret": "yuanbao-secret",
                "bot_id": "yuanbao-bot",
            },
        )
    )
    adapter._connection._ws = object()

    async def _space_mutated_receipt(_encoded: bytes, req_id: str):
        return {
            "head": {
                "status": 0,
                "msg_id": f" {req_id} ",
                "cmd": "send_direct_message",
            }
        }

    adapter._connection.send_biz_request = AsyncMock(
        side_effect=_space_mutated_receipt
    )

    result = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="direct:user-1",
            content="Deterministic receipt must match byte-for-byte",
            route={"route_kind": "direct"},
        ),
    )

    assert result.success is False
    rejection = result.raw_response["provider_rejection"]
    assert rejection["provider"] == "Yuanbao"
    assert rejection["protocol"] == "yuanbao-tim-websocket"
    assert rejection["response_sha256"] in result.error
    adapter._connection.send_biz_request.assert_awaited_once()


@pytest.mark.asyncio
async def test_dingtalk_exact_send_never_uses_cards_reactions_or_redirects() -> None:
    from plugins.platforms.dingtalk.adapter import DingTalkAdapter

    adapter = DingTalkAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "client_id": "dt-app",
                "client_secret": "dt-secret",
                "card_template_id": "card-template",
            },
        )
    )
    webhook = (
        "https://api.dingtalk.com/v1.0/robot/oToMessages/"
        "batchSend?processQueryKey=secret-session-value"
    )
    adapter._session_webhooks["chat-dt-1"] = (webhook, 0)
    response = MagicMock(status_code=200, headers={})
    response.json.return_value = {"errcode": 0, "errmsg": "ok"}
    adapter._http_client = SimpleNamespace(
        post=AsyncMock(return_value=response)
    )
    adapter._create_and_stream_card = AsyncMock(
        side_effect=AssertionError("cards are forbidden")
    )
    adapter._fire_done_reaction = MagicMock(
        side_effect=AssertionError("reactions are forbidden")
    )
    route = adapter.bind_semantic_exact_attempt_provider_route(
        chat_id="chat-dt-1"
    )

    result = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="chat-dt-1",
            content="**Plan ready**",
            route=route,
        ),
    )

    assert result.success is True
    adapter._http_client.post.assert_awaited_once()
    call = adapter._http_client.post.await_args
    assert call.args[0] == webhook
    assert call.kwargs["follow_redirects"] is False
    assert call.kwargs["json"]["markdown"]["text"] == "**Plan ready**"
    adapter._create_and_stream_card.assert_not_awaited()
    adapter._fire_done_reaction.assert_not_called()


@pytest.mark.asyncio
async def test_dingtalk_rejection_keeps_redacted_digest_evidence() -> None:
    from plugins.platforms.dingtalk.adapter import DingTalkAdapter

    adapter = DingTalkAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "client_id": "dt-app",
                "client_secret": "dt-secret",
            },
        )
    )
    webhook = (
        "https://api.dingtalk.com/v1.0/robot/oToMessages/"
        "batchSend?processQueryKey=secret-session-value"
    )
    adapter._session_webhooks["chat-dt-1"] = (webhook, 0)
    secret = "ghp_" + ("d" * 80)
    body = (
        '{"token":"'
        + secret
        + '","detail":"'
        + ("provider rejection " * 40)
        + '"}'
    )
    response = MagicMock(status_code=503, headers={}, text=body)
    response.json.side_effect = ValueError("not JSON")
    adapter._http_client = SimpleNamespace(
        post=AsyncMock(return_value=response)
    )
    route = adapter.bind_semantic_exact_attempt_provider_route(
        chat_id="chat-dt-1"
    )

    result = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="chat-dt-1",
            content="**Plan ready**",
            route=route,
        ),
    )

    rejection = result.raw_response["provider_rejection"]
    assert result.success is False
    assert result.retryable is False
    assert rejection["body_bytes"] == len(body.encode("utf-8"))
    assert rejection["truncated"] is True
    assert rejection["redacted"] is True
    assert secret not in rejection["body_preview"]
    assert rejection["body_sha256"] in result.error
    adapter._http_client.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_dingtalk_durable_route_never_persists_webhook_and_rebinds(
    tmp_path: Path,
) -> None:
    from plugins.platforms.dingtalk.adapter import DingTalkAdapter

    prior_entry = platform_registry.get("dingtalk")
    platform_registry.register(
        PlatformEntry(
            name="dingtalk",
            label="DingTalk",
            adapter_factory=lambda config: None,
            check_fn=lambda: True,
            semantic_exact_attempt=False,
            live_semantic_exact_attempt=True,
        )
    )
    ledger = tmp_path / "semantic.sqlite3"
    account = "dingtalk-account-1"
    webhook = (
        "https://api.dingtalk.com/v1.0/robot/oToMessages/"
        "batchSend?processQueryKey=must-never-enter-sqlite"
    )
    adapter = DingTalkAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "client_id": "dt-app",
                "client_secret": "dt-secret",
                "gateway_account_id": account,
            },
        )
    )
    adapter._session_webhooks["chat-dt-1"] = (webhook, 0)
    adapter._http_client = SimpleNamespace(post=AsyncMock())
    try:
        staged = stage_semantic_delivery_retry(
            delivery_id="dingtalk-rebind-1",
            contract_version=SEMANTIC_DELIVERY_CONTRACT,
            target="dingtalk:chat-dt-1",
            message="Durable preview",
            gateway_account_id=account,
            adapter=adapter,
            encoding_contract=(
                live_semantic_exact_attempt_encoding_contract(adapter)
            ),
            chat_id="chat-dt-1",
            initial_delay_seconds=0,
            ledger_path=ledger,
        )
        assert staged["provider_route"] == {
            "transport": "session_webhook"
        }
        with sqlite3.connect(ledger) as connection:
            route_json = connection.execute(
                "SELECT route_json FROM semantic_delivery_retry_outbox "
                "WHERE delivery_id='dingtalk-rebind-1'"
            ).fetchone()[0]
        assert "must-never-enter-sqlite" not in route_json
        assert webhook not in ledger.read_bytes().decode(
            "utf-8",
            errors="ignore",
        )

        # Simulate a process restart: the durable discriminator remains, but
        # the expiring secret webhook is absent. This is a safe pre-write
        # retry, not an ambiguous write and not a transport fallback.
        adapter._session_webhooks.clear()
        first = await dispatch_due_semantic_delivery_retries(
            bound_adapters={"dingtalk": adapter},
            ledger_path=ledger,
        )
        assert first["retryable"] == 1
        adapter._http_client.post.assert_not_awaited()

        # A later inbound event rebinds the same non-secret route to a fresh
        # session webhook; the original outbox row then resumes automatically.
        adapter._session_webhooks["chat-dt-1"] = (webhook, 0)
        response = MagicMock(status_code=200, headers={})
        response.json.return_value = {"errcode": 0, "errmsg": "ok"}
        adapter._http_client.post = AsyncMock(return_value=response)
        second = await dispatch_due_semantic_delivery_retries(
            bound_adapters={"dingtalk": adapter},
            ledger_path=ledger,
            due_before=time.time() + 600,
        )
        assert second["delivered"] == 1
        assert semantic_delivery_retry_status(
            delivery_id="dingtalk-rebind-1",
            ledger_path=ledger,
        )["state"] == "retired"
        adapter._http_client.post.assert_awaited_once()
    finally:
        if prior_entry is None:
            platform_registry.unregister("dingtalk")
        else:
            platform_registry.register(prior_entry)


def test_provider_route_rejects_secret_bearing_fields() -> None:
    with pytest.raises(ValueError):
        coerce_live_semantic_exact_attempt_provider_route(
            {"webhook_url": "https://api.dingtalk.com/secret"}
        )
    with pytest.raises(ValueError):
        coerce_live_semantic_exact_attempt_provider_route(
            {"transport": "https://api.dingtalk.com/secret"}
        )


def test_dingtalk_route_rebind_survives_real_process_restart(
    tmp_path: Path,
) -> None:
    from plugins.platforms.dingtalk.adapter import DingTalkAdapter

    prior_entry = platform_registry.get("dingtalk")
    platform_registry.register(
        PlatformEntry(
            name="dingtalk",
            label="DingTalk",
            adapter_factory=lambda config: None,
            check_fn=lambda: True,
            semantic_exact_attempt=False,
            live_semantic_exact_attempt=True,
        )
    )
    ledger = tmp_path / "semantic-process.sqlite3"
    account = "dingtalk-process-account"
    webhook = (
        "https://api.dingtalk.com/v1.0/robot/oToMessages/"
        "batchSend?processQueryKey=fresh-process-only-secret"
    )
    adapter = DingTalkAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "client_id": "dt-process-app",
                "client_secret": "dt-process-secret",
                "gateway_account_id": account,
            },
        )
    )
    adapter._session_webhooks["chat-dt-process"] = (webhook, 0)
    try:
        stage_semantic_delivery_retry(
            delivery_id="dingtalk-process-rebind-1",
            contract_version=SEMANTIC_DELIVERY_CONTRACT,
            target="dingtalk:chat-dt-process",
            message="Survive a real process restart",
            gateway_account_id=account,
            adapter=adapter,
            encoding_contract=(
                live_semantic_exact_attempt_encoding_contract(adapter)
            ),
            chat_id="chat-dt-process",
            initial_delay_seconds=0,
            ledger_path=ledger,
        )
    finally:
        if prior_entry is None:
            platform_registry.unregister("dingtalk")
        else:
            platform_registry.register(prior_entry)

    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            str(ROOT),
            environment.get("PYTHONPATH", ""),
        )
        if value
    )

    def _child(mode: str) -> dict:
        completed = subprocess.run(
            [
                sys.executable,
                str(PROCESS_HARNESS),
                str(ledger),
                account,
                mode,
                webhook,
            ],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        assert completed.returncode == 0, {
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
        return json.loads(completed.stdout.strip())

    missing = _child("missing")
    assert missing["retryable"] == 1
    assert missing["provider_calls"] == 0
    rebound = _child("rebind")
    assert rebound["delivered"] == 1
    assert rebound["provider_calls"] == 1
    assert webhook not in ledger.read_bytes().decode(
        "utf-8",
        errors="ignore",
    )
    assert semantic_delivery_retry_status(
        delivery_id="dingtalk-process-rebind-1",
        ledger_path=ledger,
    )["state"] == "retired"
