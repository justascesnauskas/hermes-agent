"""True provider-boundary tests for the second exact-attempt adapter wave."""

from __future__ import annotations

import asyncio
import smtplib
from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platform_registry import (
    supports_live_semantic_exact_attempt,
    supports_semantic_exact_attempt,
)
from gateway.platforms.bluebubbles import BlueBubblesAdapter
from gateway.platforms.whatsapp_cloud import WhatsAppCloudAdapter
from gateway.semantic_exact_attempt import (
    LiveSemanticExactAttemptRequest,
    live_semantic_exact_attempt_encoding_contract,
    owns_live_semantic_exact_attempt,
    send_via_exact_adapter_method,
)
from hermes_cli import semantic_delivery as semantic_delivery_module
from hermes_cli.semantic_delivery import SEMANTIC_DELIVERY_CONTRACT
from plugins.platforms.email.adapter import EmailAdapter
from plugins.platforms.email.adapter import register as register_email
from plugins.platforms.line.adapter import LineAdapter
from plugins.platforms.line.adapter import register as register_line
from plugins.platforms.wecom.adapter import WeComAdapter
from plugins.platforms.wecom.adapter import register as register_wecom


class _HttpxResponse:
    def __init__(
        self,
        status_code: int,
        *,
        payload=None,
        text: str = "",
        headers: dict | None = None,
        json_error: Exception | None = None,
    ):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers = headers or {}
        self._json_error = json_error

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._payload


class _AioResponse:
    def __init__(
        self,
        status: int,
        *,
        payload=None,
        text: str = "",
        headers: dict | None = None,
        json_error: Exception | None = None,
    ):
        self.status = status
        self._payload = payload
        self._text = text
        self.headers = headers or {}
        self._json_error = json_error

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self, *args, **kwargs):
        if self._json_error is not None:
            raise self._json_error
        return self._payload

    async def text(self):
        return self._text


class _RaisingAioContext:
    def __init__(self, error: BaseException):
        self.error = error

    async def __aenter__(self):
        raise self.error

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _AioSession:
    def __init__(self, response):
        self.response = response
        self.post = MagicMock(side_effect=self._post)
        self.close = AsyncMock()

    def _post(self, *args, **kwargs):
        return self.response


class _SMTP:
    def __init__(self):
        self.login = MagicMock()
        self.send_message = MagicMock(return_value={})
        self.quit = MagicMock()
        self.close = MagicMock()


class _WeComSocket:
    def __init__(self, response):
        self.closed = False
        self.response = response
        self.frames = []
        self.adapter = None
        self.send_json = AsyncMock(side_effect=self._send_json)

    async def _send_json(self, frame):
        self.frames.append(frame)
        if isinstance(self.response, BaseException):
            raise self.response
        if self.response is None:
            return
        response = dict(self.response)
        headers = dict(response.get("headers") or {})
        headers.setdefault("req_id", frame["headers"]["req_id"])
        response["headers"] = headers
        future = self.adapter._pending_responses[frame["headers"]["req_id"]]
        future.set_result(response)


def _whatsapp_adapter(response=None):
    adapter = object.__new__(WhatsAppCloudAdapter)
    adapter.platform = Platform.WHATSAPP_CLOUD
    adapter._phone_number_id = "123456789"
    adapter._access_token = "meta-token"
    adapter._api_version = "v20.0"
    adapter._http_client = MagicMock()
    adapter._http_client.post = AsyncMock(
        return_value=response
        or _HttpxResponse(
            200,
            payload={"messages": [{"id": "wamid.exact"}]},
        )
    )
    return adapter


def _bluebubbles_adapter(response=None):
    adapter = object.__new__(BlueBubblesAdapter)
    adapter.platform = Platform.BLUEBUBBLES
    adapter.server_url = "https://bluebubbles.example.test"
    adapter.password = "bb-password"
    adapter.client = MagicMock()
    adapter.client.post = AsyncMock(
        return_value=response
        or _HttpxResponse(
            200,
            payload={"data": {"guid": "message-guid-exact"}},
        )
    )
    adapter._private_api_enabled = True
    adapter._helper_connected = True
    return adapter


def _email_adapter():
    adapter = object.__new__(EmailAdapter)
    adapter.platform = Platform.EMAIL
    adapter._address = "agent@example.test"
    adapter._password = "smtp-password"
    adapter._smtp_host = "smtp.example.test"
    adapter._smtp_port = 587
    adapter._thread_context = {}
    adapter._semantic_smtp = _SMTP()
    adapter._connect_smtp_semantic_exact = MagicMock(
        return_value=adapter._semantic_smtp
    )
    return adapter


def _wecom_adapter(response=None):
    adapter = object.__new__(WeComAdapter)
    adapter.platform = Platform.WECOM
    adapter._pending_responses = {}
    socket = _WeComSocket(
        response
        if response is not None
        else {"errcode": 0, "body": {"msgid": "wecom-message-exact"}}
    )
    socket.adapter = adapter
    adapter._ws = socket
    return adapter


def _line_adapter():
    adapter = object.__new__(LineAdapter)
    adapter.platform = Platform("line")
    adapter.channel_access_token = "line-token"
    adapter._client = object()
    adapter._reply_tokens = {}
    return adapter


def _request(
    adapter,
    *,
    content="Exact preview",
    chat_id=None,
    reply_to=None,
):
    provider = adapter.SEMANTIC_EXACT_ATTEMPT_CAPABILITY.provider
    default_chat = {
        "whatsapp_cloud": "15551234567",
        "bluebubbles": "iMessage;-;owner@example.test",
        "email": "owner@example.test",
        "wecom": "chat-123",
        "line": "U" + "1" * 32,
    }[provider]
    resolved_chat = default_chat if chat_id is None else chat_id
    return LiveSemanticExactAttemptRequest(
        chat_id=resolved_chat,
        content=content,
        delivery_contract=SEMANTIC_DELIVERY_CONTRACT,
        delivery_id=f"delivery-{provider}-001",
        delivery_target=f"{provider}:account:{resolved_chat}",
        delivery_unit=0,
        encoding_contract=live_semantic_exact_attempt_encoding_contract(adapter),
        thread_id=None,
        reply_to=reply_to,
    )


@pytest.mark.parametrize(
    ("factory", "provider", "budget", "wire_encoding"),
    [
        (
            _whatsapp_adapter,
            "whatsapp_cloud",
            4000,
            "whatsapp-cloud-text-json-v1",
        ),
        (
            _bluebubbles_adapter,
            "bluebubbles",
            4000,
            "bluebubbles-text-json-v1",
        ),
        (_email_adapter, "email", 50_000, "smtp-mime-text-utf8-v1"),
        (
            _wecom_adapter,
            "wecom",
            4000,
            "wecom-aibot-send-msg-markdown-json-v1",
        ),
        (_line_adapter, "line", 4500, "line-push-text-json-v1"),
    ],
)
def test_wave2_capability_is_concrete_and_frozen(
    factory,
    provider,
    budget,
    wire_encoding,
):
    adapter = factory()

    assert owns_live_semantic_exact_attempt(adapter) is True
    contract = live_semantic_exact_attempt_encoding_contract(adapter)
    assert contract.provider == provider
    assert contract.max_logical_units == budget
    assert contract.length_semantics == "unicode_codepoints"
    assert contract.wire_encoding == wire_encoding


@pytest.mark.parametrize(
    ("register_fn", "call_index"),
    [
        (register_email, 0),
        (register_wecom, 0),
        (register_line, 0),
    ],
)
def test_wave2_plugin_registration_advertises_bound_live_only(
    register_fn,
    call_index,
):
    context = MagicMock()

    register_fn(context)

    kwargs = context.register_platform.call_args_list[call_index].kwargs
    assert kwargs["live_semantic_exact_attempt"] is True
    assert kwargs["semantic_exact_attempt"] is False


def test_wecom_callback_registration_advertises_bound_live_exact():
    context = MagicMock()

    register_wecom(context)

    callback_kwargs = context.register_platform.call_args_list[1].kwargs
    assert callback_kwargs["name"] == "wecom_callback"
    assert callback_kwargs["live_semantic_exact_attempt"] is True
    assert callback_kwargs["semantic_exact_attempt"] is False


@pytest.mark.parametrize(
    ("provider", "factory"),
    [
        ("whatsapp_cloud", _whatsapp_adapter),
        ("bluebubbles", _bluebubbles_adapter),
    ],
)
def test_wave2_builtin_declaration_requires_live_bound_override(
    provider,
    factory,
):
    adapter = factory()

    assert supports_semantic_exact_attempt(provider) is False
    assert supports_live_semantic_exact_attempt(provider) is True
    assert supports_live_semantic_exact_attempt(provider, adapter=adapter) is True


@pytest.mark.asyncio
async def test_whatsapp_cloud_exact_body_reaches_one_graph_post_unchanged():
    adapter = _whatsapp_adapter()
    request = _request(
        adapter,
        content="Keep **literal** text\nand emoji 🛰️",
        reply_to="wamid.inbound",
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id == "wamid.exact"
    adapter._http_client.post.assert_awaited_once()
    args, kwargs = adapter._http_client.post.await_args
    assert args[0].endswith("/123456789/messages")
    assert kwargs["json"] == {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": "15551234567",
        "type": "text",
        "text": {
            "body": request.content,
            "preview_url": False,
        },
        "context": {"message_id": "wamid.inbound"},
    }
    assert kwargs["follow_redirects"] is False


@pytest.mark.asyncio
async def test_bluebubbles_exact_body_reaches_one_pre_resolved_post_unchanged():
    adapter = _bluebubbles_adapter()
    request = _request(
        adapter,
        content="Literal _iMessage_ body\nsecond line",
        reply_to="selected-message-guid",
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id == "message-guid-exact"
    adapter.client.post.assert_awaited_once()
    args, kwargs = adapter.client.post.await_args
    assert args[0].startswith(
        "https://bluebubbles.example.test/api/v1/message/text?"
    )
    assert kwargs["json"]["chatGuid"] == request.chat_id
    assert kwargs["json"]["message"] == request.content
    assert kwargs["json"]["selectedMessageGuid"] == "selected-message-guid"
    assert kwargs["json"]["method"] == "private-api"
    assert kwargs["json"]["tempGuid"].startswith("semantic-")
    assert kwargs["follow_redirects"] is False


@pytest.mark.asyncio
async def test_email_exact_body_reaches_one_smtp_data_with_stable_message_id():
    adapter = _email_adapter()
    request = _request(
        adapter,
        content="Exact UTF-8 email body\nAntroji eilutė 🛰️",
        reply_to="<inbound-message@example.test>",
    )

    first = await send_via_exact_adapter_method(adapter, request)
    sent_message = adapter._semantic_smtp.send_message.call_args.args[0]

    assert first.success is True
    assert first.message_id.startswith("<hermes-semantic-")
    assert sent_message.get_payload(decode=True).decode("utf-8") == request.content
    assert sent_message["To"] == request.chat_id
    assert sent_message["In-Reply-To"] == request.reply_to
    assert sent_message["Message-ID"] == first.message_id
    adapter._semantic_smtp.send_message.assert_called_once()

    second_adapter = _email_adapter()
    second = await send_via_exact_adapter_method(second_adapter, request)
    assert second.message_id == first.message_id
    second_adapter._semantic_smtp.send_message.assert_called_once()


@pytest.mark.asyncio
async def test_wecom_exact_body_reaches_one_correlated_websocket_frame():
    adapter = _wecom_adapter()
    request = _request(adapter, content="Exact **WeCom** markdown\n第二行")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id == "wecom-message-exact"
    adapter._ws.send_json.assert_awaited_once()
    frame = adapter._ws.frames[0]
    assert frame["cmd"] == "aibot_send_msg"
    assert frame["body"] == {
        "chatid": "chat-123",
        "msgtype": "markdown",
        "markdown": {"content": request.content},
    }
    assert frame["headers"]["req_id"].startswith("semantic-")


@pytest.mark.parametrize(
    ("receipt_id", "accepted"),
    [
        ("  wecom-" + ("訊" * 2_048) + "-receipt  ", True),
        (7, False),
        ("wecom\u0085control", False),
        ("\ud800", False),
    ],
)
@pytest.mark.asyncio
async def test_wecom_receipt_preserves_safe_unicode_and_rejects_invalid(
    receipt_id,
    accepted: bool,
):
    adapter = _wecom_adapter(
        {"errcode": 0, "body": {"msgid": receipt_id}}
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is accepted
    if accepted:
        assert result.message_id == receipt_id
    else:
        rejection = result.raw_response["provider_rejection"]
        assert rejection["provider"] == "WeCom"
        assert rejection["protocol"] == "wecom-aibot-websocket"
        assert rejection["response_sha256"] in result.error
    adapter._ws.send_json.assert_awaited_once()


@pytest.mark.asyncio
async def test_wecom_success_without_provider_receipt_never_uses_request_id():
    adapter = _wecom_adapter({"errcode": 0, "body": {}})

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    rejection = result.raw_response["provider_rejection"]
    assert rejection["provider"] == "WeCom"
    assert rejection["protocol"] == "wecom-aibot-websocket"
    assert rejection["response_sha256"] in result.error
    adapter._ws.send_json.assert_awaited_once()


@pytest.mark.asyncio
async def test_line_exact_body_reaches_one_push_without_consuming_reply_token(
    monkeypatch,
):
    import aiohttp

    adapter = _line_adapter()
    adapter._reply_tokens[_request(adapter).chat_id] = (
        "single-use-reply-token",
        9_999_999_999.0,
    )
    response = _AioResponse(
        200,
        payload={"sentMessages": [{"id": "line-message-exact"}]},
    )
    session = _AioSession(response)
    monkeypatch.setattr(aiohttp, "ClientSession", lambda **kwargs: session)
    request = _request(adapter, content="Exact LINE body\n二行目")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id == "line-message-exact"
    assert adapter._reply_tokens[request.chat_id][0] == "single-use-reply-token"
    assert session.post.call_count == 1
    args, kwargs = session.post.call_args
    assert args[0] == "https://api.line.me/v2/bot/message/push"
    assert kwargs["json"] == {
        "to": request.chat_id,
        "messages": [{"type": "text", "text": request.content}],
    }
    assert kwargs["allow_redirects"] is False
    session.close.assert_awaited_once()


@pytest.mark.parametrize(
    "factory",
    [
        _whatsapp_adapter,
        _bluebubbles_adapter,
        _email_adapter,
        _wecom_adapter,
        _line_adapter,
    ],
)
@pytest.mark.asyncio
async def test_wave2_oversize_is_zero_provider_writes(factory, monkeypatch):
    adapter = factory()
    line_session_factory = _guard_line_session(adapter, monkeypatch)
    capability = adapter.SEMANTIC_EXACT_ATTEMPT_CAPABILITY
    request = _request(
        adapter,
        content="x" * (capability.max_logical_units + 1),
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.raw_response["provider_write_attempted"] is False
    _assert_zero_provider_writes(adapter)
    if line_session_factory is not None:
        line_session_factory.assert_not_called()


@pytest.mark.parametrize(
    "factory",
    [
        _whatsapp_adapter,
        _bluebubbles_adapter,
        _email_adapter,
        _wecom_adapter,
        _line_adapter,
    ],
)
@pytest.mark.asyncio
async def test_wave2_historical_encoder_rejection_is_zero_writes(
    factory,
    monkeypatch,
):
    adapter = factory()
    line_session_factory = _guard_line_session(adapter, monkeypatch)
    request = _request(adapter)
    request = replace(
        request,
        encoding_contract=replace(
            request.encoding_contract,
            wire_encoding="retired-encoder-v0",
        ),
    )

    with pytest.raises(
        ValueError,
        match="semantic exact-attempt encoding unsupported",
    ):
        await send_via_exact_adapter_method(adapter, request)

    _assert_zero_provider_writes(adapter)
    if line_session_factory is not None:
        line_session_factory.assert_not_called()


@pytest.mark.parametrize(
    ("factory", "invalid_chat_id"),
    [
        (_whatsapp_adapter, "+15551234567"),
        (_bluebubbles_adapter, "owner@example.test"),
        (_email_adapter, "bad\naddress@example.test"),
        (_wecom_adapter, "bad\nchat"),
        (_line_adapter, "not-a-line-id"),
    ],
)
@pytest.mark.asyncio
async def test_wave2_invalid_route_is_zero_provider_writes(
    factory,
    invalid_chat_id,
    monkeypatch,
):
    adapter = factory()
    line_session_factory = _guard_line_session(adapter, monkeypatch)
    request = _request(adapter, chat_id=invalid_chat_id)

    result = await adapter.send_semantic_exact_attempt(request)

    assert result.success is False
    assert result.raw_response["provider_write_attempted"] is False
    _assert_zero_provider_writes(adapter)
    if line_session_factory is not None:
        line_session_factory.assert_not_called()


def _guard_line_session(adapter, monkeypatch):
    if not isinstance(adapter, LineAdapter):
        return None
    import aiohttp

    session_factory = MagicMock(
        side_effect=AssertionError("LINE provider session must not be created")
    )
    monkeypatch.setattr(aiohttp, "ClientSession", session_factory)
    return session_factory


def _assert_zero_provider_writes(adapter):
    if isinstance(adapter, WhatsAppCloudAdapter):
        adapter._http_client.post.assert_not_awaited()
    elif isinstance(adapter, BlueBubblesAdapter):
        adapter.client.post.assert_not_awaited()
    elif isinstance(adapter, EmailAdapter):
        adapter._connect_smtp_semantic_exact.assert_not_called()
        adapter._semantic_smtp.send_message.assert_not_called()
    elif isinstance(adapter, WeComAdapter):
        adapter._ws.send_json.assert_not_awaited()
    elif isinstance(adapter, LineAdapter):
        # LINE creates its ephemeral HTTP session only after validation.
        assert adapter._client is not None
    else:  # pragma: no cover - test helper exhaustiveness
        raise AssertionError(type(adapter))


async def _http_result(
    provider,
    status,
    monkeypatch,
    *,
    payload=None,
    headers=None,
    text="provider response",
):
    if provider == "whatsapp_cloud":
        adapter = _whatsapp_adapter(
            _HttpxResponse(
                status,
                payload=payload,
                text=text,
                headers=headers,
            )
        )
        result = await send_via_exact_adapter_method(adapter, _request(adapter))
        return result, adapter._http_client.post
    if provider == "bluebubbles":
        adapter = _bluebubbles_adapter(
            _HttpxResponse(
                status,
                payload=payload,
                text=text,
                headers=headers,
            )
        )
        result = await send_via_exact_adapter_method(adapter, _request(adapter))
        return result, adapter.client.post
    if provider == "line":
        import aiohttp

        adapter = _line_adapter()
        session = _AioSession(
            _AioResponse(
                status,
                payload=payload,
                text=text,
                headers=headers,
            )
        )
        monkeypatch.setattr(aiohttp, "ClientSession", lambda **kwargs: session)
        result = await send_via_exact_adapter_method(adapter, _request(adapter))
        return result, session.post
    raise AssertionError(provider)


def _opaque_receipt_payload(provider: str, message_id):
    if provider == "whatsapp_cloud":
        return {"messages": [{"id": message_id}]}
    if provider == "bluebubbles":
        return {"data": {"guid": message_id}}
    if provider == "line":
        return {"sentMessages": [{"id": message_id}]}
    raise AssertionError(provider)


@pytest.mark.parametrize(
    "provider",
    ["whatsapp_cloud", "bluebubbles", "line"],
)
@pytest.mark.asyncio
async def test_wave2_opaque_provider_receipt_is_preserved_exactly(
    provider,
    monkeypatch,
):
    message_id = "  native-" + ("訊" * 2_048) + "-receipt  "

    result, post = await _http_result(
        provider,
        200,
        monkeypatch,
        payload=_opaque_receipt_payload(provider, message_id),
    )

    assert result.success is True
    assert result.message_id == message_id
    assert post.call_count == 1


@pytest.mark.parametrize(
    ("provider", "message_id"),
    [
        (provider, message_id)
        for provider in ("whatsapp_cloud", "bluebubbles", "line")
        for message_id in (42, "native\tcontrol", "native\ud800surrogate")
    ],
)
@pytest.mark.asyncio
async def test_wave2_unsafe_or_non_string_provider_receipt_is_ambiguous(
    provider,
    message_id,
    monkeypatch,
):
    result, post = await _http_result(
        provider,
        200,
        monkeypatch,
        payload=_opaque_receipt_payload(provider, message_id),
    )

    assert result.success is False
    assert result.error == "semantic_delivery_provider_receipt_invalid"
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    assert post.call_count == 1


@pytest.mark.parametrize(
    "provider",
    ["whatsapp_cloud", "bluebubbles", "line"],
)
@pytest.mark.asyncio
async def test_wave2_malformed_2xx_receipt_keeps_redacted_digest_evidence(
    provider,
    monkeypatch,
):
    secret = "ghp_" + ("m" * 80)
    response_body = (
        '{"token":"'
        + secret
        + '","detail":"'
        + ("malformed provider receipt " * 40)
        + '"}'
    )

    result, post = await _http_result(
        provider,
        200,
        monkeypatch,
        payload=_opaque_receipt_payload(provider, None),
        text=response_body,
    )

    assert result.success is False
    assert result.error == "semantic_delivery_provider_receipt_invalid"
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    rejection = result.raw_response["provider_rejection"]
    assert rejection["body_bytes"] == len(response_body.encode("utf-8"))
    assert rejection["truncated"] is True
    assert rejection["redacted"] is True
    assert secret not in rejection["body_preview"]
    assert post.call_count == 1


@pytest.mark.parametrize(
    "provider",
    ["whatsapp_cloud", "bluebubbles", "line"],
)
@pytest.mark.asyncio
async def test_wave2_redirect_is_one_non_following_rejected_write(
    provider,
    monkeypatch,
):
    result, post = await _http_result(provider, 302, monkeypatch)

    assert result.success is False
    assert result.raw_response["provider_write_attempted"] is False
    assert post.call_count == 1
    _, kwargs = post.call_args
    redirect_key = "allow_redirects" if provider == "line" else "follow_redirects"
    assert kwargs[redirect_key] is False


@pytest.mark.parametrize(
    "provider",
    ["whatsapp_cloud", "bluebubbles", "line"],
)
@pytest.mark.asyncio
async def test_wave2_explicit_http_429_is_proven_prewrite_retryable(
    provider,
    monkeypatch,
):
    result, post = await _http_result(
        provider,
        429,
        monkeypatch,
        headers={"Retry-After": "9"},
    )

    assert result.success is False
    assert result.retryable is True
    assert result.retry_after == 9.0
    assert result.raw_response["provider_write_attempted"] is False
    assert result.raw_response["provider_retryable"] is True
    assert post.call_count == 1


@pytest.mark.parametrize(
    "provider",
    ["whatsapp_cloud", "bluebubbles", "line"],
)
@pytest.mark.asyncio
async def test_wave2_http_5xx_is_ambiguous_without_blind_retry(
    provider,
    monkeypatch,
):
    secret = "ghp_" + ("w" * 80)
    response_body = (
        '{"authorization":"Bearer '
        + secret
        + '","detail":"'
        + ("provider failure " * 40)
        + '"}'
    )
    result, post = await _http_result(
        provider,
        503,
        monkeypatch,
        text=response_body,
    )

    assert result.success is False
    assert result.retryable is False
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    rejection = result.raw_response["provider_rejection"]
    assert rejection["body_bytes"] == len(response_body.encode("utf-8"))
    assert rejection["truncated"] is True
    assert rejection["redacted"] is True
    assert secret not in rejection["body_preview"]
    assert rejection["body_sha256"] in result.error
    assert post.call_count == 1


@pytest.mark.parametrize(
    "provider",
    ["whatsapp_cloud", "bluebubbles", "line"],
)
@pytest.mark.asyncio
async def test_wave2_success_without_provider_id_is_ambiguous(
    provider,
    monkeypatch,
):
    result, post = await _http_result(
        provider,
        200,
        monkeypatch,
        payload={},
    )

    assert result.success is False
    assert result.error == "semantic_delivery_provider_receipt_invalid"
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    assert post.call_count == 1


@pytest.mark.parametrize("provider", ["whatsapp_cloud", "bluebubbles"])
@pytest.mark.asyncio
async def test_wave2_http_timeout_after_post_is_ambiguous(provider):
    if provider == "whatsapp_cloud":
        adapter = _whatsapp_adapter()
        post = adapter._http_client.post
    else:
        adapter = _bluebubbles_adapter()
        post = adapter.client.post
    post.side_effect = httpx.ReadTimeout("response lost")

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    post.assert_awaited_once()


@pytest.mark.asyncio
async def test_line_timeout_after_post_is_ambiguous(monkeypatch):
    import aiohttp

    adapter = _line_adapter()
    session = _AioSession(_RaisingAioContext(asyncio.TimeoutError()))
    monkeypatch.setattr(aiohttp, "ClientSession", lambda **kwargs: session)

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    assert session.post.call_count == 1


@pytest.mark.asyncio
async def test_email_temporary_recipient_rejection_is_proven_pre_data_retry():
    adapter = _email_adapter()
    adapter._semantic_smtp.send_message.side_effect = (
        smtplib.SMTPRecipientsRefused(
            {"owner@example.test": (450, b"mailbox busy")}
        )
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert result.retryable is True
    assert result.raw_response["provider_write_attempted"] is False
    adapter._semantic_smtp.send_message.assert_called_once()


@pytest.mark.asyncio
async def test_email_data_5xx_is_ambiguous_after_one_data_attempt():
    adapter = _email_adapter()
    adapter._semantic_smtp.send_message.side_effect = smtplib.SMTPDataError(
        554,
        b"transaction failed",
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert result.retryable is False
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    adapter._semantic_smtp.send_message.assert_called_once()


@pytest.mark.asyncio
async def test_email_transport_reset_during_data_is_ambiguous():
    adapter = _email_adapter()
    adapter._semantic_smtp.send_message.side_effect = (
        smtplib.SMTPServerDisconnected("response lost")
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert result.retryable is False
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    adapter._semantic_smtp.send_message.assert_called_once()


@pytest.mark.asyncio
async def test_email_secret_transport_error_maps_to_bounded_diagnostic():
    adapter = _email_adapter()
    secret = "super-secret-smtp-token-" + ("x" * 5_000)
    adapter._semantic_smtp.send_message.side_effect = (
        smtplib.SMTPServerDisconnected(
            f"Authorization: Bearer {secret}"
        )
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))
    mapped = semantic_delivery_module._send_result_mapping(result)

    diagnostic = mapped["provider_error"]
    assert diagnostic["schema_version"] == "hermes.bounded-diagnostic/1"
    assert diagnostic["truncated"] is True
    assert diagnostic["redacted"] is True
    assert secret not in diagnostic["text_preview"]
    adapter._semantic_smtp.send_message.assert_called_once()


@pytest.mark.asyncio
async def test_email_malformed_data_receipt_is_ambiguous():
    adapter = _email_adapter()
    adapter._semantic_smtp.send_message.return_value = None

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert result.retryable is False
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    adapter._semantic_smtp.send_message.assert_called_once()


@pytest.mark.asyncio
async def test_wecom_explicit_rate_limit_ack_is_proven_prewrite_retryable():
    adapter = _wecom_adapter(
        {"errcode": 45009, "errmsg": "api frequency limit"}
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert result.retryable is True
    assert result.raw_response["provider_write_attempted"] is False
    assert result.raw_response["provider_retryable"] is True
    rejection = result.raw_response["provider_rejection"]
    assert rejection["provider"] == "WeCom"
    assert rejection["protocol"] == "wecom-aibot-websocket"
    assert '"errcode":45009' in rejection["response_preview"]
    assert rejection["response_sha256"] in result.error
    adapter._ws.send_json.assert_awaited_once()


@pytest.mark.asyncio
async def test_wecom_5xx_ack_is_ambiguous_without_retry():
    adapter = _wecom_adapter({"errcode": 503, "errmsg": "unknown outcome"})

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert result.retryable is False
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    rejection = result.raw_response["provider_rejection"]
    assert rejection["provider"] == "WeCom"
    assert rejection["protocol"] == "wecom-aibot-websocket"
    assert '"errcode":503' in rejection["response_preview"]
    adapter._ws.send_json.assert_awaited_once()


@pytest.mark.asyncio
async def test_wecom_malformed_ack_is_ambiguous():
    adapter = _wecom_adapter({"errmsg": "missing errcode"})

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    rejection = result.raw_response["provider_rejection"]
    assert rejection["provider"] == "WeCom"
    assert rejection["protocol"] == "wecom-aibot-websocket"
    assert rejection["response_sha256"] in result.error
    assert "missing errcode" in rejection["response_preview"]
    adapter._ws.send_json.assert_awaited_once()


@pytest.mark.asyncio
async def test_wecom_socket_reset_after_one_frame_is_ambiguous():
    adapter = _wecom_adapter(ConnectionResetError("response lost"))

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    adapter._ws.send_json.assert_awaited_once()


@pytest.mark.asyncio
async def test_wecom_timeout_after_one_frame_is_ambiguous(monkeypatch):
    import plugins.platforms.wecom.adapter as wecom_module

    adapter = _wecom_adapter(response=None)
    adapter._ws.response = None
    monkeypatch.setattr(wecom_module, "REQUEST_TIMEOUT_SECONDS", 0.001)

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    adapter._ws.send_json.assert_awaited_once()
