"""True-boundary exact-attempt tests for the small HTTP/socket adapters."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.semantic_exact_attempt import (
    LiveSemanticExactAttemptEncodingContract,
    LiveSemanticExactAttemptRequest,
    live_semantic_exact_attempt_encoding_contract,
    owns_live_semantic_exact_attempt,
    send_via_exact_adapter_method,
)
from hermes_cli import semantic_delivery as semantic_delivery_module
from hermes_cli.semantic_delivery import SEMANTIC_DELIVERY_CONTRACT
from plugins.platforms.homeassistant.adapter import HomeAssistantAdapter
from plugins.platforms.irc.adapter import IRCAdapter
from plugins.platforms.mattermost.adapter import MattermostAdapter
from plugins.platforms.ntfy.adapter import NtfyAdapter
from plugins.platforms.sms.adapter import SmsAdapter
from plugins.platforms.homeassistant.adapter import register as register_homeassistant
from plugins.platforms.irc.adapter import register as register_irc
from plugins.platforms.mattermost.adapter import register as register_mattermost
from plugins.platforms.ntfy.adapter import register as register_ntfy
from plugins.platforms.sms.adapter import register as register_sms


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
        self.closed = False

    def _post(self, *args, **kwargs):
        return self.response


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


class _Writer:
    def __init__(self):
        self.write = MagicMock()
        self.drain = AsyncMock()

    def is_closing(self):
        return False


def _ha_adapter(response=None):
    adapter = object.__new__(HomeAssistantAdapter)
    adapter.platform = Platform.HOMEASSISTANT
    adapter._hass_url = "https://ha.example.test"
    adapter._hass_token = "ha-token"
    adapter._rest_session = _AioSession(response or _AioResponse(201))
    return adapter


def _irc_adapter():
    adapter = object.__new__(IRCAdapter)
    adapter.platform = Platform("irc")
    adapter._writer = _Writer()
    return adapter


def _mattermost_adapter(response=None):
    adapter = object.__new__(MattermostAdapter)
    adapter.platform = Platform.MATTERMOST
    adapter._base_url = "https://mattermost.example.test"
    adapter._token = "mm-token"
    adapter._session = _AioSession(
        response or _AioResponse(201, payload={"id": "post-123"})
    )
    return adapter


def _ntfy_adapter(response=None):
    adapter = object.__new__(NtfyAdapter)
    adapter.platform = Platform("ntfy")
    adapter._server = "https://ntfy.example.test"
    adapter._token = "ntfy-token"
    adapter.config = PlatformConfig(enabled=True, extra={"markdown": False})
    adapter._http_client = MagicMock()
    adapter._http_client.post = AsyncMock(
        return_value=response
        or _HttpxResponse(200, payload={"id": "ntfy-123"})
    )
    return adapter


def _sms_adapter(response=None):
    adapter = object.__new__(SmsAdapter)
    adapter.platform = Platform.SMS
    adapter._account_sid = "AC1234567890"
    adapter._auth_token = "twilio-token"
    adapter._from_number = "+15550001111"
    adapter._http_session = _AioSession(
        response
        or _AioResponse(
            201,
            payload={"sid": "SM" + ("1" * 32)},
        )
    )
    return adapter


def _request(adapter, *, content="Exact preview", chat_id=None, thread_id=None):
    capability = adapter.SEMANTIC_EXACT_ATTEMPT_CAPABILITY
    provider = capability.provider
    default_chat = {
        "homeassistant": "ha_events",
        "irc": "#hermes",
        "mattermost": "channel-123",
        "ntfy": "alerts/team",
        "sms": "+15551234567",
    }[provider]
    return LiveSemanticExactAttemptRequest(
        chat_id=chat_id or default_chat,
        content=content,
        delivery_contract=SEMANTIC_DELIVERY_CONTRACT,
        delivery_id=f"delivery-{provider}-001",
        delivery_target=f"{provider}:account:{chat_id or default_chat}",
        delivery_unit=0,
        encoding_contract=live_semantic_exact_attempt_encoding_contract(adapter),
        thread_id=thread_id,
        reply_to=None,
    )


def _post_mock(adapter):
    if isinstance(adapter, NtfyAdapter):
        return adapter._http_client.post
    if isinstance(adapter, HomeAssistantAdapter):
        return adapter._rest_session.post
    return adapter._session.post if isinstance(adapter, MattermostAdapter) else adapter._http_session.post


@pytest.mark.parametrize(
    "register_fn",
    [
        register_homeassistant,
        register_irc,
        register_mattermost,
        register_ntfy,
        register_sms,
    ],
)
def test_provider_registration_advertises_only_live_exact_delivery(register_fn):
    context = MagicMock()

    register_fn(context)

    kwargs = context.register_platform.call_args.kwargs
    assert kwargs["live_semantic_exact_attempt"] is True
    assert kwargs["semantic_exact_attempt"] is False


@pytest.mark.parametrize(
    ("factory", "provider", "budget", "wire_encoding"),
    [
        (
            _ha_adapter,
            "homeassistant",
            4_000,
            "homeassistant-persistent-notification-json-v1",
        ),
        (_irc_adapter, "irc", 80, "irc-lf-to-u2028-utf8-v1"),
        (
            _mattermost_adapter,
            "mattermost",
            4_000,
            "mattermost-post-json-v1",
        ),
        (_ntfy_adapter, "ntfy", 4_000, "ntfy-text-utf8-v1"),
        (_sms_adapter, "sms", 1_400, "twilio-message-form-v1"),
    ],
)
def test_capability_is_structural_and_reports_frozen_encoder(
    factory, provider, budget, wire_encoding
):
    adapter = factory()
    assert owns_live_semantic_exact_attempt(adapter) is True
    contract = live_semantic_exact_attempt_encoding_contract(adapter)
    assert contract.provider == provider
    assert contract.max_logical_units == budget
    assert contract.length_semantics == "unicode_codepoints"
    assert contract.wire_encoding == wire_encoding


@pytest.mark.asyncio
async def test_homeassistant_exact_success_is_one_json_write():
    adapter = _ha_adapter(_AioResponse(201))
    request = _request(adapter, content="Revenue alert\nReview now")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id.startswith("ha_")
    assert adapter._rest_session.post.call_count == 1
    _, kwargs = adapter._rest_session.post.call_args
    assert kwargs["json"] == {
        "title": "Hermes Agent",
        "message": request.content,
    }
    assert kwargs["allow_redirects"] is False


@pytest.mark.asyncio
async def test_irc_exact_success_is_one_wire_line_with_versioned_newlines():
    adapter = _irc_adapter()
    request = _request(adapter, content="First line\nSecond line")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    adapter._writer.write.assert_called_once_with(
        "PRIVMSG #hermes :First line\u2028Second line\r\n".encode("utf-8")
    )
    adapter._writer.drain.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_mattermost_exact_success_is_one_post_without_root_probe():
    adapter = _mattermost_adapter(
        _AioResponse(201, payload={"id": "post-exact"})
    )
    request = _request(
        adapter,
        content="Exact **Mattermost** body",
        thread_id="root-123",
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id == "post-exact"
    assert adapter._session.post.call_count == 1
    _, kwargs = adapter._session.post.call_args
    assert kwargs["json"] == {
        "channel_id": "channel-123",
        "message": request.content,
        "root_id": "root-123",
    }
    assert kwargs["allow_redirects"] is False


@pytest.mark.asyncio
async def test_ntfy_exact_success_is_one_utf8_write_and_quotes_topic():
    adapter = _ntfy_adapter(
        _HttpxResponse(200, payload={"id": "ntfy-exact"})
    )
    request = _request(adapter, content="Exact ntfy body")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id == "ntfy-exact"
    adapter._http_client.post.assert_awaited_once()
    args, kwargs = adapter._http_client.post.await_args
    assert args[0] == "https://ntfy.example.test/alerts%2Fteam"
    assert kwargs["content"] == request.content.encode("utf-8")
    assert kwargs["follow_redirects"] is False


@pytest.mark.asyncio
async def test_sms_exact_success_is_one_form_write_without_markdown_rewrite():
    message_sid = "SM" + ("a" * 32)
    adapter = _sms_adapter(
        _AioResponse(201, payload={"sid": message_sid})
    )
    request = _request(adapter, content="Keep **literal** markdown")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id == message_sid
    assert adapter._http_session.post.call_count == 1
    _, kwargs = adapter._http_session.post.call_args
    fields = {
        disposition["name"]: value
        for disposition, _, value in kwargs["data"]._fields
    }
    assert fields == {
        "From": "+15550001111",
        "To": "+15551234567",
        "Body": request.content,
    }
    assert kwargs["allow_redirects"] is False


@pytest.mark.parametrize(
    "factory",
    [_ha_adapter, _irc_adapter, _mattermost_adapter, _ntfy_adapter, _sms_adapter],
)
@pytest.mark.asyncio
async def test_oversize_is_rejected_before_any_provider_write(factory):
    adapter = factory()
    capability = adapter.SEMANTIC_EXACT_ATTEMPT_CAPABILITY
    request = _request(
        adapter,
        content="x" * (capability.max_logical_units + 1),
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.raw_response["provider_write_attempted"] is False
    if isinstance(adapter, IRCAdapter):
        adapter._writer.write.assert_not_called()
    else:
        assert _post_mock(adapter).call_count == 0


@pytest.mark.parametrize(
    "factory",
    [_ha_adapter, _irc_adapter, _mattermost_adapter, _ntfy_adapter, _sms_adapter],
)
@pytest.mark.asyncio
async def test_unsupported_historical_encoder_is_zero_write(factory):
    adapter = factory()
    request = _request(adapter)
    unsupported = replace(
        request.encoding_contract,
        wire_encoding="unsupported-historical-v0",
    )
    request = replace(request, encoding_contract=unsupported)

    with pytest.raises(
        ValueError,
        match="semantic exact-attempt encoding unsupported",
    ):
        await send_via_exact_adapter_method(adapter, request)

    if isinstance(adapter, IRCAdapter):
        adapter._writer.write.assert_not_called()
    else:
        assert _post_mock(adapter).call_count == 0


@pytest.mark.parametrize(
    ("factory", "invalid_chat_id"),
    [
        (_ha_adapter, ""),
        (_irc_adapter, "#safe\r\nJOIN #evil"),
        (_mattermost_adapter, "bad\nchannel"),
        (_ntfy_adapter, "bad\ntopic"),
        (_sms_adapter, "not-an-e164-number"),
    ],
)
@pytest.mark.asyncio
async def test_invalid_route_is_rejected_before_provider_write(
    factory, invalid_chat_id
):
    adapter = factory()
    request = _request(adapter, chat_id=invalid_chat_id)
    request = replace(
        request,
        chat_id=invalid_chat_id,
        delivery_target=(
            f"{adapter.SEMANTIC_EXACT_ATTEMPT_CAPABILITY.provider}:"
            f"account:{invalid_chat_id}"
        ),
    )

    result = await adapter.send_semantic_exact_attempt(request)

    assert result.success is False
    assert result.raw_response["provider_write_attempted"] is False
    if isinstance(adapter, IRCAdapter):
        adapter._writer.write.assert_not_called()
    else:
        assert _post_mock(adapter).call_count == 0


def _http_failure_adapter(provider: str, response):
    if provider == "homeassistant":
        return _ha_adapter(response)
    if provider == "mattermost":
        return _mattermost_adapter(response)
    if provider == "ntfy":
        return _ntfy_adapter(response)
    if provider == "sms":
        return _sms_adapter(response)
    raise AssertionError(provider)


def _response(
    provider: str,
    status: int,
    *,
    payload=None,
    headers=None,
    text: str = "provider response",
):
    if provider == "ntfy":
        return _HttpxResponse(
            status,
            payload=payload,
            text=text,
            headers=headers,
        )
    return _AioResponse(
        status,
        payload=payload,
        text=text,
        headers=headers,
    )


def _opaque_receipt_payload(provider: str, message_id):
    if provider in {"mattermost", "ntfy"}:
        return {"id": message_id}
    if provider == "sms":
        return {"sid": message_id}
    raise AssertionError(provider)


@pytest.mark.parametrize("provider", ["mattermost", "ntfy"])
@pytest.mark.asyncio
async def test_opaque_provider_receipt_is_preserved_exactly(provider):
    message_id = "  native-" + ("訊" * 2_048) + "-receipt  "
    adapter = _http_failure_adapter(
        provider,
        _response(
            provider,
            201,
            payload=_opaque_receipt_payload(provider, message_id),
        ),
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is True
    assert result.message_id == message_id
    assert _post_mock(adapter).call_count == 1


@pytest.mark.parametrize(
    ("provider", "message_id"),
    [
        (provider, message_id)
        for provider in ("mattermost", "ntfy")
        for message_id in (42, "native\tcontrol", "native\ud800surrogate")
    ],
)
@pytest.mark.asyncio
async def test_unsafe_or_non_string_provider_receipt_is_ambiguous(
    provider,
    message_id,
):
    adapter = _http_failure_adapter(
        provider,
        _response(
            provider,
            201,
            payload=_opaque_receipt_payload(provider, message_id),
        ),
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert result.error == "semantic_delivery_provider_receipt_invalid"
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    assert _post_mock(adapter).call_count == 1


@pytest.mark.parametrize(
    "message_id",
    [
        42,
        " SM" + ("a" * 32),
        "SM" + ("a" * 32) + " ",
        "AC" + ("a" * 32),
        "SM-short",
        "SM" + ("g" * 32),
    ],
)
@pytest.mark.asyncio
async def test_twilio_message_sid_requires_exact_published_grammar(
    message_id,
):
    adapter = _sms_adapter(
        _AioResponse(
            201,
            payload={"sid": message_id},
            text='{"sid":"malformed"}',
        )
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert result.error == "semantic_delivery_provider_receipt_invalid"
    assert result.raw_response["provider_rejection"][
        "schema_version"
    ] == "hermes.provider-rejection-evidence/1"
    assert _post_mock(adapter).call_count == 1


@pytest.mark.parametrize("provider", ["mattermost", "ntfy", "sms"])
@pytest.mark.asyncio
async def test_malformed_2xx_receipt_keeps_redacted_digest_evidence(
    provider,
):
    secret = "ghp_" + ("m" * 80)
    response_body = (
        '{"token":"'
        + secret
        + '","detail":"'
        + ("malformed provider receipt " * 40)
        + '"}'
    )
    adapter = _http_failure_adapter(
        provider,
        _response(
            provider,
            201,
            payload=_opaque_receipt_payload(provider, None),
            text=response_body,
        ),
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert result.error == "semantic_delivery_provider_receipt_invalid"
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    rejection = result.raw_response["provider_rejection"]
    assert rejection["body_bytes"] == len(response_body.encode("utf-8"))
    assert rejection["truncated"] is True
    assert rejection["redacted"] is True
    assert secret not in rejection["body_preview"]
    assert _post_mock(adapter).call_count == 1


@pytest.mark.parametrize(
    "provider",
    ["homeassistant", "mattermost", "ntfy", "sms"],
)
@pytest.mark.asyncio
async def test_redirect_is_not_followed_and_never_causes_second_write(provider):
    adapter = _http_failure_adapter(provider, _response(provider, 302))

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert result.raw_response["provider_write_attempted"] is False
    post = _post_mock(adapter)
    assert post.call_count == 1
    _, kwargs = post.call_args
    redirect_key = (
        "follow_redirects" if provider == "ntfy" else "allow_redirects"
    )
    assert kwargs[redirect_key] is False


@pytest.mark.parametrize(
    "provider",
    ["homeassistant", "mattermost", "ntfy", "sms"],
)
@pytest.mark.asyncio
async def test_explicit_429_is_the_only_safe_http_retry_boundary(provider):
    adapter = _http_failure_adapter(
        provider,
        _response(provider, 429, headers={"Retry-After": "7"}),
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert result.retryable is True
    assert result.retry_after == 7.0
    assert result.raw_response["provider_write_attempted"] is False
    assert result.raw_response["provider_retryable"] is True
    assert _post_mock(adapter).call_count == 1


@pytest.mark.parametrize(
    "provider",
    ["homeassistant", "mattermost", "ntfy", "sms"],
)
@pytest.mark.asyncio
async def test_http_5xx_is_ambiguous_and_is_not_blindly_retryable(provider):
    secret = "ghp_" + ("z" * 80)
    response_body = (
        '{"authorization":"Bearer '
        + secret
        + '","detail":"'
        + ("provider failure " * 40)
        + '"}'
    )
    adapter = _http_failure_adapter(
        provider,
        _response(provider, 503, text=response_body),
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert result.retryable is False
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    rejection = result.raw_response["provider_rejection"]
    assert rejection["body_bytes"] == len(response_body.encode("utf-8"))
    assert rejection["truncated"] is True
    assert rejection["redacted"] is True
    assert secret not in rejection["body_preview"]
    assert rejection["body_sha256"] in result.error
    assert _post_mock(adapter).call_count == 1


@pytest.mark.parametrize(
    "provider",
    ["mattermost", "ntfy", "sms"],
)
@pytest.mark.asyncio
async def test_success_without_real_provider_receipt_is_ambiguous(provider):
    adapter = _http_failure_adapter(
        provider,
        _response(provider, 201, payload={}),
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert result.error == "semantic_delivery_provider_receipt_invalid"
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    assert _post_mock(adapter).call_count == 1


@pytest.mark.parametrize(
    "provider",
    ["homeassistant", "mattermost", "sms"],
)
@pytest.mark.asyncio
async def test_aiohttp_timeout_after_attempt_is_ambiguous(provider):
    adapter = _http_failure_adapter(
        provider,
        _RaisingAioContext(asyncio.TimeoutError()),
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    assert _post_mock(adapter).call_count == 1


@pytest.mark.asyncio
async def test_ntfy_timeout_after_attempt_is_ambiguous():
    import httpx

    adapter = _ntfy_adapter()
    adapter._http_client.post.side_effect = httpx.ReadTimeout("timed out")

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    adapter._http_client.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_irc_socket_reset_after_write_is_ambiguous_without_retry():
    adapter = _irc_adapter()
    adapter._writer.drain.side_effect = ConnectionResetError("reset")

    result = await send_via_exact_adapter_method(adapter, _request(adapter))

    assert result.success is False
    assert (result.raw_response or {}).get("provider_write_attempted") is not False
    adapter._writer.write.assert_called_once()
    adapter._writer.drain.assert_awaited_once()


@pytest.mark.asyncio
async def test_irc_secret_socket_error_maps_to_bounded_diagnostic():
    adapter = _irc_adapter()
    secret = "super-secret-irc-token-" + ("x" * 5_000)
    adapter._writer.drain.side_effect = ConnectionResetError(
        f"Authorization: Bearer {secret}"
    )

    result = await send_via_exact_adapter_method(adapter, _request(adapter))
    mapped = semantic_delivery_module._send_result_mapping(result)

    diagnostic = mapped["provider_error"]
    assert diagnostic["schema_version"] == "hermes.bounded-diagnostic/1"
    assert diagnostic["truncated"] is True
    assert diagnostic["redacted"] is True
    assert secret not in diagnostic["text_preview"]
    adapter._writer.write.assert_called_once()
    adapter._writer.drain.assert_awaited_once()
