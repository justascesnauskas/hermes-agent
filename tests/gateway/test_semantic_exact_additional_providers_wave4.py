"""Provider-boundary tests for Feishu, Google Chat, and WeCom Callback."""

from __future__ import annotations

from dataclasses import replace
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.semantic_exact_attempt import (
    LiveSemanticExactAttemptRequest,
    coerce_live_semantic_exact_attempt_provider_route,
    live_semantic_exact_attempt_encoding_contract,
    owns_live_semantic_exact_attempt,
    send_via_exact_adapter_method,
)
from hermes_cli.semantic_delivery import SEMANTIC_DELIVERY_CONTRACT
from plugins.platforms.feishu.adapter import FeishuAdapter
from plugins.platforms.feishu.adapter import register as register_feishu
from plugins.platforms.google_chat.adapter import (
    GoogleChatAdapter,
    _GoogleChatSemanticNoRedirectHttp,
)
import plugins.platforms.google_chat.adapter as google_chat_module
from plugins.platforms.google_chat.adapter import register as register_google
import plugins.platforms.simplex.adapter as simplex_module
from plugins.platforms.simplex.adapter import SimplexAdapter
from plugins.platforms.simplex.adapter import register as register_simplex
from plugins.platforms.wecom.adapter import register as register_wecom
from plugins.platforms.wecom.callback_adapter import WecomCallbackAdapter


class _CallbackResponse:
    def __init__(
        self,
        status_code: int,
        payload=None,
        *,
        headers: dict | None = None,
        json_error: Exception | None = None,
    ):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self._json_error = json_error

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._payload


class _GoogleApi:
    def __init__(self, response=None, error: Exception | None = None):
        self.response = response or {
            "name": "spaces/space-1/messages/message-exact"
        }
        self.error = error
        self.create_calls: list[dict] = []
        self.execute_calls: list[dict] = []

    def spaces(self):
        return self

    def messages(self):
        return self

    def create(self, **kwargs):
        self.create_calls.append(kwargs)
        return self

    def execute(self, **kwargs):
        self.execute_calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.response


class _GoogleError(RuntimeError):
    def __init__(self, status: int | None, *, headers: dict | None = None):
        super().__init__(f"HTTP {status}")
        self.resp = SimpleNamespace(status=status, headers=headers or {})


class _SimplexSocket:
    def __init__(self, response=None, error: Exception | None = None):
        self.closed = False
        self.response = response
        self.error = error
        self.frames: list[dict] = []
        self.adapter = None
        self.send = AsyncMock(side_effect=self._send)

    async def _send(self, raw_frame):
        frame = json.loads(raw_frame)
        self.frames.append(frame)
        if self.error is not None:
            raise self.error
        if self.response is None:
            return
        future = self.adapter._pending_responses[frame["corrId"]]
        future.set_result(self.response)


def _feishu_adapter(
    *,
    message_status: int = 200,
    message_payload=None,
    message_headers: dict | None = None,
):
    adapter = object.__new__(FeishuAdapter)
    adapter.platform = Platform.FEISHU
    adapter._app_id = "cli_app-id"
    adapter._app_secret = "bound-secret"
    adapter._domain_name = "feishu"
    responses = [
        (
            200,
            {},
            json.dumps(
                {
                    "code": 0,
                    "tenant_access_token": "tenant-token",
                }
            ).encode(),
        ),
        (
            message_status,
            message_headers or {},
            json.dumps(
                message_payload
                if message_payload is not None
                else {
                    "code": 0,
                    "data": {"message_id": "om_exact"},
                }
            ).encode(),
        ),
    ]
    adapter._semantic_http_post = MagicMock(side_effect=responses)
    return adapter


def _google_adapter(*, response=None, error: Exception | None = None):
    adapter = object.__new__(GoogleChatAdapter)
    adapter.platform = Platform("google_chat")
    adapter._credentials = object()
    adapter._chat_api = _GoogleApi(response=response, error=error)
    adapter._new_semantic_exact_authed_http = MagicMock(
        return_value=object()
    )
    return adapter


def _simplex_receipt(
    *,
    item_id: int = 731,
    text: str = "Exact preview",
    target_id: int = 42,
    target_type: str = "direct",
    status_type: str = "sndNew",
):
    chat_info = (
        {"type": "group", "groupInfo": {"groupId": target_id}}
        if target_type == "group"
        else {"type": "direct", "contact": {"contactId": target_id}}
    )
    direction = "groupSnd" if target_type == "group" else "directSnd"
    return {
        "type": "newChatItems",
        "chatItems": [
            {
                "chatInfo": chat_info,
                "chatItem": {
                    "chatDir": {"type": direction},
                    "meta": {
                        "itemId": item_id,
                        "itemStatus": {"type": status_type},
                    },
                    "content": {
                        "type": "sndMsgContent",
                        "msgContent": {"type": "text", "text": text},
                    },
                },
            }
        ],
    }


def _simplex_adapter(response=None, *, error: Exception | None = None):
    adapter = SimplexAdapter(
        PlatformConfig(
            enabled=True,
            extra={"ws_url": "ws://127.0.0.1:5225"},
        )
    )
    socket = _SimplexSocket(
        response=response if response is not None else _simplex_receipt(),
        error=error,
    )
    socket.adapter = adapter
    adapter._ws = socket
    return adapter


def _callback_config():
    return PlatformConfig(
        enabled=True,
        extra={
            "apps": [
                {
                    "name": "sales-app",
                    "corp_id": "corpA",
                    "corp_secret": "bound-secret",
                    "agent_id": "1001",
                    "token": "callback-token",
                    "encoding_aes_key": "x" * 43,
                },
                {
                    "name": "support-app",
                    "corp_id": "corpB",
                    "corp_secret": "other-secret",
                    "agent_id": "2002",
                    "token": "other-callback-token",
                    "encoding_aes_key": "y" * 43,
                },
            ]
        },
    )


def _callback_adapter(response=None):
    adapter = WecomCallbackAdapter(_callback_config())
    adapter._user_app_map["corpB:alice"] = "support-app"
    adapter._get_access_token = AsyncMock(return_value="prepared-token")
    adapter._http_client = SimpleNamespace(
        post=AsyncMock(
            return_value=response
            or _CallbackResponse(
                200,
                {"errcode": 0, "msgid": "wecom-callback-exact"},
            )
        )
    )
    return adapter


def _request(
    adapter,
    *,
    chat_id: str,
    content: str = "Exact preview",
    thread_id: str | None = None,
    reply_to: str | None = None,
    provider_route=None,
):
    provider = adapter.SEMANTIC_EXACT_ATTEMPT_CAPABILITY.provider
    return LiveSemanticExactAttemptRequest(
        chat_id=chat_id,
        content=content,
        delivery_contract=SEMANTIC_DELIVERY_CONTRACT,
        delivery_id=f"delivery-{provider}-001",
        delivery_target=f"{provider}:account:{chat_id}",
        delivery_unit=0,
        encoding_contract=live_semantic_exact_attempt_encoding_contract(
            adapter
        ),
        provider_route=coerce_live_semantic_exact_attempt_provider_route(
            provider_route
        ),
        thread_id=thread_id,
        reply_to=reply_to,
    )


@pytest.mark.parametrize(
    ("factory", "provider", "budget", "wire_encoding"),
    [
        (
            _feishu_adapter,
            "feishu",
            8000,
            "feishu-text-json-v1",
        ),
        (
            _google_adapter,
            "google_chat",
            4000,
            "google-chat-text-json-v1",
        ),
        (
            _callback_adapter,
            "wecom_callback",
            2048,
            "wecom-callback-text-json-v1",
        ),
        (
            _simplex_adapter,
            "simplex",
            8000,
            "simplex-api-send-messages-json-v1",
        ),
    ],
)
def test_wave4_capability_is_concrete_and_frozen(
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
    ("register_fn", "call_index", "expected_live"),
    [
        (register_feishu, 0, True),
        (register_google, 0, True),
        (register_wecom, 1, True),
        (register_simplex, 0, True),
    ],
)
def test_wave4_registration_matches_audited_live_capability(
    register_fn,
    call_index,
    expected_live,
):
    context = MagicMock()

    register_fn(context)

    kwargs = context.register_platform.call_args_list[call_index].kwargs
    assert kwargs["semantic_exact_attempt"] is False
    assert kwargs["live_semantic_exact_attempt"] is expected_live


@pytest.mark.asyncio
async def test_feishu_exact_body_reaches_one_message_post_unchanged():
    adapter = _feishu_adapter()
    request = _request(
        adapter,
        chat_id="oc_chat-1",
        content="Literal **Feishu** text\n第二行 🛰️",
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id == "om_exact"
    assert adapter._semantic_http_post.call_count == 2
    auth_call, message_call = adapter._semantic_http_post.call_args_list
    assert auth_call.args[0].endswith(
        "/auth/v3/tenant_access_token/internal"
    )
    assert message_call.args[0].endswith(
        "/im/v1/messages?receive_id_type=chat_id"
    )
    wire = json.loads(message_call.kwargs["body"].decode())
    assert wire["receive_id"] == request.chat_id
    assert json.loads(wire["content"]) == {"text": request.content}
    assert wire["msg_type"] == "text"
    assert wire["uuid"].startswith("semantic-")


@pytest.mark.parametrize(
    ("receipt_id", "accepted"),
    [
        ("  feishu-" + ("訊" * 2_048) + "-receipt  ", True),
        (7, False),
        ("feishu\tcontrol", False),
        ("\ud800", False),
    ],
)
@pytest.mark.asyncio
async def test_feishu_receipt_preserves_safe_unicode_and_rejects_invalid(
    receipt_id,
    accepted: bool,
):
    adapter = _feishu_adapter(
        message_payload={
            "code": 0,
            "data": {"message_id": receipt_id},
        }
    )

    result = await send_via_exact_adapter_method(
        adapter,
        _request(adapter, chat_id="oc_chat-1"),
    )

    assert result.success is accepted
    if accepted:
        assert result.message_id == receipt_id
    else:
        rejection = result.raw_response["provider_rejection"]
        assert rejection["provider"] == "Feishu"
        assert rejection["status"] == 200
        assert rejection["body_sha256"] in result.error
    assert adapter._semantic_http_post.call_count == 2


@pytest.mark.asyncio
async def test_feishu_exact_reply_has_no_create_fallback():
    adapter = _feishu_adapter(
        message_payload={"code": 230001, "msg": "message missing"},
    )
    request = _request(
        adapter,
        chat_id="oc_chat-1",
        content="Exact reply",
        thread_id="omt_thread-1",
        reply_to="om_inbound-1",
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert adapter._semantic_http_post.call_count == 2
    message_call = adapter._semantic_http_post.call_args_list[1]
    assert message_call.args[0].endswith(
        "/im/v1/messages/om_inbound-1/reply"
    )
    wire = json.loads(message_call.kwargs["body"].decode())
    assert wire["reply_in_thread"] is True


@pytest.mark.asyncio
async def test_feishu_exact_thread_create_uses_persisted_thread_directly():
    adapter = _feishu_adapter()
    request = _request(
        adapter,
        chat_id="oc_chat-1",
        thread_id="omt_thread-1",
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    message_call = adapter._semantic_http_post.call_args_list[1]
    assert message_call.args[0].endswith(
        "/im/v1/messages?receive_id_type=thread_id"
    )
    wire = json.loads(message_call.kwargs["body"].decode())
    assert wire["receive_id"] == "omt_thread-1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "payload", "retryable", "attempted_flag"),
    [
        (429, {"code": 99991402}, True, False),
        (503, {"code": 0}, False, None),
        (400, {"code": 230001}, False, False),
    ],
)
async def test_feishu_exact_classifies_provider_outcome_without_retry(
    status,
    payload,
    retryable,
    attempted_flag,
):
    adapter = _feishu_adapter(
        message_status=status,
        message_payload=payload,
        message_headers={"Retry-After": "3"},
    )
    request = _request(adapter, chat_id="oc_chat-1")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.retryable is retryable
    assert adapter._semantic_http_post.call_count == 2
    if attempted_flag is None:
        assert "provider_write_attempted" not in (result.raw_response or {})
    else:
        assert result.raw_response["provider_write_attempted"] is attempted_flag


@pytest.mark.asyncio
async def test_feishu_malformed_success_receipt_is_ambiguous():
    secret = "super-secret-provider-token-" + ("x" * 5_000)
    adapter = _feishu_adapter(
        message_payload={
            "code": 0,
            "data": {"access_token": secret},
        }
    )
    request = _request(adapter, chat_id="oc_chat-1")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.retryable is False
    assert "provider_write_attempted" not in result.raw_response
    rejection = result.raw_response["provider_rejection"]
    assert rejection["provider"] == "Feishu"
    assert rejection["status"] == 200
    assert rejection["body_sha256"] in result.error
    assert rejection["truncated"] is True
    assert rejection["redacted"] is True
    assert secret not in rejection["body_preview"]
    assert secret not in result.error
    assert adapter._semantic_http_post.call_count == 2


@pytest.mark.asyncio
async def test_feishu_connection_reset_after_message_write_is_ambiguous():
    adapter = _feishu_adapter()
    token_response = (
        200,
        {},
        json.dumps(
            {
                "code": 0,
                "tenant_access_token": "tenant-token",
            }
        ).encode(),
    )
    adapter._semantic_http_post.side_effect = [
        token_response,
        ConnectionResetError("response lost"),
    ]
    request = _request(adapter, chat_id="oc_chat-1")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.retryable is False
    assert result.raw_response is None
    assert adapter._semantic_http_post.call_count == 2


def test_feishu_http_primitive_refuses_redirects(monkeypatch):
    opened = {}

    class _Response:
        headers = {"Content-Type": "application/json"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def getcode(self):
            return 200

        def read(self):
            return b"{}"

    class _Opener:
        def open(self, request, timeout):
            opened["request"] = request
            opened["timeout"] = timeout
            return _Response()

    def _build(handler):
        opened["handler"] = handler
        return _Opener()

    monkeypatch.setattr(
        "plugins.platforms.feishu.adapter.build_opener",
        _build,
    )

    status, _, body = FeishuAdapter._semantic_http_post(
        "https://open.feishu.cn/exact",
        headers={"Content-Type": "application/json"},
        body=b'{"text":"exact"}',
    )

    assert status == 200
    assert body == b"{}"
    assert (
        opened["handler"].redirect_request(
            None, None, 302, "redirect", {}, "https://evil.test"
        )
        is None
    )
    assert opened["request"].data == b'{"text":"exact"}'


@pytest.mark.asyncio
async def test_google_chat_exact_body_reaches_one_create_execute_unchanged():
    adapter = _google_adapter()
    request = _request(
        adapter,
        chat_id="spaces/space-1",
        content="Literal **Google Chat** text\nsecond line 🛰️",
        thread_id="spaces/space-1/threads/thread-1",
        reply_to="spaces/space-1/messages/inbound-1",
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id == "spaces/space-1/messages/message-exact"
    assert len(adapter._chat_api.create_calls) == 1
    assert len(adapter._chat_api.execute_calls) == 1
    create = adapter._chat_api.create_calls[0]
    assert create["parent"] == request.chat_id
    assert create["body"] == {
        "text": request.content,
        "thread": {"name": request.thread_id},
    }
    assert create["messageReplyOption"] == "REPLY_MESSAGE_OR_FAIL"
    assert create["messageId"].startswith("client-")
    assert adapter._chat_api.execute_calls[0]["num_retries"] == 0


def test_google_chat_http_wrapper_forces_zero_redirects():
    delegate = MagicMock()
    delegate.request.return_value = ("response", b"body")
    wrapper = _GoogleChatSemanticNoRedirectHttp(delegate)

    result = wrapper.request(
        "https://chat.googleapis.com/v1/spaces/x/messages",
        method="POST",
        body=b"{}",
        headers={"Content-Type": "application/json"},
        redirections=9,
    )

    assert result == ("response", b"body")
    assert delegate.request.call_args.kwargs["redirections"] == 0


def test_google_chat_exact_auth_transport_disables_401_replay(monkeypatch):
    captured = {}

    class _HttpLib:
        @staticmethod
        def Http(*, timeout):
            captured["timeout"] = timeout
            return object()

    def _authorized(credentials, *, http, max_refresh_attempts):
        captured["credentials"] = credentials
        captured["http"] = http
        captured["max_refresh_attempts"] = max_refresh_attempts
        return MagicMock()

    monkeypatch.setattr(google_chat_module, "httplib2", _HttpLib)
    monkeypatch.setattr(google_chat_module, "AuthorizedHttp", _authorized)
    adapter = object.__new__(GoogleChatAdapter)
    adapter._credentials = object()

    transport = adapter._new_semantic_exact_authed_http()

    assert isinstance(transport, _GoogleChatSemanticNoRedirectHttp)
    assert captured["credentials"] is adapter._credentials
    assert captured["timeout"] == 30
    assert captured["max_refresh_attempts"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "retryable", "attempted_flag"),
    [
        (429, True, False),
        (503, False, None),
        (408, False, None),
        (400, False, False),
        (302, False, False),
    ],
)
async def test_google_chat_exact_classifies_one_execute_without_retry(
    status,
    retryable,
    attempted_flag,
):
    adapter = _google_adapter(
        error=_GoogleError(status, headers={"Retry-After": "2"})
    )
    request = _request(adapter, chat_id="spaces/space-1")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.retryable is retryable
    assert len(adapter._chat_api.execute_calls) == 1
    if attempted_flag is None:
        assert "provider_write_attempted" not in (result.raw_response or {})
    else:
        assert result.raw_response["provider_write_attempted"] is attempted_flag


@pytest.mark.asyncio
async def test_google_chat_malformed_receipt_is_ambiguous():
    adapter = _google_adapter(response={"name": ""})
    request = _request(adapter, chat_id="spaces/space-1")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    rejection = result.raw_response["provider_rejection"]
    assert rejection["schema_version"] == (
        "hermes.provider-rejection-evidence/1"
    )
    assert rejection["provider"] == "Google Chat"
    assert rejection["status"] == 200
    assert rejection["body_preview"] == '{"name":""}'
    assert rejection["truncated"] is False
    assert len(adapter._chat_api.execute_calls) == 1


@pytest.mark.asyncio
async def test_google_chat_connection_reset_after_execute_is_ambiguous():
    adapter = _google_adapter(error=ConnectionResetError("response lost"))
    request = _request(adapter, chat_id="spaces/space-1")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.retryable is False
    assert result.raw_response is None
    assert len(adapter._chat_api.execute_calls) == 1


@pytest.mark.asyncio
async def test_simplex_exact_body_reaches_one_correlated_structured_frame():
    adapter = _simplex_adapter()
    request = _request(
        adapter,
        chat_id="42",
        content="Exact SimpleX text\n第二行 🛰️",
    )
    adapter._ws.response = _simplex_receipt(text=request.content)

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id == "731"
    assert result.raw_response["gateway_acceptance"] is True
    adapter._ws.send.assert_awaited_once()
    frame = adapter._ws.frames[0]
    assert frame["corrId"].startswith("hermes-semantic-")
    assert frame["cmd"].startswith("/_send @42 json ")
    composed = json.loads(frame["cmd"].split(" json ", 1)[1])
    assert composed == [
        {"msgContent": {"type": "text", "text": request.content}}
    ]


@pytest.mark.asyncio
async def test_simplex_exact_group_uses_numeric_group_route_and_real_item_id():
    adapter = _simplex_adapter(
        _simplex_receipt(
            item_id=881,
            target_id=99,
            target_type="group",
            text="Group exact",
            status_type="sndSent",
        )
    )
    request = _request(
        adapter,
        chat_id="group:99",
        content="Group exact",
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id == "881"
    assert result.raw_response["gateway_acceptance"] is False
    assert adapter._ws.frames[0]["cmd"].startswith(
        "/_send #99 json "
    )


@pytest.mark.asyncio
async def test_simplex_correlated_command_error_is_definite_rejection():
    adapter = _simplex_adapter(
        {
            "type": "chatCmdError",
            "chatError": {"type": "error"},
        }
    )
    request = _request(adapter, chat_id="42")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.retryable is False
    assert result.raw_response["provider_write_attempted"] is False
    rejection = result.raw_response["provider_rejection"]
    assert rejection["schema_version"] == (
        "hermes.provider-protocol-rejection-evidence/1"
    )
    assert rejection["provider"] == "SimpleX"
    assert rejection["protocol"] == "simplex-json-websocket"
    assert '"type":"chatCmdError"' in rejection["response_preview"]
    assert rejection["response_sha256"] in result.error
    adapter._ws.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_simplex_connection_loss_after_frame_is_ambiguous():
    adapter = _simplex_adapter(error=ConnectionResetError("lost"))
    request = _request(adapter, chat_id="42")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.retryable is False
    assert result.raw_response is None
    adapter._ws.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_simplex_malformed_correlated_receipt_is_ambiguous():
    adapter = _simplex_adapter(
        {"type": "newChatItems", "chatItems": []}
    )
    request = _request(adapter, chat_id="42")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.retryable is False
    assert "provider_write_attempted" not in result.raw_response
    adapter._ws.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_simplex_ack_timeout_is_ambiguous_and_never_resends(
    monkeypatch,
):
    adapter = _simplex_adapter()
    adapter._ws.response = None
    monkeypatch.setattr(
        simplex_module,
        "SIMPLEX_EXACT_ACK_TIMEOUT_SECONDS",
        0.01,
    )
    request = _request(adapter, chat_id="42")

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.retryable is False
    assert result.raw_response is None
    adapter._ws.send.assert_awaited_once()
    assert adapter._pending_responses == {}


def test_wecom_callback_route_binds_app_not_secret_or_ephemeral_map():
    adapter = _callback_adapter()

    route = adapter.bind_semantic_exact_attempt_provider_route(
        chat_id="corpB:alice"
    )
    adapter._user_app_map.clear()

    assert route == {"app": "support-app", "corp_id": "corpB"}
    assert "secret" not in repr(route)
    assert "token" not in repr(route)


@pytest.mark.asyncio
async def test_wecom_callback_exact_uses_persisted_app_and_one_post():
    adapter = _callback_adapter()
    route = adapter.bind_semantic_exact_attempt_provider_route(
        chat_id="corpB:alice"
    )
    adapter._user_app_map.clear()
    request = _request(
        adapter,
        chat_id="corpB:alice",
        content="Exact callback body\n第二行",
        provider_route=route,
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is True
    assert result.message_id == "wecom-callback-exact"
    adapter._get_access_token.assert_awaited_once()
    adapter._http_client.post.assert_awaited_once()
    _, kwargs = adapter._http_client.post.await_args
    assert kwargs["json"] == {
        "touser": "alice",
        "msgtype": "text",
        "agentid": 2002,
        "text": {"content": request.content},
        "safe": 0,
    }
    assert kwargs["follow_redirects"] is False


@pytest.mark.parametrize(
    ("receipt_id", "accepted"),
    [
        ("  callback-" + ("訊" * 2_048) + "-receipt  ", True),
        (7, False),
        ("callback\u0085control", False),
        ("\ud800", False),
    ],
)
@pytest.mark.asyncio
async def test_wecom_callback_receipt_preserves_safe_unicode_and_rejects_invalid(
    receipt_id,
    accepted: bool,
):
    adapter = _callback_adapter(
        _CallbackResponse(
            200,
            {"errcode": 0, "msgid": receipt_id},
        )
    )
    route = adapter.bind_semantic_exact_attempt_provider_route(
        chat_id="corpB:alice"
    )

    result = await send_via_exact_adapter_method(
        adapter,
        _request(
            adapter,
            chat_id="corpB:alice",
            provider_route=route,
        ),
    )

    assert result.success is accepted
    if accepted:
        assert result.message_id == receipt_id
    else:
        rejection = result.raw_response["provider_rejection"]
        assert rejection["provider"] == "WeCom Callback"
        assert rejection["status"] == 200
        assert rejection["body_sha256"] in result.error
    adapter._http_client.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_wecom_callback_token_rejection_defers_refresh_to_next_attempt():
    adapter = _callback_adapter(
        _CallbackResponse(
            200,
            {"errcode": 40001, "errmsg": "invalid credential"},
        )
    )
    adapter._access_tokens["support-app"] = {
        "token": "stale",
        "expires_at": 9_999_999_999,
    }
    route = adapter.bind_semantic_exact_attempt_provider_route(
        chat_id="corpB:alice"
    )
    request = _request(
        adapter,
        chat_id="corpB:alice",
        provider_route=route,
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.retryable is True
    assert result.raw_response["provider_write_attempted"] is False
    assert "support-app" not in adapter._access_tokens
    adapter._http_client.post.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "retryable", "attempted_flag"),
    [
        (
            _CallbackResponse(
                429,
                {"errcode": 45009},
                headers={"Retry-After": "4"},
            ),
            True,
            False,
        ),
        (_CallbackResponse(503, {"errcode": 0}), False, None),
        (_CallbackResponse(400, {"errcode": 60020}), False, False),
        (
            _CallbackResponse(
                200,
                json_error=ValueError("not json"),
            ),
            False,
            None,
        ),
    ],
)
async def test_wecom_callback_classifies_one_post_without_retry(
    response,
    retryable,
    attempted_flag,
):
    adapter = _callback_adapter(response)
    route = adapter.bind_semantic_exact_attempt_provider_route(
        chat_id="corpB:alice"
    )
    request = _request(
        adapter,
        chat_id="corpB:alice",
        provider_route=route,
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.retryable is retryable
    adapter._http_client.post.assert_awaited_once()
    if attempted_flag is None:
        assert "provider_write_attempted" not in (result.raw_response or {})
    else:
        assert result.raw_response["provider_write_attempted"] is attempted_flag


@pytest.mark.asyncio
async def test_wecom_callback_connection_reset_after_post_is_ambiguous():
    adapter = _callback_adapter()
    adapter._http_client.post.side_effect = ConnectionResetError(
        "response lost"
    )
    route = adapter.bind_semantic_exact_attempt_provider_route(
        chat_id="corpB:alice"
    )
    request = _request(
        adapter,
        chat_id="corpB:alice",
        provider_route=route,
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.retryable is False
    assert result.raw_response is None
    adapter._http_client.post.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("factory", "chat_id", "route"),
    [
        (_feishu_adapter, "oc_chat-1", None),
        (_google_adapter, "spaces/space-1", None),
        (
            _callback_adapter,
            "corpB:alice",
            {"app": "support-app", "corp_id": "corpB"},
        ),
        (_simplex_adapter, "42", None),
    ],
)
async def test_wave4_oversize_rejects_before_provider_message_write(
    factory,
    chat_id,
    route,
):
    adapter = factory()
    budget = adapter.SEMANTIC_EXACT_ATTEMPT_CAPABILITY.max_logical_units
    request = _request(
        adapter,
        chat_id=chat_id,
        content="x" * (budget + 1),
        provider_route=route,
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.raw_response["provider_write_attempted"] is False
    if isinstance(adapter, FeishuAdapter):
        adapter._semantic_http_post.assert_not_called()
    elif isinstance(adapter, GoogleChatAdapter):
        assert adapter._chat_api.execute_calls == []
    elif isinstance(adapter, SimplexAdapter):
        adapter._ws.send.assert_not_awaited()
    else:
        adapter._get_access_token.assert_not_awaited()
        adapter._http_client.post.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("factory", "chat_id", "route"),
    [
        (_feishu_adapter, "oc_chat-1", None),
        (_google_adapter, "spaces/space-1", None),
        (
            _callback_adapter,
            "corpB:alice",
            {"app": "support-app", "corp_id": "corpB"},
        ),
        (_simplex_adapter, "42", None),
    ],
)
async def test_wave4_historical_encoder_rejects_before_adapter_write(
    factory,
    chat_id,
    route,
):
    adapter = factory()
    request = _request(
        adapter,
        chat_id=chat_id,
        provider_route=route,
    )
    request = replace(
        request,
        encoding_contract=replace(
            request.encoding_contract,
            wire_encoding="retired-wire-v0",
        ),
    )

    with pytest.raises(ValueError, match="encoding unsupported"):
        await send_via_exact_adapter_method(adapter, request)

    if isinstance(adapter, FeishuAdapter):
        adapter._semantic_http_post.assert_not_called()
    elif isinstance(adapter, GoogleChatAdapter):
        assert adapter._chat_api.execute_calls == []
    elif isinstance(adapter, SimplexAdapter):
        adapter._ws.send.assert_not_awaited()
    else:
        adapter._get_access_token.assert_not_awaited()
        adapter._http_client.post.assert_not_awaited()


@pytest.mark.asyncio
async def test_wecom_callback_wrong_persisted_app_rejects_before_token():
    adapter = _callback_adapter()
    request = _request(
        adapter,
        chat_id="corpB:alice",
        provider_route={"app": "sales-app", "corp_id": "corpB"},
    )

    result = await send_via_exact_adapter_method(adapter, request)

    assert result.success is False
    assert result.raw_response["provider_write_attempted"] is False
    adapter._get_access_token.assert_not_awaited()
    adapter._http_client.post.assert_not_awaited()
