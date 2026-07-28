"""Tests for _send_mattermost, _send_matrix, _send_homeassistant, _send_dingtalk."""

import asyncio
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

# ``_send_dingtalk`` and ``_send_matrix`` moved into their bundled plugins
# (``plugins/platforms/<x>/adapter.py::_standalone_send``) in #41112. Keep
# thin pre-migration-shaped shims so existing test bodies work unchanged.
from plugins.platforms.dingtalk.adapter import (
    _standalone_send as _dingtalk_standalone_send,
)
from plugins.platforms.matrix.adapter import (
    _standalone_send as _matrix_standalone_send,
    register as _register_matrix,
)


async def _send_dingtalk(extra, chat_id, message):
    """Pre-migration ``(extra, chat_id, message)`` shim around the dingtalk
    plugin's ``_standalone_send(pconfig, chat_id, message)``."""
    pconfig = SimpleNamespace(token=None, extra=extra or {})
    return await _dingtalk_standalone_send(pconfig, chat_id, message)


async def _send_matrix(token, extra, chat_id, message):
    """Pre-migration ``(token, extra, chat_id, message)`` shim around the matrix
    plugin's ``_standalone_send(pconfig, chat_id, message)``."""
    pconfig = SimpleNamespace(token=token, extra=extra or {})
    return await _matrix_standalone_send(pconfig, chat_id, message)


async def _send_matrix_semantic(
    resp,
    *,
    message="exact matrix message",
    media_files=None,
):
    """Exercise Matrix's registry-shaped exact standalone entrypoint."""
    from hermes_cli.semantic_delivery import SEMANTIC_DELIVERY_CONTRACT

    session_ctx, session = _make_aiohttp_session(resp)
    pconfig = SimpleNamespace(
        token="syt_semantic",
        extra={"homeserver": "https://matrix.example.com"},
    )
    with patch("aiohttp.ClientSession", return_value=session_ctx):
        result = await _matrix_standalone_send(
            pconfig,
            "!room:example.com",
            message,
            delivery_contract=SEMANTIC_DELIVERY_CONTRACT,
            delivery_id="delivery-matrix-standalone-exact",
            delivery_target="matrix:!room:example.com",
            delivery_unit=0,
            semantic_exact_attempt=True,
            media_files=media_files,
        )
    return result, session

# ``_send_mattermost`` moved into the mattermost plugin
# (``plugins/platforms/mattermost/adapter.py::_standalone_send``).  Keep a
# thin ``(token, extra, chat_id, message)``-shaped wrapper so existing test
# bodies continue to work without rewriting every signature.
from plugins.platforms.mattermost.adapter import (
    _standalone_send as _mattermost_standalone_send,
)


async def _send_mattermost(token, extra, chat_id, message):
    """Pre-migration ``(token, extra, chat_id, message)`` shim around the
    plugin's ``_standalone_send(pconfig, chat_id, message)``.
    """
    pconfig = SimpleNamespace(token=token, extra=extra or {})
    return await _mattermost_standalone_send(pconfig, chat_id, message)


# ``_send_homeassistant`` moved into the homeassistant plugin
# (``plugins/platforms/homeassistant/adapter.py::_standalone_send``).  Same
# shim pattern as ``_send_mattermost`` above.
from plugins.platforms.homeassistant.adapter import (
    _standalone_send as _homeassistant_standalone_send,
)


async def _send_homeassistant(token, extra, chat_id, message):
    """Pre-migration ``(token, extra, chat_id, message)`` shim around the
    plugin's ``_standalone_send(pconfig, chat_id, message)``.
    """
    pconfig = SimpleNamespace(token=token, extra=extra or {})
    return await _homeassistant_standalone_send(pconfig, chat_id, message)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_aiohttp_resp(status, json_data=None, text_data=None):
    """Build a minimal async-context-manager mock for an aiohttp response."""
    resp = AsyncMock()
    resp.status = status
    resp.json = AsyncMock(return_value=json_data or {})
    resp.text = AsyncMock(return_value=text_data or "")
    return resp


def _make_aiohttp_session(resp):
    """Wrap a response mock in a session mock that supports async-with for post/put."""
    request_ctx = MagicMock()
    request_ctx.__aenter__ = AsyncMock(return_value=resp)
    request_ctx.__aexit__ = AsyncMock(return_value=False)

    session = MagicMock()
    session.post = MagicMock(return_value=request_ctx)
    session.put = MagicMock(return_value=request_ctx)

    session_ctx = MagicMock()
    session_ctx.__aenter__ = AsyncMock(return_value=session)
    session_ctx.__aexit__ = AsyncMock(return_value=False)
    return session_ctx, session


# ---------------------------------------------------------------------------
# _send_mattermost
# ---------------------------------------------------------------------------


class TestSendMattermost:
    def test_success(self):
        resp = _make_aiohttp_resp(201, json_data={"id": "post123"})
        session_ctx, session = _make_aiohttp_session(resp)

        with patch("aiohttp.ClientSession", return_value=session_ctx), \
             patch.dict(os.environ, {"MATTERMOST_URL": "", "MATTERMOST_TOKEN": ""}, clear=False):
            extra = {"url": "https://mm.example.com"}
            result = asyncio.run(_send_mattermost("tok-abc", extra, "channel1", "hello"))

        assert result == {"success": True, "platform": "mattermost", "chat_id": "channel1", "message_id": "post123"}
        session.post.assert_called_once()
        call_kwargs = session.post.call_args
        assert call_kwargs[0][0] == "https://mm.example.com/api/v4/posts"
        assert call_kwargs[1]["headers"]["Authorization"] == "Bearer tok-abc"
        assert call_kwargs[1]["json"] == {"channel_id": "channel1", "message": "hello"}

    def test_http_error(self):
        resp = _make_aiohttp_resp(400, text_data="Bad Request")
        session_ctx, _ = _make_aiohttp_session(resp)

        with patch("aiohttp.ClientSession", return_value=session_ctx):
            result = asyncio.run(_send_mattermost(
                "tok", {"url": "https://mm.example.com"}, "ch", "hi"
            ))

        assert "error" in result
        assert "400" in result["error"]
        assert "Bad Request" in result["error"]

    def test_missing_config(self):
        with patch.dict(os.environ, {"MATTERMOST_URL": "", "MATTERMOST_TOKEN": ""}, clear=False):
            result = asyncio.run(_send_mattermost("", {}, "ch", "hi"))

        assert "error" in result
        assert "MATTERMOST_URL" in result["error"] or "not configured" in result["error"]

    def test_env_var_fallback(self):
        resp = _make_aiohttp_resp(200, json_data={"id": "p99"})
        session_ctx, session = _make_aiohttp_session(resp)

        with patch("aiohttp.ClientSession", return_value=session_ctx), \
             patch.dict(os.environ, {"MATTERMOST_URL": "https://mm.env.com", "MATTERMOST_TOKEN": "env-tok"}, clear=False):
            result = asyncio.run(_send_mattermost("", {}, "ch", "hi"))

        assert result["success"] is True
        call_kwargs = session.post.call_args
        assert "https://mm.env.com" in call_kwargs[0][0]
        assert call_kwargs[1]["headers"]["Authorization"] == "Bearer env-tok"


# ---------------------------------------------------------------------------
# _send_matrix
# ---------------------------------------------------------------------------


class TestSendMatrix:
    def test_success(self):
        resp = _make_aiohttp_resp(200, json_data={"event_id": "$abc123"})
        session_ctx, session = _make_aiohttp_session(resp)

        with patch("aiohttp.ClientSession", return_value=session_ctx), \
             patch.dict(os.environ, {"MATRIX_HOMESERVER": "", "MATRIX_ACCESS_TOKEN": ""}, clear=False):
            extra = {"homeserver": "https://matrix.example.com"}
            result = asyncio.run(_send_matrix("syt_tok", extra, "!room:example.com", "hello matrix"))

        assert result == {
            "success": True,
            "platform": "matrix",
            "chat_id": "!room:example.com",
            "message_id": "$abc123",
        }
        session.put.assert_called_once()
        call_kwargs = session.put.call_args
        url = call_kwargs[0][0]
        assert url.startswith("https://matrix.example.com/_matrix/client/v3/rooms/%21room%3Aexample.com/send/m.room.message/")
        assert call_kwargs[1]["headers"]["Authorization"] == "Bearer syt_tok"
        payload = call_kwargs[1]["json"]
        assert payload["msgtype"] == "m.text"
        assert payload["body"] == "hello matrix"

    def test_http_error(self):
        resp = _make_aiohttp_resp(403, text_data="Forbidden")
        session_ctx, _ = _make_aiohttp_session(resp)

        with patch("aiohttp.ClientSession", return_value=session_ctx):
            result = asyncio.run(_send_matrix(
                "tok", {"homeserver": "https://matrix.example.com"},
                "!room:example.com", "hi"
            ))

        assert "error" in result
        assert "403" in result["error"]
        assert "Forbidden" in result["error"]

    def test_missing_config(self):
        with patch.dict(os.environ, {"MATRIX_HOMESERVER": "", "MATRIX_ACCESS_TOKEN": ""}, clear=False):
            result = asyncio.run(_send_matrix("", {}, "!room:example.com", "hi"))

        assert "error" in result
        assert "MATRIX_HOMESERVER" in result["error"] or "not configured" in result["error"]

    def test_env_var_fallback(self):
        resp = _make_aiohttp_resp(200, json_data={"event_id": "$ev1"})
        session_ctx, session = _make_aiohttp_session(resp)

        with patch("aiohttp.ClientSession", return_value=session_ctx), \
             patch.dict(os.environ, {
                 "MATRIX_HOMESERVER": "https://matrix.env.com",
                 "MATRIX_ACCESS_TOKEN": "env-tok",
             }, clear=False):
            result = asyncio.run(_send_matrix("", {}, "!r:env.com", "hi"))

        assert result["success"] is True
        url = session.put.call_args[0][0]
        assert "matrix.env.com" in url

    def test_txn_id_is_unique_across_calls(self):
        """Each call should generate a distinct transaction ID in the URL."""
        txn_ids = []

        def capture(*args, **kwargs):
            url = args[0]
            txn_ids.append(url.rsplit("/", 1)[-1])
            ctx = MagicMock()
            ctx.__aenter__ = AsyncMock(return_value=_make_aiohttp_resp(200, json_data={"event_id": "$x"}))
            ctx.__aexit__ = AsyncMock(return_value=False)
            return ctx

        session = MagicMock()
        session.put = capture
        session_ctx = MagicMock()
        session_ctx.__aenter__ = AsyncMock(return_value=session)
        session_ctx.__aexit__ = AsyncMock(return_value=False)

        extra = {"homeserver": "https://matrix.example.com"}

        import time
        with patch("aiohttp.ClientSession", return_value=session_ctx):
            asyncio.run(_send_matrix("tok", extra, "!r:example.com", "first"))
        time.sleep(0.002)
        with patch("aiohttp.ClientSession", return_value=session_ctx):
            asyncio.run(_send_matrix("tok", extra, "!r:example.com", "second"))

        assert len(txn_ids) == 2
        assert txn_ids[0] != txn_ids[1]


class TestSendMatrixSemanticExact:
    def test_media_is_rejected_before_provider_write(self):
        resp = _make_aiohttp_resp(
            200,
            json_data={"event_id": "$must-not-send"},
        )

        result, session = asyncio.run(
            _send_matrix_semantic(
                resp,
                media_files=["/tmp/semantic-attachment.png"],
            )
        )

        assert result == {
            "error": "semantic_delivery_media_shape_unsupported",
            "provider_write_attempted": False,
            "provider_retryable": False,
        }
        session.put.assert_not_called()

    def test_preserves_native_event_id_exactly_without_generic_cap(self):
        event_id = "$" + ("訊" * 10_000) + ":example.com"
        resp = _make_aiohttp_resp(
            200,
            json_data={"event_id": event_id},
        )

        result, session = asyncio.run(_send_matrix_semantic(resp))

        assert result == {
            "success": True,
            "platform": "matrix",
            "chat_id": "!room:example.com",
            "message_id": event_id,
        }
        assert session.put.call_count == 1
        assert "/dh_" in session.put.call_args.args[0]
        assert (
            session.put.call_args.kwargs["allow_redirects"] is False
        )

    def test_rejects_non_native_or_malformed_matrix_event_ids(self):
        class StringSubclass(str):
            pass

        invalid_event_ids = (
            None,
            42,
            "$",
            "event-without-dollar",
            "$event with space",
            "$event\x00control",
            "$event\ud800surrogate",
            StringSubclass("$subclass"),
        )
        for event_id in invalid_event_ids:
            response_body = '{"event_id":"malformed"}'
            resp = _make_aiohttp_resp(
                200,
                json_data={"event_id": event_id},
                text_data=response_body,
            )

            result, session = asyncio.run(_send_matrix_semantic(resp))

            assert (
                result["error"]
                == "semantic_delivery_provider_receipt_invalid"
            )
            assert result["provider_retryable"] is False
            assert result.get("provider_write_attempted") is not False
            rejection = result["provider_rejection"]
            assert (
                rejection["schema_version"]
                == "hermes.provider-rejection-evidence/1"
            )
            assert rejection["provider"] == "Matrix"
            assert rejection["status"] == 200
            assert session.put.call_count == 1

    def test_malformed_2xx_receipt_evidence_is_bounded_and_redacted(self):
        secret = "ghp_" + ("m" * 80)
        response_body = (
            '{"token":"'
            + secret
            + '","detail":"'
            + ("malformed Matrix receipt " * 40)
            + '"}'
        )
        resp = _make_aiohttp_resp(
            200,
            json_data={"event_id": None},
            text_data=response_body,
        )

        result, _ = asyncio.run(_send_matrix_semantic(resp))

        rejection = result["provider_rejection"]
        assert rejection["body_bytes"] == len(response_body.encode("utf-8"))
        assert rejection["truncated"] is True
        assert rejection["redacted"] is True
        assert secret not in rejection["body_preview"]

    def test_non_2xx_rejection_has_safe_structured_evidence(self):
        secret = "ghp_" + ("n" * 80)
        response_body = (
            '{"token":"'
            + secret
            + '","detail":"'
            + ("Matrix rejected request " * 40)
            + '"}'
        )
        resp = _make_aiohttp_resp(
            403,
            text_data=response_body,
        )

        result, session = asyncio.run(_send_matrix_semantic(resp))

        assert result["provider_write_attempted"] is False
        assert result["provider_retryable"] is False
        assert secret not in result["error"]
        assert response_body not in result["error"]
        rejection = result["provider_rejection"]
        assert (
            rejection["schema_version"]
            == "hermes.provider-rejection-evidence/1"
        )
        assert rejection["status"] == 403
        assert rejection["body_sha256"] in result["error"]
        assert rejection["body_bytes"] == len(response_body.encode("utf-8"))
        assert rejection["truncated"] is True
        assert rejection["redacted"] is True
        assert secret not in rejection["body_preview"]
        assert session.put.call_count == 1

    def test_http_outcome_ambiguity_is_not_marked_retryable(self):
        response_body = '{"errcode":"M_UNKNOWN","error":"upstream lost"}'
        for status in (408, 503):
            resp = _make_aiohttp_resp(
                status,
                text_data=response_body,
            )

            result, session = asyncio.run(_send_matrix_semantic(resp))

            assert result["provider_retryable"] is False
            assert "provider_write_attempted" not in result
            rejection = result["provider_rejection"]
            assert rejection["status"] == status
            assert rejection["body_sha256"] in result["error"]
            assert session.put.call_count == 1

    def test_transport_ambiguity_is_not_marked_retryable(self):
        session_ctx, session = _make_aiohttp_session(
            _make_aiohttp_resp(200)
        )
        session.put.side_effect = ConnectionResetError("lost after PUT")
        pconfig = SimpleNamespace(
            token="syt_semantic",
            extra={"homeserver": "https://matrix.example.com"},
        )
        from hermes_cli.semantic_delivery import SEMANTIC_DELIVERY_CONTRACT

        with patch("aiohttp.ClientSession", return_value=session_ctx):
            result = asyncio.run(
                _matrix_standalone_send(
                    pconfig,
                    "!room:example.com",
                    "exact matrix message",
                    delivery_contract=SEMANTIC_DELIVERY_CONTRACT,
                    delivery_id="delivery-matrix-transport-loss",
                    delivery_target="matrix:!room:example.com",
                    delivery_unit=0,
                    semantic_exact_attempt=True,
                )
            )

        assert result["provider_retryable"] is False
        assert "provider_write_attempted" not in result
        rejection = result["provider_rejection"]
        assert (
            rejection["schema_version"]
            == "hermes.provider-protocol-rejection-evidence/1"
        )
        assert rejection["protocol"] == "matrix-client-server-http"
        assert session.put.call_count == 1

    def test_registry_declaration_matches_standalone_sender_contract(self):
        ctx = SimpleNamespace(register_platform=MagicMock())

        _register_matrix(ctx)

        registration = ctx.register_platform.call_args.kwargs
        assert registration["standalone_sender_fn"] is _matrix_standalone_send
        assert registration["semantic_exact_attempt"] is True
        assert registration["live_semantic_exact_attempt"] is True


# ---------------------------------------------------------------------------
# _send_homeassistant
# ---------------------------------------------------------------------------


class TestSendHomeAssistant:
    def test_success(self):
        resp = _make_aiohttp_resp(200)
        session_ctx, session = _make_aiohttp_session(resp)

        with patch("aiohttp.ClientSession", return_value=session_ctx), \
             patch.dict(os.environ, {"HASS_URL": "", "HASS_TOKEN": ""}, clear=False):
            extra = {"url": "https://hass.example.com"}
            result = asyncio.run(_send_homeassistant("hass-tok", extra, "mobile_app_phone", "alert!"))

        assert result == {"success": True, "platform": "homeassistant", "chat_id": "mobile_app_phone"}
        session.post.assert_called_once()
        call_kwargs = session.post.call_args
        assert call_kwargs[0][0] == "https://hass.example.com/api/services/notify/notify"
        assert call_kwargs[1]["headers"]["Authorization"] == "Bearer hass-tok"
        assert call_kwargs[1]["json"] == {"message": "alert!", "target": "mobile_app_phone"}

    def test_http_error(self):
        resp = _make_aiohttp_resp(401, text_data="Unauthorized")
        session_ctx, _ = _make_aiohttp_session(resp)

        with patch("aiohttp.ClientSession", return_value=session_ctx):
            result = asyncio.run(_send_homeassistant(
                "bad-tok", {"url": "https://hass.example.com"},
                "target", "msg"
            ))

        assert "error" in result
        assert "401" in result["error"]
        assert "Unauthorized" in result["error"]

    def test_missing_config(self):
        with patch.dict(os.environ, {"HASS_URL": "", "HASS_TOKEN": ""}, clear=False):
            result = asyncio.run(_send_homeassistant("", {}, "target", "msg"))

        assert "error" in result
        assert "HASS_URL" in result["error"] or "not configured" in result["error"]

    def test_env_var_fallback(self):
        resp = _make_aiohttp_resp(200)
        session_ctx, session = _make_aiohttp_session(resp)

        with patch("aiohttp.ClientSession", return_value=session_ctx), \
             patch.dict(os.environ, {"HASS_URL": "https://hass.env.com", "HASS_TOKEN": "env-tok"}, clear=False):
            result = asyncio.run(_send_homeassistant("", {}, "notify_target", "hi"))

        assert result["success"] is True
        url = session.post.call_args[0][0]
        assert "hass.env.com" in url


# ---------------------------------------------------------------------------
# _send_dingtalk
# ---------------------------------------------------------------------------


class TestSendDingtalk:
    def _make_httpx_resp(self, status_code=200, json_data=None):
        resp = MagicMock()
        resp.status_code = status_code
        resp.json = MagicMock(return_value=json_data or {"errcode": 0, "errmsg": "ok"})
        resp.raise_for_status = MagicMock()
        return resp

    def _make_httpx_client(self, resp):
        client = AsyncMock()
        client.post = AsyncMock(return_value=resp)
        client_ctx = MagicMock()
        client_ctx.__aenter__ = AsyncMock(return_value=client)
        client_ctx.__aexit__ = AsyncMock(return_value=False)
        return client_ctx, client

    def test_success(self):
        resp = self._make_httpx_resp(json_data={"errcode": 0, "errmsg": "ok"})
        client_ctx, client = self._make_httpx_client(resp)

        with patch("httpx.AsyncClient", return_value=client_ctx):
            extra = {"webhook_url": "https://oapi.dingtalk.com/robot/send?access_token=abc"}
            result = asyncio.run(_send_dingtalk(extra, "ignored", "hello dingtalk"))

        assert result == {"success": True, "platform": "dingtalk", "chat_id": "ignored"}
        client.post.assert_awaited_once()
        call_kwargs = client.post.await_args
        assert call_kwargs[0][0] == "https://oapi.dingtalk.com/robot/send?access_token=abc"
        assert call_kwargs[1]["json"] == {"msgtype": "text", "text": {"content": "hello dingtalk"}}

    def test_api_error_in_response_body(self):
        """DingTalk always returns HTTP 200 but signals errors via errcode."""
        resp = self._make_httpx_resp(json_data={"errcode": 310000, "errmsg": "sign not match"})
        client_ctx, _ = self._make_httpx_client(resp)

        with patch("httpx.AsyncClient", return_value=client_ctx):
            result = asyncio.run(_send_dingtalk(
                {"webhook_url": "https://oapi.dingtalk.com/robot/send?access_token=bad"},
                "ch", "hi"
            ))

        assert "error" in result
        assert "sign not match" in result["error"]

    def test_http_error(self):
        """If raise_for_status throws, the error is caught and returned."""
        resp = self._make_httpx_resp(status_code=429)
        resp.raise_for_status = MagicMock(side_effect=Exception("429 Too Many Requests"))
        client_ctx, _ = self._make_httpx_client(resp)

        with patch("httpx.AsyncClient", return_value=client_ctx):
            result = asyncio.run(_send_dingtalk(
                {"webhook_url": "https://oapi.dingtalk.com/robot/send?access_token=tok"},
                "ch", "hi"
            ))

        assert "error" in result
        assert "DingTalk send failed" in result["error"]

    def test_http_error_redacts_access_token_in_exception_text(self):
        token = "supersecret-access-token-123456789"
        resp = self._make_httpx_resp(status_code=401)
        resp.raise_for_status = MagicMock(
            side_effect=Exception(
                f"POST https://oapi.dingtalk.com/robot/send?access_token={token} returned 401"
            )
        )
        client_ctx, _ = self._make_httpx_client(resp)

        with patch("httpx.AsyncClient", return_value=client_ctx):
            result = asyncio.run(
                _send_dingtalk(
                    {"webhook_url": f"https://oapi.dingtalk.com/robot/send?access_token={token}"},
                    "ch",
                    "hi",
                )
            )

        assert "error" in result
        assert token not in result["error"]
        assert "access_token=***" in result["error"]

    def test_missing_config(self):
        with patch.dict(os.environ, {"DINGTALK_WEBHOOK_URL": ""}, clear=False):
            result = asyncio.run(_send_dingtalk({}, "ch", "hi"))

        assert "error" in result
        assert "DINGTALK_WEBHOOK_URL" in result["error"] or "not configured" in result["error"]

    def test_env_var_fallback(self):
        resp = self._make_httpx_resp(json_data={"errcode": 0, "errmsg": "ok"})
        client_ctx, client = self._make_httpx_client(resp)

        with patch("httpx.AsyncClient", return_value=client_ctx), \
             patch.dict(os.environ, {"DINGTALK_WEBHOOK_URL": "https://oapi.dingtalk.com/robot/send?access_token=env"}, clear=False):
            result = asyncio.run(_send_dingtalk({}, "ch", "hi"))

        assert result["success"] is True
        call_kwargs = client.post.await_args
        assert "access_token=env" in call_kwargs[0][0]
