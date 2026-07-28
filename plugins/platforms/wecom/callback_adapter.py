"""WeCom callback-mode adapter for self-built enterprise applications.

Unlike the bot/websocket adapter in ``wecom.py``, this handles the standard
WeCom callback flow: WeCom POSTs encrypted XML to an HTTP endpoint, the
adapter decrypts it, queues the message for the agent, and immediately
acknowledges.  The agent's reply is delivered later via the proactive
``message/send`` API using an access-token.

Supports multiple self-built apps under one gateway instance, scoped by
``corp_id:user_id`` to avoid cross-corp collisions.
"""

from __future__ import annotations

import asyncio
import logging
import socket as _socket
import time
from typing import Any, Dict, List, Optional
# Security: parse untrusted, pre-auth request bodies (WeCom callbacks) with
# defusedxml to block billion-laughs / entity-expansion (and XXE) DoS. The
# parsing API (fromstring) is a drop-in for the stdlib calls used below;
# response-building XML lives in wecom_crypto.py and is not parsed here.
try:
    import defusedxml.ElementTree as ET

    DEFUSEDXML_AVAILABLE = True
except ImportError:
    ET = None  # type: ignore[assignment]
    DEFUSEDXML_AVAILABLE = False

try:
    from aiohttp import web

    AIOHTTP_AVAILABLE = True
except ImportError:
    web = None  # type: ignore[assignment]
    AIOHTTP_AVAILABLE = False

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:
    httpx = None  # type: ignore[assignment]
    HTTPX_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult
from gateway.semantic_exact_attempt import (
    LiveSemanticExactAttemptCapability,
    coerce_live_semantic_exact_attempt_request,
    live_semantic_exact_attempt_provider_route_mapping,
    provider_rejection_error,
    provider_rejection_evidence,
    semantic_exact_attempt_encoding_contract,
)
from plugins.platforms.wecom.wecom_crypto import WXBizMsgCrypt, WeComCryptoError

logger = logging.getLogger(__name__)

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8645
DEFAULT_PATH = "/wecom/callback"
# Cap pre-auth request bodies. WeCom callbacks are small encrypted XML
# envelopes (media is delivered out-of-band via MediaId, never inline), so
# 64 KB is ample for any legitimate message while bounding the work an
# unauthenticated POST can force before signature verification.
_MAX_BODY = 65_536
ACCESS_TOKEN_TTL_SECONDS = 7200
MESSAGE_DEDUP_TTL_SECONDS = 300


def check_wecom_callback_requirements() -> bool:
    return AIOHTTP_AVAILABLE and HTTPX_AVAILABLE and DEFUSEDXML_AVAILABLE


class WecomCallbackAdapter(BasePlatformAdapter):
    SEMANTIC_EXACT_ATTEMPT_CAPABILITY = LiveSemanticExactAttemptCapability(
        provider="wecom_callback",
        contract="hermes-live-semantic-exact-attempt/1",
        segmentation_version="wecom-callback-logical-v1",
        max_logical_units=2048,
        length_semantics="unicode_codepoints",
        wire_encoding="wecom-callback-text-json-v1",
    )

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.WECOM_CALLBACK)
        extra = config.extra or {}
        self._host = str(extra.get("host") or DEFAULT_HOST)
        self._port = int(extra.get("port") or DEFAULT_PORT)
        self._path = str(extra.get("path") or DEFAULT_PATH)
        self._apps: List[Dict[str, Any]] = self._normalize_apps(extra)
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._app: Optional[web.Application] = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._message_queue: asyncio.Queue[MessageEvent] = asyncio.Queue()
        self._poll_task: Optional[asyncio.Task] = None
        self._seen_messages: Dict[str, float] = {}
        self._user_app_map: Dict[str, str] = {}
        self._access_tokens: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # App normalisation
    # ------------------------------------------------------------------

    @staticmethod
    def _user_app_key(corp_id: str, user_id: str) -> str:
        return f"{corp_id}:{user_id}" if corp_id else user_id

    @staticmethod
    def _normalize_apps(extra: Dict[str, Any]) -> List[Dict[str, Any]]:
        apps = extra.get("apps")
        if isinstance(apps, list) and apps:
            return [dict(app) for app in apps if isinstance(app, dict)]
        if extra.get("corp_id"):
            return [
                {
                    "name": extra.get("name") or "default",
                    "corp_id": extra.get("corp_id", ""),
                    "corp_secret": extra.get("corp_secret", ""),
                    "agent_id": str(extra.get("agent_id", "")),
                    "token": extra.get("token", ""),
                    "encoding_aes_key": extra.get("encoding_aes_key", ""),
                }
            ]
        return []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        # ``is_reconnect`` is forwarded by GatewayRunner on every retry per
        # the BasePlatformAdapter.connect contract. Callback adapters have
        # no server-side queue to preserve, so the flag is accepted-and-
        # ignored — but the kwarg MUST be present or the reconnect watcher
        # dies with TypeError and the platform silently stays offline.
        del is_reconnect
        if not self._apps:
            logger.warning("[WecomCallback] No callback apps configured")
            return False
        if not check_wecom_callback_requirements():
            logger.warning("[WecomCallback] aiohttp/httpx not installed")
            return False

        # Quick port-in-use check.
        try:
            with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as sock:
                sock.settimeout(1)
                sock.connect(("127.0.0.1", self._port))
            logger.error("[WecomCallback] Port %d already in use", self._port)
            return False
        except (ConnectionRefusedError, OSError):
            pass

        try:
            # Tighter keepalive so idle CLOSE_WAIT drains promptly (#18451).
            from gateway.platforms._http_client_limits import platform_httpx_limits
            self._http_client = httpx.AsyncClient(timeout=20.0, limits=platform_httpx_limits())
            # client_max_size rejects oversized bodies at the aiohttp layer
            # (413) before our handler — and before any signature work — runs.
            self._app = web.Application(client_max_size=_MAX_BODY)
            self._app.router.add_get("/health", self._handle_health)
            self._app.router.add_get(self._path, self._handle_verify)
            self._app.router.add_post(self._path, self._handle_callback)
            self._runner = web.AppRunner(self._app)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self._host, self._port)
            await self._site.start()
            self._poll_task = asyncio.create_task(self._poll_loop())
            self._mark_connected()
            logger.info(
                "[WecomCallback] HTTP server listening on %s:%s%s",
                self._host, self._port, self._path,
            )
            for app in self._apps:
                try:
                    await self._refresh_access_token(app)
                except Exception as exc:
                    logger.warning(
                        "[WecomCallback] Initial token refresh failed for app '%s': %s",
                        app.get("name", "default"), exc,
                    )
            return True
        except Exception:
            await self._cleanup()
            logger.exception("[WecomCallback] Failed to start")
            return False

    async def disconnect(self) -> None:
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        await self._cleanup()
        self._mark_disconnected()
        logger.info("[WecomCallback] Disconnected")

    async def _cleanup(self) -> None:
        self._site = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        self._app = None
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    # ------------------------------------------------------------------
    # Outbound: proactive send via access-token API
    # ------------------------------------------------------------------

    def bind_semantic_exact_attempt_provider_route(
        self,
        *,
        chat_id: str,
        thread_id: str | None = None,
        reply_to: str | None = None,
    ) -> Dict[str, str]:
        """Freeze the callback app; never rediscover or choose app zero."""

        if thread_id is not None or reply_to is not None:
            raise ValueError("WeCom callback semantic route invalid")
        if (
            not isinstance(chat_id, str)
            or chat_id.count(":") != 1
            or len(chat_id) > 256
        ):
            raise ValueError("WeCom callback semantic route invalid")
        corp_id, user_id = chat_id.split(":", 1)
        if (
            not corp_id
            or not user_id
            or any(
                ord(character) < 32 or ord(character) == 127
                for character in chat_id
            )
        ):
            raise ValueError("WeCom callback semantic route invalid")

        mapped_name = self._user_app_map.get(chat_id)
        app = self._get_app_by_name(mapped_name) if mapped_name else None
        matching = [
            candidate
            for candidate in self._apps
            if str(candidate.get("corp_id") or "") == corp_id
        ]
        if app is None and len(matching) == 1:
            app = matching[0]
        if (
            app is None
            or app not in matching
            or not str(app.get("name") or "").strip()
        ):
            raise LookupError("WeCom callback app route unavailable")
        return {
            "app": str(app["name"]),
            "corp_id": corp_id,
        }

    async def send_semantic_exact_attempt(self, request) -> SendResult:
        """Perform one message/send POST; token repair is next-attempt only."""

        try:
            request = coerce_live_semantic_exact_attempt_request(request)
            provider_route = (
                live_semantic_exact_attempt_provider_route_mapping(
                    request.provider_route
                )
            )
        except (TypeError, ValueError):
            return SendResult(
                success=False,
                error="semantic_delivery_request_invalid",
                raw_response={
                    "provider_write_attempted": False,
                    "provider_retryable": False,
                },
            )
        from hermes_cli.semantic_delivery import (
            SEMANTIC_DELIVERY_CONTRACT,
            exact_provider_message_id,
        )

        capability = self.SEMANTIC_EXACT_ATTEMPT_CAPABILITY
        chat_parts = (
            request.chat_id.split(":", 1)
            if isinstance(request.chat_id, str)
            and request.chat_id.count(":") == 1
            else []
        )
        corp_id = chat_parts[0] if len(chat_parts) == 2 else ""
        user_id = chat_parts[1] if len(chat_parts) == 2 else ""
        app_name = provider_route.get("app", "")
        app = self._get_app_by_name(app_name)
        try:
            request.content.encode("utf-8", errors="strict")
            content_is_utf8 = True
            agent_id = int(str((app or {}).get("agent_id") or ""))
        except (AttributeError, TypeError, UnicodeEncodeError, ValueError):
            content_is_utf8 = False
            agent_id = 0
        if (
            request.delivery_contract != SEMANTIC_DELIVERY_CONTRACT
            or request.encoding_contract
            != semantic_exact_attempt_encoding_contract(capability)
            or set(provider_route) != {"app", "corp_id"}
            or provider_route.get("corp_id") != corp_id
            or app is None
            or str(app.get("corp_id") or "") != corp_id
            or not corp_id
            or not user_id
            or len(request.chat_id) > 256
            or any(
                ord(character) < 32 or ord(character) == 127
                for character in request.chat_id
            )
            or request.thread_id is not None
            or request.reply_to is not None
            or not isinstance(request.content, str)
            or not content_is_utf8
            or not request.content.strip()
            or len(request.content) > capability.max_logical_units
            or agent_id <= 0
            or getattr(self, "_http_client", None) is None
        ):
            return SendResult(
                success=False,
                error="semantic_delivery_message_shape_invalid",
                raw_response={
                    "provider_write_attempted": False,
                    "provider_retryable": False,
                },
            )

        try:
            token = await self._get_access_token(app)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return SendResult(
                success=False,
                error=f"WeCom callback semantic auth failed: {exc}",
                retryable=True,
                raw_response={
                    "provider_write_attempted": False,
                    "provider_retryable": True,
                },
            )
        payload = {
            "touser": user_id,
            "msgtype": "text",
            "agentid": agent_id,
            "text": {"content": request.content},
            "safe": 0,
        }
        try:
            response = await self._http_client.post(
                (
                    "https://qyapi.weixin.qq.com/cgi-bin/message/send"
                    f"?access_token={token}"
                ),
                json=payload,
                timeout=20.0,
                follow_redirects=False,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return SendResult(
                success=False,
                error=f"WeCom callback semantic transport failed: {exc}",
            )

        status = int(response.status_code)
        response_body = getattr(response, "content", None)
        try:
            data = response.json()
        except Exception:
            data = None
        if not isinstance(response_body, (bytes, str)):
            response_body = data
        if 200 <= status < 300:
            errcode = data.get("errcode") if isinstance(data, dict) else None
            if (
                isinstance(errcode, int)
                and not isinstance(errcode, bool)
                and errcode == 0
            ):
                message_id = exact_provider_message_id(
                    data.get("msgid")
                )
                if message_id:
                    return SendResult(
                        success=True,
                        message_id=message_id,
                        raw_response=data,
                    )
                rejection = provider_rejection_evidence(
                    provider="WeCom Callback",
                    status=status,
                    body=response_body,
                )
                return SendResult(
                    success=False,
                    error=provider_rejection_error(rejection),
                    raw_response={
                        "status": status,
                        "provider_rejection": rejection,
                    },
                )
            if errcode in {40001, 42001}:
                rejection = provider_rejection_evidence(
                    provider="WeCom Callback",
                    status=status,
                    body=response_body,
                )
                self._access_tokens.pop(app_name, None)
                return SendResult(
                    success=False,
                    error=provider_rejection_error(rejection),
                    retryable=True,
                    raw_response={
                        "provider_write_attempted": False,
                        "provider_retryable": True,
                        "status": status,
                        "provider_rejection": rejection,
                    },
                )
            if errcode == 45009:
                rejection = provider_rejection_evidence(
                    provider="WeCom Callback",
                    status=status,
                    body=response_body,
                )
                retry_after = (
                    getattr(response, "headers", {}) or {}
                ).get("Retry-After")
                try:
                    retry_after_value = float(retry_after)
                except (TypeError, ValueError):
                    retry_after_value = None
                return SendResult(
                    success=False,
                    error=provider_rejection_error(rejection),
                    retryable=True,
                    retry_after=retry_after_value,
                    raw_response={
                        "provider_write_attempted": False,
                        "provider_retryable": True,
                        "retry_after": retry_after_value,
                        "status": status,
                        "provider_rejection": rejection,
                    },
                )
            if (
                isinstance(errcode, int)
                and not isinstance(errcode, bool)
                and (
                    500 <= errcode < 600
                    or 50_000 <= errcode < 60_000
                )
            ):
                rejection = provider_rejection_evidence(
                    provider="WeCom Callback",
                    status=status,
                    body=response_body,
                )
                return SendResult(
                    success=False,
                    error=provider_rejection_error(rejection),
                    raw_response={
                        "status": status,
                        "provider_rejection": rejection,
                    },
                )
            if isinstance(data, dict) and isinstance(errcode, int):
                rejection = provider_rejection_evidence(
                    provider="WeCom Callback",
                    status=status,
                    body=response_body,
                )
                return SendResult(
                    success=False,
                    error=provider_rejection_error(rejection),
                    raw_response={
                        "provider_write_attempted": False,
                        "provider_retryable": False,
                        "status": status,
                        "provider_rejection": rejection,
                    },
                )
            rejection = provider_rejection_evidence(
                provider="WeCom Callback",
                status=status,
                body=response_body,
            )
            return SendResult(
                success=False,
                error="semantic_delivery_provider_receipt_invalid",
                raw_response={
                    "status": status,
                    "provider_rejection": rejection,
                },
            )
        if status == 429:
            rejection = provider_rejection_evidence(
                provider="WeCom Callback",
                status=status,
                body=response_body,
            )
            retry_after = (
                getattr(response, "headers", {}) or {}
            ).get("Retry-After")
            try:
                retry_after_value = float(retry_after)
            except (TypeError, ValueError):
                retry_after_value = None
            return SendResult(
                success=False,
                error=provider_rejection_error(rejection),
                retryable=True,
                retry_after=retry_after_value,
                raw_response={
                    "provider_write_attempted": False,
                    "provider_retryable": True,
                    "retry_after": retry_after_value,
                    "status": status,
                    "provider_rejection": rejection,
                },
            )
        rejection = provider_rejection_evidence(
            provider="WeCom Callback",
            status=status,
            body=response_body,
        )
        if status == 408 or status >= 500:
            return SendResult(
                success=False,
                error=provider_rejection_error(rejection),
                raw_response={
                    "status": status,
                    "provider_rejection": rejection,
                },
            )
        return SendResult(
            success=False,
            error=provider_rejection_error(rejection),
            raw_response={
                "provider_write_attempted": False,
                "provider_retryable": False,
                "status": status,
                "provider_rejection": rejection,
            },
        )

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        app = self._resolve_app_for_chat(chat_id)
        touser = chat_id.split(":", 1)[1] if ":" in chat_id else chat_id
        try:
            payload = {
                "touser": touser,
                "msgtype": "text",
                "agentid": int(str(app.get("agent_id") or 0)),
                "text": {"content": content[:2048]},
                "safe": 0,
            }
            for _attempt in range(2):
                token = await self._get_access_token(app)
                resp = await self._http_client.post(
                    f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={token}",
                    json=payload,
                )
                data = resp.json()
                errcode = data.get("errcode")
                if errcode in {40001, 42001} and _attempt == 0:
                    # WeCom rejected the token — evict the cached entry so
                    # the next _get_access_token call forces a fresh fetch.
                    logger.warning(
                        "[WecomCallback] Token rejected for app '%s' (errcode=%s), refreshing",
                        app.get("name", "default"), errcode,
                    )
                    self._access_tokens.pop(app["name"], None)
                    continue
                if errcode != 0:
                    return SendResult(success=False, error=str(data))
                return SendResult(
                    success=True,
                    message_id=str(data.get("msgid", "")),
                    raw_response=data,
                )
            return SendResult(success=False, error="send failed after token refresh")
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    def _resolve_app_for_chat(self, chat_id: str) -> Dict[str, Any]:
        """Pick the app associated with *chat_id*, falling back sensibly."""
        app_name = self._user_app_map.get(chat_id)
        if not app_name and ":" not in chat_id:
            # Legacy bare user_id — try to find a unique match.
            matching = [k for k in self._user_app_map if k.endswith(f":{chat_id}")]
            if len(matching) == 1:
                app_name = self._user_app_map.get(matching[0])
        app = self._get_app_by_name(app_name) if app_name else None
        return app or self._apps[0]

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "dm"}

    # ------------------------------------------------------------------
    # Inbound: HTTP callback handlers
    # ------------------------------------------------------------------

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "platform": "wecom_callback"})

    async def _handle_verify(self, request: web.Request) -> web.Response:
        """GET endpoint — WeCom URL verification handshake."""
        msg_signature = request.query.get("msg_signature", "")
        timestamp = request.query.get("timestamp", "")
        nonce = request.query.get("nonce", "")
        echostr = request.query.get("echostr", "")
        for app in self._apps:
            try:
                crypt = self._crypt_for_app(app)
                plain = crypt.verify_url(msg_signature, timestamp, nonce, echostr)
                return web.Response(text=plain, content_type="text/plain")
            except Exception:
                continue
        return web.Response(status=403, text="signature verification failed")

    async def _handle_callback(self, request: web.Request) -> web.Response:
        """POST endpoint — receive an encrypted message callback."""
        msg_signature = request.query.get("msg_signature", "")
        timestamp = request.query.get("timestamp", "")
        nonce = request.query.get("nonce", "")
        # Explicit guard in addition to client_max_size: rejects oversized
        # payloads before any XML parse / signature check (DoS, zip bombs).
        body_bytes = await request.read()
        if len(body_bytes) > _MAX_BODY:
            logger.warning("[WecomCallback] Payload too large (%d bytes) — rejected", len(body_bytes))
            return web.Response(status=413, text="payload too large")
        body = body_bytes.decode("utf-8", errors="replace")

        for app in self._apps:
            try:
                decrypted = self._decrypt_request(
                    app, body, msg_signature, timestamp, nonce,
                )
                event = self._build_event(app, decrypted)
                if event is not None:
                    # Deduplicate: WeCom retries callbacks on timeout,
                    # producing duplicate inbound messages (#10305).
                    if event.message_id:
                        now = time.time()
                        if event.message_id in self._seen_messages:
                            if now - self._seen_messages[event.message_id] < MESSAGE_DEDUP_TTL_SECONDS:
                                logger.debug("[WecomCallback] Duplicate MsgId %s, skipping", event.message_id)
                                return web.Response(text="success", content_type="text/plain")
                            del self._seen_messages[event.message_id]
                        self._seen_messages[event.message_id] = now
                        # Prune expired entries when cache grows large
                        if len(self._seen_messages) > 2000:
                            cutoff = now - MESSAGE_DEDUP_TTL_SECONDS
                            self._seen_messages = {k: v for k, v in self._seen_messages.items() if v > cutoff}
                    # Record which app this user belongs to.
                    if event.source and event.source.user_id:
                        map_key = self._user_app_key(
                            str(app.get("corp_id") or ""), event.source.user_id,
                        )
                        self._user_app_map[map_key] = app["name"]
                    await self._message_queue.put(event)
                # Immediately acknowledge — the agent's reply will arrive
                # later via the proactive message/send API.
                return web.Response(text="success", content_type="text/plain")
            except WeComCryptoError:
                continue
            except Exception:
                logger.exception("[WecomCallback] Error handling message")
                break
        return web.Response(status=400, text="invalid callback payload")

    async def _poll_loop(self) -> None:
        """Drain the message queue and dispatch to the gateway runner."""
        while True:
            event = await self._message_queue.get()
            try:
                task = asyncio.create_task(self.handle_message(event))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
            except Exception:
                logger.exception("[WecomCallback] Failed to enqueue event")

    # ------------------------------------------------------------------
    # XML / crypto helpers
    # ------------------------------------------------------------------

    def _decrypt_request(
        self, app: Dict[str, Any], body: str,
        msg_signature: str, timestamp: str, nonce: str,
    ) -> str:
        root = ET.fromstring(body)
        encrypt = root.findtext("Encrypt", default="")
        crypt = self._crypt_for_app(app)
        return crypt.decrypt(msg_signature, timestamp, nonce, encrypt).decode("utf-8")

    def _build_event(self, app: Dict[str, Any], xml_text: str) -> Optional[MessageEvent]:
        root = ET.fromstring(xml_text)
        msg_type = (root.findtext("MsgType") or "").lower()
        # Silently acknowledge lifecycle events.
        if msg_type == "event":
            event_name = (root.findtext("Event") or "").lower()
            if event_name in {"enter_agent", "subscribe"}:
                return None
        if msg_type not in {"text", "event"}:
            return None

        user_id = root.findtext("FromUserName", default="")
        corp_id = root.findtext("ToUserName", default=app.get("corp_id", ""))
        scoped_chat_id = self._user_app_key(corp_id, user_id)
        content = root.findtext("Content", default="").strip()
        if not content and msg_type == "event":
            content = "/start"
        msg_id = (
            root.findtext("MsgId")
            or f"{user_id}:{root.findtext('CreateTime', default='0')}"
        )
        source = self.build_source(
            chat_id=scoped_chat_id,
            chat_name=user_id,
            chat_type="dm",
            user_id=user_id,
            user_name=user_id,
        )
        return MessageEvent(
            text=content,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=xml_text,
            message_id=msg_id,
        )

    def _crypt_for_app(self, app: Dict[str, Any]) -> WXBizMsgCrypt:
        return WXBizMsgCrypt(
            token=str(app.get("token") or ""),
            encoding_aes_key=str(app.get("encoding_aes_key") or ""),
            receive_id=str(app.get("corp_id") or ""),
        )

    def _get_app_by_name(self, name: Optional[str]) -> Optional[Dict[str, Any]]:
        if not name:
            return None
        for app in self._apps:
            if app.get("name") == name:
                return app
        return None

    # ------------------------------------------------------------------
    # Access-token management
    # ------------------------------------------------------------------

    async def _get_access_token(self, app: Dict[str, Any]) -> str:
        cached = self._access_tokens.get(app["name"])
        now = time.time()
        if cached and cached.get("expires_at", 0) > now + 60:
            return cached["token"]
        return await self._refresh_access_token(app)

    async def _refresh_access_token(self, app: Dict[str, Any]) -> str:
        resp = await self._http_client.get(
            "https://qyapi.weixin.qq.com/cgi-bin/gettoken",
            params={
                "corpid": app.get("corp_id"),
                "corpsecret": app.get("corp_secret"),
            },
            timeout=20.0,
            follow_redirects=False,
        )
        data = resp.json()
        if data.get("errcode") != 0:
            raise RuntimeError(f"WeCom token refresh failed: {data}")
        token = data["access_token"]
        expires_in = int(data.get("expires_in", ACCESS_TOKEN_TTL_SECONDS))
        self._access_tokens[app["name"]] = {
            "token": token,
            "expires_at": time.time() + expires_in,
        }
        logger.info(
            "[WecomCallback] Token refreshed for app '%s' (corp=%s), expires in %ss",
            app.get("name", "default"),
            app.get("corp_id", ""),
            expires_in,
        )
        return token
