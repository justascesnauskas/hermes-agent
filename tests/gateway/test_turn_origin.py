"""TurnOriginV1 gateway-to-agent/plugin propagation tests."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import MessageEvent
from gateway.session import SessionSource
from hermes_cli.turn_origin import (
    TURN_ORIGIN_SCHEMA_VERSION,
    TurnOriginV1,
    get_current_turn_origin,
    get_current_turn_user_text,
)


def _skip_only_runner() -> object:
    from gateway.run import GatewayRunner

    platform_config = PlatformConfig(
        enabled=True,
        extra={"gateway_account_id": "account-primary"},
    )
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig(
        platforms={Platform.WHATSAPP: platform_config},
    )
    runner.adapters = {
        Platform.WHATSAPP: SimpleNamespace(
            config=platform_config,
            send=AsyncMock(),
        )
    }
    runner.pairing_store = MagicMock()
    runner.session_store = MagicMock()
    runner._running_agents = {}
    runner._update_prompt_pending = {}
    return runner


def _event(*, chat_id: str) -> MessageEvent:
    return MessageEvent(
        text="identical text",
        message_id="message-7",
        timestamp=datetime(2026, 7, 27, 12, 30, tzinfo=timezone.utc),
        source=SessionSource(
            platform=Platform.WHATSAPP,
            chat_id=chat_id,
            chat_type="dm",
            user_id="sender-1",
        ),
    )


@pytest.mark.asyncio
async def test_identical_text_from_distinct_chats_keeps_distinct_origin(
    monkeypatch,
):
    """Text is never an identity key; chat-scoped events stay distinguishable."""

    observed_origins = []

    def _capture_and_skip(name, **kwargs):
        if name == "pre_gateway_dispatch":
            observed_origins.append(kwargs["turn_origin"])
            return [{"action": "skip", "reason": "origin captured"}]
        return []

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", _capture_and_skip)
    runner = _skip_only_runner()

    await runner._handle_message(_event(chat_id="chat-a"))
    await runner._handle_message(_event(chat_id="chat-b"))

    assert [origin["chat_id"] for origin in observed_origins] == [
        "chat-a",
        "chat-b",
    ]
    assert {
        origin["gateway_account_id"] for origin in observed_origins
    } == {"account-primary"}
    assert {
        origin["schema_version"] for origin in observed_origins
    } == {TURN_ORIGIN_SCHEMA_VERSION}
    assert observed_origins[0]["event_id"] != observed_origins[1]["event_id"]
    assert all("identical text" not in str(origin) for origin in observed_origins)


def test_missing_optional_origin_fields_are_null_safe():
    event = MessageEvent(
        text="hello",
        timestamp=None,
        source=SessionSource(
            platform=Platform.WEBHOOK,
            chat_id="",
            chat_type="",
        ),
    )

    payload = event.ensure_turn_origin().to_dict()

    assert payload == {
        "schema_version": TURN_ORIGIN_SCHEMA_VERSION,
        "provider": "webhook",
        "gateway_account_id": None,
        "chat_id": None,
        "thread_id": None,
        "message_id": None,
        "sender_id": None,
        "chat_type": None,
        "source_timestamp": None,
        "event_id": None,
    }


def test_relay_wire_preserves_origin_identity_and_source_timestamp():
    from gateway.relay.ws_transport import _event_from_wire

    event = _event_from_wire(
        {
            "text": "relay hello",
            "message_id": "message-relay",
            "event_id": "event-relay",
            "source_timestamp": "2026-07-27T10:30:00Z",
            "source": {
                "platform": "discord",
                "gateway_account_id": "relay-bot",
                "chat_id": "channel-relay",
                "thread_id": "thread-relay",
                "user_id": "user-relay",
                "chat_type": "thread",
            },
        }
    )

    assert event.ensure_turn_origin().to_dict() == {
        "schema_version": TURN_ORIGIN_SCHEMA_VERSION,
        "provider": "discord",
        "gateway_account_id": "relay-bot",
        "chat_id": "channel-relay",
        "thread_id": "thread-relay",
        "message_id": "message-relay",
        "sender_id": "user-relay",
        "chat_type": "thread",
        "source_timestamp": "2026-07-27T10:30:00Z",
        "event_id": "event-relay",
    }


def test_conversation_origin_reaches_pre_llm_and_tool_plugin_hooks(monkeypatch):
    """One scoped turn supplies the same envelope to LLM and tool observers."""

    import hermes_cli.plugins as plugins
    import model_tools
    from run_agent import AIAgent
    from tools.thread_context import propagate_context_to_thread

    origin = TurnOriginV1(
        provider="telegram",
        gateway_account_id="bot-main",
        chat_id="chat-42",
        thread_id="topic-3",
        message_id="message-9",
        sender_id="user-8",
        chat_type="thread",
        source_timestamp="2026-07-27T12:30:00Z",
        event_id="event-explicit",
    )
    expected = origin.to_dict()
    observed = {}
    manager = plugins.PluginManager()

    def _capture(hook_name):
        def _observer(**kwargs):
            observed[hook_name] = kwargs

        return _observer

    for hook_name in (
        "pre_llm_call",
        "pre_tool_call",
        "post_tool_call",
        "transform_tool_result",
    ):
        manager._hooks[hook_name] = [_capture(hook_name)]

    def _capture_tool_request_middleware(**kwargs):
        observed["tool_request_middleware"] = kwargs

    manager._middleware["tool_request"] = [_capture_tool_request_middleware]

    monkeypatch.setattr(plugins, "_plugin_manager", manager)
    monkeypatch.setattr(
        model_tools.registry,
        "dispatch",
        lambda *_args, **_kwargs: '{"ok":true}',
    )

    def _fake_conversation_loop(*_args, **kwargs):
        assert kwargs["turn_origin"] == origin
        assert get_current_turn_user_text() == "hello"
        plugins.invoke_hook(
            "pre_llm_call",
            session_id="session-1",
            user_message="hello",
            platform="telegram",
            sender_id="user-8",
        )
        def _tool_worker():
            return model_tools.handle_function_call(
                "web_search",
                {"q": "origin"},
                task_id="task-1",
                tool_call_id="call-1",
                session_id="session-1",
            )

        with ThreadPoolExecutor(max_workers=1) as executor:
            tool_result = executor.submit(
                propagate_context_to_thread(_tool_worker)
            ).result()
        assert tool_result == '{"ok":true}'
        return {"final_response": "ok"}

    monkeypatch.setattr(
        "agent.conversation_loop.run_conversation",
        _fake_conversation_loop,
    )

    agent = object.__new__(AIAgent)
    agent.session_id = "session-1"
    agent._session_db = None

    result = agent.run_conversation(
        "[API-only observed context]\nhello",
        persist_user_message="hello",
        turn_origin=origin,
    )

    assert result == {"final_response": "ok"}
    for hook_name in (
        "pre_llm_call",
        "pre_tool_call",
        "post_tool_call",
        "transform_tool_result",
        "tool_request_middleware",
    ):
        assert observed[hook_name]["turn_origin"] == expected
    # Existing compatibility fields remain present alongside the envelope.
    assert observed["pre_llm_call"]["platform"] == "telegram"
    assert observed["pre_llm_call"]["sender_id"] == "user-8"
    assert get_current_turn_origin() is None
    assert get_current_turn_user_text() is None
    assert not hasattr(agent, "_current_turn_origin")


def test_gateway_origin_adds_opaque_attachment_ingress_identity() -> None:
    event = _event(chat_id="chat-media")
    event.media_urls = [
        "/gateway/private/cache/design.png",
        "/gateway/private/cache/schema.pdf",
    ]
    event.metadata["attachment_ids"] = ["provider-file-7", "provider-file-8"]

    payload = event.ensure_turn_origin(
        gateway_account_id="account-primary"
    ).to_dict()

    assert [item["ingress_ordinal"] for item in payload["attachments"]] == [1, 2]
    assert all(
        item["attachment_id"].startswith("att_v1_")
        for item in payload["attachments"]
    )
    rendered = str(payload)
    assert "provider-file-7" not in rendered
    assert "/gateway/private/cache" not in rendered
