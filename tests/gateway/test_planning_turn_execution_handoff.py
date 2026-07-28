"""Process-level proof for queued Planning turn/outbox ownership handoff."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from gateway.config import GatewayConfig, Platform
from gateway.platforms.base import MessageEvent, MessageType
from gateway.run import GatewayRunner
from gateway.session import SessionSource, SessionStore
from hermes_cli.gateway_turn_queue import (
    acknowledge_turn,
    enqueue_turn,
    queue_depth,
)
from hermes_cli.turn_origin import TurnOriginV1


CRASH_EXIT = 86
HARNESS = (
    Path(__file__).with_name("process_harness_planning_turn_handoff.py")
)


def _origin(event_id: str) -> TurnOriginV1:
    return TurnOriginV1(
        provider="telegram",
        gateway_account_id="telegram-account",
        chat_id="planning-chat",
        thread_id="planning-thread",
        message_id=f"message-{event_id}",
        sender_id="planning-owner",
        chat_type="thread",
        source_timestamp="2026-07-28T10:00:00Z",
        event_id=event_id,
    )


def _event(event_id: str = "planning-event") -> MessageEvent:
    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="planning-chat",
        chat_type="thread",
        user_id="planning-owner",
        thread_id="planning-thread",
        message_id=f"message-{event_id}",
        gateway_account_id="telegram-account",
    )
    return MessageEvent(
        text="Create the implementation plan.",
        message_type=MessageType.TEXT,
        source=source,
        message_id=f"message-{event_id}",
        timestamp="2026-07-28T10:00:00Z",
        turn_origin=_origin(event_id),
    )


def _session_store(
    profile_home: Path,
    event: MessageEvent,
) -> tuple[SessionStore, str]:
    store = SessionStore(
        sessions_dir=profile_home / "gateway-sessions",
        config=GatewayConfig(),
    )
    # The routing JSON mirror is the artifact under test. Avoid the
    # process-global SessionDB singleton when this test switches profiles.
    store._db = None
    entry = store.get_or_create_session(event.source)
    assert store.persist_active_turn_origin(
        entry.session_key,
        event.turn_origin,
    )
    return store, entry.session_key


def _runner(
    *,
    owner: str,
    store: SessionStore,
    session_key: str,
    origin: TurnOriginV1,
) -> GatewayRunner:
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._turn_queue_owner = owner
    runner.session_store = store
    runner._active_turn_origins = {
        session_key: origin.to_dict(),
    }
    return runner


def _run_crash(
    *,
    profile_home: Path,
    session_key: str,
    queue_id: str,
    mode: str,
    model_counter: Path,
    provider_counter: Path,
) -> None:
    env = dict(os.environ)
    env["HERMES_HOME"] = str(profile_home)
    repository_root = Path(__file__).parents[2]
    env["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            str(repository_root),
            env.get("PYTHONPATH", ""),
        )
        if value
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(HARNESS),
            str(profile_home),
            session_key,
            queue_id,
            mode,
            str(model_counter),
            str(provider_counter),
        ],
        cwd=repository_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert completed.returncode == CRASH_EXIT, (
        completed.stdout,
        completed.stderr,
    )


def _read_counter(path: Path) -> int:
    return int(path.read_text(encoding="utf-8")) if path.exists() else 0


@pytest.mark.asyncio
async def test_first_idle_turn_is_journaled_and_claimed_before_model_boundary(
    monkeypatch,
    tmp_path,
) -> None:
    """The first non-busy turn uses the same durable gate as queued recursion."""

    profile_home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(profile_home))
    event = _event("first-idle")
    session_key = (
        "agent:main:telegram:thread:planning-chat:planning-thread"
    )
    store_identity = object()

    class _AsyncStore:
        _store = store_identity

        async def get_or_create_session(self, _source):
            return SimpleNamespace(session_key=session_key)

    runner = GatewayRunner.__new__(GatewayRunner)
    runner._turn_queue_owner = "idle-first-runner"
    runner.session_store = store_identity
    runner._async_session_store = _AsyncStore()
    runner._recover_telegram_topic_thread_id = lambda _source: None
    runner._durable_turn_queue_home = lambda _event: profile_home
    observed: dict[str, object] = {}

    class _BeforeModel(RuntimeError):
        pass

    async def stop_at_bind(**kwargs) -> None:
        observed["confirmation_event"] = kwargs["confirmation_event"]
        queued_event = kwargs["queue_event"]
        observed["queue_event"] = queued_event
        queue_id = getattr(
            queued_event,
            "_hermes_durable_queue_id",
            "",
        )
        observed["queue_id"] = queue_id
        observed["origin"] = queued_event.turn_origin
        assert queue_id
        assert queue_depth(
            session_key,
            profile_home=profile_home,
        ) == 1
        raise _BeforeModel

    runner._bind_turn_delivery_authority = stop_at_bind
    with pytest.raises(_BeforeModel):
        await GatewayRunner._handle_message_with_agent(
            runner,
            event,
            event.source,
            "idle-first",
            1,
        )

    queue_id = str(observed["queue_id"])
    assert observed["confirmation_event"] is event
    assert observed["confirmation_event"] is not observed["queue_event"]
    assert observed["origin"].event_id == "first-idle"
    assert acknowledge_turn(
        queue_id,
        owner=runner._turn_queue_owner,
        profile_home=profile_home,
    )


@pytest.mark.parametrize("mode", ["before-stage", "partial-stage"])
def test_crash_before_full_outbox_commit_keeps_turn_replayable_once(
    monkeypatch,
    tmp_path,
    mode: str,
) -> None:
    profile_home = tmp_path / mode
    monkeypatch.setenv("HERMES_HOME", str(profile_home))
    event = _event(mode)
    store, session_key = _session_store(profile_home, event)
    queue_id = enqueue_turn(
        session_key,
        event,
        profile_home=profile_home,
    )
    model_counter = tmp_path / f"{mode}-model-count"
    provider_counter = tmp_path / f"{mode}-provider-count"

    _run_crash(
        profile_home=profile_home,
        session_key=session_key,
        queue_id=queue_id,
        mode=mode,
        model_counter=model_counter,
        provider_counter=provider_counter,
    )

    fresh_store = SessionStore(
        sessions_dir=profile_home / "gateway-sessions",
        config=GatewayConfig(),
    )
    fresh_store._db = None
    fresh = _runner(
        owner=f"fresh-{mode}",
        store=fresh_store,
        session_key=session_key,
        origin=event.turn_origin,
    )
    replay = fresh._claim_next_durable_turn(
        session_key,
        profile_home=profile_home,
    )

    assert replay is not None
    assert replay.turn_origin.event_id == mode
    assert _read_counter(model_counter) == 1
    assert _read_counter(provider_counter) == 0
    assert queue_depth(session_key, profile_home=profile_home) == 1
    assert acknowledge_turn(
        queue_id,
        owner=fresh._turn_queue_owner,
        profile_home=profile_home,
    )


def test_fresh_runner_retires_fully_staged_turn_without_model_replay(
    monkeypatch,
    tmp_path,
) -> None:
    profile_home = tmp_path / "full-stage"
    monkeypatch.setenv("HERMES_HOME", str(profile_home))
    event = _event("full-stage")
    store, session_key = _session_store(profile_home, event)
    queue_id = enqueue_turn(
        session_key,
        event,
        profile_home=profile_home,
    )
    model_counter = tmp_path / "full-stage-model-count"
    provider_counter = tmp_path / "full-stage-provider-count"

    _run_crash(
        profile_home=profile_home,
        session_key=session_key,
        queue_id=queue_id,
        mode="full-stage",
        model_counter=model_counter,
        provider_counter=provider_counter,
    )

    fresh_store = SessionStore(
        sessions_dir=profile_home / "gateway-sessions",
        config=GatewayConfig(),
    )
    fresh_store._db = None
    fresh = _runner(
        owner="fresh-full-stage",
        store=fresh_store,
        session_key=session_key,
        origin=event.turn_origin,
    )
    replay = fresh._claim_next_durable_turn(
        session_key,
        profile_home=profile_home,
    )

    assert replay is None
    assert _read_counter(model_counter) == 1
    assert _read_counter(provider_counter) == 0
    assert queue_depth(session_key, profile_home=profile_home) == 0
    verified_store = SessionStore(
        sessions_dir=profile_home / "gateway-sessions",
        config=GatewayConfig(),
    )
    verified_store._db = None
    verified_store._ensure_loaded()
    assert (
        verified_store._entries[session_key].resume_turn_origin
        is None
    )


def test_committed_proof_is_resolved_from_each_claims_profile_home(
    monkeypatch,
    tmp_path,
) -> None:
    profile_a = tmp_path / "profile-a"
    profile_b = tmp_path / "profile-b"
    event_a = _event("shared-profile-event")
    event_b = _event("shared-profile-event")

    monkeypatch.setenv("HERMES_HOME", str(profile_a))
    store_a, session_key_a = _session_store(profile_a, event_a)
    queue_id_a = enqueue_turn(
        session_key_a,
        event_a,
        profile_home=profile_a,
    )
    monkeypatch.setenv("HERMES_HOME", str(profile_b))
    store_b, session_key_b = _session_store(profile_b, event_b)
    queue_id_b = enqueue_turn(
        session_key_b,
        event_b,
        profile_home=profile_b,
    )
    assert session_key_a == session_key_b
    assert queue_id_a == queue_id_b

    model_counter = tmp_path / "profile-a-model-count"
    provider_counter = tmp_path / "profile-a-provider-count"
    _run_crash(
        profile_home=profile_a,
        session_key=session_key_a,
        queue_id=queue_id_a,
        mode="full-stage",
        model_counter=model_counter,
        provider_counter=provider_counter,
    )

    fresh_a = _runner(
        owner="fresh-profile-a",
        store=store_a,
        session_key=session_key_a,
        origin=event_a.turn_origin,
    )
    assert (
        fresh_a._claim_next_durable_turn(
            session_key_a,
            profile_home=profile_a,
        )
        is None
    )
    assert queue_depth(session_key_a, profile_home=profile_a) == 0

    fresh_b = _runner(
        owner="fresh-profile-b",
        store=store_b,
        session_key=session_key_b,
        origin=event_b.turn_origin,
    )
    replay_b = fresh_b._claim_next_durable_turn(
        session_key_b,
        profile_home=profile_b,
    )
    assert replay_b is not None
    assert replay_b.turn_origin.event_id == "shared-profile-event"
    assert queue_depth(session_key_b, profile_home=profile_b) == 1
    assert acknowledge_turn(
        queue_id_b,
        owner=fresh_b._turn_queue_owner,
        profile_home=profile_b,
    )
