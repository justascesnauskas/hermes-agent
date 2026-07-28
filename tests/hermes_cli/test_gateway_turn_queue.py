import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from gateway.config import Platform
from gateway.platforms.base import MessageEvent, MessageType
from gateway.session import SessionSource
from hermes_cli.gateway_turn_queue import (
    DurableTurnQueueError,
    acknowledge_turn,
    cancel_session_turns,
    claim_next_turn,
    claim_session_heads,
    enqueue_turn,
    park_turn_for_retry,
    pending_queue_count,
    queue_depth,
)
import hermes_cli.gateway_turn_queue as queue_module


def _event(
    index: int,
    *,
    text: str | None = None,
    media_path: Path | None = None,
    profile: str | None = None,
) -> MessageEvent:
    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="chat-1",
        chat_type="dm",
        user_id="user-1",
        thread_id="thread-1",
        message_id=f"message-{index}",
        profile=profile,
        gateway_account_id="account-1",
    )
    event = MessageEvent(
        text=text if text is not None else f"turn-{index}",
        message_type=(
            MessageType.DOCUMENT
            if media_path is not None
            else MessageType.TEXT
        ),
        source=source,
        message_id=f"message-{index}",
        media_urls=[str(media_path)] if media_path is not None else [],
        media_types=["text/plain"] if media_path is not None else [],
        metadata={
            "provider_source_timestamp": (
                f"2026-07-28T10:{(index // 60) % 60:02d}:{index % 60:02d}Z"
            )
        },
    )
    event.ensure_turn_origin()
    return event


def test_fifo_has_no_product_cap_and_keeps_one_claimed_head(tmp_path):
    session_key = "agent:main:telegram:dm:chat-1:thread-1"
    queue_ids = [
        enqueue_turn(session_key, _event(index), profile_home=tmp_path)
        for index in range(137)
    ]

    assert queue_depth(session_key, profile_home=tmp_path) == 137
    assert pending_queue_count(profile_home=tmp_path) == 137

    first = claim_next_turn(
        session_key,
        owner="worker-a",
        profile_home=tmp_path,
    )
    assert first is not None
    assert first.queue_id == queue_ids[0]
    assert first.event.text == "turn-0"

    # A second live worker cannot skip the claimed head and process row two.
    assert (
        claim_next_turn(
            session_key,
            owner="worker-b",
            profile_home=tmp_path,
        )
        is None
    )
    heads, cursor = claim_session_heads(
        owner="worker-b",
        profile_home=tmp_path,
    )
    assert heads == []
    assert cursor is None

    for index, queue_id in enumerate(queue_ids):
        if index:
            first = claim_next_turn(
                session_key,
                owner="worker-a",
                profile_home=tmp_path,
            )
            assert first is not None
            assert first.queue_id == queue_id
            assert first.event.text == f"turn-{index}"
        assert acknowledge_turn(
            queue_id,
            owner="worker-a",
            profile_home=tmp_path,
        )

    assert queue_depth(session_key, profile_home=tmp_path) == 0


@pytest.mark.skipif(
    queue_module.fcntl is None,
    reason="process-lifetime queue locks require fcntl",
)
def test_dead_process_owner_is_reclaimed_without_waiting_for_ttl(tmp_path):
    session_key = "agent:main:telegram:dm:chat-1:thread-1"
    queue_id = enqueue_turn(
        session_key,
        _event(1),
        profile_home=tmp_path,
    )
    program = """
import os
import sys
from pathlib import Path
from hermes_cli.gateway_turn_queue import claim_next_turn

claim = claim_next_turn(
    sys.argv[2],
    owner="dead-process",
    profile_home=Path(sys.argv[1]),
    claim_seconds=86400,
)
os._exit(0 if claim is not None else 9)
"""
    completed = subprocess.run(
        [sys.executable, "-c", program, str(tmp_path), session_key],
        cwd=os.getcwd(),
        check=False,
        timeout=20,
    )
    assert completed.returncode == 0

    replay = claim_next_turn(
        session_key,
        owner="fresh-process",
        profile_home=tmp_path,
    )
    assert replay is not None
    assert replay.queue_id == queue_id
    assert replay.event.turn_origin.event_id == _event(1).turn_origin.event_id


def test_attachment_is_rebound_to_private_snapshot_and_survives_source_loss(
    tmp_path,
):
    profile_home = tmp_path / "profile"
    source_path = tmp_path / "evidence.txt"
    source_path.write_bytes(b"immutable evidence\n")
    event = _event(7, media_path=source_path)
    original_attachment_id = event.turn_origin.attachments[0].attachment_id

    queue_id = enqueue_turn(
        "agent:main:telegram:dm:chat-1:thread-1",
        event,
        profile_home=profile_home,
    )
    snapshot_path = Path(event.media_urls[0])
    assert snapshot_path != source_path
    assert snapshot_path.read_bytes() == b"immutable evidence\n"
    assert event.turn_origin.attachments[0].attachment_id == original_attachment_id
    assert event.turn_origin.attachments[0].local_path == str(snapshot_path)

    source_path.unlink()
    claimed = claim_next_turn(
        "agent:main:telegram:dm:chat-1:thread-1",
        owner="worker-a",
        profile_home=profile_home,
    )
    assert claimed is not None
    assert claimed.queue_id == queue_id
    assert Path(claimed.event.media_urls[0]).read_bytes() == b"immutable evidence\n"
    assert (
        claimed.event.turn_origin.attachments[0].attachment_id
        == original_attachment_id
    )

    assert acknowledge_turn(
        queue_id,
        owner="worker-a",
        profile_home=profile_home,
    )
    assert not snapshot_path.exists()


def test_corrupt_attachment_snapshot_fails_closed_before_replay(tmp_path):
    source_path = tmp_path / "evidence.txt"
    source_path.write_bytes(b"original")
    event = _event(8, media_path=source_path)
    session_key = "agent:main:telegram:dm:chat-1:thread-1"
    enqueue_turn(session_key, event, profile_home=tmp_path)
    snapshot_path = Path(event.media_urls[0])
    snapshot_path.chmod(0o600)
    snapshot_path.write_bytes(b"tampered")

    with pytest.raises(
        DurableTurnQueueError,
        match="gateway.turn_queue_attachment_snapshot_corrupt",
    ):
        claim_next_turn(
            session_key,
            owner="worker-a",
            profile_home=tmp_path,
        )


def test_provider_redelivery_dedupes_but_identity_conflict_fails_closed(tmp_path):
    session_key = "agent:main:telegram:dm:chat-1:thread-1"
    first = _event(9)
    duplicate = _event(9)
    conflicting = _event(9, text="different bytes under the same provider event")

    first_id = enqueue_turn(session_key, first, profile_home=tmp_path)
    assert enqueue_turn(session_key, duplicate, profile_home=tmp_path) == first_id
    assert queue_depth(session_key, profile_home=tmp_path) == 1

    with pytest.raises(
        DurableTurnQueueError,
        match="gateway.turn_queue_identity_conflict",
    ):
        enqueue_turn(session_key, conflicting, profile_home=tmp_path)
    assert queue_depth(session_key, profile_home=tmp_path) == 1


def test_completion_receipt_prevents_late_provider_redelivery(tmp_path):
    session_key = "agent:main:telegram:dm:chat-1:thread-1"
    first = _event(10)
    queue_id = enqueue_turn(session_key, first, profile_home=tmp_path)
    claimed = claim_next_turn(
        session_key,
        owner="worker-a",
        profile_home=tmp_path,
    )
    assert claimed is not None
    assert acknowledge_turn(
        queue_id,
        owner="worker-a",
        profile_home=tmp_path,
    )

    duplicate = _event(10)
    assert enqueue_turn(
        session_key,
        duplicate,
        profile_home=tmp_path,
    ) == queue_id
    assert getattr(duplicate, "_hermes_durable_turn_completed") is True
    assert queue_depth(session_key, profile_home=tmp_path) == 0
    assert (
        claim_next_turn(
            session_key,
            owner="worker-b",
            profile_home=tmp_path,
        )
        is None
    )

    with pytest.raises(
        DurableTurnQueueError,
        match="gateway.turn_queue_identity_conflict",
    ):
        enqueue_turn(
            session_key,
            _event(10, text="changed payload under completed identity"),
            profile_home=tmp_path,
        )


def test_completion_receipt_dedupes_in_fresh_process(tmp_path):
    session_key = "agent:main:telegram:dm:chat-1:thread-1"
    queue_id = enqueue_turn(
        session_key,
        _event(12),
        profile_home=tmp_path,
    )
    claimed = claim_next_turn(
        session_key,
        owner="worker-a",
        profile_home=tmp_path,
    )
    assert claimed is not None
    assert acknowledge_turn(
        queue_id,
        owner="worker-a",
        profile_home=tmp_path,
    )

    program = """
import json
import sys
from pathlib import Path
from tests.hermes_cli.test_gateway_turn_queue import _event
from hermes_cli.gateway_turn_queue import enqueue_turn, queue_depth

home = Path(sys.argv[1])
session_key = sys.argv[2]
event = _event(12)
queue_id = enqueue_turn(session_key, event, profile_home=home)
print(json.dumps({
    "queueId": queue_id,
    "completed": getattr(event, "_hermes_durable_turn_completed", False),
    "depth": queue_depth(session_key, profile_home=home),
}))
"""
    completed = subprocess.run(
        [sys.executable, "-c", program, str(tmp_path), session_key],
        cwd=os.getcwd(),
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result == {
        "queueId": queue_id,
        "completed": True,
        "depth": 0,
    }


def test_fresh_process_converges_receipt_before_snapshot_cleanup_crash(
    tmp_path,
    monkeypatch,
):
    session_key = "agent:main:telegram:dm:chat-1:thread-1"
    source_path = tmp_path / "crash-evidence.txt"
    source_path.write_bytes(b"cleanup survives receipt-first crash")
    event = _event(13, media_path=source_path)
    queue_id = enqueue_turn(
        session_key,
        event,
        profile_home=tmp_path,
    )
    snapshot_path = Path(event.media_urls[0])
    claimed = claim_next_turn(
        session_key,
        owner="worker-a",
        profile_home=tmp_path,
    )
    assert claimed is not None
    monkeypatch.setattr(
        queue_module,
        "_discard_snapshot",
        lambda *_args, **_kwargs: False,
    )
    assert acknowledge_turn(
        queue_id,
        owner="worker-a",
        profile_home=tmp_path,
    )
    assert snapshot_path.exists()

    program = """
import json
import sys
from pathlib import Path
from hermes_cli.gateway_turn_queue import cleanup_completed_snapshots

cleaned, has_more = cleanup_completed_snapshots(
    profile_home=Path(sys.argv[1]),
)
print(json.dumps({"cleaned": cleaned, "hasMore": has_more}))
"""
    completed = subprocess.run(
        [sys.executable, "-c", program, str(tmp_path)],
        cwd=os.getcwd(),
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == {
        "cleaned": 1,
        "hasMore": False,
    }
    assert not snapshot_path.exists()


def test_session_cancel_is_atomic_and_profile_local(tmp_path):
    session_key = "agent:main:telegram:dm:chat-1:thread-1"
    profile_a = tmp_path / "a"
    profile_b = tmp_path / "b"
    enqueue_turn(
        session_key,
        _event(1, profile="a"),
        profile_home=profile_a,
    )
    enqueue_turn(
        session_key,
        _event(2, profile="b"),
        profile_home=profile_b,
    )

    assert cancel_session_turns(
        session_key,
        profile_home=profile_a,
    ) == 1
    assert queue_depth(session_key, profile_home=profile_a) == 0
    assert queue_depth(session_key, profile_home=profile_b) == 1


def test_explicit_session_cancel_tombstones_provider_event(tmp_path):
    session_key = "agent:main:telegram:dm:chat-1:thread-1"
    original = _event(11)
    queue_id = enqueue_turn(
        session_key,
        original,
        profile_home=tmp_path,
    )
    assert cancel_session_turns(
        session_key,
        profile_home=tmp_path,
    ) == 1

    redelivery = _event(11)
    assert enqueue_turn(
        session_key,
        redelivery,
        profile_home=tmp_path,
    ) == queue_id
    assert getattr(redelivery, "_hermes_durable_turn_completed") is True
    assert queue_depth(session_key, profile_home=tmp_path) == 0


def test_retry_epoch_is_persisted_and_never_skips_fifo_head(
    tmp_path,
    monkeypatch,
):
    now = [1_000.0]
    monkeypatch.setattr(queue_module.time, "time", lambda: now[0])
    session_key = "agent:main:telegram:dm:chat-1:thread-1"
    first_id = enqueue_turn(session_key, _event(1), profile_home=tmp_path)
    enqueue_turn(session_key, _event(2), profile_home=tmp_path)

    first = claim_next_turn(
        session_key,
        owner="worker-a",
        profile_home=tmp_path,
    )
    assert first is not None
    delay = park_turn_for_retry(
        first_id,
        owner="worker-a",
        profile_home=tmp_path,
    )
    assert delay is not None
    assert 0.8 <= delay <= 1.2

    # Neither the parked head nor its successor is claimable before the epoch.
    assert (
        claim_next_turn(
            session_key,
            owner="worker-a",
            profile_home=tmp_path,
        )
        is None
    )
    now[0] += 2
    replay = claim_next_turn(
        session_key,
        owner="worker-a",
        profile_home=tmp_path,
    )
    assert replay is not None
    assert replay.queue_id == first_id


def test_unsafe_symlinked_queue_root_is_rejected(tmp_path):
    profile_home = tmp_path / "profile"
    outside = tmp_path / "outside"
    outside.mkdir()
    profile_home.mkdir()
    (profile_home / "gateway-turn-queue").symlink_to(
        outside,
        target_is_directory=True,
    )

    with pytest.raises(
        DurableTurnQueueError,
        match="gateway.turn_queue_root_unsafe",
    ):
        enqueue_turn(
            "agent:main:telegram:dm:chat-1:thread-1",
            _event(1),
            profile_home=profile_home,
        )


def test_real_gateway_runner_journals_137_turns_but_hydrates_one(tmp_path):
    from gateway.run import GatewayRunner

    runner = GatewayRunner.__new__(GatewayRunner)
    runner._turn_queue_owner = "gateway-process"
    runner._queued_events = {}
    runner._session_sources = {}
    runner.config = SimpleNamespace(multiplex_profiles=False)
    adapter = SimpleNamespace(_pending_messages={})
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._resolve_profile_home_for_source = lambda _source: tmp_path
    session_key = "agent:main:telegram:dm:chat-1:thread-1"

    for index in range(137):
        runner._queue_or_replace_pending_event(
            session_key,
            _event(index),
        )

    assert list(adapter._pending_messages) == [session_key]
    assert adapter._pending_messages[session_key].text == "turn-0"
    assert runner._queued_events == {}
    assert runner._queue_depth(session_key, adapter=adapter) == 137


def test_conversation_boundary_cancels_exact_profile_queue(tmp_path):
    from gateway.run import GatewayRunner

    runner = GatewayRunner.__new__(GatewayRunner)
    runner._turn_queue_owner = "gateway-process"
    runner._queued_events = {}
    runner._session_sources = {}
    runner.config = SimpleNamespace(multiplex_profiles=False)
    adapter = SimpleNamespace(_pending_messages={})
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._resolve_profile_home_for_source = lambda _source: tmp_path
    source = _event(1).source
    session_key = "agent:main:telegram:dm:chat-1:thread-1"
    runner._queue_or_replace_pending_event(session_key, _event(1))
    runner._queue_or_replace_pending_event(session_key, _event(2))

    runner._clear_conversation_scope(
        session_key,
        reason="test_reset",
        source=source,
    )

    assert queue_depth(session_key, profile_home=tmp_path) == 0
    assert adapter._pending_messages == {}


@pytest.mark.asyncio
async def test_idle_agent_turn_is_journaled_and_claimed_before_model_work(
    tmp_path,
):
    from gateway.run import GatewayRunner

    class StopBeforeModel(RuntimeError):
        pass

    session_key = "agent:main:telegram:dm:chat-1:thread-1"
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._turn_queue_owner = "gateway-process"
    runner._session_sources = {}
    runner._active_turn_origins = {}
    runner.config = SimpleNamespace(multiplex_profiles=False)
    runner.session_store = SimpleNamespace(
        get_or_create_session=lambda _source: SimpleNamespace(
            session_key=session_key,
            session_id="session-1",
        )
    )
    runner._resolve_profile_home_for_source = lambda _source: tmp_path
    runner._recover_telegram_topic_thread_id = lambda _source: None
    captured: dict[str, object] = {}

    async def _stop_before_model(**kwargs):
        captured.update(kwargs)
        raise StopBeforeModel

    runner._bind_turn_delivery_authority = _stop_before_model
    event = _event(21)

    with pytest.raises(StopBeforeModel):
        await runner._handle_message_with_agent(
            event,
            event.source,
            session_key,
            1,
        )

    claimed_event = captured["queue_event"]
    assert getattr(claimed_event, "_hermes_durable_queue_id", "").startswith(
        "qturn_"
    )
    assert queue_depth(session_key, profile_home=tmp_path) == 1


def test_restart_retires_full_semantic_handoff_from_exact_profile(
    tmp_path,
    monkeypatch,
):
    from gateway.run import GatewayRunner
    import hermes_cli.semantic_delivery as semantic_delivery

    session_key = "agent:main:telegram:dm:chat-1:thread-1"
    queue_id = enqueue_turn(
        session_key,
        _event(22),
        profile_home=tmp_path,
    )
    claim = claim_next_turn(
        session_key,
        owner="fresh-process",
        profile_home=tmp_path,
    )
    assert claim is not None

    observed: dict[str, object] = {}

    def _committed(ref, *, ledger_path=None):
        observed["ref"] = ref
        observed["ledger_path"] = ledger_path
        return True

    monkeypatch.setattr(
        semantic_delivery,
        "semantic_retry_turn_execution_committed",
        _committed,
    )
    origin = claim.event.turn_origin
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._turn_queue_owner = "fresh-process"
    runner._active_turn_origins = {
        session_key: origin.to_dict(),
    }
    confirmed: list[tuple[str, str | None]] = []
    runner.session_store = SimpleNamespace(
        confirm_turn_delivery=lambda key, *, expected_event_id: (
            confirmed.append((key, expected_event_id)) or True
        )
    )

    assert runner._retire_semantically_committed_turn(claim)
    assert observed == {
        "ref": queue_id,
        "ledger_path": (
            tmp_path
            / "state"
            / "semantic-delivery"
            / "ledger.sqlite3"
        ),
    }
    assert confirmed == [(session_key, origin.event_id)]
    assert queue_depth(session_key, profile_home=tmp_path) == 0
    assert runner._active_turn_origins == {}
