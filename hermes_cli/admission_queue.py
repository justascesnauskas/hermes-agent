"""Durable FIFO for gateway messages waiting on global session capacity.

The per-session adapter queue handles follow-ups to an already-running chat.
This queue handles the other case: a message for a *different* chat arrives
while ``max_concurrent_sessions`` slots are occupied.  Entries survive a
gateway restart and are replayed through the normal adapter ingress, so auth,
session routing, delivery, and transcript rules are not bypassed.
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from hermes_constants import get_hermes_home

from .active_sessions import _FileLock, _pid_alive

logger = logging.getLogger(__name__)

_MAX_ENTRIES = 1000


class AdmissionQueueCorrupt(RuntimeError):
    pass


@dataclass(frozen=True)
class AdmissionReceipt:
    item_id: str
    position: int
    depth: int
    coalesced: bool = False


def _queue_path() -> Path:
    return Path(get_hermes_home()) / "runtime" / "admission_queue.json"


def _lock_path() -> Path:
    return Path(get_hermes_home()) / "runtime" / "admission_queue.lock"


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        if isinstance(value, dict):
            return {str(key): _json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [_json_safe(item) for item in value]
        return str(value)


def _read(path: Path) -> list[dict[str, Any]]:
    try:
        with open(path, encoding="utf-8") as fh:
            payload = json.load(fh)
    except FileNotFoundError:
        return []
    except Exception as exc:
        raise AdmissionQueueCorrupt(
            f"Could not read durable gateway admission queue: {path}"
        ) from exc
    entries = payload.get("entries") if isinstance(payload, dict) else payload
    if not isinstance(entries, list):
        raise AdmissionQueueCorrupt(
            f"Durable gateway admission queue has an invalid shape: {path}"
        )
    return [entry for entry in entries if isinstance(entry, dict)]


def _write(path: Path, entries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"version": 1, "entries": entries}, fh, sort_keys=True)
    tmp.chmod(0o600)
    os.replace(tmp, path)


def _event_payload(event: Any) -> dict[str, Any]:
    source = getattr(event, "source", None)
    source_payload = source.to_dict() if source is not None else {}
    message_type = getattr(event, "message_type", None)
    message_type_value = getattr(message_type, "value", message_type)
    timestamp = getattr(event, "timestamp", None)
    return {
        "text": str(getattr(event, "text", "") or ""),
        "message_type": str(message_type_value or "text"),
        "source": source_payload,
        "source_runtime": {
            "is_bot": bool(getattr(source, "is_bot", False)),
            "role_authorized": bool(getattr(source, "role_authorized", False)),
            # This file is local gateway state (0600), not a peer-controlled
            # wire payload. Preserve authenticated relay provenance so queued
            # Team Gateway messages retain the ingress authorization decision.
            "delivered_via_upstream_relay": bool(
                getattr(source, "delivered_via_upstream_relay", False)
            ),
        },
        "message_id": getattr(event, "message_id", None),
        "platform_update_id": getattr(event, "platform_update_id", None),
        "media_urls": list(getattr(event, "media_urls", None) or []),
        "media_types": list(getattr(event, "media_types", None) or []),
        "reply_to_message_id": getattr(event, "reply_to_message_id", None),
        "reply_to_text": getattr(event, "reply_to_text", None),
        "reply_to_author_id": getattr(event, "reply_to_author_id", None),
        "reply_to_author_name": getattr(event, "reply_to_author_name", None),
        "reply_to_is_own_message": bool(
            getattr(event, "reply_to_is_own_message", False)
        ),
        "auto_skill": _json_safe(getattr(event, "auto_skill", None)),
        "channel_prompt": getattr(event, "channel_prompt", None),
        "channel_context": getattr(event, "channel_context", None),
        "internal": bool(getattr(event, "internal", False)),
        "metadata": _json_safe(getattr(event, "metadata", None) or {}),
        "timestamp": (
            timestamp.isoformat()
            if isinstance(timestamp, datetime)
            else datetime.now().isoformat()
        ),
    }


def enqueue_message_event(event: Any, *, session_key: str) -> AdmissionReceipt:
    """Append once, coalescing repeated arrivals for the same waiting chat."""
    path = _queue_path()
    payload = _event_payload(event)
    message_id = str(payload.get("message_id") or "")
    with _FileLock(_lock_path()):
        entries = _read(path)
        for index, existing in enumerate(entries):
            if str(existing.get("session_key") or "") != session_key:
                continue
            # The claimed payload has already been reconstructed for dispatch.
            # A later arrival must be a new FIFO item, otherwise mutating the
            # on-disk copy and then acknowledging it would drop that arrival.
            if existing.get("state") == "claimed":
                continue
            existing_event = existing.get("event") or {}
            existing_message_id = str(existing_event.get("message_id") or "")
            if message_id and existing_message_id == message_id:
                return AdmissionReceipt(
                    item_id=str(existing.get("item_id") or ""),
                    position=index + 1,
                    depth=len(entries),
                    coalesced=True,
                )
            incoming_text = str(payload.get("text") or "").strip()
            previous_text = str(existing_event.get("text") or "").strip()
            if incoming_text and incoming_text != previous_text:
                existing_event["text"] = (
                    f"{previous_text}\n\n{incoming_text}".strip()
                )
            existing_event["metadata"] = payload.get("metadata") or {}
            existing_event["message_id"] = payload.get("message_id")
            existing["event"] = existing_event
            existing["updated_at"] = datetime.now().isoformat()
            _write(path, entries)
            return AdmissionReceipt(
                item_id=str(existing.get("item_id") or ""),
                position=index + 1,
                depth=len(entries),
                coalesced=True,
            )
        if len(entries) >= _MAX_ENTRIES:
            raise RuntimeError(
                f"Gateway admission queue is full ({len(entries)}/{_MAX_ENTRIES})"
            )
        item_id = uuid.uuid4().hex
        entries.append(
            {
                "item_id": item_id,
                "session_key": str(session_key),
                "enqueued_at": datetime.now().isoformat(),
                "event": payload,
            }
        )
        _write(path, entries)
        return AdmissionReceipt(
            item_id=item_id,
            position=len(entries),
            depth=len(entries),
        )


def pop_next_message_event() -> Optional[tuple[str, Any]]:
    """Durably claim the oldest entry and reconstruct its MessageEvent.

    The entry stays on disk until :func:`acknowledge_message_event` runs after
    normal ingress has claimed a foreground slot. A gateway crash therefore
    reclaims the dead process's item instead of losing the message in a
    pop-before-dispatch window.
    """
    path = _queue_path()
    with _FileLock(_lock_path()):
        entries = _read(path)
        if not entries:
            return None
        entry = None
        for candidate in entries:
            claimed_pid = candidate.get("claim_pid")
            if candidate.get("state") != "claimed" or not _pid_alive(claimed_pid):
                entry = candidate
                break
        if entry is None:
            return None
        entry["state"] = "claimed"
        entry["claim_pid"] = os.getpid()
        entry["claimed_at"] = datetime.now().isoformat()
        _write(path, entries)
    raw = entry.get("event") or {}
    try:
        from gateway.platforms.base import MessageEvent, MessageType
        from gateway.session import SessionSource

        timestamp_raw = str(raw.get("timestamp") or "")
        try:
            timestamp = datetime.fromisoformat(timestamp_raw)
        except ValueError:
            timestamp = datetime.now()
        source = SessionSource.from_dict(dict(raw.get("source") or {}))
        source_runtime = raw.get("source_runtime") or {}
        source.is_bot = bool(source_runtime.get("is_bot"))
        source.role_authorized = bool(source_runtime.get("role_authorized"))
        source.delivered_via_upstream_relay = bool(
            source_runtime.get("delivered_via_upstream_relay")
        )
        event = MessageEvent(
            text=str(raw.get("text") or ""),
            message_type=MessageType(str(raw.get("message_type") or "text")),
            source=source,
            message_id=raw.get("message_id"),
            platform_update_id=raw.get("platform_update_id"),
            media_urls=list(raw.get("media_urls") or []),
            media_types=list(raw.get("media_types") or []),
            reply_to_message_id=raw.get("reply_to_message_id"),
            reply_to_text=raw.get("reply_to_text"),
            reply_to_author_id=raw.get("reply_to_author_id"),
            reply_to_author_name=raw.get("reply_to_author_name"),
            reply_to_is_own_message=bool(raw.get("reply_to_is_own_message")),
            auto_skill=raw.get("auto_skill"),
            channel_prompt=raw.get("channel_prompt"),
            channel_context=raw.get("channel_context"),
            internal=bool(raw.get("internal")),
            metadata=dict(raw.get("metadata") or {}),
            timestamp=timestamp,
        )
        setattr(event, "_hermes_admission_replay", True)
        return str(entry.get("item_id") or ""), event
    except Exception:
        logger.exception(
            "Malformed durable gateway admission item %s remains queued",
            entry.get("item_id"),
        )
        release_message_event_claim(str(entry.get("item_id") or ""))
        return None


def acknowledge_message_event(item_id: str) -> bool:
    """Delete only an item claimed by this process after ingress acceptance."""
    path = _queue_path()
    with _FileLock(_lock_path()):
        entries = _read(path)
        kept = [
            entry
            for entry in entries
            if not (
                str(entry.get("item_id") or "") == item_id
                and entry.get("state") == "claimed"
                and entry.get("claim_pid") == os.getpid()
            )
        ]
        if len(kept) == len(entries):
            return False
        _write(path, kept)
        return True


def release_message_event_claim(item_id: str) -> bool:
    """Return this process's unaccepted claim to the head of the FIFO."""
    path = _queue_path()
    with _FileLock(_lock_path()):
        entries = _read(path)
        for entry in entries:
            if (
                str(entry.get("item_id") or "") == item_id
                and entry.get("state") == "claimed"
                and entry.get("claim_pid") == os.getpid()
            ):
                entry.pop("state", None)
                entry.pop("claim_pid", None)
                entry.pop("claimed_at", None)
                _write(path, entries)
                return True
        return False


def admission_queue_depth() -> int:
    with _FileLock(_lock_path()):
        return len(_read(_queue_path()))
