"""Durable, profile-local FIFO for admitted gateway follow-up turns.

The active adapter keeps at most one hydrated ``MessageEvent`` per session.
Every additional admitted turn lives in this SQLite journal until the exact
provider delivery callback acknowledges it.  A process death therefore
replays the oldest unacknowledged turn instead of dropping an in-memory tail.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import time
from typing import Any, BinaryIO, Iterable, Mapping, Optional

from hermes_constants import get_hermes_home
from hermes_cli.turn_origin import (
    TurnAttachmentOriginV1,
    TurnOriginV1,
    coerce_turn_origin,
    turn_attachment_path_fingerprint,
)

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows falls back to bounded leases.
    fcntl = None


_SCHEMA_VERSION = "gateway-turn-queue/1"
_QUEUE_RELATIVE_PATH = Path("gateway-turn-queue")
_DB_FILENAME = "queue.sqlite3"
_ATTACHMENTS_DIRECTORY = "attachments"
_OWNERS_DIRECTORY = "owners"
_CLAIM_SECONDS = 30 * 60
_COPY_CHUNK_BYTES = 1024 * 1024
_OWNER_LOCKS: dict[tuple[str, str], BinaryIO] = {}


class DurableTurnQueueError(RuntimeError):
    """The gateway could not durably admit or recover a queued turn."""


@dataclass(frozen=True, slots=True)
class ClaimedTurn:
    queue_id: str
    session_key: str
    event: Any
    claim_owner: str
    profile_home: Path


def _root(profile_home: Optional[Path] = None) -> Path:
    home = (
        Path(profile_home)
        if profile_home is not None
        else Path(get_hermes_home())
    )
    return home.expanduser().resolve() / _QUEUE_RELATIVE_PATH


def _secure_root(profile_home: Optional[Path] = None) -> Path:
    root = _root(profile_home)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        mode = root.lstat().st_mode
    except OSError as exc:
        raise DurableTurnQueueError("gateway.turn_queue_root_unavailable") from exc
    if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
        raise DurableTurnQueueError("gateway.turn_queue_root_unsafe")
    try:
        root.chmod(0o700)
    except OSError:
        pass
    attachments = root / _ATTACHMENTS_DIRECTORY
    attachments.mkdir(mode=0o700, parents=True, exist_ok=True)
    attachment_mode = attachments.lstat().st_mode
    if not stat.S_ISDIR(attachment_mode) or stat.S_ISLNK(attachment_mode):
        raise DurableTurnQueueError("gateway.turn_queue_attachments_unsafe")
    try:
        attachments.chmod(0o700)
    except OSError:
        pass
    owners = root / _OWNERS_DIRECTORY
    owners.mkdir(mode=0o700, parents=True, exist_ok=True)
    owner_mode = owners.lstat().st_mode
    if not stat.S_ISDIR(owner_mode) or stat.S_ISLNK(owner_mode):
        raise DurableTurnQueueError("gateway.turn_queue_owners_unsafe")
    try:
        owners.chmod(0o700)
    except OSError:
        pass
    return root


def _connect(profile_home: Optional[Path] = None) -> sqlite3.Connection:
    root = _secure_root(profile_home)
    path = root / _DB_FILENAME
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            stat.S_IRUSR | stat.S_IWUSR,
        )
        os.close(descriptor)
    else:
        if not stat.S_ISREG(mode) or stat.S_ISLNK(mode):
            raise DurableTurnQueueError("gateway.turn_queue_database_unsafe")
    connection = sqlite3.connect(path, timeout=30, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS queued_turns (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            queue_id TEXT NOT NULL UNIQUE,
            session_key TEXT NOT NULL,
            provider TEXT NOT NULL,
            event_id TEXT NOT NULL,
            payload_digest TEXT NOT NULL,
            event_json TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending'
                CHECK (state IN ('pending', 'claimed')),
            claim_owner TEXT,
            claim_until REAL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            next_attempt_at REAL NOT NULL DEFAULT 0,
            created_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_queued_turns_session_sequence
            ON queued_turns(session_key, sequence);
        CREATE INDEX IF NOT EXISTS ix_queued_turns_claim
            ON queued_turns(state, claim_until, sequence);
        CREATE TABLE IF NOT EXISTS completed_turns (
            queue_id TEXT PRIMARY KEY,
            session_key TEXT NOT NULL,
            provider TEXT NOT NULL,
            event_id TEXT NOT NULL,
            payload_digest TEXT NOT NULL,
            disposition TEXT NOT NULL
                CHECK (disposition IN ('delivered', 'cancelled')),
            completed_at REAL NOT NULL,
            snapshot_cleaned_at REAL
        );
        CREATE INDEX IF NOT EXISTS ix_completed_turns_session
            ON completed_turns(session_key, completed_at);
        """
    )
    columns = {
        str(row["name"])
        for row in connection.execute(
            "PRAGMA table_info(queued_turns)"
        ).fetchall()
    }
    for name, definition in (
        ("attempt_count", "INTEGER NOT NULL DEFAULT 0"),
        ("next_attempt_at", "REAL NOT NULL DEFAULT 0"),
    ):
        if name in columns:
            continue
        try:
            connection.execute(
                f"ALTER TABLE queued_turns ADD COLUMN {name} {definition}"
            )
        except sqlite3.OperationalError:
            refreshed = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(queued_turns)"
                ).fetchall()
            }
            if name not in refreshed:
                raise
    completed_columns = {
        str(row["name"])
        for row in connection.execute(
            "PRAGMA table_info(completed_turns)"
        ).fetchall()
    }
    if "snapshot_cleaned_at" not in completed_columns:
        try:
            connection.execute(
                "ALTER TABLE completed_turns "
                "ADD COLUMN snapshot_cleaned_at REAL"
            )
        except sqlite3.OperationalError:
            refreshed = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(completed_turns)"
                ).fetchall()
            }
            if "snapshot_cleaned_at" not in refreshed:
                raise
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return connection


def _owner_lock_path(root: Path, owner: str) -> Path:
    digest = hashlib.sha256(str(owner).encode("utf-8")).hexdigest()
    return root / _OWNERS_DIRECTORY / f"{digest}.lock"


def _ensure_owner_lock(profile_home: Path, owner: str) -> None:
    """Hold a process-lifetime lock so a fresh process can reclaim immediately."""

    if not owner:
        raise DurableTurnQueueError("gateway.turn_queue_claim_owner_missing")
    if fcntl is None:
        return
    root = _secure_root(profile_home)
    key = (str(root), str(owner))
    if key in _OWNER_LOCKS:
        return
    path = _owner_lock_path(root, owner)
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, stat.S_IRUSR | stat.S_IWUSR)
    stream = os.fdopen(descriptor, "r+b", buffering=0)
    try:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError) as exc:
        stream.close()
        raise DurableTurnQueueError(
            "gateway.turn_queue_claim_owner_collision"
        ) from exc
    _OWNER_LOCKS[key] = stream


def _owner_is_live(profile_home: Path, owner: Optional[str]) -> bool:
    if not owner:
        return False
    if fcntl is None:
        return True
    root = _secure_root(profile_home)
    key = (str(root), str(owner))
    if key in _OWNER_LOCKS:
        return True
    path = _owner_lock_path(root, str(owner))
    try:
        flags = os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        return False
    except OSError:
        return True
    stream = os.fdopen(descriptor, "r+b", buffering=0)
    try:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError):
        stream.close()
        return True
    try:
        fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
    finally:
        stream.close()
    return False


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {
            str(key): _json_safe(item)
            for key, item in value.items()
            if isinstance(key, (str, int, float, bool))
        }
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return {"__hermes_type__": type(value).__name__}


def _copy_snapshot(source: Path, destination: Path) -> str:
    digest = hashlib.sha256()
    descriptor = None
    try:
        with source.open("rb") as source_stream:
            before = os.fstat(source_stream.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise DurableTurnQueueError(
                    "gateway.turn_queue_attachment_not_regular"
                )
            descriptor = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                stat.S_IRUSR | stat.S_IWUSR,
            )
            with os.fdopen(descriptor, "wb") as output:
                descriptor = None
                while chunk := source_stream.read(_COPY_CHUNK_BYTES):
                    output.write(chunk)
                    digest.update(chunk)
                output.flush()
                os.fsync(output.fileno())
            after = os.fstat(source_stream.fileno())
    except DurableTurnQueueError:
        raise
    except OSError as exc:
        raise DurableTurnQueueError(
            "gateway.turn_queue_attachment_snapshot_failed"
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)

    before_mtime = getattr(before, "st_mtime_ns", None)
    after_mtime = getattr(after, "st_mtime_ns", None)
    if (
        before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
        or before.st_size != after.st_size
        or before_mtime != after_mtime
    ):
        destination.unlink(missing_ok=True)
        raise DurableTurnQueueError("gateway.turn_queue_attachment_changed")
    try:
        destination.chmod(stat.S_IRUSR)
    except OSError:
        pass
    return f"sha256:{digest.hexdigest()}"


def _snapshot_media(
    queue_id: str,
    media_urls: Iterable[Any],
    *,
    profile_home: Optional[Path],
) -> list[str]:
    sources = [Path(str(value)).expanduser() for value in media_urls]
    if not sources:
        return []
    root = _secure_root(profile_home)
    digest = queue_id.removeprefix("qturn_")
    final_directory = root / _ATTACHMENTS_DIRECTORY / digest
    staging = root / _ATTACHMENTS_DIRECTORY / f".{digest}.{os.urandom(8).hex()}"
    staging.mkdir(mode=0o700)
    snapshots: list[str] = []
    manifest: list[dict[str, Any]] = []
    try:
        for index, source in enumerate(sources):
            safe_suffix = source.suffix[:20] if source.suffix else ".bin"
            name = f"{index:08d}{safe_suffix}"
            destination = staging / name
            checksum = _copy_snapshot(source, destination)
            snapshots.append(name)
            manifest.append(
                {
                    "position": index,
                    "filename": name,
                    "checksum": checksum,
                    "sizeBytes": destination.stat().st_size,
                }
            )
        manifest_path = staging / "manifest.json"
        encoded = _canonical_json(
            {
                "schemaVersion": _SCHEMA_VERSION,
                "queueId": queue_id,
                "files": manifest,
            }
        ).encode("utf-8")
        descriptor = os.open(
            manifest_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            stat.S_IRUSR | stat.S_IWUSR,
        )
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            manifest_path.chmod(stat.S_IRUSR)
        except OSError:
            pass
        if final_directory.exists():
            shutil.rmtree(staging)
        else:
            os.replace(staging, final_directory)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return [str(final_directory / name) for name in snapshots]


def _validate_media_snapshots(
    media_urls: Iterable[Any],
    *,
    queue_id: str,
    profile_home: Path,
) -> None:
    paths = [str(value) for value in media_urls]
    if not paths:
        return
    digest = str(queue_id).removeprefix("qturn_")
    directory = (
        _secure_root(profile_home)
        / _ATTACHMENTS_DIRECTORY
        / digest
    )
    try:
        directory_mode = directory.lstat().st_mode
        manifest_path = directory / "manifest.json"
        manifest_mode = manifest_path.lstat().st_mode
        if (
            not stat.S_ISDIR(directory_mode)
            or stat.S_ISLNK(directory_mode)
            or not stat.S_ISREG(manifest_mode)
            or stat.S_ISLNK(manifest_mode)
        ):
            raise DurableTurnQueueError(
                "gateway.turn_queue_attachment_manifest_unsafe"
            )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except DurableTurnQueueError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise DurableTurnQueueError(
            "gateway.turn_queue_attachment_manifest_invalid"
        ) from exc
    files = manifest.get("files") if isinstance(manifest, dict) else None
    if (
        manifest.get("schemaVersion") != _SCHEMA_VERSION
        or manifest.get("queueId") != queue_id
        or not isinstance(files, list)
        or len(files) != len(paths)
    ):
        raise DurableTurnQueueError(
            "gateway.turn_queue_attachment_manifest_invalid"
        )
    for index, (path, descriptor) in enumerate(
        zip(paths, files, strict=True)
    ):
        if not isinstance(descriptor, dict):
            raise DurableTurnQueueError(
                "gateway.turn_queue_attachment_manifest_invalid"
            )
        expected_name = f"{index:08d}{Path(path).suffix[:20] or '.bin'}"
        name = str(descriptor.get("filename") or "")
        expected_path = str(directory / name)
        if (
            int(descriptor.get("position", -1)) != index
            or name != expected_name
            or path != expected_path
        ):
            raise DurableTurnQueueError(
                "gateway.turn_queue_attachment_path_mismatch"
            )
        snapshot_path = Path(path)
        checksum = hashlib.sha256()
        try:
            flags = os.O_RDONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            file_descriptor = os.open(snapshot_path, flags)
            with os.fdopen(file_descriptor, "rb") as stream:
                before = os.fstat(stream.fileno())
                if not stat.S_ISREG(before.st_mode):
                    raise DurableTurnQueueError(
                        "gateway.turn_queue_attachment_not_regular"
                    )
                while chunk := stream.read(_COPY_CHUNK_BYTES):
                    checksum.update(chunk)
                after = os.fstat(stream.fileno())
        except DurableTurnQueueError:
            raise
        except OSError as exc:
            raise DurableTurnQueueError(
                "gateway.turn_queue_attachment_snapshot_unavailable"
            ) from exc
        if (
            before.st_dev != after.st_dev
            or before.st_ino != after.st_ino
            or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
            or int(descriptor.get("sizeBytes", -1)) != after.st_size
            or str(descriptor.get("checksum") or "")
            != f"sha256:{checksum.hexdigest()}"
        ):
            raise DurableTurnQueueError(
                "gateway.turn_queue_attachment_snapshot_corrupt"
            )


def _rebind_snapshot_media(
    event: Any,
    origin: TurnOriginV1,
    snapshot_paths: list[str],
) -> TurnOriginV1:
    """Point the admitted event and its private attachment capabilities at CAS."""

    prior_attachments = tuple(origin.attachments or ())
    if len(prior_attachments) != len(snapshot_paths):
        raise DurableTurnQueueError(
            "gateway.turn_queue_attachment_origin_mismatch"
        )
    rebound: list[TurnAttachmentOriginV1] = []
    for attachment, path in zip(
        prior_attachments,
        snapshot_paths,
        strict=True,
    ):
        fingerprint = turn_attachment_path_fingerprint(path)
        if fingerprint is None:
            raise DurableTurnQueueError(
                "gateway.turn_queue_attachment_snapshot_invalid"
            )
        rebound.append(
            TurnAttachmentOriginV1(
                attachment_id=attachment.attachment_id,
                ingress_ordinal=attachment.ingress_ordinal,
                path_fingerprint=fingerprint,
                local_path=path,
            )
        )
    rebound_origin = replace(origin, attachments=tuple(rebound))
    event.media_urls = list(snapshot_paths)
    event.turn_origin = rebound_origin
    return rebound_origin


def _serialize_event(
    event: Any,
    *,
    session_key: str,
    profile_home: Optional[Path],
) -> tuple[str, str, str, str]:
    origin = coerce_turn_origin(getattr(event, "turn_origin", None))
    if origin is None:
        origin = event.ensure_turn_origin()
    if not origin.event_id:
        raise DurableTurnQueueError(
            "gateway.turn_queue_origin_event_id_missing"
        )
    source = getattr(event, "source", None)
    if source is None or not hasattr(source, "to_dict"):
        raise DurableTurnQueueError("gateway.turn_queue_source_missing")
    identity = {
        "schemaVersion": _SCHEMA_VERSION,
        "sessionKey": str(session_key),
        "provider": origin.provider,
        "eventId": origin.event_id,
    }
    queue_id = "qturn_" + hashlib.sha256(
        _canonical_json(identity).encode("utf-8")
    ).hexdigest()
    media_urls = _snapshot_media(
        queue_id,
        getattr(event, "media_urls", ()) or (),
        profile_home=profile_home,
    )
    origin = _rebind_snapshot_media(event, origin, media_urls)
    message_type = getattr(event, "message_type", None)
    message_type_value = getattr(message_type, "value", message_type)
    payload = {
        "schemaVersion": _SCHEMA_VERSION,
        "queueId": queue_id,
        "sessionKey": str(session_key),
        "text": str(getattr(event, "text", "") or ""),
        "messageType": str(message_type_value or "text"),
        "source": _json_safe(source.to_dict()),
        "messageId": getattr(event, "message_id", None),
        "platformUpdateId": getattr(event, "platform_update_id", None),
        "mediaUrls": media_urls,
        "mediaTypes": [
            str(value)
            for value in (getattr(event, "media_types", ()) or ())
        ],
        "replyToMessageId": getattr(event, "reply_to_message_id", None),
        "replyToText": getattr(event, "reply_to_text", None),
        "replyToAuthorId": getattr(event, "reply_to_author_id", None),
        "replyToAuthorName": getattr(event, "reply_to_author_name", None),
        "replyToIsOwnMessage": bool(
            getattr(event, "reply_to_is_own_message", False)
        ),
        "autoSkill": _json_safe(getattr(event, "auto_skill", None)),
        "channelPrompt": getattr(event, "channel_prompt", None),
        "channelContext": getattr(event, "channel_context", None),
        "internal": bool(getattr(event, "internal", False)),
        "metadata": _json_safe(getattr(event, "metadata", {}) or {}),
        # Local arrival time is not provider identity and changes on
        # redelivery. Preserve only the authoritative source timestamp that is
        # already bound into TurnOrigin.
        "timestamp": origin.source_timestamp,
        "eventId": origin.event_id,
        "turnOrigin": origin.to_dict(),
    }
    event_json = _canonical_json(payload)
    payload_digest = "sha256:" + hashlib.sha256(
        event_json.encode("utf-8")
    ).hexdigest()
    return queue_id, origin.provider, payload_digest, event_json


def _discard_snapshot(queue_id: str, profile_home: Path) -> bool:
    digest = str(queue_id).removeprefix("qturn_")
    path = _root(profile_home) / _ATTACHMENTS_DIRECTORY / digest
    shutil.rmtree(
        path,
        ignore_errors=True,
    )
    return not os.path.lexists(path)


def cleanup_completed_snapshots(
    *,
    profile_home: Optional[Path] = None,
    limit: int = 200,
) -> tuple[int, bool]:
    """Converge receipt-owned attachment cleanup in a bounded page.

    The completion receipt commits before filesystem cleanup. A hard process
    exit in that window therefore leaves a durable cleanup intent, not an
    untracked private-file leak. Repeated calls are idempotent and bounded;
    callers continue while ``has_more`` is true.
    """

    home = (
        Path(profile_home).expanduser().resolve()
        if profile_home is not None
        else Path(get_hermes_home()).expanduser().resolve()
    )
    bounded_limit = max(1, min(int(limit), 200))
    with _connect(home) as connection:
        rows = connection.execute(
            """
            SELECT queue_id
            FROM completed_turns
            WHERE snapshot_cleaned_at IS NULL
            ORDER BY completed_at, queue_id
            LIMIT ?
            """,
            (bounded_limit,),
        ).fetchall()
    cleaned_ids = [
        str(row["queue_id"])
        for row in rows
        if _discard_snapshot(str(row["queue_id"]), home)
    ]
    if cleaned_ids:
        cleaned_at = time.time()
        with _connect(home) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.executemany(
                """
                UPDATE completed_turns
                SET snapshot_cleaned_at=?
                WHERE queue_id=? AND snapshot_cleaned_at IS NULL
                """,
                [
                    (cleaned_at, queue_id)
                    for queue_id in cleaned_ids
                ],
            )
            connection.execute("COMMIT")
    with _connect(home) as connection:
        remaining = connection.execute(
            """
            SELECT 1 FROM completed_turns
            WHERE snapshot_cleaned_at IS NULL
            LIMIT 1
            """
        ).fetchone()
    return len(cleaned_ids), remaining is not None


def _mark_completed_snapshot_cleaned(
    queue_ids: Iterable[str],
    *,
    profile_home: Path,
) -> None:
    ids = [str(queue_id) for queue_id in queue_ids]
    if not ids:
        return
    cleaned_at = time.time()
    with _connect(profile_home) as connection:
        connection.executemany(
            """
            UPDATE completed_turns
            SET snapshot_cleaned_at=?
            WHERE queue_id=? AND snapshot_cleaned_at IS NULL
            """,
            [(cleaned_at, queue_id) for queue_id in ids],
        )


def _deserialize_event(
    event_json: str,
    *,
    queue_id: str,
    profile_home: Path,
) -> Any:
    from gateway.config import Platform
    from gateway.platforms.base import MessageEvent, MessageType
    from gateway.session import SessionSource

    try:
        payload = json.loads(event_json)
        if (
            not isinstance(payload, dict)
            or payload.get("schemaVersion") != _SCHEMA_VERSION
            or payload.get("queueId") != queue_id
        ):
            raise ValueError("invalid queue envelope")
        _validate_media_snapshots(
            payload.get("mediaUrls") or (),
            queue_id=queue_id,
            profile_home=profile_home,
        )
        source = SessionSource.from_dict(dict(payload["source"]))
        origin = TurnOriginV1.from_mapping(dict(payload["turnOrigin"]))
        message_type = MessageType(str(payload.get("messageType") or "text"))
        timestamp_raw = payload.get("timestamp")
        timestamp = (
            datetime.fromisoformat(timestamp_raw)
            if isinstance(timestamp_raw, str) and timestamp_raw
            else datetime.now()
        )
        event = MessageEvent(
            text=str(payload.get("text") or ""),
            message_type=message_type,
            source=source,
            message_id=payload.get("messageId"),
            platform_update_id=payload.get("platformUpdateId"),
            media_urls=[
                str(value) for value in (payload.get("mediaUrls") or ())
            ],
            media_types=[
                str(value) for value in (payload.get("mediaTypes") or ())
            ],
            reply_to_message_id=payload.get("replyToMessageId"),
            reply_to_text=payload.get("replyToText"),
            reply_to_author_id=payload.get("replyToAuthorId"),
            reply_to_author_name=payload.get("replyToAuthorName"),
            reply_to_is_own_message=bool(
                payload.get("replyToIsOwnMessage", False)
            ),
            auto_skill=payload.get("autoSkill"),
            channel_prompt=payload.get("channelPrompt"),
            channel_context=payload.get("channelContext"),
            internal=bool(payload.get("internal", False)),
            metadata=dict(payload.get("metadata") or {}),
            timestamp=timestamp,
            event_id=origin.event_id,
            turn_origin=origin,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise DurableTurnQueueError(
            "gateway.turn_queue_payload_invalid"
        ) from exc
    setattr(event, "_hermes_durable_queue_id", queue_id)
    setattr(event, "_hermes_durable_queue_home", str(profile_home))
    return event


def enqueue_turn(
    session_key: str,
    event: Any,
    *,
    profile_home: Optional[Path] = None,
) -> str:
    """Durably append one exact provider event; identical redelivery dedupes."""

    home = (
        Path(profile_home).expanduser().resolve()
        if profile_home is not None
        else Path(get_hermes_home()).expanduser().resolve()
    )
    queue_id, provider, payload_digest, event_json = _serialize_event(
        event,
        session_key=session_key,
        profile_home=home,
    )
    origin = coerce_turn_origin(getattr(event, "turn_origin", None))
    if origin is None:
        raise DurableTurnQueueError("gateway.turn_queue_origin_missing")
    completed_redelivery = False
    with _connect(home) as connection:
        connection.execute("BEGIN IMMEDIATE")
        existing = connection.execute(
            "SELECT payload_digest FROM queued_turns WHERE queue_id=?",
            (queue_id,),
        ).fetchone()
        completed = connection.execute(
            "SELECT payload_digest FROM completed_turns WHERE queue_id=?",
            (queue_id,),
        ).fetchone()
        if existing is not None and completed is not None:
            connection.execute("ROLLBACK")
            raise DurableTurnQueueError(
                "gateway.turn_queue_receipt_overlap"
            )
        if completed is not None:
            completed_redelivery = True
            if str(completed["payload_digest"]) != payload_digest:
                connection.execute("ROLLBACK")
                _discard_snapshot(queue_id, home)
                raise DurableTurnQueueError(
                    "gateway.turn_queue_identity_conflict"
                )
        elif existing is None:
            connection.execute(
                """
                INSERT INTO queued_turns (
                    queue_id, session_key, provider, event_id,
                    payload_digest, event_json, state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    queue_id,
                    str(session_key),
                    provider,
                    origin.event_id,
                    payload_digest,
                    event_json,
                    time.time(),
                ),
            )
        elif str(existing["payload_digest"]) != payload_digest:
            connection.execute("ROLLBACK")
            raise DurableTurnQueueError(
                "gateway.turn_queue_identity_conflict"
            )
        connection.execute("COMMIT")
    if completed_redelivery:
        # The durable completion receipt owns provider-redelivery dedupe after
        # the queue row has been retired. _serialize_event may have recreated
        # the deterministic attachment directory before this lookup; those
        # bytes are no longer an admitted capability.
        if _discard_snapshot(queue_id, home):
            _mark_completed_snapshot_cleaned(
                [queue_id],
                profile_home=home,
            )
        setattr(event, "_hermes_durable_turn_completed", True)
    setattr(event, "_hermes_durable_queue_id", queue_id)
    setattr(event, "_hermes_durable_queue_home", str(home))
    return queue_id


def _claim_row(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    owner: str,
    profile_home: Path,
    claim_seconds: float,
) -> Optional[sqlite3.Row]:
    now = time.time()
    existing_owner = (
        str(row["claim_owner"]) if row["claim_owner"] is not None else None
    )
    lease_expired = (
        row["claim_until"] is None
        or float(row["claim_until"]) <= now
    )
    if (
        str(row["state"]) == "pending"
        and float(row["next_attempt_at"] or 0) > now
    ):
        return None
    if (
        str(row["state"]) == "claimed"
        and existing_owner != owner
        and not lease_expired
        and _owner_is_live(profile_home, existing_owner)
    ):
        return None
    if str(row["state"]) == "claimed" and existing_owner != owner:
        connection.execute(
            """
            UPDATE queued_turns
            SET state='pending', claim_owner=NULL, claim_until=NULL
            WHERE queue_id=? AND claim_owner=?
            """,
            (str(row["queue_id"]), existing_owner),
        )
    updated = connection.execute(
        """
        UPDATE queued_turns
        SET state='claimed', claim_owner=?, claim_until=?
        WHERE queue_id=?
          AND (
            state='pending'
            OR claim_until IS NULL
            OR claim_until <= ?
            OR claim_owner=?
          )
        """,
        (
            owner,
            now + max(float(claim_seconds), 1.0),
            str(row["queue_id"]),
            now,
            owner,
        ),
    )
    if updated.rowcount != 1:
        return None
    return connection.execute(
        "SELECT * FROM queued_turns WHERE queue_id=?",
        (str(row["queue_id"]),),
    ).fetchone()


def claim_turn(
    queue_id: str,
    *,
    owner: str,
    profile_home: Optional[Path] = None,
    claim_seconds: float = _CLAIM_SECONDS,
) -> Optional[ClaimedTurn]:
    home = (
        Path(profile_home).expanduser().resolve()
        if profile_home is not None
        else Path(get_hermes_home()).expanduser().resolve()
    )
    _ensure_owner_lock(home, owner)
    with _connect(home) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT * FROM queued_turns WHERE queue_id=?",
            (str(queue_id),),
        ).fetchone()
        if row is None:
            connection.execute("COMMIT")
            return None
        predecessor = connection.execute(
            """
            SELECT 1 FROM queued_turns
            WHERE session_key=? AND sequence < ?
            LIMIT 1
            """,
            (str(row["session_key"]), int(row["sequence"])),
        ).fetchone()
        if predecessor is not None:
            connection.execute("COMMIT")
            return None
        row = _claim_row(
            connection,
            row,
            owner=owner,
            profile_home=home,
            claim_seconds=claim_seconds,
        )
        if row is None:
            connection.execute("COMMIT")
            return None
        connection.execute("COMMIT")
    return ClaimedTurn(
        queue_id=str(row["queue_id"]),
        session_key=str(row["session_key"]),
        event=_deserialize_event(
            str(row["event_json"]),
            queue_id=str(row["queue_id"]),
            profile_home=home,
        ),
        claim_owner=owner,
        profile_home=home,
    )


def claim_next_turn(
    session_key: str,
    *,
    owner: str,
    profile_home: Optional[Path] = None,
    claim_seconds: float = _CLAIM_SECONDS,
) -> Optional[ClaimedTurn]:
    home = (
        Path(profile_home).expanduser().resolve()
        if profile_home is not None
        else Path(get_hermes_home()).expanduser().resolve()
    )
    _ensure_owner_lock(home, owner)
    with _connect(home) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            """
            SELECT * FROM queued_turns
            WHERE session_key=?
            ORDER BY sequence
            LIMIT 1
            """,
            (str(session_key),),
        ).fetchone()
        if row is None:
            connection.execute("COMMIT")
            return None
        row = _claim_row(
            connection,
            row,
            owner=owner,
            profile_home=home,
            claim_seconds=claim_seconds,
        )
        if row is None:
            connection.execute("COMMIT")
            return None
        connection.execute("COMMIT")
    return ClaimedTurn(
        queue_id=str(row["queue_id"]),
        session_key=str(row["session_key"]),
        event=_deserialize_event(
            str(row["event_json"]),
            queue_id=str(row["queue_id"]),
            profile_home=home,
        ),
        claim_owner=owner,
        profile_home=home,
    )


def claim_session_heads(
    *,
    owner: str,
    profile_home: Optional[Path] = None,
    after_session_key: Optional[str] = None,
    limit: int = 50,
    claim_seconds: float = _CLAIM_SECONDS,
) -> tuple[list[ClaimedTurn], Optional[str]]:
    """Claim one oldest turn per session using a bounded opaque scan page."""

    home = (
        Path(profile_home).expanduser().resolve()
        if profile_home is not None
        else Path(get_hermes_home()).expanduser().resolve()
    )
    _ensure_owner_lock(home, owner)
    bounded_limit = max(1, min(int(limit), 200))
    with _connect(home) as connection:
        connection.execute("BEGIN IMMEDIATE")
        rows = connection.execute(
            """
            SELECT queued_turns.*
            FROM queued_turns
            JOIN (
                SELECT session_key, MIN(sequence) AS first_sequence
                FROM queued_turns
                WHERE session_key > ?
                GROUP BY session_key
                ORDER BY session_key
                LIMIT ?
            ) AS heads
              ON heads.first_sequence = queued_turns.sequence
            ORDER BY queued_turns.session_key
            """,
            (str(after_session_key or ""), bounded_limit),
        ).fetchall()
        claimed_rows: list[sqlite3.Row] = []
        for row in rows:
            # This process already scheduled the claimed head. Startup/watch
            # scans must not re-emit it; recursive in-band draining uses the
            # explicit claim_turn/claim_next_turn APIs instead.
            if (
                str(row["state"]) == "claimed"
                and str(row["claim_owner"] or "") == str(owner)
            ):
                continue
            claimed = _claim_row(
                connection,
                row,
                owner=owner,
                profile_home=home,
                claim_seconds=claim_seconds,
            )
            if claimed is not None:
                claimed_rows.append(
                    claimed
                )
        connection.execute("COMMIT")
    claimed = [
        ClaimedTurn(
            queue_id=str(row["queue_id"]),
            session_key=str(row["session_key"]),
            event=_deserialize_event(
                str(row["event_json"]),
                queue_id=str(row["queue_id"]),
                profile_home=home,
            ),
            claim_owner=owner,
            profile_home=home,
        )
        for row in claimed_rows
    ]
    next_cursor = (
        str(rows[-1]["session_key"])
        if len(rows) == bounded_limit
        else None
    )
    return claimed, next_cursor


def acknowledge_turn(
    queue_id: str,
    *,
    owner: str,
    profile_home: Optional[Path] = None,
) -> bool:
    home = (
        Path(profile_home).expanduser().resolve()
        if profile_home is not None
        else Path(get_hermes_home()).expanduser().resolve()
    )
    with _connect(home) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            """
            SELECT queue_id, session_key, provider, event_id, payload_digest
            FROM queued_turns
            WHERE queue_id=? AND state='claimed' AND claim_owner=?
            """,
            (str(queue_id), str(owner)),
        ).fetchone()
        if row is None:
            connection.execute("COMMIT")
            return False
        completed = connection.execute(
            "SELECT payload_digest FROM completed_turns WHERE queue_id=?",
            (str(queue_id),),
        ).fetchone()
        if (
            completed is not None
            and str(completed["payload_digest"])
            != str(row["payload_digest"])
        ):
            connection.execute("ROLLBACK")
            raise DurableTurnQueueError(
                "gateway.turn_queue_receipt_conflict"
            )
        connection.execute(
            """
            INSERT OR IGNORE INTO completed_turns (
                queue_id, session_key, provider, event_id, payload_digest,
                disposition, completed_at
            ) VALUES (?, ?, ?, ?, ?, 'delivered', ?)
            """,
            (
                str(row["queue_id"]),
                str(row["session_key"]),
                str(row["provider"]),
                str(row["event_id"]),
                str(row["payload_digest"]),
                time.time(),
            ),
        )
        removed = connection.execute(
            """
            DELETE FROM queued_turns
            WHERE queue_id=? AND state='claimed' AND claim_owner=?
            """,
            (str(queue_id), str(owner)),
        )
        connection.execute("COMMIT")
    if removed.rowcount != 1:
        return False
    if _discard_snapshot(queue_id, home):
        _mark_completed_snapshot_cleaned(
            [queue_id],
            profile_home=home,
        )
    return True


def release_turn(
    queue_id: str,
    *,
    owner: str,
    profile_home: Optional[Path] = None,
) -> bool:
    home = (
        Path(profile_home).expanduser().resolve()
        if profile_home is not None
        else Path(get_hermes_home()).expanduser().resolve()
    )
    with _connect(home) as connection:
        result = connection.execute(
            """
            UPDATE queued_turns
            SET state='pending', claim_owner=NULL, claim_until=NULL
            WHERE queue_id=? AND state='claimed' AND claim_owner=?
            """,
            (str(queue_id), str(owner)),
        )
    return result.rowcount == 1


def park_turn_for_retry(
    queue_id: str,
    *,
    owner: str,
    profile_home: Optional[Path] = None,
) -> Optional[float]:
    """Release a claim onto an unbounded persisted exponential retry epoch."""

    home = (
        Path(profile_home).expanduser().resolve()
        if profile_home is not None
        else Path(get_hermes_home()).expanduser().resolve()
    )
    with _connect(home) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            """
            SELECT attempt_count FROM queued_turns
            WHERE queue_id=? AND state='claimed' AND claim_owner=?
            """,
            (str(queue_id), str(owner)),
        ).fetchone()
        if row is None:
            connection.execute("COMMIT")
            return None
        attempt = int(row["attempt_count"] or 0) + 1
        base = min(300.0, float(2 ** min(attempt - 1, 8)))
        digest = hashlib.sha256(
            f"{queue_id}:{attempt}".encode("utf-8")
        ).digest()
        jitter = 0.8 + (int.from_bytes(digest[:2], "big") / 65535.0) * 0.4
        delay = base * jitter
        next_attempt_at = time.time() + delay
        updated = connection.execute(
            """
            UPDATE queued_turns
            SET state='pending',
                claim_owner=NULL,
                claim_until=NULL,
                attempt_count=?,
                next_attempt_at=?
            WHERE queue_id=? AND state='claimed' AND claim_owner=?
            """,
            (
                attempt,
                next_attempt_at,
                str(queue_id),
                str(owner),
            ),
        )
        connection.execute("COMMIT")
    return delay if updated.rowcount == 1 else None


def queue_depth(
    session_key: str,
    *,
    profile_home: Optional[Path] = None,
) -> int:
    with _connect(profile_home) as connection:
        row = connection.execute(
            "SELECT COUNT(*) AS count FROM queued_turns WHERE session_key=?",
            (str(session_key),),
        ).fetchone()
    return int(row["count"] if row is not None else 0)


def pending_queue_count(
    *,
    profile_home: Optional[Path] = None,
) -> int:
    with _connect(profile_home) as connection:
        row = connection.execute(
            "SELECT COUNT(*) AS count FROM queued_turns"
        ).fetchone()
    return int(row["count"] if row is not None else 0)


def cancel_session_turns(
    session_key: str,
    *,
    profile_home: Optional[Path] = None,
) -> int:
    """Atomically retire a conversation's admitted turns at an explicit boundary."""

    home = (
        Path(profile_home).expanduser().resolve()
        if profile_home is not None
        else Path(get_hermes_home()).expanduser().resolve()
    )
    with _connect(home) as connection:
        connection.execute("BEGIN IMMEDIATE")
        rows = connection.execute(
            """
            SELECT queue_id, session_key, provider, event_id, payload_digest
            FROM queued_turns
            WHERE session_key=?
            """,
            (str(session_key),),
        ).fetchall()
        completed_at = time.time()
        for row in rows:
            completed = connection.execute(
                "SELECT payload_digest FROM completed_turns WHERE queue_id=?",
                (str(row["queue_id"]),),
            ).fetchone()
            if (
                completed is not None
                and str(completed["payload_digest"])
                != str(row["payload_digest"])
            ):
                connection.execute("ROLLBACK")
                raise DurableTurnQueueError(
                    "gateway.turn_queue_receipt_conflict"
                )
            connection.execute(
                """
                INSERT OR IGNORE INTO completed_turns (
                    queue_id, session_key, provider, event_id, payload_digest,
                    disposition, completed_at
                ) VALUES (?, ?, ?, ?, ?, 'cancelled', ?)
                """,
                (
                    str(row["queue_id"]),
                    str(row["session_key"]),
                    str(row["provider"]),
                    str(row["event_id"]),
                    str(row["payload_digest"]),
                    completed_at,
                ),
            )
        connection.execute(
            "DELETE FROM queued_turns WHERE session_key=?",
            (str(session_key),),
        )
        connection.execute("COMMIT")
    cleaned = [
        str(row["queue_id"])
        for row in rows
        if _discard_snapshot(str(row["queue_id"]), home)
    ]
    _mark_completed_snapshot_cleaned(
        cleaned,
        profile_home=home,
    )
    return len(rows)
