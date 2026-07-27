"""Restart-safe local ingress spool for Dev Hub planning artifacts.

The spool is a short-lived handoff boundary, not the long-term artifact
store.  It snapshots one immutable source file before the first Hub request,
keeps that snapshot while the remote outcome is unknown, and deletes it only
after the caller has observed a convergent Hub success.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
import threading
from typing import Any, Mapping, Optional

from hermes_constants import get_hermes_home
from hermes_cli.dev_hub_planning_v2 import (
    PlanningOriginPayload,
    PlanningV2ConfigError,
)


_RECORD_SCHEMA_VERSION = "1.0"
_TOKEN_PREFIX = "artrec_v2_"
_TOKEN_RE = re.compile(r"^artrec_v2_([0-9a-f]{64})$")
_METADATA_FILENAME = "recovery.json"
_SPOOL_RELATIVE_PATH = Path("planning-v2") / "artifact-ingress"
_COPY_CHUNK_BYTES = 1024 * 1024
_SCOPE_FIELDS = (
    "provider",
    "gatewayAccountId",
    "chatId",
    "threadId",
    "senderId",
)
_ORIGIN_FIELDS = {
    "schemaVersion",
    "provider",
    "gatewayInstanceId",
    "gatewayAccountId",
    "chatId",
    "threadId",
    "messageId",
    "senderId",
    "chatType",
    "sourceTimestamp",
    "providerEventId",
}
_ORIGIN_NULLABLE_FIELDS = {"threadId", "chatType", "sourceTimestamp"}
_ROLE_RE = re.compile(r"^[a-z][a-z0-9._-]*$")
_store_lock = threading.RLock()


@dataclass(frozen=True, slots=True)
class ArtifactRecoveryRecord:
    """One immutable artifact retry contract loaded from the local spool."""

    token: str
    thread_id: str
    snapshot_path: str
    origin: PlanningOriginPayload
    role: str
    position: int
    required: bool
    idempotency_key: str
    content_type: Optional[str]
    retain_until: Optional[str]
    attachment_identity: Optional[str]
    ingress_ordinal: Optional[int]
    checksum: str
    size_bytes: int


def _spool_root(hermes_home: Optional[Path] = None) -> Path:
    home = Path(hermes_home) if hermes_home is not None else get_hermes_home()
    return home.expanduser().resolve() / _SPOOL_RELATIVE_PATH


def _secure_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        path_stat = path.lstat()
    except OSError as exc:
        raise PlanningV2ConfigError(
            "planning.artifact_spool_write_failed",
            detail="Hermes could not open its private artifact ingress spool.",
        ) from exc
    if not stat.S_ISDIR(path_stat.st_mode) or stat.S_ISLNK(
        path_stat.st_mode
    ):
        raise PlanningV2ConfigError(
            "planning.artifact_spool_write_failed",
            detail="The configured artifact ingress spool is not a safe directory.",
        )
    try:
        path.chmod(0o700)
    except OSError:
        # Windows does not enforce POSIX mode bits.
        pass


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    try:
        descriptor = os.open(str(path), flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        # Directory fsync is unavailable on some supported filesystems.
        pass
    finally:
        os.close(descriptor)


def _record_digest(idempotency_key: str) -> str:
    exact_key = str(idempotency_key or "").strip()
    if not exact_key:
        raise PlanningV2ConfigError(
            "planning.artifact_idempotency_key_missing"
        )
    return hashlib.sha256(exact_key.encode("utf-8")).hexdigest()


def _token_for_digest(digest: str) -> str:
    return f"{_TOKEN_PREFIX}{digest}"


def _digest_from_token(token: str) -> str:
    match = _TOKEN_RE.fullmatch(str(token or "").strip())
    if match is None:
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_unavailable",
            detail=(
                "The gateway-private recovery token is invalid or is not "
                "available in this Hermes profile."
            ),
        )
    return match.group(1)


def _record_path(root: Path, digest: str) -> Path:
    return root / digest


def _snapshot_filename(source: Path, digest: str) -> str:
    filename = source.name
    if (
        not filename
        or filename in {".", ".."}
        or len(filename.encode("utf-8")) > 240
        or any(ord(character) < 32 or ord(character) == 127 for character in filename)
    ):
        return f"artifact-{digest[:16]}.bin"
    return filename


def _remove_staging_directory(path: Path) -> None:
    """Remove only files created inside one unpublished staging directory."""

    try:
        for child in path.iterdir():
            if child.is_file() or child.is_symlink():
                child.unlink(missing_ok=True)
        path.rmdir()
    except OSError:
        # A failed best-effort cleanup must not mask the typed spool failure.
        pass


def _remove_acknowledged_directory(path: Path) -> bool:
    """Delete one already-retired record without touching live records."""

    if not path.name.startswith(".acknowledged-"):
        return False
    try:
        metadata = _read_metadata(path)
        snapshot_filename = metadata.get("snapshotFilename")
        if (
            not isinstance(snapshot_filename, str)
            or Path(snapshot_filename).name != snapshot_filename
            or snapshot_filename in {".", ".."}
        ):
            return False
        allowed = {_METADATA_FILENAME, snapshot_filename}
        if any(child.name not in allowed for child in path.iterdir()):
            return False
        (path / snapshot_filename).unlink(missing_ok=True)
        (path / _METADATA_FILENAME).unlink(missing_ok=True)
        path.rmdir()
        return True
    except (OSError, PlanningV2ConfigError):
        return False


def _cleanup_acknowledged_records(root: Path) -> None:
    changed = False
    try:
        candidates = tuple(root.iterdir())
    except OSError:
        return
    for candidate in candidates:
        if candidate.name.startswith(".acknowledged-"):
            changed = _remove_acknowledged_directory(candidate) or changed
    if changed:
        _fsync_directory(root)


def _copy_immutable_snapshot(source: Path, destination: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size_bytes = 0
    try:
        with source.open("rb") as source_stream:
            before = os.fstat(source_stream.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise PlanningV2ConfigError(
                    "planning.artifact_path_invalid",
                    detail="The gateway-cached attachment must be a regular file.",
                )
            descriptor = os.open(
                str(destination),
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                stat.S_IRUSR | stat.S_IWUSR,
            )
            with os.fdopen(descriptor, "wb") as snapshot:
                while True:
                    chunk = source_stream.read(_COPY_CHUNK_BYTES)
                    if not chunk:
                        break
                    snapshot.write(chunk)
                    digest.update(chunk)
                    size_bytes += len(chunk)
                snapshot.flush()
                os.fsync(snapshot.fileno())
            after = os.fstat(source_stream.fileno())
    except PlanningV2ConfigError:
        raise
    except OSError as exc:
        raise PlanningV2ConfigError(
            "planning.artifact_file_unreadable",
            detail=(
                "The gateway-cached attachment could not be durably "
                "snapshotted before upload."
            ),
        ) from exc

    before_mtime = getattr(before, "st_mtime_ns", int(before.st_mtime * 1e9))
    after_mtime = getattr(after, "st_mtime_ns", int(after.st_mtime * 1e9))
    if (
        before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
        or before.st_size != after.st_size
        or before_mtime != after_mtime
        or size_bytes != after.st_size
    ):
        raise PlanningV2ConfigError(
            "planning.artifact_file_changed",
            detail=(
                "The gateway-cached attachment changed while Hermes created "
                "its durable upload snapshot."
            ),
        )
    return f"sha256:{digest.hexdigest()}", size_bytes


def _write_metadata(path: Path, metadata: Mapping[str, Any]) -> None:
    encoded = json.dumps(
        dict(metadata),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    descriptor = os.open(
        str(path),
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        stat.S_IRUSR | stat.S_IWUSR,
    )
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def _read_metadata(record_path: Path) -> dict[str, Any]:
    metadata_path = record_path / _METADATA_FILENAME
    try:
        record_stat = record_path.lstat()
        metadata_stat = metadata_path.lstat()
        if (
            not stat.S_ISDIR(record_stat.st_mode)
            or stat.S_ISLNK(record_stat.st_mode)
            or not stat.S_ISREG(metadata_stat.st_mode)
            or stat.S_ISLNK(metadata_stat.st_mode)
        ):
            raise OSError("unsafe artifact recovery record")
        raw = metadata_path.read_bytes()
        decoded = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_corrupt",
            detail=(
                "The durable artifact recovery record is incomplete or "
                "corrupt; the source attachment was not guessed or replaced."
            ),
        ) from exc
    if not isinstance(decoded, dict):
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_corrupt"
        )
    return decoded


def _optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_corrupt"
        )
    return value


def _valid_origin(origin: Mapping[str, Any]) -> bool:
    if set(origin) != _ORIGIN_FIELDS:
        return False
    for name in _ORIGIN_FIELDS - _ORIGIN_NULLABLE_FIELDS:
        value = origin.get(name)
        if not isinstance(value, str) or not value or value != value.strip():
            return False
    for name in _ORIGIN_NULLABLE_FIELDS:
        value = origin.get(name)
        if value is not None and (
            not isinstance(value, str) or not value or value != value.strip()
        ):
            return False
    provider = origin.get("provider")
    return provider == str(provider).lower()


def _metadata_digest(metadata: Mapping[str, Any]) -> Optional[str]:
    idempotency_key = metadata.get("idempotencyKey")
    if not isinstance(idempotency_key, str) or not idempotency_key.strip():
        return None
    return hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()


def _decode_record(
    *,
    token: str,
    digest: str,
    record_path: Path,
    verify_snapshot: bool,
) -> ArtifactRecoveryRecord:
    metadata = _read_metadata(record_path)
    expected_keys = {
        "schemaVersion",
        "token",
        "recordDigest",
        "threadId",
        "origin",
        "role",
        "position",
        "required",
        "idempotencyKey",
        "contentType",
        "retainUntil",
        "attachmentIdentity",
        "ingressOrdinal",
        "snapshotFilename",
        "checksum",
        "sizeBytes",
    }
    origin = metadata.get("origin")
    snapshot_filename = metadata.get("snapshotFilename")
    position = metadata.get("position")
    ingress_ordinal = metadata.get("ingressOrdinal")
    size_bytes = metadata.get("sizeBytes")
    thread_id = metadata.get("threadId")
    role = metadata.get("role")
    idempotency_key = metadata.get("idempotencyKey")
    attachment_identity = metadata.get("attachmentIdentity")
    if (
        set(metadata) != expected_keys
        or metadata.get("schemaVersion") != _RECORD_SCHEMA_VERSION
        or metadata.get("token") != token
        or metadata.get("recordDigest") != digest
        or _metadata_digest(metadata) != digest
        or not isinstance(origin, dict)
        or not _valid_origin(origin)
        or not isinstance(thread_id, str)
        or not thread_id
        or thread_id != thread_id.strip()
        or not isinstance(role, str)
        or _ROLE_RE.fullmatch(role) is None
        or not isinstance(idempotency_key, str)
        or not idempotency_key
        or idempotency_key != idempotency_key.strip()
        or (
            attachment_identity is not None
            and (
                not isinstance(attachment_identity, str)
                or not attachment_identity
                or attachment_identity != attachment_identity.strip()
            )
        )
        or not isinstance(snapshot_filename, str)
        or not snapshot_filename
        or Path(snapshot_filename).name != snapshot_filename
        or snapshot_filename in {".", ".."}
        or isinstance(position, bool)
        or not isinstance(position, int)
        or position < 1
        or not isinstance(metadata.get("required"), bool)
        or not isinstance(size_bytes, int)
        or isinstance(size_bytes, bool)
        or size_bytes < 0
        or (
            ingress_ordinal is not None
            and (
                isinstance(ingress_ordinal, bool)
                or not isinstance(ingress_ordinal, int)
                or ingress_ordinal < 1
            )
        )
        or not isinstance(metadata.get("checksum"), str)
        or not re.fullmatch(
            r"sha256:[0-9a-f]{64}",
            str(metadata.get("checksum")),
        )
    ):
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_corrupt"
        )
    snapshot_path = record_path / snapshot_filename
    try:
        snapshot_stat = snapshot_path.lstat()
    except OSError as exc:
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_corrupt"
        ) from exc
    if not stat.S_ISREG(snapshot_stat.st_mode) or stat.S_ISLNK(
        snapshot_stat.st_mode
    ):
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_corrupt"
        )
    if snapshot_stat.st_size != size_bytes:
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_corrupt"
        )
    if verify_snapshot:
        actual_digest = hashlib.sha256()
        try:
            with snapshot_path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(_COPY_CHUNK_BYTES), b""):
                    actual_digest.update(chunk)
        except OSError as exc:
            raise PlanningV2ConfigError(
                "planning.artifact_recovery_corrupt"
            ) from exc
        if (
            f"sha256:{actual_digest.hexdigest()}"
            != metadata["checksum"]
        ):
            raise PlanningV2ConfigError(
                "planning.artifact_recovery_corrupt"
            )
    return ArtifactRecoveryRecord(
        token=token,
        thread_id=thread_id,
        snapshot_path=str(snapshot_path.resolve()),
        origin=dict(origin),
        role=role,
        position=position,
        required=metadata["required"],
        idempotency_key=idempotency_key,
        content_type=_optional_text(metadata["contentType"]),
        retain_until=_optional_text(metadata["retainUntil"]),
        attachment_identity=_optional_text(attachment_identity),
        ingress_ordinal=ingress_ordinal,
        checksum=str(metadata["checksum"]),
        size_bytes=size_bytes,
    )


def _expected_contract(
    *,
    token: str,
    digest: str,
    thread_id: str,
    origin: PlanningOriginPayload,
    role: str,
    position: int,
    required: bool,
    idempotency_key: str,
    content_type: Optional[str],
    retain_until: Optional[str],
    attachment_identity: Optional[str],
    ingress_ordinal: Optional[int],
) -> dict[str, Any]:
    return {
        "token": token,
        "recordDigest": digest,
        "threadId": thread_id,
        "origin": dict(origin),
        "role": role,
        "position": position,
        "required": required,
        "idempotencyKey": idempotency_key,
        "contentType": content_type,
        "retainUntil": retain_until,
        "attachmentIdentity": attachment_identity,
        "ingressOrdinal": ingress_ordinal,
    }


def _assert_existing_contract(
    record: ArtifactRecoveryRecord,
    expected: Mapping[str, Any],
) -> None:
    actual = {
        "token": record.token,
        "recordDigest": _digest_from_token(record.token),
        "threadId": record.thread_id,
        "origin": dict(record.origin),
        "role": record.role,
        "position": record.position,
        "required": record.required,
        "idempotencyKey": record.idempotency_key,
        "contentType": record.content_type,
        "retainUntil": record.retain_until,
        "attachmentIdentity": record.attachment_identity,
        "ingressOrdinal": record.ingress_ordinal,
    }
    if actual != dict(expected):
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_contract_conflict",
            detail=(
                "This immutable artifact replay identity already belongs to "
                "different upload metadata; Hermes kept the original snapshot."
            ),
        )


def register_artifact_recovery(
    *,
    thread_id: str,
    local_path: str,
    origin: PlanningOriginPayload,
    role: str,
    position: int,
    required: bool,
    idempotency_key: str,
    content_type: Optional[str],
    retain_until: Optional[str],
    attachment_identity: Optional[str],
    ingress_ordinal: Optional[int],
    hermes_home: Optional[Path] = None,
) -> str:
    """Persist one immutable byte snapshot and return its stable opaque token."""

    digest = _record_digest(idempotency_key)
    token = _token_for_digest(digest)
    root = _spool_root(hermes_home)
    expected = _expected_contract(
        token=token,
        digest=digest,
        thread_id=thread_id,
        origin=origin,
        role=role,
        position=position,
        required=required,
        idempotency_key=idempotency_key,
        content_type=content_type,
        retain_until=retain_until,
        attachment_identity=attachment_identity,
        ingress_ordinal=ingress_ordinal,
    )
    with _store_lock:
        _secure_directory(root)
        _fsync_directory(root.parent)
        _cleanup_acknowledged_records(root)
        final_path = _record_path(root, digest)
        if final_path.exists():
            existing = _decode_record(
                token=token,
                digest=digest,
                record_path=final_path,
                verify_snapshot=True,
            )
            _assert_existing_contract(existing, expected)
            return token

        source = Path(local_path).expanduser()
        if not source.is_absolute():
            raise PlanningV2ConfigError(
                "planning.artifact_path_invalid",
                detail=(
                    "Artifact path must be the exact absolute gateway-cached "
                    "attachment path."
                ),
            )
        staging = root / (
            f".staging-{digest}-{os.getpid()}-{secrets.token_hex(6)}"
        )
        try:
            staging.mkdir(mode=0o700)
            filename = _snapshot_filename(source, digest)
            snapshot_path = staging / filename
            checksum, size_bytes = _copy_immutable_snapshot(
                source,
                snapshot_path,
            )
            metadata = {
                "schemaVersion": _RECORD_SCHEMA_VERSION,
                **expected,
                "snapshotFilename": filename,
                "checksum": checksum,
                "sizeBytes": size_bytes,
            }
            _write_metadata(staging / _METADATA_FILENAME, metadata)
            _fsync_directory(staging)
            try:
                os.rename(staging, final_path)
            except OSError:
                if not final_path.exists():
                    raise
                _remove_staging_directory(staging)
                existing = _decode_record(
                    token=token,
                    digest=digest,
                    record_path=final_path,
                    verify_snapshot=True,
                )
                _assert_existing_contract(existing, expected)
                return token
            _fsync_directory(root)
        except PlanningV2ConfigError:
            _remove_staging_directory(staging)
            raise
        except OSError as exc:
            _remove_staging_directory(staging)
            raise PlanningV2ConfigError(
                "planning.artifact_spool_write_failed",
                detail=(
                    "Hermes could not durably stage the attachment before "
                    "upload; no remote write was attempted."
                ),
            ) from exc
    return token


def _load_artifact_recovery(
    token: str,
    *,
    current_origin: PlanningOriginPayload,
    verify_snapshot: bool,
    hermes_home: Optional[Path] = None,
) -> ArtifactRecoveryRecord:
    digest = _digest_from_token(token)
    root = _spool_root(hermes_home)
    record_path = _record_path(root, digest)
    with _store_lock:
        if not record_path.exists():
            raise PlanningV2ConfigError(
                "planning.artifact_recovery_unavailable",
                detail=(
                    "The gateway-private recovery token is not available in "
                    "this Hermes profile. No cached absolute path was exposed."
                ),
            )
        record = _decode_record(
            token=token,
            digest=digest,
            record_path=record_path,
            verify_snapshot=verify_snapshot,
        )
    if any(
        current_origin.get(name) != record.origin.get(name)
        for name in _SCOPE_FIELDS
    ):
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_scope_mismatch",
            detail="The recovery token belongs to another conversation or user.",
        )
    return record


def load_artifact_recovery(
    token: str,
    *,
    current_origin: PlanningOriginPayload,
    hermes_home: Optional[Path] = None,
) -> ArtifactRecoveryRecord:
    """Load, checksum-verify, and scope-check a durable recovery contract."""

    return _load_artifact_recovery(
        token,
        current_origin=current_origin,
        verify_snapshot=True,
        hermes_home=hermes_home,
    )


def load_registered_artifact_recovery(
    token: str,
    *,
    current_origin: PlanningOriginPayload,
    hermes_home: Optional[Path] = None,
) -> ArtifactRecoveryRecord:
    """Load a contract just fsynced or verified by successful registration."""

    return _load_artifact_recovery(
        token,
        current_origin=current_origin,
        verify_snapshot=False,
        hermes_home=hermes_home,
    )


def acknowledge_artifact_recovery(
    token: str,
    *,
    hermes_home: Optional[Path] = None,
) -> bool:
    """Atomically retire and then remove one acknowledged local snapshot."""

    digest = _digest_from_token(token)
    root = _spool_root(hermes_home)
    record_path = _record_path(root, digest)
    with _store_lock:
        if not record_path.exists():
            _cleanup_acknowledged_records(root)
            return True
        acknowledged_path = root / (
            f".acknowledged-{digest}-{os.getpid()}-{secrets.token_hex(6)}"
        )
        try:
            os.rename(record_path, acknowledged_path)
            _fsync_directory(root)
        except OSError:
            # The live record remains available. A replay of the convergent
            # Hub operation can safely retry retirement with the same token.
            return False
        removed = _remove_acknowledged_directory(acknowledged_path)
        if removed:
            _fsync_directory(root)
        return removed


__all__ = [
    "ArtifactRecoveryRecord",
    "acknowledge_artifact_recovery",
    "load_artifact_recovery",
    "load_registered_artifact_recovery",
    "register_artifact_recovery",
]
