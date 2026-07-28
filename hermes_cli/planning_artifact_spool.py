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
_COMPLETION_FILENAME = "completion.json"
_COMPLETION_SCHEMA_VERSION = "1.0"
_SPOOL_RELATIVE_PATH = Path("planning-v2") / "artifact-ingress"
_TOMBSTONES_DIRECTORY = ".acknowledged-tombstones"
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


def _tombstone_path(root: Path, digest: str) -> Path:
    return root / _TOMBSTONES_DIRECTORY / digest


def _has_acknowledged_tombstone(root: Path, digest: str) -> bool:
    path = _tombstone_path(root, digest)
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_corrupt",
            detail="Hermes could not inspect an artifact ACK tombstone.",
        ) from exc
    if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_corrupt",
            detail="An artifact ACK tombstone is not a safe directory.",
        )
    return True


def _materialize_acknowledged_tombstone(
    root: Path,
    digest: str,
    *,
    completion_receipt: Optional[Mapping[str, Any]] = None,
) -> None:
    tombstone_root = root / _TOMBSTONES_DIRECTORY
    _secure_directory(tombstone_root)
    tombstone = tombstone_root / digest
    try:
        tombstone.mkdir(mode=0o700)
    except FileExistsError:
        if not _has_acknowledged_tombstone(root, digest):
            raise PlanningV2ConfigError(
                "planning.artifact_recovery_corrupt"
            )
    except OSError as exc:
        raise PlanningV2ConfigError(
            "planning.artifact_spool_write_failed",
            detail=(
                "Hermes could not durably record the acknowledged artifact "
                "before removing its private byte snapshot."
            ),
        ) from exc
    completion_path = tombstone / _COMPLETION_FILENAME
    if completion_receipt is not None:
        expected = dict(completion_receipt)
        if completion_path.exists():
            try:
                current = json.loads(
                    completion_path.read_text(encoding="utf-8")
                )
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise PlanningV2ConfigError(
                    "planning.artifact_recovery_corrupt",
                    detail="The artifact completion receipt is corrupt.",
                ) from exc
            if current != expected:
                raise PlanningV2ConfigError(
                    "planning.artifact_recovery_contract_conflict",
                    detail=(
                        "The acknowledged artifact already has a different "
                        "immutable completion receipt."
                    ),
                )
        else:
            _write_metadata(completion_path, expected)
            _fsync_directory(tombstone)
    _fsync_directory(tombstone_root)
    _fsync_directory(root)


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
    """Tombstone, then delete one retired record without touching live rows."""

    match = re.fullmatch(
        r"\.acknowledged-([0-9a-f]{64})-[^-]+-[0-9a-f]+",
        path.name,
    )
    if match is None:
        return False
    try:
        metadata = _read_metadata(path)
        digest = match.group(1)
        if _metadata_digest(metadata) != digest:
            return False
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
        # The empty directory marker is the durable no-resurrection fence.
        # It is committed before either the journal metadata or bytes vanish.
        _materialize_acknowledged_tombstone(path.parent, digest)
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


def _scope_digest(
    *,
    thread_id: str,
    origin: Mapping[str, Any],
) -> str:
    payload = {
        "threadId": thread_id,
        "origin": {
            name: origin.get(name)
            for name in _SCOPE_FIELDS
        },
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _completion_text(value: Any) -> Optional[str]:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 2_000
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        return None
    return value


def _normalize_completion(
    completion: Mapping[str, Any],
    *,
    thread_id: str,
) -> dict[str, Any]:
    expected_root = {
        "ok",
        "action",
        "threadId",
        "artifact",
        "storageMode",
        "uploadDisposition",
        "uploadReplayed",
        "inputStored",
        "inputReplayed",
        "previewInvalidated",
    }
    artifact = completion.get("artifact")
    if (
        set(completion) != expected_root
        or completion.get("ok") is not True
        or completion.get("action") != "upload_artifact"
        or completion.get("threadId") != thread_id
        or not isinstance(artifact, Mapping)
    ):
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_completion_invalid"
        )
    required_artifact = {
        "schemaVersion",
        "artifactId",
        "artifactRef",
        "sourceReference",
        "referenceId",
        "checksum",
        "sizeBytes",
        "contentType",
        "role",
        "position",
        "required",
    }
    artifact_keys = set(artifact)
    if (
        not required_artifact.issubset(artifact_keys)
        or artifact_keys - (required_artifact | {"filename"})
        or artifact.get("schemaVersion") != "1.0"
        or any(
            _completion_text(artifact.get(name)) is None
            for name in (
                "artifactId",
                "artifactRef",
                "sourceReference",
                "referenceId",
                "checksum",
                "contentType",
                "role",
            )
        )
        or not re.fullmatch(
            r"sha256:[0-9a-f]{64}",
            str(artifact.get("checksum") or ""),
        )
        or isinstance(artifact.get("sizeBytes"), bool)
        or not isinstance(artifact.get("sizeBytes"), int)
        or artifact["sizeBytes"] < 0
        or isinstance(artifact.get("position"), bool)
        or not isinstance(artifact.get("position"), int)
        or artifact["position"] < 1
        or not isinstance(artifact.get("required"), bool)
        or (
            "filename" in artifact
            and (
                _completion_text(artifact["filename"]) is None
                or Path(str(artifact["filename"])).name
                != artifact["filename"]
                or artifact["filename"] in {".", ".."}
            )
        )
        or any(
            not isinstance(completion.get(name), bool)
            for name in (
                "uploadReplayed",
                "inputStored",
                "inputReplayed",
                "previewInvalidated",
            )
        )
        or completion.get("inputStored") is not True
        or _completion_text(completion.get("uploadDisposition")) is None
        or (
            completion.get("storageMode") is not None
            and _completion_text(completion.get("storageMode")) is None
        )
    ):
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_completion_invalid"
        )
    normalized_artifact = {
        name: artifact[name]
        for name in sorted(artifact)
    }
    return {
        "ok": True,
        "action": "upload_artifact",
        "threadId": thread_id,
        "artifact": normalized_artifact,
        "storageMode": completion.get("storageMode"),
        "uploadDisposition": completion["uploadDisposition"],
        "uploadReplayed": bool(completion["uploadReplayed"]),
        "inputStored": True,
        "inputReplayed": bool(completion["inputReplayed"]),
        "previewInvalidated": bool(completion["previewInvalidated"]),
    }


def _completion_receipt(
    *,
    record: ArtifactRecoveryRecord,
    completion: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schemaVersion": _COMPLETION_SCHEMA_VERSION,
        "scopeDigest": _scope_digest(
            thread_id=record.thread_id,
            origin=record.origin,
        ),
        "completion": _normalize_completion(
            completion,
            thread_id=record.thread_id,
        ),
    }


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
    comparable_expected = dict(expected)
    if (
        actual["attachmentIdentity"]
        and actual["attachmentIdentity"]
        == comparable_expected.get("attachmentIdentity")
        and actual["ingressOrdinal"]
        == comparable_expected.get("ingressOrdinal")
    ):
        # Provider ingress identity owns replay. Role and position are
        # model-facing labels that can be rephrased on a later retry; the
        # first durable snapshot remains authoritative for those labels.
        comparable_expected["role"] = actual["role"]
        comparable_expected["position"] = actual["position"]
    if actual != comparable_expected:
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
        if _has_acknowledged_tombstone(root, digest):
            if final_path.exists():
                acknowledged_path = root / (
                    f".acknowledged-{digest}-{os.getpid()}-"
                    f"{secrets.token_hex(6)}"
                )
                try:
                    os.rename(final_path, acknowledged_path)
                    _fsync_directory(root)
                except OSError as exc:
                    raise PlanningV2ConfigError(
                        "planning.artifact_recovery_corrupt",
                        detail=(
                            "A completed artifact receipt could not retire "
                            "its remaining private byte snapshot."
                        ),
                    ) from exc
                _remove_acknowledged_directory(acknowledged_path)
            # An identical provider-event retry has already converged with
            # Dev Hub. Never reopen the source path or recreate its byte spool.
            return token
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


def list_artifact_recoveries(
    *,
    current_origin: PlanningOriginPayload,
    thread_id: str,
    hermes_home: Optional[Path] = None,
) -> tuple[ArtifactRecoveryRecord, ...]:
    """Return every live recovery owned by this conversation and thread.

    Enumeration is deliberately unbounded: filesystem batching is an
    implementation detail, never an attachment-count product limit.  Every
    candidate is checksum-verified before it can be retried, and records from
    other chats or users are excluded by the same scope fence as token lookup.
    """

    exact_thread_id = str(thread_id or "").strip()
    if not exact_thread_id:
        raise PlanningV2ConfigError(
            "planning.artifact_recovery_thread_required"
        )
    root = _spool_root(hermes_home)
    if not root.exists():
        return ()
    records: list[ArtifactRecoveryRecord] = []
    with _store_lock:
        _cleanup_acknowledged_records(root)
        try:
            candidates = sorted(
                candidate
                for candidate in root.iterdir()
                if re.fullmatch(r"[0-9a-f]{64}", candidate.name)
            )
        except OSError as exc:
            raise PlanningV2ConfigError(
                "planning.artifact_recovery_unavailable",
                detail="Hermes could not inspect its private artifact ingress spool.",
            ) from exc
        for candidate in candidates:
            digest = candidate.name
            record = _decode_record(
                token=_token_for_digest(digest),
                digest=digest,
                record_path=candidate,
                verify_snapshot=True,
            )
            if record.thread_id != exact_thread_id:
                continue
            if any(
                current_origin.get(name) != record.origin.get(name)
                for name in _SCOPE_FIELDS
            ):
                continue
            records.append(record)
    records.sort(
        key=lambda item: (
            item.position,
            item.ingress_ordinal or item.position,
            item.token,
        )
    )
    return tuple(records)


def load_artifact_recovery_completion(
    token: str,
    *,
    current_origin: PlanningOriginPayload,
    hermes_home: Optional[Path] = None,
) -> Optional[dict[str, Any]]:
    """Return a scoped, byte-free completion replay, when one is durable."""

    digest = _digest_from_token(token)
    root = _spool_root(hermes_home)
    completion_path = (
        _tombstone_path(root, digest) / _COMPLETION_FILENAME
    )
    with _store_lock:
        if not _has_acknowledged_tombstone(root, digest):
            return None
        try:
            path_stat = completion_path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise PlanningV2ConfigError(
                "planning.artifact_recovery_corrupt"
            ) from exc
        if not stat.S_ISREG(path_stat.st_mode) or stat.S_ISLNK(
            path_stat.st_mode
        ):
            raise PlanningV2ConfigError(
                "planning.artifact_recovery_corrupt"
            )
        try:
            receipt = json.loads(
                completion_path.read_text(encoding="utf-8")
            )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PlanningV2ConfigError(
                "planning.artifact_recovery_corrupt"
            ) from exc
        if (
            not isinstance(receipt, dict)
            or set(receipt)
            != {
                "schemaVersion",
                "scopeDigest",
                "completion",
            }
            or receipt.get("schemaVersion")
            != _COMPLETION_SCHEMA_VERSION
            or not isinstance(receipt.get("completion"), Mapping)
        ):
            raise PlanningV2ConfigError(
                "planning.artifact_recovery_corrupt"
            )
        completion = _normalize_completion(
            receipt["completion"],
            thread_id=str(receipt["completion"].get("threadId") or ""),
        )
        expected_scope = _scope_digest(
            thread_id=completion["threadId"],
            origin=current_origin,
        )
        if receipt.get("scopeDigest") != expected_scope:
            raise PlanningV2ConfigError(
                "planning.artifact_recovery_scope_mismatch",
                detail=(
                    "The acknowledged artifact belongs to another "
                    "conversation or user."
                ),
            )
        return completion


def acknowledge_artifact_recovery(
    token: str,
    *,
    completion: Optional[Mapping[str, Any]] = None,
    hermes_home: Optional[Path] = None,
) -> bool:
    """Atomically retire and then remove one acknowledged local snapshot."""

    digest = _digest_from_token(token)
    root = _spool_root(hermes_home)
    record_path = _record_path(root, digest)
    with _store_lock:
        if not record_path.exists():
            _cleanup_acknowledged_records(root)
            if _has_acknowledged_tombstone(root, digest):
                if completion is not None:
                    existing = (
                        _tombstone_path(root, digest)
                        / _COMPLETION_FILENAME
                    )
                    if not existing.exists():
                        raise PlanningV2ConfigError(
                            "planning.artifact_recovery_unavailable",
                            detail=(
                                "The acknowledged byte snapshot is already "
                                "retired and cannot bind a new completion."
                            ),
                        )
                return True
            return True
        record = _decode_record(
            token=token,
            digest=digest,
            record_path=record_path,
            verify_snapshot=False,
        )
        completion_receipt = (
            _completion_receipt(
                record=record,
                completion=completion,
            )
            if completion is not None
            else None
        )
        _materialize_acknowledged_tombstone(
            root,
            digest,
            completion_receipt=completion_receipt,
        )
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
    "list_artifact_recoveries",
    "load_artifact_recovery",
    "load_artifact_recovery_completion",
    "load_registered_artifact_recovery",
    "register_artifact_recovery",
]
