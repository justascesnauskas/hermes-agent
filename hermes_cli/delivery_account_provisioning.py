"""Provision stable semantic-delivery account identities across profiles.

The semantic delivery registry is intentionally read-only.  Stable account
identities are created here instead, from explicit configuration lifecycle
surfaces (setup, migration, update, and the dedicated config command).

Safety properties:

* every profile is inspected in its own credential-isolated, read-only child;
  the root-lock-owning parent performs the fenced atomic config mutation;
* account ids are random opaque values, never derived from provider secrets;
* one root lock serializes writers across all profiles;
* a durable plan is written before the first config mutation, so a killed
  process resumes with exactly the same generated ids;
* each profile config is replaced atomically and read back before success;
* bounded lock/child waits emit a typed, unbounded safe continuation instead
  of escalating a transient delay into an administrator/terminal state;
* dry-run performs no filesystem writes at all.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable


PROVISIONING_CONTRACT = "hermes-delivery-account-provisioning/1"
PROVISIONING_RECEIPT_CONTRACT = (
    "hermes-delivery-account-provisioning-receipt/1"
)
PROVISIONING_SCOPE_CONTRACT = (
    "hermes-delivery-account-provisioning-scope/1"
)
PROVISIONING_CONTINUATION_CONTRACT = (
    "hermes-delivery-account-provisioning-continuation/1"
)
PROVISIONING_RETIREMENT_CONTRACT = (
    "hermes-delivery-account-provisioning-retirement/1"
)
_JOURNAL_VERSION = 1
_CHILD_PROTOCOL_VERSION = 1
_ROOT_LOCK_TIMEOUT_SECONDS = 60.0
_CHILD_TIMEOUT_SECONDS = 45.0
_CONTINUATION_RETRY_AFTER_SECONDS = 1.0
_ACCOUNT_ID = re.compile(r"^acct_[0-9a-f]{48}$")
_PROFILE_INSTANCE_ID = re.compile(r"^profile_[0-9a-f]{48}$")
_PROVIDER_ID = re.compile(r"^[a-z][a-z0-9_-]{0,119}$")
_PROFILE_ID = re.compile(r"^(?:default|[a-z0-9][a-z0-9_-]{0,63})$")
_CHILD_ENV_ALLOWLIST = frozenset(
    {
        "COMSPEC",
        "HOME",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "LD_LIBRARY_PATH",
        "LOGNAME",
        "NO_COLOR",
        "PATH",
        "PATHEXT",
        "PYTHONHOME",
        "PYTHONIOENCODING",
        "PYTHONPATH",
        "PYTHONUTF8",
        "SYSTEMROOT",
        "TEMP",
        "TERM",
        "TMP",
        "TMPDIR",
        "USER",
        "VIRTUAL_ENV",
        "WINDIR",
    }
)
_PROFILE_ENV_RESERVED = _CHILD_ENV_ALLOWLIST | frozenset(
    {
        "HERMES_HOME",
        "HERMES_PROFILE",
        "HERMES_CONFIG",
        "HERMES_ENV",
        "HERMES_DELIVERY_ACCOUNT_PROVISIONING_CHILD",
    }
)


class ProvisioningRetryableContinuation(TimeoutError):
    """A bounded wait ended; the same operation remains safe to resume."""

    def __init__(
        self,
        reason: str,
        *,
        scope_kind: str,
        action: str,
        profile: str = "",
        retry_after_seconds: float = _CONTINUATION_RETRY_AFTER_SECONDS,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.scope_kind = scope_kind
        self.action = action
        self.profile = profile
        self.retry_after_seconds = max(0.0, float(retry_after_seconds))


def _valid_account_id(value: object) -> bool:
    """Accept legacy explicit ids as well as newly generated opaque ids."""

    if not isinstance(value, str):
        return False
    return bool(
        value
        and value == value.strip()
        and len(value) <= 500
        and all(ord(character) >= 32 and ord(character) != 127 for character in value)
    )


def _private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        path.chmod(0o700)
    except OSError:
        pass


def _private_file(path: Path) -> None:
    try:
        path.chmod(0o600)
    except OSError:
        pass


def _state_directory(root_home: Path) -> Path:
    return root_home / "state" / "semantic-delivery"


def _journal_path(root_home: Path) -> Path:
    return _state_directory(root_home) / "account-provisioning-plan.json"


def _retired_journal_path(root_home: Path) -> Path:
    return _state_directory(root_home) / "account-provisioning-last-completed.json"


def _lock_path(root_home: Path) -> Path:
    return _state_directory(root_home) / "account-provisioning.lock"


def _profile_instance_marker_path(profile_home: Path) -> Path:
    return profile_home / "state" / "profile-instance-id"


class _RootFileLock:
    """Portable blocking advisory lock for the cross-profile writer."""

    def __init__(
        self,
        path: Path,
        *,
        timeout_seconds: float = _ROOT_LOCK_TIMEOUT_SECONDS,
    ):
        self.path = path
        self.timeout_seconds = timeout_seconds
        self._file = None

    def __enter__(self) -> "_RootFileLock":
        _private_directory(self.path.parent)
        # Windows byte-range locking needs one byte to exist.
        self._file = self.path.open("a+b")
        if self.path.stat().st_size == 0:
            self._file.write(b" ")
            self._file.flush()
        _private_file(self.path)
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            try:
                if os.name == "nt":
                    import msvcrt

                    self._file.seek(0)
                    msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(
                        self._file.fileno(),
                        fcntl.LOCK_EX | fcntl.LOCK_NB,
                    )
                return self
            except (BlockingIOError, OSError):
                if time.monotonic() >= deadline:
                    self._file.close()
                    self._file = None
                    raise ProvisioningRetryableContinuation(
                        "delivery_account_lock_wait_elapsed",
                        scope_kind="installation",
                        action="acquire_root_lock",
                    )
                time.sleep(0.05)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._file is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._file.close()
            self._file = None


def _atomic_json_write(path: Path, payload: dict[str, Any]) -> None:
    _private_directory(path.parent)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        _private_file(temporary)
        os.replace(temporary, path)
        _private_file(path)
        if os.name != "nt":
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _config_digest(config_path: Path) -> str:
    try:
        raw = config_path.read_bytes()
    except FileNotFoundError:
        raw = b""
    return hashlib.sha256(raw).hexdigest()


def _strict_child_env(profile_home: Path) -> dict[str, str]:
    child_env = {
        key: value
        for key, value in os.environ.items()
        if key in _CHILD_ENV_ALLOWLIST
    }
    child_env["HERMES_HOME"] = str(profile_home)
    child_env["HERMES_DELIVERY_ACCOUNT_PROVISIONING_CHILD"] = "1"
    return child_env


def _decode_child(stdout: str) -> dict[str, Any]:
    for line in reversed(stdout.splitlines()):
        clean = line.strip()
        if not clean:
            continue
        try:
            payload = json.loads(clean)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    raise ValueError("profile child did not emit a JSON object")


@dataclass(frozen=True)
class _ProfileTarget:
    profile: str
    home: Path
    instance_id: str


@dataclass(frozen=True)
class _AccountSnapshot:
    profile: str
    home: Path
    provider: str
    gateway_account_id: str | None
    invalid_account_id: bool
    config_digest: str


ChildRunner = Callable[
    [str, _ProfileTarget, dict[str, Any] | None],
    dict[str, Any],
]


def _subprocess_child_runner(
    action: str,
    target: _ProfileTarget,
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    command = [
        sys.executable,
        "-m",
        "hermes_cli.delivery_account_provisioning",
        "--child-action",
        action,
        "--profile",
        target.profile,
        "--profile-home",
        str(target.home),
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=str(Path(__file__).resolve().parents[1]),
            env=_strict_child_env(target.home),
            input=(
                json.dumps(payload, sort_keys=True, separators=(",", ":"))
                if payload is not None
                else None
            ),
            capture_output=True,
            text=True,
            timeout=_CHILD_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        # ``subprocess.run`` kills and waits for its child before raising.
        # Inspection is read-only; any parent config mutation is already
        # fenced by the durable active journal. The same command can therefore
        # resume without an operator, duplicate id, or fixed terminal cap.
        raise ProvisioningRetryableContinuation(
            "delivery_account_profile_child_timeout",
            scope_kind="profile",
            action=f"child_{action}",
            profile=target.profile,
        ) from exc
    if completed.returncode != 0:
        detail = ""
        try:
            detail = str(_decode_child(completed.stdout).get("error") or "")
        except Exception:
            detail = ""
        raise RuntimeError(
            f"profile {target.profile!r} {action} child exited "
            f"{completed.returncode}"
            + (f": {detail}" if detail else "")
        )
    return _decode_child(completed.stdout)


def _read_profile_instance_id(home: Path) -> str | None:
    marker = _profile_instance_marker_path(home)
    if marker.parent.is_symlink():
        raise ValueError("refusing a symlinked profile state directory")
    if marker.is_symlink():
        return None
    try:
        raw = marker.read_text(encoding="ascii")
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError):
        return None
    candidate = raw.removesuffix("\n")
    if raw != f"{candidate}\n":
        return None
    if _PROFILE_INSTANCE_ID.fullmatch(candidate) is None:
        return None
    return candidate


def _publish_profile_instance_id(home: Path) -> str:
    """Publish one durable directory-incarnation marker under the root lock."""

    marker = _profile_instance_marker_path(home)
    state_directory = marker.parent
    if state_directory.is_symlink():
        raise ValueError("refusing a symlinked profile state directory")
    _private_directory(state_directory)

    existing = _read_profile_instance_id(home)
    if existing is not None:
        return existing

    # A crash or external truncation must mint a new incarnation and fence any
    # journal bound to the old value. Preserve the bad object for diagnostics
    # rather than asking an administrator to repair it.
    if marker.exists() or marker.is_symlink():
        quarantine = marker.with_name(
            f"{marker.name}.invalid.{time.time_ns()}.{secrets.token_hex(4)}"
        )
        os.replace(marker, quarantine)
        _private_file(quarantine)
        _fsync_directory(state_directory)

    instance_id = f"profile_{secrets.token_hex(24)}"
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{marker.name}.",
        suffix=".tmp",
        dir=str(state_directory),
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="ascii") as handle:
            handle.write(f"{instance_id}\n")
            handle.flush()
            os.fsync(handle.fileno())
        _private_file(temporary)
        # All supported writers hold the root provisioning lock here. The
        # fully fsynced same-directory temp is therefore published atomically
        # without exposing an empty/partial canonical marker.
        if marker.exists() or marker.is_symlink():
            winner = _read_profile_instance_id(home)
            if winner is not None:
                return winner
            quarantine = marker.with_name(
                f"{marker.name}.invalid.{time.time_ns()}."
                f"{secrets.token_hex(4)}"
            )
            os.replace(marker, quarantine)
            _private_file(quarantine)
        os.replace(temporary, marker)
        _private_file(marker)
        _fsync_directory(state_directory)
        return instance_id
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _profile_instance_id(
    home: Path,
    *,
    create: bool = False,
    ephemeral_if_missing: bool = False,
) -> str:
    existing = _read_profile_instance_id(home)
    if existing is not None:
        return existing
    if create:
        return _publish_profile_instance_id(home)
    if ephemeral_if_missing:
        return f"ephemeral_{secrets.token_hex(24)}"
    return "missing"


def _profile_targets(
    root_home: Path,
    *,
    create_markers: bool,
) -> list[_ProfileTarget]:
    root_resolved = root_home.resolve()
    rows = [
        _ProfileTarget(
            profile="default",
            home=root_resolved,
            instance_id=_profile_instance_id(
                root_resolved,
                create=create_markers,
                ephemeral_if_missing=not create_markers,
            ),
        )
    ]
    profiles_root = root_home / "profiles"
    if profiles_root.is_dir() and not profiles_root.is_symlink():
        profiles_root_resolved = profiles_root.resolve()
        for child in sorted(profiles_root.iterdir()):
            if child.is_symlink():
                continue
            try:
                child_resolved = child.resolve(strict=True)
            except OSError:
                continue
            if (
                child.is_dir()
                and child.name != "default"
                and _PROFILE_ID.fullmatch(child.name) is not None
                and child_resolved.parent == profiles_root_resolved
            ):
                rows.append(
                    _ProfileTarget(
                        profile=child.name,
                        home=child_resolved,
                        instance_id=_profile_instance_id(
                            child_resolved,
                            create=create_markers,
                            ephemeral_if_missing=not create_markers,
                        ),
                    )
                )
    rows.sort(key=lambda row: row.profile)
    return rows


def _inspect_targets(
    targets: list[_ProfileTarget],
    child_runner: ChildRunner,
) -> list[_AccountSnapshot]:
    if targets:
        with ThreadPoolExecutor(max_workers=min(8, len(targets))) as executor:
            payloads = list(
                executor.map(
                    lambda target: (
                        target,
                        child_runner("inspect", target, None),
                    ),
                    targets,
                )
            )
    else:
        payloads = []

    snapshots: list[_AccountSnapshot] = []
    for target, payload in payloads:
        if (
            payload.get("protocol_version") != _CHILD_PROTOCOL_VERSION
            or payload.get("profile") != target.profile
            or not isinstance(payload.get("config_digest"), str)
            or not isinstance(payload.get("accounts"), list)
        ):
            raise ValueError(
                f"profile {target.profile!r} emitted an invalid inspection contract"
            )
        digest = payload["config_digest"]
        if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValueError(
                f"profile {target.profile!r} emitted an invalid config digest"
            )
        seen_providers: set[str] = set()
        for raw in payload["accounts"]:
            if not isinstance(raw, dict):
                raise ValueError(
                    f"profile {target.profile!r} emitted an invalid account row"
                )
            provider = str(raw.get("provider") or "")
            if (
                _PROVIDER_ID.fullmatch(provider) is None
                or provider in seen_providers
            ):
                raise ValueError(
                    f"profile {target.profile!r} emitted an invalid provider"
                )
            seen_providers.add(provider)
            invalid = raw.get("invalid_account_id") is True
            account_id = raw.get("gateway_account_id")
            if account_id is not None:
                account_id = str(account_id)
            if invalid:
                account_id = None
            elif account_id is not None and not _valid_account_id(account_id):
                raise ValueError(
                    f"profile {target.profile!r} emitted an invalid account id"
                )
            snapshots.append(
                _AccountSnapshot(
                    profile=target.profile,
                    home=target.home,
                    provider=provider,
                    gateway_account_id=account_id,
                    invalid_account_id=invalid,
                    config_digest=digest,
                )
            )
    snapshots.sort(key=lambda row: (row.profile, row.provider))
    return snapshots


def _load_journal(root_home: Path) -> dict[str, Any] | None:
    path = _journal_path(root_home)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("delivery-account provisioning journal is unreadable") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("journal_version") != _JOURNAL_VERSION
        or not isinstance(payload.get("plan_id"), str)
        or not isinstance(payload.get("assignments"), list)
    ):
        raise ValueError("delivery-account provisioning journal is invalid")
    return payload


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _quarantine_invalid_journal(root_home: Path) -> None:
    source = _journal_path(root_home)
    if not source.exists():
        return
    target = source.with_name(
        f"{source.name}.corrupt.{time.time_ns()}.{secrets.token_hex(4)}"
    )
    os.replace(source, target)
    _private_file(target)
    _fsync_directory(source.parent)


def _retire_journal(root_home: Path, payload: dict[str, Any]) -> None:
    """Durably archive completion, then remove active replay authority."""

    active = _journal_path(root_home)
    _atomic_json_write(_retired_journal_path(root_home), payload)
    try:
        active.unlink()
    except FileNotFoundError:
        pass
    _fsync_directory(active.parent)


def _journal_assignments(
    journal: dict[str, Any] | None,
) -> tuple[
    dict[tuple[str, str], str],
    dict[tuple[str, str], str],
]:
    result: dict[tuple[str, str], str] = {}
    instances: dict[tuple[str, str], str] = {}
    if journal is None:
        return result, instances
    seen_identities: set[tuple[str, str]] = set()
    for raw in journal["assignments"]:
        if not isinstance(raw, dict):
            raise ValueError("delivery-account provisioning journal is invalid")
        profile = str(raw.get("profile") or "")
        provider = str(raw.get("provider") or "")
        account_id = str(raw.get("gateway_account_id") or "")
        profile_instance = str(raw.get("profile_instance") or "")
        key = (profile, provider)
        identity = (provider, account_id)
        if (
            _PROFILE_ID.fullmatch(profile) is None
            or _PROVIDER_ID.fullmatch(provider) is None
            or _ACCOUNT_ID.fullmatch(account_id) is None
            or not profile_instance
            or key in result
            or identity in seen_identities
        ):
            raise ValueError("delivery-account provisioning journal is invalid")
        result[key] = account_id
        instances[key] = profile_instance
        seen_identities.add(identity)
    return result, instances


def _new_account_id(used: set[str]) -> str:
    while True:
        candidate = f"acct_{secrets.token_hex(24)}"
        if candidate not in used:
            used.add(candidate)
            return candidate


def _row(
    snapshot: _AccountSnapshot,
    *,
    account_id: str | None,
    status: str,
    reason: str = "",
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "profile": snapshot.profile,
        "provider": snapshot.provider,
        "gateway_account_id": account_id or "",
        "status": status,
    }
    if reason:
        result["reason"] = reason
    return result


def _result(
    *,
    dry_run: bool,
    rows: list[dict[str, Any]],
    ok: bool,
    error: str = "",
    plan_id: str = "",
) -> dict[str, Any]:
    ordered = sorted(
        rows,
        key=lambda row: (
            str(row.get("profile") or ""),
            str(row.get("provider") or ""),
            str(row.get("status") or ""),
        ),
    )
    payload: dict[str, Any] = {
        "contract": PROVISIONING_CONTRACT,
        "dry_run": dry_run,
        "ok": ok,
        "rows": ordered,
        "summary": {
            status: sum(1 for row in ordered if row.get("status") == status)
            for status in ("planned", "applied", "preserved", "conflict")
        },
    }
    if error:
        payload["error"] = error
    if plan_id:
        payload["plan_id"] = plan_id
    return payload


def _installation_scope_id(root_home: Path) -> str:
    """Return an opaque, deterministic label for one provisioning root."""

    normalized = os.path.normcase(str(root_home.resolve()))
    digest = hashlib.sha256(
        f"{PROVISIONING_SCOPE_CONTRACT}\x1f{normalized}".encode("utf-8")
    ).hexdigest()
    return f"install_{digest[:48]}"


def _retryable_continuation_result(
    *,
    root_home: Path,
    signal: ProvisioningRetryableContinuation,
    plan_id: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Describe a self-service resume without converting a wait into failure.

    Receipt
        Stable evidence of the exact bounded wait that ended.
    Scope
        The installation/profile/action the receipt is allowed to describe.
    Continuation
        The same command (or a later lifecycle hook) retries without a new
        plan, approval, administrator, or attempt-count terminal state.
    Retirement
        A successful locked reinspection retires this continuation; when an
        active plan exists, its normal completed-journal transition is the
        durable retirement evidence.
    """

    scope: dict[str, Any] = {
        "contract": PROVISIONING_SCOPE_CONTRACT,
        "installation_id": _installation_scope_id(root_home),
        "kind": signal.scope_kind,
        "action": signal.action,
    }
    if signal.profile:
        scope["profile"] = signal.profile
    if plan_id:
        scope["plan_id"] = plan_id

    receipt_identity = {
        "contract": PROVISIONING_RECEIPT_CONTRACT,
        "reason": signal.reason,
        "scope": scope,
    }
    receipt_digest = hashlib.sha256(
        json.dumps(
            receipt_identity,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    receipt = {
        **receipt_identity,
        "receipt_id": f"provisioning_receipt_{receipt_digest[:48]}",
        "outcome": "retryable_continuation",
        "observed_at_unix_ms": time.time_ns() // 1_000_000,
        "write_state": (
            "dry_run_zero_write"
            if dry_run
            else (
                "active_journal_preserved"
                if plan_id
                else "no_account_assignment_committed"
            )
        ),
    }
    continuation = {
        "contract": PROVISIONING_CONTINUATION_CONTRACT,
        "state": "retryable",
        "action": "retry_same_command",
        "command": (
            "hermes config provision-delivery-accounts --dry-run"
            if dry_run
            else "hermes config provision-delivery-accounts"
        ),
        "trigger": "same_command_or_next_lifecycle",
        "resume_authority": (
            "active_journal"
            if plan_id
            else "fresh_locked_inspection"
        ),
        "retry_after_seconds": signal.retry_after_seconds,
        # There is intentionally no attempt-count transition to an admin or
        # terminal state. Each bounded invocation either completes or emits
        # another receipt for the same safe continuation.
        "retry_limit": None,
        "requires_approval": False,
        "requires_admin": False,
    }
    retirement = {
        "contract": PROVISIONING_RETIREMENT_CONTRACT,
        "state": "pending",
        "condition": "same_scope_provisioning_complete",
        "automatic": True,
        "evidence": (
            "completed_journal"
            if plan_id
            else "complete_provisioning_result"
        ),
        "requires_admin": False,
    }
    result = _result(
        dry_run=dry_run,
        rows=[],
        # The bounded invocation successfully produced a valid continuation.
        # ``complete`` remains the authoritative completion bit.
        ok=True,
        plan_id=plan_id,
    )
    result.update(
        {
            "state": "continuation",
            "complete": False,
            "retryable": True,
            "reason": signal.reason,
            "receipt": receipt,
            "scope": scope,
            "continuation": continuation,
            "retirement": retirement,
        }
    )
    return result


def _validate_current_accounts(
    snapshots: list[_AccountSnapshot],
) -> tuple[list[dict[str, Any]], set[tuple[str, str]]]:
    conflicts: list[dict[str, Any]] = []
    duplicate_keys: set[tuple[str, str]] = set()
    owners: dict[tuple[str, str], list[_AccountSnapshot]] = {}
    for snapshot in snapshots:
        if snapshot.invalid_account_id:
            conflicts.append(
                _row(
                    snapshot,
                    account_id=None,
                    status="conflict",
                    reason="invalid_gateway_account_id",
                )
            )
            continue
        if snapshot.gateway_account_id is None:
            continue
        owners.setdefault(
            (snapshot.provider, snapshot.gateway_account_id),
            [],
        ).append(snapshot)
    for identity, identity_owners in owners.items():
        if len(identity_owners) < 2:
            continue
        for snapshot in identity_owners:
            duplicate_keys.add((snapshot.profile, snapshot.provider))
            conflicts.append(
                _row(
                    snapshot,
                    account_id=identity[1],
                    status="conflict",
                    reason="duplicate_provider_account",
                )
            )
    return conflicts, duplicate_keys


def _journal_payload(
    *,
    plan_id: str,
    assignments: dict[tuple[str, str], str],
    assignment_instances: dict[tuple[str, str], str],
    status: str,
    applied_profiles: set[str],
) -> dict[str, Any]:
    return {
        "journal_version": _JOURNAL_VERSION,
        "plan_id": plan_id,
        "status": status,
        "assignments": [
            {
                "profile": profile,
                "provider": provider,
                "gateway_account_id": account_id,
                "profile_instance": assignment_instances[(profile, provider)],
            }
            for (profile, provider), account_id in sorted(assignments.items())
        ],
        "applied_profiles": sorted(applied_profiles),
    }


def _patch_profile_config_locked(
    *,
    target: _ProfileTarget,
    expected_config_digest: str,
    assignments: dict[str, str],
) -> None:
    """Patch one raw config while the parent still owns the root lock.

    Credentials remain uninterpreted YAML values. Runtime enablement and
    readback stay in the isolated child; keeping the actual replacement in
    the lock-owning parent prevents a SIGKILL from leaving an unfenced orphan
    writer that can mutate config after a recovery process acquires the lock.
    """

    config_path = target.home / "config.yaml"
    if _profile_instance_id(target.home) != target.instance_id:
        raise ValueError("profile directory changed after locked inspection")
    if config_path.is_symlink():
        raise ValueError("refusing to replace a symlinked profile config")
    if _config_digest(config_path) != expected_config_digest:
        raise ValueError("profile config changed after the locked inspection")
    import yaml

    try:
        raw = (
            yaml.safe_load(config_path.read_text(encoding="utf-8"))
            if config_path.exists()
            else {}
        ) or {}
    except Exception as exc:
        raise ValueError("profile config is not valid YAML") from exc
    if not isinstance(raw, dict):
        raise ValueError("profile config root must be a mapping")
    platforms = raw.get("platforms")
    if not isinstance(platforms, dict):
        platforms = {}
        raw["platforms"] = platforms
    for provider, account_id in assignments.items():
        platform = platforms.get(provider)
        if not isinstance(platform, dict):
            platform = {}
            platforms[provider] = platform
        extra = platform.get("extra")
        if not isinstance(extra, dict):
            extra = {}
            platform["extra"] = extra
        existing = extra.get("gateway_account_id")
        if existing is not None and existing != account_id:
            raise ValueError(
                f"provider {provider!r} canonical account id changed"
            )
        extra["gateway_account_id"] = account_id

    from hermes_cli.config import atomic_config_write

    target.home.mkdir(parents=True, exist_ok=True)
    atomic_config_write(config_path, raw, sort_keys=False)
    try:
        raw_after = yaml.safe_load(
            config_path.read_text(encoding="utf-8")
        ) or {}
    except Exception as exc:
        raise RuntimeError("atomic config readback is invalid") from exc
    for provider, account_id in assignments.items():
        actual = (
            ((raw_after.get("platforms") or {}).get(provider) or {})
            .get("extra", {})
            .get("gateway_account_id")
        )
        if actual != account_id:
            raise RuntimeError(
                "atomic config readback did not match assignment"
            )


def _provision_locked(
    *,
    root_home: Path,
    targets: list[_ProfileTarget],
    child_runner: ChildRunner,
) -> dict[str, Any]:
    snapshots = _inspect_targets(targets, child_runner)
    base_conflicts, duplicate_keys = _validate_current_accounts(snapshots)
    if base_conflicts:
        rows = list(base_conflicts)
        rows.extend(
            _row(
                snapshot,
                account_id=snapshot.gateway_account_id,
                status=(
                    "preserved"
                    if snapshot.gateway_account_id is not None
                    else "conflict"
                ),
                reason=(
                    ""
                    if snapshot.gateway_account_id is not None
                    else "plan_blocked_by_existing_conflict"
                ),
            )
            for snapshot in snapshots
            if (snapshot.profile, snapshot.provider) not in duplicate_keys
            and not snapshot.invalid_account_id
        )
        return _result(
            dry_run=False,
            rows=rows,
            ok=False,
            error="delivery_account_plan_conflict",
        )

    journal_recovered = False
    try:
        journal = _load_journal(root_home)
        assignments, assignment_instances = _journal_assignments(journal)
    except ValueError:
        # Config files are the authority. A torn/corrupt side journal must not
        # brick migration forever: quarantine it under the already-held root
        # lock, preserve every explicit config id, and re-plan only missing
        # rows.
        try:
            _quarantine_invalid_journal(root_home)
        except OSError:
            return _result(
                dry_run=False,
                rows=[],
                ok=False,
                error="delivery_account_journal_recovery_failed",
            )
        journal = None
        assignments = {}
        assignment_instances = {}
        journal_recovered = True

    snapshot_by_key = {
        (snapshot.profile, snapshot.provider): snapshot
        for snapshot in snapshots
    }
    target_by_profile = {target.profile: target for target in targets}
    # A deleted/recreated profile is a new authority even if it reused the old
    # profile name and config text. Never replay a crashed plan into a new
    # directory incarnation.
    for key in list(assignments):
        target = target_by_profile.get(key[0])
        if (
            target is None
            or assignment_instances.get(key) != target.instance_id
        ):
            assignments.pop(key, None)
            assignment_instances.pop(key, None)

    rows: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    used_ids = {
        snapshot.gateway_account_id
        for snapshot in snapshots
        if snapshot.gateway_account_id is not None
    }
    used_ids.update(assignments.values())

    current_identity_owners = {
        (snapshot.provider, snapshot.gateway_account_id): (
            snapshot.profile,
            snapshot.provider,
        )
        for snapshot in snapshots
        if snapshot.gateway_account_id is not None
    }
    for key, planned_id in assignments.items():
        current = snapshot_by_key.get(key)
        if current is None:
            continue
        owner = current_identity_owners.get((key[1], planned_id))
        if owner is not None and owner != key:
            conflicts.append(
                _row(
                    current,
                    account_id=planned_id,
                    status="conflict",
                    reason="journal_provider_account_collision",
                )
            )
            continue
        if (
            current.gateway_account_id is not None
            and current.gateway_account_id != planned_id
        ):
            conflicts.append(
                _row(
                    current,
                    account_id=current.gateway_account_id,
                    status="conflict",
                    reason="journal_identity_conflict",
                )
            )

    if conflicts:
        return _result(
            dry_run=False,
            rows=conflicts,
            ok=False,
            error="delivery_account_plan_conflict",
            plan_id=str((journal or {}).get("plan_id") or ""),
        )

    for snapshot in snapshots:
        key = (snapshot.profile, snapshot.provider)
        if snapshot.gateway_account_id is None and key not in assignments:
            assignments[key] = _new_account_id(used_ids)
            assignment_instances[key] = target_by_profile[
                snapshot.profile
            ].instance_id

    pending = {
        key: account_id
        for key, account_id in assignments.items()
        if key in snapshot_by_key
        and snapshot_by_key[key].gateway_account_id is None
    }
    if not pending:
        for snapshot in snapshots:
            rows.append(
                _row(
                    snapshot,
                    account_id=snapshot.gateway_account_id,
                    status="preserved",
                    reason=(
                        "resumed_from_journal"
                        if (
                            (snapshot.profile, snapshot.provider) in assignments
                            and assignments[
                                (snapshot.profile, snapshot.provider)
                            ]
                            == snapshot.gateway_account_id
                        )
                        else ""
                    ),
                )
            )
        completed_plan_id = str((journal or {}).get("plan_id") or "")
        if journal is not None:
            completed_payload = _journal_payload(
                plan_id=completed_plan_id,
                assignments=assignments,
                assignment_instances=assignment_instances,
                status="complete",
                applied_profiles={
                    str(profile)
                    for profile in journal.get("applied_profiles", [])
                    if isinstance(profile, str)
                },
            )
            _retire_journal(root_home, completed_payload)
        result = _result(
            dry_run=False,
            rows=rows,
            ok=True,
            plan_id=completed_plan_id,
        )
        if journal_recovered:
            result["journal_recovered"] = True
        return result

    plan_id = str((journal or {}).get("plan_id") or "")
    if not plan_id:
        plan_id = f"provision_{secrets.token_hex(16)}"
    applied_profiles = {
        str(profile)
        for profile in (journal or {}).get("applied_profiles", [])
        if isinstance(profile, str)
    }
    _atomic_json_write(
        _journal_path(root_home),
        _journal_payload(
            plan_id=plan_id,
            assignments=assignments,
            assignment_instances=assignment_instances,
            status="applying",
            applied_profiles=applied_profiles,
        ),
    )

    pending_by_profile: dict[str, dict[str, str]] = {}
    for (profile, provider), account_id in pending.items():
        pending_by_profile.setdefault(profile, {})[provider] = account_id

    applied_keys: set[tuple[str, str]] = set()
    for profile in sorted(pending_by_profile):
        target = target_by_profile[profile]
        expected_digest = next(
            snapshot.config_digest
            for snapshot in snapshots
            if snapshot.profile == profile
        )
        try:
            _patch_profile_config_locked(
                target=target,
                expected_config_digest=expected_digest,
                assignments=pending_by_profile[profile],
            )
            profile_readback = _inspect_targets([target], child_runner)
        except ProvisioningRetryableContinuation:
            # The active journal was fsynced before any config mutation. Keep
            # it as the sole replay authority and let the top-level boundary
            # emit a typed continuation instead of mislabelling a timeout as a
            # permanent profile conflict.
            raise
        except Exception:
            for applied_profile, applied_provider in sorted(applied_keys):
                applied_snapshot = snapshot_by_key[
                    (applied_profile, applied_provider)
                ]
                rows.append(
                    _row(
                        applied_snapshot,
                        account_id=assignments[
                            (applied_profile, applied_provider)
                        ],
                        status="applied",
                    )
                )
            for provider, account_id in sorted(
                pending_by_profile[profile].items()
            ):
                snapshot = snapshot_by_key[(profile, provider)]
                rows.append(
                    _row(
                        snapshot,
                        account_id=account_id,
                        status="conflict",
                        reason="profile_apply_failed",
                    )
                )
            return _result(
                dry_run=False,
                rows=rows,
                ok=False,
                error="delivery_account_apply_failed",
                plan_id=plan_id,
            )
        readback_ids = {
            snapshot.provider: snapshot.gateway_account_id
            for snapshot in profile_readback
        }
        verified = {
            provider
            for provider, account_id in pending_by_profile[profile].items()
            if readback_ids.get(provider) == account_id
        }
        if verified != set(pending_by_profile[profile]):
            return _result(
                dry_run=False,
                rows=rows,
                ok=False,
                error="delivery_account_readback_failed",
                plan_id=plan_id,
            )
        applied_profiles.add(profile)
        applied_keys.update((profile, provider) for provider in verified)
        _atomic_json_write(
            _journal_path(root_home),
            _journal_payload(
                plan_id=plan_id,
                assignments=assignments,
                assignment_instances=assignment_instances,
                status="applying",
                applied_profiles=applied_profiles,
            ),
        )

    # Parent-side post-write inspection catches a child that lied, wrote the
    # wrong profile, or returned before its atomic replacement became visible.
    verified_snapshots = _inspect_targets(targets, child_runner)
    verified_by_key = {
        (snapshot.profile, snapshot.provider): snapshot
        for snapshot in verified_snapshots
    }
    verify_conflicts, _ = _validate_current_accounts(verified_snapshots)
    if verify_conflicts:
        return _result(
            dry_run=False,
            rows=verify_conflicts,
            ok=False,
            error="delivery_account_readback_failed",
            plan_id=plan_id,
        )
    for key, account_id in assignments.items():
        current = verified_by_key.get(key)
        if current is not None and current.gateway_account_id != account_id:
            return _result(
                dry_run=False,
                rows=[
                    _row(
                        current,
                        account_id=current.gateway_account_id,
                        status="conflict",
                        reason="post_write_identity_mismatch",
                    )
                ],
                ok=False,
                error="delivery_account_readback_failed",
                plan_id=plan_id,
            )

    completed_payload = _journal_payload(
        plan_id=plan_id,
        assignments=assignments,
        assignment_instances=assignment_instances,
        status="complete",
        applied_profiles=applied_profiles,
    )
    _retire_journal(root_home, completed_payload)
    for snapshot in verified_snapshots:
        key = (snapshot.profile, snapshot.provider)
        rows.append(
            _row(
                snapshot,
                account_id=snapshot.gateway_account_id,
                status="applied" if key in applied_keys else "preserved",
                reason=(
                    "resumed_from_journal"
                    if key in assignments and key not in applied_keys
                    else ""
                ),
            )
        )
    result = _result(
        dry_run=False,
        rows=rows,
        ok=True,
        plan_id=plan_id,
    )
    if journal_recovered:
        result["journal_recovered"] = True
    return result


def _provision_dry_run(
    *,
    targets: list[_ProfileTarget],
    child_runner: ChildRunner,
) -> dict[str, Any]:
    """Build a read-only proposal.

    No lock or state directory is created: an advisory preview must remain a
    literal zero-write operation.  Apply performs a fresh locked inspection,
    so a concurrent change can never make this preview authoritative.
    """

    snapshots = _inspect_targets(targets, child_runner)
    conflicts, duplicate_keys = _validate_current_accounts(snapshots)
    if conflicts:
        rows = list(conflicts)
        rows.extend(
            _row(
                snapshot,
                account_id=snapshot.gateway_account_id,
                status=(
                    "preserved"
                    if snapshot.gateway_account_id is not None
                    else "conflict"
                ),
                reason=(
                    ""
                    if snapshot.gateway_account_id is not None
                    else "plan_blocked_by_existing_conflict"
                ),
            )
            for snapshot in snapshots
            if (snapshot.profile, snapshot.provider) not in duplicate_keys
            and not snapshot.invalid_account_id
        )
        return _result(
            dry_run=True,
            rows=rows,
            ok=False,
            error="delivery_account_plan_conflict",
        )
    used = {
        snapshot.gateway_account_id
        for snapshot in snapshots
        if snapshot.gateway_account_id is not None
    }
    rows = []
    for snapshot in snapshots:
        if snapshot.gateway_account_id is not None:
            rows.append(
                _row(
                    snapshot,
                    account_id=snapshot.gateway_account_id,
                    status="preserved",
                )
            )
        else:
            rows.append(
                _row(
                    snapshot,
                    account_id=_new_account_id(used),
                    status="planned",
                )
            )
    return _result(dry_run=True, rows=rows, ok=True)


def provision_delivery_accounts(
    *,
    dry_run: bool = False,
    root_home: Path | str | None = None,
    child_runner: ChildRunner | None = None,
    lock_timeout_seconds: float = _ROOT_LOCK_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Provision every enabled account in default + valid named profiles."""

    from hermes_constants import get_default_hermes_root

    root = Path(root_home or get_default_hermes_root()).resolve()
    runner = child_runner or _subprocess_child_runner
    if dry_run:
        try:
            return _provision_dry_run(
                targets=_profile_targets(root, create_markers=False),
                child_runner=runner,
            )
        except ProvisioningRetryableContinuation as signal:
            return _retryable_continuation_result(
                root_home=root,
                signal=signal,
                dry_run=True,
            )
    try:
        with _RootFileLock(
            _lock_path(root),
            timeout_seconds=lock_timeout_seconds,
        ):
            try:
                return _provision_locked(
                    root_home=root,
                    targets=_profile_targets(root, create_markers=True),
                    child_runner=runner,
                )
            except ProvisioningRetryableContinuation as signal:
                # We still own the root lock here, so reading the active plan
                # is authoritative and cannot race a second writer.
                try:
                    journal = _load_journal(root)
                except ValueError:
                    journal = None
                plan_id = str((journal or {}).get("plan_id") or "")
                return _retryable_continuation_result(
                    root_home=root,
                    signal=signal,
                    plan_id=plan_id,
                )
    except ProvisioningRetryableContinuation as signal:
        # This invocation never acquired writer authority. Do not inspect or
        # alter another process's active journal; issue a root-scoped receipt
        # that simply retries the same command later.
        return _retryable_continuation_result(
            root_home=root,
            signal=signal,
        )


def format_provisioning_result(result: dict[str, Any]) -> str:
    lines = ["Semantic delivery accounts"]
    for row in result.get("rows", []):
        status = str(row.get("status") or "").upper()
        profile = str(row.get("profile") or "")
        provider = str(row.get("provider") or "")
        account_id = str(row.get("gateway_account_id") or "")
        reason = str(row.get("reason") or "")
        line = f"  {status:<9} {profile:<18} {provider:<18} {account_id}"
        if reason:
            line += f" ({reason})"
        lines.append(line.rstrip())
    summary = result.get("summary") or {}
    lines.append(
        "  "
        + ", ".join(
            f"{name}={int(summary.get(name, 0))}"
            for name in ("planned", "applied", "preserved", "conflict")
        )
    )
    if result.get("state") == "continuation":
        continuation = result.get("continuation") or {}
        receipt = result.get("receipt") or {}
        lines.append(
            "  continuation=retryable "
            f"receipt={receipt.get('receipt_id', '')}"
        )
        lines.append(
            "  next="
            f"{continuation.get('command', '')} "
            "(same journal; safe to retry)"
        )
    if result.get("error"):
        lines.append(f"  error={result['error']}")
    return "\n".join(lines)


def run_config_command(args: argparse.Namespace) -> dict[str, Any]:
    result = provision_delivery_accounts(
        dry_run=bool(getattr(args, "dry_run", False))
    )
    if bool(getattr(args, "json", False)):
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    else:
        print(format_provisioning_result(result))
    if not result.get("ok"):
        raise SystemExit(1)
    return result


def run_lifecycle_provisioning(*, quiet: bool = False) -> dict[str, Any]:
    """Best-effort lifecycle hook with a typed result for the caller."""

    try:
        result = provision_delivery_accounts(dry_run=False)
    except Exception as exc:
        result = _result(
            dry_run=False,
            rows=[],
            ok=False,
            error="delivery_account_provisioning_unavailable",
        )
        result["detail"] = type(exc).__name__
    if not quiet:
        summary = result.get("summary") or {}
        applied = int(summary.get("applied", 0))
        conflicts = int(summary.get("conflict", 0))
        if applied:
            print(f"  ✓ Provisioned {applied} semantic delivery account id(s)")
        if result.get("state") == "continuation":
            continuation = result.get("continuation") or {}
            print(
                "  ↻ Semantic delivery account provisioning will resume "
                "from the same journal on the next lifecycle run. "
                f"Manual retry: {continuation.get('command', '')}",
                file=sys.stderr,
            )
        elif not result.get("ok"):
            print(
                "  ⚠ Semantic delivery account provisioning needs attention: "
                f"{result.get('error', 'unknown_error')}",
                file=sys.stderr,
            )
        elif conflicts:
            print(
                f"  ⚠ Semantic delivery account conflicts: {conflicts}",
                file=sys.stderr,
            )
    return result


def _read_only_profile_env(profile_home: Path) -> None:
    """Load one profile dotenv without sanitizer or external-source writes."""

    env_path = profile_home / ".env"
    if not env_path.is_file():
        return
    try:
        from dotenv import dotenv_values

        values = dotenv_values(env_path)
    except Exception:
        return
    for key, value in values.items():
        if (
            isinstance(key, str)
            and key
            and key not in _PROFILE_ENV_RESERVED
            and isinstance(value, str)
        ):
            # Child starts from a secret-free allowlist, so profile values are
            # authoritative while system/runtime variables remain intact.
            os.environ[key] = value


def _platform_name(value: object) -> str:
    return str(getattr(value, "value", value) or "").strip().lower()


def _child_inspect(profile: str, profile_home: Path) -> dict[str, Any]:
    _read_only_profile_env(profile_home)
    if Path(os.environ.get("HERMES_HOME", "")).resolve() != profile_home:
        raise ValueError("profile inspection scope changed")
    from gateway.config import load_gateway_config

    config = load_gateway_config()
    accounts: list[dict[str, Any]] = []
    for platform, platform_config in config.platforms.items():
        if not platform_config.enabled:
            continue
        provider = _platform_name(platform)
        if _PROVIDER_ID.fullmatch(provider) is None:
            raise ValueError("enabled platform has an invalid provider id")
        extra = (
            platform_config.extra
            if isinstance(platform_config.extra, dict)
            else {}
        )
        raw_account_id = extra.get("gateway_account_id")
        invalid = (
            raw_account_id is not None
            and not _valid_account_id(raw_account_id)
        )
        accounts.append(
            {
                "provider": provider,
                "gateway_account_id": (
                    str(raw_account_id)
                    if raw_account_id is not None and not invalid
                    else None
                ),
                "invalid_account_id": invalid,
            }
        )
    accounts.sort(key=lambda row: row["provider"])
    return {
        "protocol_version": _CHILD_PROTOCOL_VERSION,
        "profile": profile,
        "config_digest": _config_digest(profile_home / "config.yaml"),
        "accounts": accounts,
    }


def _child_main(args: argparse.Namespace) -> int:
    if os.environ.get("HERMES_DELIVERY_ACCOUNT_PROVISIONING_CHILD") != "1":
        print(
            json.dumps(
                {
                    "protocol_version": _CHILD_PROTOCOL_VERSION,
                    "error": "provisioning_child_not_authorized",
                }
            )
        )
        return 2
    profile = str(args.profile or "")
    profile_home = Path(str(args.profile_home or "")).resolve()
    if (
        _PROFILE_ID.fullmatch(profile) is None
        or Path(os.environ.get("HERMES_HOME", "")).resolve() != profile_home
    ):
        print(
            json.dumps(
                {
                    "protocol_version": _CHILD_PROTOCOL_VERSION,
                    "error": "provisioning_child_scope_invalid",
                }
            )
        )
        return 2
    try:
        if args.child_action != "inspect":
            raise ValueError("unknown child action")
        result = _child_inspect(profile, profile_home)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "protocol_version": _CHILD_PROTOCOL_VERSION,
                    "profile": profile,
                    "error": type(exc).__name__,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


def _parse_child_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--child-action",
        choices=("inspect",),
        required=True,
    )
    parser.add_argument("--profile", required=True)
    parser.add_argument("--profile-home", required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(_child_main(_parse_child_args()))
