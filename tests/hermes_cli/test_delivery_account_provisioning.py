from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

import pytest
import yaml

from hermes_cli import delivery_account_provisioning as provisioning_module
from hermes_cli.delivery_account_provisioning import (
    PROVISIONING_CONTINUATION_CONTRACT,
    PROVISIONING_RECEIPT_CONTRACT,
    PROVISIONING_RETIREMENT_CONTRACT,
    PROVISIONING_SCOPE_CONTRACT,
    _RootFileLock,
    _journal_path,
    _lock_path,
    _profile_instance_id,
    _profile_instance_marker_path,
    _retired_journal_path,
    format_provisioning_result,
    provision_delivery_accounts,
)


_OPAQUE_ID = re.compile(r"^acct_[0-9a-f]{48}$")
_PROFILE_INSTANCE = re.compile(r"^profile_[0-9a-f]{48}$")


def _write_enabled(
    home: Path,
    provider: str,
    *,
    account_id: str | None = None,
) -> None:
    home.mkdir(parents=True, exist_ok=True)
    extra = {}
    if account_id is not None:
        extra["gateway_account_id"] = account_id
    payload = {
        "platforms": {
            provider: {
                "enabled": True,
                "token": f"{provider}-test-token",
                "extra": extra,
            }
        }
    }
    (home / "config.yaml").write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )


def _account_id(home: Path, provider: str) -> str | None:
    payload = yaml.safe_load(
        (home / "config.yaml").read_text(encoding="utf-8")
    )
    return (
        payload.get("platforms", {})
        .get(provider, {})
        .get("extra", {})
        .get("gateway_account_id")
    )


def _clean_parent_platform_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in tuple(os.environ):
        if (
            key.endswith(("_BOT_TOKEN", "_ACCESS_TOKEN"))
            or key
            in {
                "SIGNAL_ACCOUNT",
                "SIGNAL_HTTP_URL",
                "BLUEBUBBLES_SERVER_URL",
                "BLUEBUBBLES_PASSWORD",
            }
        ):
            monkeypatch.delenv(key, raising=False)


def _local_child_runner(action, target, payload):
    assert action == "inspect"
    assert payload is None
    config_path = target.home / "config.yaml"
    raw_bytes = config_path.read_bytes() if config_path.exists() else b""
    raw = yaml.safe_load(raw_bytes) or {}
    accounts = []
    for provider, platform in sorted((raw.get("platforms") or {}).items()):
        if not isinstance(platform, dict) or platform.get("enabled") is not True:
            continue
        account_id = (platform.get("extra") or {}).get("gateway_account_id")
        valid = (
            account_id is None
            or (
                isinstance(account_id, str)
                and account_id
                and account_id == account_id.strip()
                and len(account_id) <= 500
                and all(
                    ord(character) >= 32 and ord(character) != 127
                    for character in account_id
                )
            )
        )
        accounts.append(
            {
                "provider": provider,
                "gateway_account_id": account_id if valid else None,
                "invalid_account_id": not valid,
            }
        )
    return {
        "protocol_version": 1,
        "profile": target.profile,
        "config_digest": hashlib.sha256(raw_bytes).hexdigest(),
        "accounts": accounts,
    }


def test_default_named_profiles_and_env_credentials_are_isolated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    named = root / "profiles" / "writer"
    ignored = root / "profiles" / "INVALID"
    outside = tmp_path / "outside-profile"
    _write_enabled(root, "discord")
    named.mkdir(parents=True)
    (named / ".env").write_text(
        "SLACK_BOT_TOKEN=named-only-secret\n"
        f"HERMES_HOME={outside}\n"
        "HOME=/must/not/replace/process/home\n",
        encoding="utf-8",
    )
    _write_enabled(ignored, "matrix")
    _write_enabled(outside, "matrix")
    _clean_parent_platform_env(monkeypatch)
    # A parent-shell credential must not leak into either isolated child.
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "parent-must-not-leak")

    result = provision_delivery_accounts(root_home=root)

    assert result["ok"] is True
    assert {
        (row["profile"], row["provider"])
        for row in result["rows"]
    } == {("default", "discord"), ("writer", "slack")}
    default_id = _account_id(root, "discord")
    named_id = _account_id(named, "slack")
    assert _OPAQUE_ID.fullmatch(default_id or "")
    assert _OPAQUE_ID.fullmatch(named_id or "")
    assert default_id != named_id
    assert "gateway_account_id" not in (
        ignored / "config.yaml"
    ).read_text(encoding="utf-8")
    assert "parent-must-not-leak" not in (
        root / "config.yaml"
    ).read_text(encoding="utf-8")
    assert "named-only-secret" not in (
        named / "config.yaml"
    ).read_text(encoding="utf-8")
    assert _account_id(outside, "matrix") is None


def test_valid_explicit_id_is_preserved_without_config_rewrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    _write_enabled(root, "slack", account_id="legacy-slack-account")
    _clean_parent_platform_env(monkeypatch)
    before = (root / "config.yaml").read_bytes()
    before_mtime = (root / "config.yaml").stat().st_mtime_ns

    result = provision_delivery_accounts(
        root_home=root,
        child_runner=_local_child_runner,
    )

    assert result["ok"] is True
    assert result["rows"] == [
        {
            "profile": "default",
            "provider": "slack",
            "gateway_account_id": "legacy-slack-account",
            "status": "preserved",
        }
    ]
    assert (root / "config.yaml").read_bytes() == before
    assert (root / "config.yaml").stat().st_mtime_ns == before_mtime


def test_duplicate_existing_provider_account_fails_before_config_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    named = root / "profiles" / "ops"
    _write_enabled(root, "discord", account_id="same-account")
    _write_enabled(named, "discord", account_id="same-account")
    _clean_parent_platform_env(monkeypatch)
    before = {
        home: (home / "config.yaml").read_bytes()
        for home in (root, named)
    }

    result = provision_delivery_accounts(
        root_home=root,
        child_runner=_local_child_runner,
    )

    assert result["ok"] is False
    assert result["error"] == "delivery_account_plan_conflict"
    assert result["summary"]["conflict"] == 2
    assert not _journal_path(root).exists()
    assert {
        home: (home / "config.yaml").read_bytes()
        for home in (root, named)
    } == before


def test_dry_run_is_literal_zero_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    _write_enabled(root, "telegram")
    _clean_parent_platform_env(monkeypatch)
    before = (root / "config.yaml").read_bytes()
    before_entries = sorted(
        path.relative_to(root)
        for path in root.rglob("*")
    )

    result = provision_delivery_accounts(
        root_home=root,
        dry_run=True,
        child_runner=_local_child_runner,
    )

    assert result["ok"] is True
    assert result["summary"] == {
        "planned": 1,
        "applied": 0,
        "preserved": 0,
        "conflict": 0,
    }
    assert _OPAQUE_ID.fullmatch(result["rows"][0]["gateway_account_id"])
    assert (root / "config.yaml").read_bytes() == before
    assert sorted(path.relative_to(root) for path in root.rglob("*")) == before_entries
    assert not (root / "state").exists()


def test_lock_wait_is_typed_unbounded_continuation_then_resumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    _write_enabled(root, "discord")
    _clean_parent_platform_env(monkeypatch)
    config_before = (root / "config.yaml").read_bytes()

    with _RootFileLock(_lock_path(root), timeout_seconds=1):
        first = provision_delivery_accounts(
            root_home=root,
            child_runner=_local_child_runner,
            lock_timeout_seconds=0.01,
        )
        second = provision_delivery_accounts(
            root_home=root,
            child_runner=_local_child_runner,
            lock_timeout_seconds=0.01,
        )

    for result in (first, second):
        assert result["ok"] is True
        assert result["complete"] is False
        assert result["state"] == "continuation"
        assert result["retryable"] is True
        assert result["reason"] == "delivery_account_lock_wait_elapsed"
        assert result["receipt"]["contract"] == (
            PROVISIONING_RECEIPT_CONTRACT
        )
        assert result["receipt"]["outcome"] == "retryable_continuation"
        assert result["receipt"]["write_state"] == (
            "no_account_assignment_committed"
        )
        assert result["scope"]["contract"] == PROVISIONING_SCOPE_CONTRACT
        assert result["scope"]["kind"] == "installation"
        assert result["scope"]["action"] == "acquire_root_lock"
        assert result["scope"]["installation_id"].startswith("install_")
        assert "plan_id" not in result["scope"]
        assert result["continuation"]["contract"] == (
            PROVISIONING_CONTINUATION_CONTRACT
        )
        assert result["continuation"]["action"] == "retry_same_command"
        assert result["continuation"]["retry_limit"] is None
        assert result["continuation"]["requires_approval"] is False
        assert result["continuation"]["requires_admin"] is False
        assert result["retirement"]["contract"] == (
            PROVISIONING_RETIREMENT_CONTRACT
        )
        assert result["retirement"]["automatic"] is True
        assert result["retirement"]["requires_admin"] is False
        assert "error" not in result

    # Repeated bounded waits describe the same resumable obligation rather
    # than consuming a retry budget or minting a replacement plan.
    assert first["receipt"]["receipt_id"] == second["receipt"]["receipt_id"]
    assert (root / "config.yaml").read_bytes() == config_before
    assert _account_id(root, "discord") is None
    assert not _journal_path(root).exists()
    assert not _profile_instance_marker_path(root).exists()
    assert "needs attention" not in format_provisioning_result(first).lower()

    completed = provision_delivery_accounts(
        root_home=root,
        child_runner=_local_child_runner,
    )

    assert completed["ok"] is True
    assert completed["summary"]["applied"] == 1
    assert _OPAQUE_ID.fullmatch(_account_id(root, "discord") or "")


def test_child_timeout_preserves_journal_and_same_command_retires_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    _write_enabled(root, "slack")
    _clean_parent_platform_env(monkeypatch)
    calls = 0

    def deterministic_child(command, **kwargs):
        nonlocal calls
        calls += 1
        assert kwargs["timeout"] == 45.0
        if calls == 2:
            raise subprocess.TimeoutExpired(
                cmd=command,
                timeout=kwargs["timeout"],
            )
        profile = command[command.index("--profile") + 1]
        home = Path(command[command.index("--profile-home") + 1])
        target = provisioning_module._ProfileTarget(
            profile=profile,
            home=home,
            instance_id="test-only",
        )
        payload = _local_child_runner("inspect", target, None)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(payload) + "\n",
            stderr="",
        )

    monkeypatch.setattr(
        provisioning_module.subprocess,
        "run",
        deterministic_child,
    )

    continued = provision_delivery_accounts(root_home=root)

    assert continued["ok"] is True
    assert continued["complete"] is False
    assert continued["state"] == "continuation"
    assert continued["reason"] == "delivery_account_profile_child_timeout"
    assert continued["scope"]["kind"] == "profile"
    assert continued["scope"]["profile"] == "default"
    assert continued["scope"]["action"] == "child_inspect"
    assert continued["scope"]["plan_id"] == continued["plan_id"]
    assert continued["receipt"]["write_state"] == (
        "active_journal_preserved"
    )
    assert continued["continuation"]["resume_authority"] == (
        "active_journal"
    )
    assert continued["continuation"]["retry_limit"] is None
    assert continued["continuation"]["requires_admin"] is False
    planned_id = _account_id(root, "slack")
    assert _OPAQUE_ID.fullmatch(planned_id or "")
    active = json.loads(_journal_path(root).read_text(encoding="utf-8"))
    assert active["plan_id"] == continued["plan_id"]
    assert active["status"] == "applying"
    assert active["assignments"][0]["gateway_account_id"] == planned_id

    completed = provision_delivery_accounts(root_home=root)

    assert completed["ok"] is True
    assert completed["plan_id"] == continued["plan_id"]
    assert completed["summary"]["preserved"] == 1
    assert _account_id(root, "slack") == planned_id
    assert not _journal_path(root).exists()
    retired = json.loads(
        _retired_journal_path(root).read_text(encoding="utf-8")
    )
    assert retired["plan_id"] == continued["plan_id"]
    assert retired["status"] == "complete"
    assert len(retired["assignments"]) == 1


def test_dry_run_child_timeout_is_typed_literal_zero_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    _write_enabled(root, "telegram")
    _clean_parent_platform_env(monkeypatch)
    before_config = (root / "config.yaml").read_bytes()
    before_entries = sorted(
        path.relative_to(root)
        for path in root.rglob("*")
    )

    def timed_out(command, **kwargs):
        raise subprocess.TimeoutExpired(
            cmd=command,
            timeout=kwargs["timeout"],
        )

    monkeypatch.setattr(
        provisioning_module.subprocess,
        "run",
        timed_out,
    )

    result = provision_delivery_accounts(
        root_home=root,
        dry_run=True,
    )

    assert result["ok"] is True
    assert result["state"] == "continuation"
    assert result["complete"] is False
    assert result["dry_run"] is True
    assert result["receipt"]["write_state"] == "dry_run_zero_write"
    assert result["continuation"]["command"].endswith("--dry-run")
    assert result["continuation"]["retry_limit"] is None
    assert (root / "config.yaml").read_bytes() == before_config
    assert sorted(
        path.relative_to(root)
        for path in root.rglob("*")
    ) == before_entries
    assert not (root / "state").exists()


def test_apply_is_idempotent_and_does_not_rotate_generated_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    _write_enabled(root, "matrix")
    _clean_parent_platform_env(monkeypatch)

    first = provision_delivery_accounts(
        root_home=root,
        child_runner=_local_child_runner,
    )
    first_id = _account_id(root, "matrix")
    first_instance = _profile_instance_id(root)
    config_bytes = (root / "config.yaml").read_bytes()
    config_mtime = (root / "config.yaml").stat().st_mtime_ns
    second = provision_delivery_accounts(
        root_home=root,
        child_runner=_local_child_runner,
    )

    assert first["ok"] is True
    assert first["summary"]["applied"] == 1
    assert second["ok"] is True
    assert second["summary"]["preserved"] == 1
    assert _account_id(root, "matrix") == first_id
    assert _PROFILE_INSTANCE.fullmatch(first_instance)
    assert _profile_instance_id(root) == first_instance
    assert (root / "config.yaml").read_bytes() == config_bytes
    assert (root / "config.yaml").stat().st_mtime_ns == config_mtime


def _write_active_journal(
    root: Path,
    rows: list[tuple[str, str, str, str]],
) -> None:
    path = _journal_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "journal_version": 1,
                "plan_id": "provision_test",
                "status": "applying",
                "assignments": [
                    {
                        "profile": profile,
                        "provider": provider,
                        "gateway_account_id": account_id,
                        "profile_instance": instance,
                    }
                    for profile, provider, account_id, instance in rows
                ],
                "applied_profiles": [],
            }
        ),
        encoding="utf-8",
    )


def test_journal_and_current_identity_collision_fails_before_config_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    named = root / "profiles" / "ops"
    planned = "acct_" + ("1" * 48)
    _write_enabled(root, "discord")
    _write_enabled(named, "discord", account_id=planned)
    _write_active_journal(
        root,
        [
            (
                "default",
                "discord",
                planned,
                _profile_instance_id(root, create=True),
            )
        ],
    )
    _clean_parent_platform_env(monkeypatch)
    before = {
        home: (home / "config.yaml").read_bytes()
        for home in (root, named)
    }

    result = provision_delivery_accounts(
        root_home=root,
        child_runner=_local_child_runner,
    )

    assert result["ok"] is False
    assert result["error"] == "delivery_account_plan_conflict"
    assert result["rows"][0]["reason"] == "journal_provider_account_collision"
    assert {
        home: (home / "config.yaml").read_bytes()
        for home in (root, named)
    } == before


def test_truncated_journal_is_quarantined_and_replanned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    _write_enabled(root, "telegram")
    journal = _journal_path(root)
    journal.parent.mkdir(parents=True, exist_ok=True)
    journal.write_text('{"journal_version":1,"assign', encoding="utf-8")
    _clean_parent_platform_env(monkeypatch)

    result = provision_delivery_accounts(
        root_home=root,
        child_runner=_local_child_runner,
    )

    assert result["ok"] is True
    assert result["journal_recovered"] is True
    assert _OPAQUE_ID.fullmatch(_account_id(root, "telegram") or "")
    assert not journal.exists()
    assert _retired_journal_path(root).exists()
    assert list(journal.parent.glob(f"{journal.name}.corrupt.*"))


def test_all_written_crash_boundary_retires_active_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    planned = "acct_" + ("2" * 48)
    _write_enabled(root, "matrix", account_id=planned)
    _write_active_journal(
        root,
        [
            (
                "default",
                "matrix",
                planned,
                _profile_instance_id(root, create=True),
            )
        ],
    )
    _clean_parent_platform_env(monkeypatch)

    result = provision_delivery_accounts(
        root_home=root,
        child_runner=_local_child_runner,
    )

    assert result["ok"] is True
    assert result["summary"]["preserved"] == 1
    assert not _journal_path(root).exists()
    retired = json.loads(
        _retired_journal_path(root).read_text(encoding="utf-8")
    )
    assert retired["status"] == "complete"
    assert _account_id(root, "matrix") == planned


def test_recreated_profile_does_not_reuse_crashed_plan_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    root.mkdir(parents=True)
    named = root / "profiles" / "writer"
    old_planned = "acct_" + ("3" * 48)
    _write_enabled(named, "slack")
    old_instance = _profile_instance_id(named, create=True)
    _write_active_journal(
        root,
        [("writer", "slack", old_planned, old_instance)],
    )
    shutil.rmtree(named)
    _write_enabled(named, "slack")
    new_instance = _profile_instance_id(named, create=True)
    assert new_instance != old_instance
    _clean_parent_platform_env(monkeypatch)

    result = provision_delivery_accounts(
        root_home=root,
        child_runner=_local_child_runner,
    )

    assert result["ok"] is True
    new_id = _account_id(named, "slack")
    assert _OPAQUE_ID.fullmatch(new_id or "")
    assert new_id != old_planned
    assert not _journal_path(root).exists()


def test_invalid_profile_marker_self_heals_and_fences_old_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    named = root / "profiles" / "writer"
    old_planned = "acct_" + ("4" * 48)
    _write_enabled(named, "slack")
    old_instance = _profile_instance_id(named, create=True)
    _write_active_journal(
        root,
        [("writer", "slack", old_planned, old_instance)],
    )
    marker = _profile_instance_marker_path(named)
    marker.write_text("profile_partial", encoding="ascii")
    _clean_parent_platform_env(monkeypatch)

    result = provision_delivery_accounts(
        root_home=root,
        child_runner=_local_child_runner,
    )

    assert result["ok"] is True
    new_instance = _profile_instance_id(named)
    assert _PROFILE_INSTANCE.fullmatch(new_instance)
    assert new_instance != old_instance
    assert _account_id(named, "slack") != old_planned
    assert list(marker.parent.glob(f"{marker.name}.invalid.*"))


def test_profile_directory_rename_preserves_instance_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    writer = root / "profiles" / "writer"
    renamed = root / "profiles" / "ops"
    _write_enabled(writer, "matrix")
    _clean_parent_platform_env(monkeypatch)

    first = provision_delivery_accounts(
        root_home=root,
        child_runner=_local_child_runner,
    )
    first_instance = _profile_instance_id(writer)
    first_account = _account_id(writer, "matrix")
    writer.rename(renamed)
    second = provision_delivery_accounts(
        root_home=root,
        child_runner=_local_child_runner,
    )

    assert first["ok"] is True
    assert second["ok"] is True
    assert _PROFILE_INSTANCE.fullmatch(first_instance)
    assert _profile_instance_id(renamed) == first_instance
    assert _account_id(renamed, "matrix") == first_account


def test_named_profile_symlink_outside_root_is_skipped_without_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    profiles = root / "profiles"
    outside = tmp_path / "outside-writer"
    profiles.mkdir(parents=True)
    _write_enabled(outside, "slack")
    (profiles / "writer").symlink_to(outside, target_is_directory=True)
    before = (outside / "config.yaml").read_bytes()
    _clean_parent_platform_env(monkeypatch)

    result = provision_delivery_accounts(
        root_home=root,
        dry_run=True,
        child_runner=_local_child_runner,
    )

    assert result["ok"] is True
    assert result["rows"] == []
    assert (outside / "config.yaml").read_bytes() == before
    assert not (root / "state").exists()


def test_symlinked_profiles_root_is_never_followed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    outside_profiles = tmp_path / "outside-profiles"
    outside = outside_profiles / "writer"
    root.mkdir(parents=True)
    _write_enabled(outside, "slack")
    (root / "profiles").symlink_to(
        outside_profiles,
        target_is_directory=True,
    )
    before = (outside / "config.yaml").read_bytes()
    _clean_parent_platform_env(monkeypatch)

    result = provision_delivery_accounts(
        root_home=root,
        dry_run=True,
        child_runner=_local_child_runner,
    )

    assert result["ok"] is True
    assert result["rows"] == []
    assert (outside / "config.yaml").read_bytes() == before
    assert not (root / "state").exists()


def test_symlinked_config_is_never_followed_or_replaced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    root.mkdir(parents=True)
    outside_config = tmp_path / "outside-config.yaml"
    outside_config.write_text(
        yaml.safe_dump(
            {
                "platforms": {
                    "discord": {
                        "enabled": True,
                        "extra": {},
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (root / "config.yaml").symlink_to(outside_config)
    before = outside_config.read_bytes()
    _clean_parent_platform_env(monkeypatch)

    result = provision_delivery_accounts(
        root_home=root,
        child_runner=_local_child_runner,
    )

    assert result["ok"] is False
    assert result["error"] == "delivery_account_apply_failed"
    assert outside_config.read_bytes() == before
    assert (root / "config.yaml").is_symlink()


def _process_env(root: Path) -> dict[str, str]:
    keep = {
        key: value
        for key, value in os.environ.items()
        if key
        in {
            "HOME",
            "LANG",
            "LC_ALL",
            "PATH",
            "PYTHONPATH",
            "VIRTUAL_ENV",
        }
    }
    keep["HERMES_HOME"] = str(root)
    return keep


def _process_command() -> list[str]:
    script = (
        "import json;"
        "from hermes_cli.delivery_account_provisioning import "
        "provision_delivery_accounts;"
        "print(json.dumps(provision_delivery_accounts(),sort_keys=True))"
    )
    return [sys.executable, "-c", script]


def test_concurrent_invocations_converge_on_one_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    _write_enabled(root, "slack")
    _clean_parent_platform_env(monkeypatch)
    command = _process_command()
    processes = [
        subprocess.Popen(
            command,
            cwd=Path(__file__).resolve().parents[2],
            env=_process_env(root),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for _ in range(2)
    ]
    results = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=90)
        assert process.returncode == 0, stderr
        results.append(json.loads(stdout.splitlines()[-1]))

    final_id = _account_id(root, "slack")
    final_instance = _profile_instance_id(root)
    assert _OPAQUE_ID.fullmatch(final_id or "")
    assert _PROFILE_INSTANCE.fullmatch(final_instance)
    assert all(result["ok"] is True for result in results)
    assert sum(result["summary"]["applied"] for result in results) == 1
    assert sum(result["summary"]["preserved"] for result in results) == 1
    marker = _profile_instance_marker_path(root)
    assert marker.read_text(encoding="ascii") == f"{final_instance}\n"
    assert not list(marker.parent.glob(f"{marker.name}.invalid.*"))


def test_partial_process_death_restarts_with_same_written_ids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "hermes"
    homes = [root]
    for index in range(2):
        homes.append(root / "profiles" / f"p{index}")
    for home in homes:
        _write_enabled(home, "discord")
    _clean_parent_platform_env(monkeypatch)

    process = subprocess.Popen(
        _process_command(),
        cwd=Path(__file__).resolve().parents[2],
        env=_process_env(root),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    first_home = root / "profiles" / "p0"
    written_id = None
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        written_id = _account_id(first_home, "discord")
        if written_id:
            break
        if process.poll() is not None:
            break
        time.sleep(0.02)
    assert _OPAQUE_ID.fullmatch(written_id or "")
    process.kill()
    process.communicate(timeout=10)
    recovered = provision_delivery_accounts(root_home=root)

    assert recovered["ok"] is True
    assert _account_id(first_home, "discord") == written_id
    final_ids = [_account_id(home, "discord") for home in homes]
    assert all(_OPAQUE_ID.fullmatch(value or "") for value in final_ids)
    assert len(set(final_ids)) == len(final_ids)
    assert not _journal_path(root).exists()
    journal = json.loads(
        _retired_journal_path(root).read_text(encoding="utf-8")
    )
    assert journal["status"] == "complete"
