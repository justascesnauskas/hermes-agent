"""Real-process contract tests for Dev Hub semantic delivery authority."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from typing import Any

import pytest

from hermes_cli.semantic_delivery import SEMANTIC_DELIVERY_CONTRACT


ROOT = Path(__file__).resolve().parents[2]


def _write_profile(
    hermes_root: Path,
    profile: str,
    platforms: dict[str, dict[str, Any]],
) -> Path:
    profile_home = (
        hermes_root
        if profile == "default"
        else hermes_root / "profiles" / profile
    )
    profile_home.mkdir(parents=True, exist_ok=True)
    (profile_home / "config.yaml").write_text(
        json.dumps({"platforms": platforms}),
        encoding="utf-8",
    )
    return profile_home


def _environment(hermes_root: Path, **extra: str) -> dict[str, str]:
    environment = dict(os.environ)
    environment["HERMES_HOME"] = str(hermes_root)
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (str(ROOT), environment.get("PYTHONPATH", ""))
        if value
    )
    environment.update(extra)
    return environment


def _decode_stdout(stdout: str) -> dict[str, Any]:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, dict):
        return payload
    for line in reversed(stdout.splitlines()):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    raise AssertionError(
        {
            "message": "subprocess did not emit JSON",
            "stdout": stdout,
        }
    )


def _run(
    hermes_root: Path,
    *arguments: str,
    expected: int,
    extra_env: dict[str, str] | None = None,
) -> dict[str, Any]:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "hermes_cli.main",
            "-p",
            "default",
            "send",
            "--json",
            *arguments,
        ],
        cwd=ROOT,
        env=_environment(hermes_root, **(extra_env or {})),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == expected, {
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
    return _decode_stdout(completed.stdout)


def _run_profile(
    hermes_root: Path,
    profile: str,
    *arguments: str,
    expected: int,
) -> dict[str, Any]:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "hermes_cli.main",
            "-p",
            profile,
            "send",
            "--json",
            *arguments,
        ],
        cwd=ROOT,
        env=_environment(hermes_root),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == expected, {
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
    return _decode_stdout(completed.stdout)


def _enabled_account(account_id: str) -> dict[str, Any]:
    return {
        "enabled": True,
        "extra": {"gateway_account_id": account_id},
    }


def test_registry_discovers_exact_isolated_profile_rows(tmp_path: Path) -> None:
    hermes_root = tmp_path / "hermes"
    _write_profile(
        hermes_root,
        "default",
        {
            "discord": _enabled_account("discord-default"),
            "signal": _enabled_account("signal-default"),
        },
    )
    _write_profile(
        hermes_root,
        "writer",
        {"slack": _enabled_account("slack-writer")},
    )

    payload = _run(
        hermes_root,
        "--delivery-scopes",
        expected=0,
    )

    assert payload["delivery_contract"] == SEMANTIC_DELIVERY_CONTRACT
    assert len(payload["accounts"]) == 3
    assert {
        (
            row["profile"],
            row["provider"],
            row["gateway_account_id"],
        )
        for row in payload["accounts"]
    } == {
        ("default", "discord", "discord-default"),
        ("default", "signal", "signal-default"),
        ("writer", "slack", "slack-writer"),
    }
    assert all(
        set(row)
        == {
            "profile",
            "provider",
            "gateway_account_id",
            "delivery_scope_id",
        }
        for row in payload["accounts"]
    )
    scopes = {
        row["profile"]: row["delivery_scope_id"]
        for row in payload["accounts"]
    }
    assert scopes["default"] != scopes["writer"]


def test_registry_rejects_duplicate_provider_account_pair(
    tmp_path: Path,
) -> None:
    hermes_root = tmp_path / "hermes"
    _write_profile(
        hermes_root,
        "default",
        {"slack": _enabled_account("shared-account")},
    )
    _write_profile(
        hermes_root,
        "writer",
        {"slack": _enabled_account("shared-account")},
    )

    payload = _run(
        hermes_root,
        "--delivery-scopes",
        expected=1,
    )

    assert payload == {
        "accounts": [],
        "delivery_contract": SEMANTIC_DELIVERY_CONTRACT,
        "error": "semantic_delivery_scope_discovery_failed",
        "outcome": "rejected",
    }


def test_registry_child_does_not_inherit_parent_provider_token(
    tmp_path: Path,
) -> None:
    hermes_root = tmp_path / "hermes"
    _write_profile(hermes_root, "default", {})
    # With no explicit enabled flag, this account becomes enabled only if the
    # parent SLACK_BOT_TOKEN leaks into the child.
    _write_profile(
        hermes_root,
        "writer",
        {
            "slack": {
                "extra": {"gateway_account_id": "must-stay-disabled"},
            }
        },
    )

    payload = _run(
        hermes_root,
        "--delivery-scopes",
        expected=0,
        extra_env={"SLACK_BOT_TOKEN": "parent-sentinel-must-not-leak"},
    )

    assert payload["accounts"] == []


@pytest.mark.skipif(os.name == "nt", reason="symlink authority test is POSIX")
def test_registry_skips_symlinked_named_profile_outside_root(
    tmp_path: Path,
) -> None:
    hermes_root = tmp_path / "hermes"
    _write_profile(hermes_root, "default", {})
    outside_home = tmp_path / "outside-writer"
    outside_home.mkdir()
    (outside_home / "config.yaml").write_text(
        json.dumps(
            {
                "platforms": {
                    "slack": _enabled_account("outside-account"),
                }
            }
        ),
        encoding="utf-8",
    )
    profiles_root = hermes_root / "profiles"
    profiles_root.mkdir()
    (profiles_root / "writer").symlink_to(
        outside_home,
        target_is_directory=True,
    )

    payload = _run(
        hermes_root,
        "--delivery-scopes",
        expected=0,
    )

    assert payload["accounts"] == []
    assert not (outside_home / "state" / "semantic-delivery").exists()


@pytest.mark.skipif(os.name == "nt", reason="symlink authority test is POSIX")
def test_registry_skips_symlinked_profiles_root(
    tmp_path: Path,
) -> None:
    hermes_root = tmp_path / "hermes"
    _write_profile(hermes_root, "default", {})
    outside_profiles = tmp_path / "outside-profiles"
    _write_profile(
        outside_profiles,
        "writer",
        {"slack": _enabled_account("outside-root-account")},
    )
    (hermes_root / "profiles").symlink_to(
        outside_profiles / "profiles",
        target_is_directory=True,
    )

    payload = _run(
        hermes_root,
        "--delivery-scopes",
        expected=0,
    )

    assert payload["accounts"] == []
    assert not (
        outside_profiles
        / "profiles"
        / "writer"
        / "state"
        / "semantic-delivery"
    ).exists()


def test_exact_profile_account_and_scope_prevalidation_isolated(
    tmp_path: Path,
) -> None:
    hermes_root = tmp_path / "hermes"
    default_home = _write_profile(
        hermes_root,
        "default",
        {"slack": _enabled_account("slack-default")},
    )
    writer_home = _write_profile(
        hermes_root,
        "writer",
        {"slack": _enabled_account("slack-writer")},
    )
    registry = _run(
        hermes_root,
        "--delivery-scopes",
        expected=0,
    )
    rows = {row["profile"]: row for row in registry["accounts"]}

    wrong_account = _run_profile(
        hermes_root,
        "writer",
        "--to",
        "slack:C123",
        "--delivery-contract",
        SEMANTIC_DELIVERY_CONTRACT,
        "--delivery-id",
        "delivery-wrong-account",
        "--delivery-scope-id",
        rows["writer"]["delivery_scope_id"],
        "--gateway-account-id",
        "slack-default",
        "must never reach provider",
        expected=1,
    )
    stale_scope = _run_profile(
        hermes_root,
        "writer",
        "--to",
        "slack:C123",
        "--delivery-contract",
        SEMANTIC_DELIVERY_CONTRACT,
        "--delivery-id",
        "delivery-stale-scope",
        "--delivery-scope-id",
        rows["default"]["delivery_scope_id"],
        "--gateway-account-id",
        "slack-writer",
        "must never reach provider",
        expected=1,
    )
    missing_status = _run_profile(
        hermes_root,
        "writer",
        "--delivery-status",
        "--delivery-contract",
        SEMANTIC_DELIVERY_CONTRACT,
        "--delivery-id",
        "delivery-never-sent",
        "--delivery-scope-id",
        rows["writer"]["delivery_scope_id"],
        "--delivery-provider",
        "slack",
        "--gateway-account-id",
        "slack-writer",
        expected=1,
    )

    assert wrong_account["error"] == "semantic_delivery_authority_changed"
    assert wrong_account["provider_write_attempted"] is False
    assert stale_scope["error"] == "semantic_delivery_authority_changed"
    assert stale_scope["delivery_scope_id"] == rows["default"][
        "delivery_scope_id"
    ]
    assert stale_scope["current_delivery_scope_id"] == rows["writer"][
        "delivery_scope_id"
    ]
    assert missing_status["outcome"] == "retryable"
    assert missing_status["error"] == "semantic_delivery_missing_prewrite"
    assert missing_status["provider_write_attempted"] is False
    assert missing_status["provider_write_started"] is False
    assert missing_status["gateway_account_id"] == "slack-writer"

    for profile_home in (default_home, writer_home):
        ledger = (
            profile_home
            / "state"
            / "semantic-delivery"
            / "ledger.sqlite3"
        )
        with sqlite3.connect(ledger) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM semantic_deliveries"
            ).fetchone()[0]
        assert count == 0
