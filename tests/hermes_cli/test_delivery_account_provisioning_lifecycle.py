from __future__ import annotations

import argparse
from types import SimpleNamespace

from hermes_cli.subcommands.config import build_config_parser


def _continuation_result() -> dict:
    return {
        "contract": "hermes-delivery-account-provisioning/1",
        "dry_run": False,
        "ok": True,
        "complete": False,
        "state": "continuation",
        "retryable": True,
        "rows": [],
        "summary": {
            "planned": 0,
            "applied": 0,
            "preserved": 0,
            "conflict": 0,
        },
        "continuation": {
            "command": "hermes config provision-delivery-accounts",
            "requires_admin": False,
            "retry_limit": None,
        },
    }


def test_config_parser_exposes_global_provisioning_dry_run() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    marker = object()
    build_config_parser(subparsers, cmd_config=marker)

    args = parser.parse_args(
        [
            "config",
            "provision-delivery-accounts",
            "--dry-run",
            "--json",
        ]
    )

    assert args.command == "config"
    assert args.config_command == "provision-delivery-accounts"
    assert args.dry_run is True
    assert args.json is True
    assert args.func is marker


def test_config_command_routes_explicit_provisioning(monkeypatch) -> None:
    from hermes_cli import config as config_module
    from hermes_cli import delivery_account_provisioning as provisioning

    calls = []
    monkeypatch.setattr(
        provisioning,
        "run_config_command",
        lambda args: calls.append(args) or {"ok": True},
    )
    args = SimpleNamespace(
        config_command="provision-delivery-accounts",
        dry_run=True,
        json=True,
    )

    result = config_module.config_command(args)

    assert result == {"ok": True}
    assert calls == [args]


def test_explicit_config_timeout_returns_continuation_without_terminal_exit(
    monkeypatch,
    capsys,
) -> None:
    from hermes_cli import delivery_account_provisioning as provisioning

    monkeypatch.setattr(
        provisioning,
        "provision_delivery_accounts",
        lambda **_kwargs: _continuation_result(),
    )

    result = provisioning.run_config_command(
        SimpleNamespace(dry_run=False, json=True)
    )

    assert result["state"] == "continuation"
    assert result["retryable"] is True
    assert '"complete":false' in capsys.readouterr().out


def test_lifecycle_timeout_is_resume_notice_not_attention(
    monkeypatch,
    capsys,
) -> None:
    from hermes_cli import delivery_account_provisioning as provisioning

    monkeypatch.setattr(
        provisioning,
        "provision_delivery_accounts",
        lambda **_kwargs: _continuation_result(),
    )

    result = provisioning.run_lifecycle_provisioning(quiet=False)

    captured = capsys.readouterr()
    assert result["state"] == "continuation"
    assert "will resume from the same journal" in captured.err
    assert "needs attention" not in captured.err.lower()


def test_current_config_migrate_still_runs_account_provisioning(
    monkeypatch,
    capsys,
) -> None:
    from hermes_cli import config as config_module
    from hermes_cli import delivery_account_provisioning as provisioning

    monkeypatch.setattr(config_module, "get_missing_env_vars", lambda **_: [])
    monkeypatch.setattr(config_module, "get_missing_config_fields", lambda: [])
    monkeypatch.setattr(config_module, "check_config_version", lambda: (33, 33))
    calls = []
    monkeypatch.setattr(
        provisioning,
        "run_lifecycle_provisioning",
        lambda **kwargs: calls.append(kwargs)
        or {"ok": True, "summary": {"applied": 0}},
    )

    config_module.config_command(SimpleNamespace(config_command="migrate"))

    assert calls == [{"quiet": False}]
    assert "Configuration is up to date" in capsys.readouterr().out


def test_setup_gateway_provisions_once_after_all_selected_platforms(
    monkeypatch,
) -> None:
    from hermes_cli import delivery_account_provisioning as provisioning
    from hermes_cli import gateway as gateway_module
    from hermes_cli import setup as setup_module

    platforms = [
        {"key": "discord", "emoji": "D", "label": "Discord"},
        {"key": "slack", "emoji": "S", "label": "Slack"},
    ]
    configured = []
    monkeypatch.setattr(gateway_module, "_all_platforms", lambda: platforms)
    monkeypatch.setattr(
        gateway_module,
        "_platform_status",
        lambda _platform: "not configured",
    )
    monkeypatch.setattr(
        gateway_module,
        "_configure_platform",
        lambda platform: configured.append(platform["key"]),
    )
    monkeypatch.setattr(
        setup_module,
        "prompt_checklist",
        lambda *_args, **_kwargs: [0, 1],
    )
    calls = []
    monkeypatch.setattr(
        provisioning,
        "run_lifecycle_provisioning",
        lambda **kwargs: calls.append(kwargs) or {"ok": True},
    )

    setup_module.setup_gateway({})

    assert configured == ["discord", "slack"]
    assert calls == [{"quiet": False}]


def test_standalone_gateway_setup_provisions_once_after_done(
    monkeypatch,
) -> None:
    from hermes_cli import delivery_account_provisioning as provisioning
    from hermes_cli import gateway as gateway_module

    platform = {"key": "discord", "emoji": "D", "label": "Discord"}
    monkeypatch.setattr(gateway_module, "is_managed", lambda: False)
    monkeypatch.setattr(gateway_module, "_is_service_installed", lambda: False)
    monkeypatch.setattr(gateway_module, "_is_service_running", lambda: False)
    monkeypatch.setattr(
        gateway_module,
        "supports_systemd_services",
        lambda: False,
    )
    monkeypatch.setattr(gateway_module, "_all_platforms", lambda: [platform])
    monkeypatch.setattr(
        gateway_module,
        "_platform_status",
        lambda _platform: "not configured",
    )
    monkeypatch.setattr(
        gateway_module,
        "prompt_choice",
        lambda *_args, **_kwargs: 1,
    )
    calls = []
    monkeypatch.setattr(
        provisioning,
        "run_lifecycle_provisioning",
        lambda **kwargs: calls.append(kwargs) or {"ok": True},
    )

    gateway_module.gateway_setup()

    assert calls == [{"quiet": False}]
