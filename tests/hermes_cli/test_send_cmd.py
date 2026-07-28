"""Tests for the ``hermes send`` CLI subcommand.

Covers the argument parsing / stdin / file / list behavior of
``hermes_cli.send_cmd``. The underlying ``send_message_tool`` is stubbed so
no network I/O or gateway is required.
"""

from __future__ import annotations

import io
import json

import pytest

from hermes_cli import send_cmd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse(argv):
    """Build the top-level parser and return the parsed args for ``argv``."""
    import argparse

    parser = argparse.ArgumentParser(prog="hermes")
    subparsers = parser.add_subparsers(dest="command")
    send_cmd.register_send_subparser(subparsers)
    return parser.parse_args(["send", *argv])


class _FakeTool:
    """Replacement for ``tools.send_message_tool.send_message_tool``."""

    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def __call__(self, args, **_kw):
        self.calls.append(dict(args))
        return json.dumps(self.payload)


@pytest.fixture
def fake_tool(monkeypatch):
    """Install a fake send_message_tool and return the stub for inspection."""
    import sys
    import types

    fake = _FakeTool({"success": True, "message_id": "m123"})

    mod = types.ModuleType("tools.send_message_tool")
    mod.send_message_tool = fake
    # Register the stub so ``from tools.send_message_tool import ...`` inside
    # cmd_send resolves to our fake. Also patch the parent ``tools`` package
    # entry so attribute lookup works.
    monkeypatch.setitem(sys.modules, "tools.send_message_tool", mod)
    return fake


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_positional_message_success(fake_tool, capsys):
    args = _parse(["--to", "telegram", "hello world"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 0
    assert fake_tool.calls == [
        {"action": "send", "target": "telegram", "message": "hello world"}
    ]
    out = capsys.readouterr()
    assert "sent" in out.out or out.out == ""  # "sent" is the default success banner


def test_stdin_message(fake_tool, monkeypatch, capsys):
    # Piped stdin (not a tty) should be consumed as the message body.
    monkeypatch.setattr("sys.stdin", io.StringIO("piped body\n"))
    # Force isatty to return False so the CLI reads from stdin.
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    args = _parse(["--to", "discord:#ops"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 0
    assert fake_tool.calls[0]["message"] == "piped body\n"
    assert fake_tool.calls[0]["target"] == "discord:#ops"


def test_file_message(fake_tool, tmp_path):
    body = tmp_path / "msg.txt"
    body.write_text("from a file\n")
    args = _parse(["--to", "slack:#eng", "--file", str(body)])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 0
    assert fake_tool.calls[0]["message"] == "from a file\n"


def test_file_dash_means_stdin(fake_tool, monkeypatch):
    monkeypatch.setattr("sys.stdin", io.StringIO("dash body"))
    args = _parse(["--to", "telegram", "--file", "-"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 0
    assert fake_tool.calls[0]["message"] == "dash body"


def test_subject_prepends_header(fake_tool):
    args = _parse(["--to", "telegram", "--subject", "[CI]", "body text"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 0
    assert fake_tool.calls[0]["message"] == "[CI]\n\nbody text"


def test_json_mode_emits_payload(fake_tool, capsys):
    args = _parse(["--to", "telegram", "--json", "hi"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload.get("success") is True
    assert payload.get("message_id") == "m123"


def test_quiet_suppresses_stdout(fake_tool, capsys):
    args = _parse(["--to", "telegram", "--quiet", "shh"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 0
    out = capsys.readouterr()
    assert out.out == ""


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


def test_missing_target(fake_tool, capsys, monkeypatch):
    # Ensure stdin is a tty so the CLI does not try to consume it as a body.
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    args = _parse(["hello"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "--to" in err


def test_missing_message(fake_tool, capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    args = _parse(["--to", "telegram"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "no message" in err.lower()


def test_file_not_found_is_usage_error(fake_tool, capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    args = _parse(["--to", "telegram", "--file", "/nonexistent/does-not-exist.txt"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "cannot read" in err.lower()


def test_file_decode_error_suggests_media_directive(fake_tool, capsys, monkeypatch, tmp_path):
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    bad = tmp_path / "bad-bytes.bin"
    bad.write_bytes(b"\xff\xfe\x00")

    args = _parse(["--to", "telegram", "--file", str(bad)])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "not a text file" in err.lower()
    assert f"MEDIA:{bad}" in err
    assert "[[as_document]]" in err


def test_tool_error_returns_failure_exit(monkeypatch, capsys):
    import sys as _sys
    import types as _types

    fake_mod = _types.ModuleType("tools.send_message_tool")

    def _bad_tool(args, **_kw):
        return json.dumps({"error": "platform blew up"})

    fake_mod.send_message_tool = _bad_tool
    monkeypatch.setitem(_sys.modules, "tools.send_message_tool", fake_mod)

    args = _parse(["--to", "telegram", "nope"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "platform blew up" in err


def test_skipped_result_is_success(monkeypatch):
    import sys as _sys
    import types as _types

    fake_mod = _types.ModuleType("tools.send_message_tool")
    fake_mod.send_message_tool = lambda args, **_kw: json.dumps(
        {"success": True, "skipped": True, "reason": "duplicate"}
    )
    monkeypatch.setitem(_sys.modules, "tools.send_message_tool", fake_mod)

    args = _parse(["--to", "telegram", "dup"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 0


# ---------------------------------------------------------------------------
# --list
# ---------------------------------------------------------------------------


def test_list_human_output(monkeypatch, capsys):
    import sys as _sys
    import types as _types

    fake_dir = _types.ModuleType("gateway.channel_directory")
    fake_dir.format_directory_for_display = lambda: "Available messaging targets:\n\nTelegram:\n  telegram:-100123\n"
    fake_dir.load_directory = lambda: {
        "platforms": {"telegram": [{"id": "-100123", "name": "Test Group"}]}
    }
    monkeypatch.setitem(_sys.modules, "gateway.channel_directory", fake_dir)

    args = _parse(["--list"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "Telegram" in out


def test_list_json(monkeypatch, capsys):
    import sys as _sys
    import types as _types

    fake_dir = _types.ModuleType("gateway.channel_directory")
    fake_dir.format_directory_for_display = lambda: "(ignored in json mode)"
    fake_dir.load_directory = lambda: {
        "platforms": {"telegram": [{"id": "-100123", "name": "Test Group"}]}
    }
    monkeypatch.setitem(_sys.modules, "gateway.channel_directory", fake_dir)

    args = _parse(["--list", "--json"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["platforms"]["telegram"][0]["name"] == "Test Group"


def test_list_filter_platform(monkeypatch, capsys):
    import sys as _sys
    import types as _types

    fake_dir = _types.ModuleType("gateway.channel_directory")
    fake_dir.format_directory_for_display = lambda: "(should not be called when filter set)"
    fake_dir.load_directory = lambda: {
        "platforms": {
            "telegram": [{"id": "-100123", "name": "TG Chat"}],
            "discord": [{"id": "555", "name": "bot-home"}],
        }
    }
    monkeypatch.setitem(_sys.modules, "gateway.channel_directory", fake_dir)

    # When --list is set, argparse puts the optional bareword in the
    # `message` positional slot (where the send-mode body would go).
    args = _parse(["--list", "telegram"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "telegram" in out.lower()
    assert "discord" not in out.lower()


def test_list_unknown_platform_fails(monkeypatch, capsys):
    import sys as _sys
    import types as _types

    fake_dir = _types.ModuleType("gateway.channel_directory")
    fake_dir.format_directory_for_display = lambda: ""
    fake_dir.load_directory = lambda: {"platforms": {"telegram": []}}
    monkeypatch.setitem(_sys.modules, "gateway.channel_directory", fake_dir)

    args = _parse(["--list", "pigeon-post"])
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "pigeon-post" in err


# ---------------------------------------------------------------------------
# Parser registration contract
# ---------------------------------------------------------------------------


def test_register_send_subparser_is_reusable():
    """Sanity check: the registrar returns a parser and wires ``cmd_send``."""
    import argparse

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    send_parser = send_cmd.register_send_subparser(subparsers)
    assert send_parser is not None
    args = parser.parse_args(["send", "--to", "telegram", "hi"])
    assert args.func is send_cmd.cmd_send
    assert args.to == "telegram"
    assert args.message == "hi"


# ---------------------------------------------------------------------------
# Env loader
# ---------------------------------------------------------------------------


def test_load_hermes_env_bridges_config_yaml_scalars(tmp_path, monkeypatch):
    """Top-level config.yaml scalars should be bridged into os.environ.

    This mirrors the gateway/run.py bootstrap behavior: without this, running
    ``hermes send`` from a fresh shell cannot resolve the home channel
    because ``TELEGRAM_HOME_CHANNEL`` (saved by ``hermes config set``) lives
    in config.yaml, not in .env — and the gateway's config loader reads via
    ``os.getenv(...)``.
    """
    import os

    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / ".env").write_text("SOME_TOKEN=abc123\n")
    (hermes_home / "config.yaml").write_text(
        "TELEGRAM_HOME_CHANNEL: '5550001111'\nnested:\n  ignored: true\n"
    )

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.delenv("TELEGRAM_HOME_CHANNEL", raising=False)
    monkeypatch.delenv("SOME_TOKEN", raising=False)

    # Force get_hermes_home() to re-resolve under the patched env.
    from importlib import reload

    import hermes_cli.config as _hc_config
    reload(_hc_config)

    send_cmd._load_hermes_env()

    assert os.environ.get("SOME_TOKEN") == "abc123"
    assert os.environ.get("TELEGRAM_HOME_CHANNEL") == "5550001111"


def test_load_hermes_env_does_not_override_existing(tmp_path, monkeypatch):
    """Existing env vars must not be clobbered by config.yaml values."""
    import os

    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / "config.yaml").write_text("TELEGRAM_HOME_CHANNEL: yaml_value\n")

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("TELEGRAM_HOME_CHANNEL", "env_value")

    from importlib import reload
    import hermes_cli.config as _hc_config
    reload(_hc_config)

    send_cmd._load_hermes_env()

    assert os.environ.get("TELEGRAM_HOME_CHANNEL") == "env_value"


def test_load_hermes_env_handles_missing_files(tmp_path, monkeypatch):
    """No .env or config.yaml should be a silent no-op, not an exception."""
    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))

    from importlib import reload
    import hermes_cli.config as _hc_config
    reload(_hc_config)

    # Should not raise.
    send_cmd._load_hermes_env()


def test_semantic_send_and_status_are_exact_account_scoped(
    fake_tool,
    tmp_path,
    monkeypatch,
    capsys,
):
    from hermes_cli.semantic_delivery import (
        SEMANTIC_DELIVERY_CONTRACT,
        semantic_delivery_scope_id,
    )

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profile"))
    monkeypatch.setattr(
        send_cmd,
        "_delivery_account_is_configured",
        lambda provider, account: (
            provider,
            account,
        ) == ("slack", "slack-primary"),
    )
    scope_id = semantic_delivery_scope_id()
    send_args = _parse(
        [
            "--to",
            "slack:C123",
            "--json",
            "--delivery-contract",
            SEMANTIC_DELIVERY_CONTRACT,
            "--delivery-id",
            "delivery-cli-exact",
            "--delivery-scope-id",
            scope_id,
            "--gateway-account-id",
            "slack-primary",
            "one exact message",
        ]
    )
    with pytest.raises(SystemExit) as send_exit:
        send_cmd.cmd_send(send_args)
    assert send_exit.value.code == 0
    send_payload = json.loads(capsys.readouterr().out)
    assert send_payload["gateway_account_id"] == "slack-primary"
    assert send_payload["delivery_scope_id"] == scope_id
    assert len(fake_tool.calls) == 1

    status_args = _parse(
        [
            "--json",
            "--delivery-status",
            "--delivery-contract",
            SEMANTIC_DELIVERY_CONTRACT,
            "--delivery-id",
            "delivery-cli-exact",
            "--delivery-scope-id",
            scope_id,
            "--delivery-provider",
            "slack",
            "--gateway-account-id",
            "slack-primary",
        ]
    )
    with pytest.raises(SystemExit) as status_exit:
        send_cmd.cmd_send(status_args)
    assert status_exit.value.code == 0
    status_payload = json.loads(capsys.readouterr().out)
    assert status_payload["outcome"] == "delivered"
    assert status_payload["gateway_account_id"] == "slack-primary"
    assert len(fake_tool.calls) == 1


def test_semantic_authority_change_is_typed_before_provider(
    fake_tool,
    monkeypatch,
    capsys,
):
    from hermes_cli.semantic_delivery import SEMANTIC_DELIVERY_CONTRACT

    monkeypatch.setattr(
        send_cmd,
        "_delivery_account_is_configured",
        lambda _provider, _account: False,
    )
    args = _parse(
        [
            "--to",
            "slack:C123",
            "--json",
            "--delivery-contract",
            SEMANTIC_DELIVERY_CONTRACT,
            "--delivery-id",
            "delivery-cli-authority",
            "--delivery-scope-id",
            "scope_" + ("a" * 48),
            "--gateway-account-id",
            "slack-missing",
            "one exact message",
        ]
    )

    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "delivery_contract": SEMANTIC_DELIVERY_CONTRACT,
        "delivery_id": "delivery-cli-authority",
        "delivery_scope_id": "scope_" + ("a" * 48),
        "error": "semantic_delivery_authority_changed",
        "gateway_account_id": "slack-missing",
        "outcome": "retryable",
        "provider": "slack",
        "provider_write_attempted": False,
        "provider_write_started": False,
        "replay_strategy": "none",
        "replayed": False,
        "target": "slack:C123",
    }
    assert fake_tool.calls == []


def test_semantic_scope_change_echoes_requested_and_current_scope(
    fake_tool,
    monkeypatch,
    capsys,
):
    from hermes_cli.semantic_delivery import SEMANTIC_DELIVERY_CONTRACT

    monkeypatch.setattr(
        send_cmd,
        "_delivery_account_is_configured",
        lambda _provider, _account: True,
    )
    monkeypatch.setattr(
        "hermes_cli.semantic_delivery.semantic_delivery_scope_id",
        lambda: "scope_" + ("b" * 48),
    )
    args = _parse(
        [
            "--to",
            "slack:C123",
            "--json",
            "--delivery-contract",
            SEMANTIC_DELIVERY_CONTRACT,
            "--delivery-id",
            "delivery-cli-stale-scope",
            "--delivery-scope-id",
            "scope_" + ("a" * 48),
            "--gateway-account-id",
            "slack-primary",
            "one exact message",
        ]
    )

    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["outcome"] == "retryable"
    assert payload["error"] == "semantic_delivery_authority_changed"
    assert payload["delivery_scope_id"] == "scope_" + ("a" * 48)
    assert payload["current_delivery_scope_id"] == "scope_" + ("b" * 48)
    assert fake_tool.calls == []


def test_delivery_status_requires_explicit_provider(
    monkeypatch,
    capsys,
):
    from hermes_cli.semantic_delivery import SEMANTIC_DELIVERY_CONTRACT

    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    args = _parse(
        [
            "--delivery-status",
            "--delivery-contract",
            SEMANTIC_DELIVERY_CONTRACT,
            "--delivery-id",
            "delivery-status-no-provider",
            "--delivery-scope-id",
            "scope_" + ("a" * 48),
            "--gateway-account-id",
            "slack-primary",
        ]
    )
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 2
    assert "--delivery-provider" in capsys.readouterr().err


def test_delivery_provider_is_rejected_outside_status(
    fake_tool,
    capsys,
):
    args = _parse(
        [
            "--to",
            "slack:C123",
            "--delivery-provider",
            "slack",
            "one exact message",
        ]
    )
    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)
    assert exc.value.code == 2
    assert "--delivery-status" in capsys.readouterr().err
    assert fake_tool.calls == []


def test_in_flight_result_is_never_translated_to_success(capsys):
    exit_code = send_cmd._emit_result(
        json.dumps(
            {
                "delivery_contract": "hermes-semantic-delivery/1",
                "delivery_id": "delivery-in-flight",
                "error": "semantic_delivery_in_flight",
                "gateway_account_id": "slack-primary",
                "outcome": "in_flight",
            }
        ),
        json_mode=True,
        quiet=False,
    )

    assert exit_code == 1
    assert json.loads(capsys.readouterr().out)["outcome"] == "in_flight"


def test_list_rejects_delivery_provider_before_short_circuit(
    monkeypatch,
    capsys,
):
    called: list[str] = []
    monkeypatch.setattr(
        send_cmd,
        "_list_targets",
        lambda *_args, **_kwargs: called.append("listed") or 0,
    )
    args = _parse(["--list", "--delivery-provider", "slack"])

    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)

    assert exc.value.code == 2
    assert "semantic delivery" in capsys.readouterr().err
    assert called == []


def test_delivery_identity_transport_bounds_are_exact():
    assert send_cmd._DELIVERY_PROVIDER.fullmatch("a" * 120)
    assert send_cmd._DELIVERY_PROVIDER.fullmatch("a" * 121) is None
    assert send_cmd._valid_gateway_account_id("a" * 500)
    assert not send_cmd._valid_gateway_account_id("a" * 501)


def test_delivery_registry_omits_enabled_incapable_provider(
    monkeypatch,
):
    from gateway.config import Platform

    class UnknownPlatform:
        value = "unknown-transport"

    config = type(
        "Config",
        (),
        {
            "platforms": {
                Platform.SIGNAL: type(
                    "PlatformConfig",
                    (),
                    {
                        "enabled": True,
                        "extra": {
                            "gateway_account_id": "signal-primary",
                        },
                    },
                )(),
                UnknownPlatform(): type(
                    "PlatformConfig",
                    (),
                    {
                        "enabled": True,
                        "extra": {
                            "gateway_account_id": "unsupported-primary",
                        },
                    },
                )(),
            }
        },
    )()
    monkeypatch.setattr(
        "gateway.config.load_gateway_config",
        lambda: config,
    )

    assert send_cmd._configured_delivery_accounts() == [
        ("signal", "signal-primary"),
    ]


def test_delivery_scopes_rejects_list_mode(capsys):
    args = _parse(["--list", "--delivery-scopes"])

    with pytest.raises(SystemExit) as exc:
        send_cmd.cmd_send(args)

    assert exc.value.code == 2
    assert "another send mode" in capsys.readouterr().err


def test_parallel_registry_results_are_sorted_deterministically(
    monkeypatch,
    tmp_path,
):
    from hermes_cli.semantic_delivery import SEMANTIC_DELIVERY_CONTRACT

    monkeypatch.setattr(
        send_cmd,
        "_semantic_registry_profiles",
        lambda _root_home: ["writer", "default"],
    )
    monkeypatch.setattr(
        "hermes_constants.get_default_hermes_root",
        lambda: tmp_path,
    )

    def child(profile, root_home):
        assert root_home == tmp_path
        account = f"{profile}-account"
        scope_character = "a" if profile == "default" else "b"
        return profile, {
            "delivery_contract": SEMANTIC_DELIVERY_CONTRACT,
            "accounts": [
                {
                    "profile": profile,
                    "provider": "slack",
                    "gateway_account_id": account,
                    "delivery_scope_id": (
                        "scope_" + (scope_character * 48)
                    ),
                }
            ],
        }

    monkeypatch.setattr(send_cmd, "_registry_profile_child", child)

    payload = send_cmd._all_delivery_scope_registry()

    assert [
        row["profile"] for row in payload["accounts"]
    ] == ["default", "writer"]


def test_parallel_registry_child_failure_rejects_whole_discovery(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(
        send_cmd,
        "_semantic_registry_profiles",
        lambda _root_home: ["default", "broken"],
    )
    monkeypatch.setattr(
        "hermes_constants.get_default_hermes_root",
        lambda: tmp_path,
    )

    def child(profile, _root_home):
        if profile == "broken":
            raise RuntimeError("child failed")
        return profile, {
            "delivery_contract": "hermes-semantic-delivery/1",
            "accounts": [],
        }

    monkeypatch.setattr(send_cmd, "_registry_profile_child", child)

    with pytest.raises(RuntimeError, match="child failed"):
        send_cmd._all_delivery_scope_registry()
