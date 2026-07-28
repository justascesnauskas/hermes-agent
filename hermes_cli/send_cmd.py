"""CLI subcommand: ``hermes send`` — pipe text from shell scripts to any
configured messaging platform (Telegram, Discord, Slack, Signal, SMS, etc.).

This is a thin wrapper around ``tools.send_message_tool.send_message_tool``
that exposes its functionality as a standalone CLI entry point so ops
scripts, cron jobs, CI hooks, and monitoring daemons can reuse the gateway's
already-configured credentials without having to reimplement each platform's
REST API client.

Design notes:

* No LLM, no agent loop — the subcommand just resolves arguments, reads the
  message body, calls the shared tool function, and prints/returns the
  result. It is intentionally fast, cheap, and side-effect-only.
* For platforms that send via bot token (Telegram, Discord, Slack, Signal,
  SMS, WhatsApp-CloudAPI, …) no running gateway is required. The tool
  talks directly to each platform's REST endpoint. For platforms that rely
  on a persistent adapter connection (plugin platforms, Matrix in some
  modes, …) a live gateway is needed; the underlying tool surfaces that
  error to the caller.
* Exit codes follow the classic Unix convention:
    0 — delivery (or list) succeeded
    1 — delivery failed at the platform level
    2 — usage / argument / config error (argparse already uses 2)
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Optional


_USAGE_EXIT = 2
_FAILURE_EXIT = 1
_SUCCESS_EXIT = 0
_DELIVERY_SCOPE_ID = re.compile(r"^scope_[0-9a-f]{48}$")
_DELIVERY_PROVIDER = re.compile(r"^[a-z][a-z0-9_-]{0,119}$")
_DELIVERY_PROFILE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")

_REGISTRY_CHILD_ENV_ALLOWLIST = frozenset(
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


def _valid_gateway_account_id(value: str) -> bool:
    return bool(
        value
        and value == value.strip()
        and len(value) <= 500
        and all(ord(character) >= 32 and ord(character) != 127 for character in value)
    )


def _read_message_body(
    positional: Optional[str],
    file_path: Optional[str],
) -> Optional[str]:
    """Resolve the message body from (in order):

    1. An explicit positional message argument.
    2. ``--file PATH`` or ``--file -`` (where ``-`` means stdin).
    3. Piped stdin when it is not attached to a TTY.

    Returns ``None`` when nothing is available — callers must treat that as
    a usage error.
    """
    if positional:
        return positional

    if file_path:
        if file_path == "-":
            return sys.stdin.read()
        try:
            return Path(file_path).read_text(encoding="utf-8")
        except UnicodeDecodeError:
            print(
                f"hermes send: {file_path} is not a text file. --file reads the "
                "message *body* (logs, reports, markdown).\n"
                "To send an image/document/audio file as a native attachment, "
                "reference it with MEDIA: in the message text instead:\n"
                f'  hermes send --to telegram "MEDIA:{file_path}"\n'
                f'  hermes send --to telegram "optional caption MEDIA:{file_path}"\n'
                "Add [[as_document]] to deliver an image as an uncompressed file:\n"
                f'  hermes send --to telegram "[[as_document]] MEDIA:{file_path}"',
                file=sys.stderr,
            )
            sys.exit(_USAGE_EXIT)
        except OSError as exc:
            print(f"hermes send: cannot read {file_path}: {exc}", file=sys.stderr)
            sys.exit(_USAGE_EXIT)

    # Piped input: only consume stdin when it is not a TTY. Reading from a
    # TTY would block the user in a half-broken "type your message" state,
    # which is a poor default for an ops CLI.
    if not sys.stdin.isatty():
        data = sys.stdin.read()
        if data:
            return data

    return None


def _resolve_target(arg_to: Optional[str]) -> Optional[str]:
    """Return a cleaned ``--to`` value, or ``None`` when nothing is set."""
    if arg_to and arg_to.strip():
        return arg_to.strip()
    return None


def _emit_result(
    result_json: str,
    *,
    json_mode: bool,
    quiet: bool,
) -> int:
    """Print the tool result in the requested format and return the exit code.

    The underlying ``send_message_tool`` always returns a JSON string. We
    parse it, decide success/failure, and format accordingly.
    """
    try:
        payload = json.loads(result_json) if result_json else {}
    except json.JSONDecodeError:
        # Shouldn't happen with the shared tool, but be defensive — pass the
        # raw string through so the user can still see what went wrong.
        payload = {"error": "invalid JSON from send_message_tool", "raw": result_json}

    if json_mode:
        print(json.dumps(payload, indent=2))
    elif quiet:
        pass
    else:
        if payload.get("error"):
            print(f"hermes send: {payload['error']}", file=sys.stderr)
        elif payload.get("success"):
            note = payload.get("note")
            if note:
                print(note)
            else:
                print("sent")
        else:
            # Unknown shape — dump it so nothing is silently dropped.
            print(json.dumps(payload, indent=2))

    if payload.get("error"):
        return _FAILURE_EXIT
    if payload.get("skipped"):
        return _SUCCESS_EXIT
    if payload.get("success"):
        return _SUCCESS_EXIT
    # Unknown / unexpected — treat as failure so scripts notice.
    return _FAILURE_EXIT


def _list_targets(platform_filter: Optional[str], *, json_mode: bool) -> int:
    """Print the channel directory (all configured targets across platforms).

    Uses ``load_directory()`` for structured JSON output and
    ``format_directory_for_display()`` for the human-readable rendering that
    the send_message tool itself shows to the model — keeps the two surfaces
    identical.
    """
    try:
        from gateway.channel_directory import (
            format_directory_for_display,
            load_directory,
        )
    except Exception as exc:
        print(f"hermes send: failed to load channel directory: {exc}", file=sys.stderr)
        return _FAILURE_EXIT

    try:
        raw = load_directory()
    except Exception as exc:
        print(f"hermes send: failed to read channel directory: {exc}", file=sys.stderr)
        return _FAILURE_EXIT

    platforms = dict(raw.get("platforms") or {})

    if platform_filter:
        key = platform_filter.strip().lower()
        filtered = {k: v for k, v in platforms.items() if k.lower() == key}
        if not filtered:
            print(
                f"hermes send: no targets found for platform '{platform_filter}'. "
                f"Configured: {', '.join(sorted(platforms)) or '(none)'}",
                file=sys.stderr,
            )
            return _FAILURE_EXIT
        platforms = filtered

    if json_mode:
        print(json.dumps({"platforms": platforms}, indent=2, default=str))
        return _SUCCESS_EXIT

    if not any(platforms.values()):
        print("No messaging platforms configured or no channels discovered yet.")
        print("Set one up with `hermes gateway setup`, or run the gateway once so")
        print("channel discovery can populate ~/.hermes/channel_directory.json.")
        return _SUCCESS_EXIT

    # Human display — when unfiltered, reuse the shared formatter the agent
    # already sees. When filtered, build a minimal view ourselves.
    if platform_filter is None:
        print(format_directory_for_display())
        return _SUCCESS_EXIT

    for plat_name in sorted(platforms):
        channels = platforms[plat_name]
        print(f"{plat_name}:")
        if not channels:
            print("  (no channels discovered yet)")
            continue
        for ch in channels:
            name = ch.get("name", "?")
            chat_id = ch.get("id") or ch.get("chat_id") or ""
            suffix = f"  [{chat_id}]" if chat_id and chat_id != name else ""
            print(f"  {plat_name}:{name}{suffix}")
        print()

    return _SUCCESS_EXIT


def _load_hermes_env() -> None:
    """Populate ``os.environ`` from ``~/.hermes/.env`` AND bridge top-level
    ``config.yaml`` keys into the environment so the underlying gateway
    config loader sees platform credentials and home channel IDs.

    ``send_message_tool`` reads tokens and home-channel IDs via
    ``os.getenv(...)`` on each call. The gateway process does two things at
    startup that ``hermes send`` must replicate when invoked standalone:

    1. ``load_dotenv(~/.hermes/.env)`` — brings bot tokens into the env.
    2. Bridge top-level simple values from ``~/.hermes/config.yaml`` into
       ``os.environ`` (without overriding existing env vars). This is where
       ``TELEGRAM_HOME_CHANNEL`` and friends live when the user saved them
       via ``hermes config set``.

    See ``gateway/run.py`` for the canonical version of this bridge — we
    intentionally reimplement the minimum needed here so ``hermes send``
    doesn't pull in the full gateway module just to resolve a home channel.
    """
    # Step 1: dotenv
    try:
        from dotenv import load_dotenv
    except Exception:
        load_dotenv = None  # type: ignore[assignment]

    try:
        from hermes_cli.config import get_hermes_home
        home = get_hermes_home()
    except Exception:
        return

    env_path = home / ".env"
    if load_dotenv and env_path.exists():
        try:
            load_dotenv(str(env_path), override=True, encoding="utf-8")
        except UnicodeDecodeError:
            try:
                load_dotenv(str(env_path), override=True, encoding="latin-1")
            except Exception:
                pass
        except Exception:
            pass

    # Step 2: bridge top-level config.yaml values into the environment so
    # gateway.config.load_gateway_config() sees them. Scalars only; don't
    # override values already in the env.
    import os
    config_path = home / "config.yaml"
    if not config_path.exists():
        return

    try:
        import yaml  # type: ignore[import-not-found]
    except Exception:
        return

    try:
        with open(config_path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
    except Exception:
        return

    try:
        from hermes_cli.config import _expand_env_vars
        raw = _expand_env_vars(raw)
    except Exception:
        pass

    # Managed scope: overlay administrator-pinned values before bridging to env,
    # so a managed top-level scalar wins here too. Fail-open via the helper.
    try:
        from hermes_cli import managed_scope
        raw = managed_scope.apply_managed_overlay(raw if isinstance(raw, dict) else {})
    except Exception:
        pass

    if not isinstance(raw, dict):
        return

    for key, val in raw.items():
        if not isinstance(val, (str, int, float, bool)):
            continue
        if key in os.environ:
            continue
        os.environ[key] = str(val)


def _platform_name(value: object) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").strip().lower()


def _configured_delivery_accounts() -> list[tuple[str, str]]:
    """Return enabled, explicitly identified accounts for the active profile."""

    from gateway.config import load_gateway_config
    from gateway.platform_registry import supports_semantic_exact_attempt

    config = load_gateway_config()
    accounts: list[tuple[str, str]] = []
    for platform, platform_config in config.platforms.items():
        if not platform_config.enabled:
            continue
        provider = _platform_name(platform)
        # Registry rows are dispatch authority, not a generic inventory.
        # Publishing an incapable account would let Hub fence work that this
        # Hermes route can only reject.
        if not supports_semantic_exact_attempt(provider):
            continue
        extra = (
            platform_config.extra
            if isinstance(platform_config.extra, dict)
            else {}
        )
        raw_gateway_account_id = extra.get("gateway_account_id")
        if raw_gateway_account_id is None:
            continue
        gateway_account_id = str(raw_gateway_account_id)
        if (
            _DELIVERY_PROVIDER.fullmatch(provider) is None
            or not _valid_gateway_account_id(gateway_account_id)
        ):
            raise ValueError(
                f"invalid semantic delivery account for provider {provider!r}"
            )
        accounts.append((provider, gateway_account_id))
    return sorted(set(accounts))


def _delivery_account_is_configured(
    provider: str,
    gateway_account_id: str,
) -> bool:
    exact = (provider.strip().lower(), gateway_account_id.strip())
    try:
        return exact in _configured_delivery_accounts()
    except Exception:
        return False


def _current_delivery_scope_registry() -> dict:
    """Describe the active profile without reading any sibling profile."""

    from hermes_cli.profiles import get_active_profile_name
    from hermes_cli.semantic_delivery import (
        SEMANTIC_DELIVERY_CONTRACT,
        semantic_delivery_scope_id,
    )

    profile = get_active_profile_name()
    scope_id = semantic_delivery_scope_id()
    if _DELIVERY_SCOPE_ID.fullmatch(scope_id) is None:
        raise ValueError("semantic delivery ledger emitted an invalid scope id")
    accounts = [
        {
            "profile": profile,
            "provider": provider,
            "gateway_account_id": gateway_account_id,
            "delivery_scope_id": scope_id,
        }
        for provider, gateway_account_id in _configured_delivery_accounts()
    ]
    return {
        "delivery_contract": SEMANTIC_DELIVERY_CONTRACT,
        "accounts": accounts,
    }


def _registry_child_env(root_home: Path) -> dict[str, str]:
    """Build a non-secret child environment for one exact profile."""

    child_env = {
        key: value
        for key, value in os.environ.items()
        if key in _REGISTRY_CHILD_ENV_ALLOWLIST
    }
    # ``-p`` resolves named profiles relative to this root. No provider token,
    # API key, account id, or profile dotenv value is inherited from the parent.
    child_env["HERMES_HOME"] = str(root_home)
    child_env["HERMES_DELIVERY_REGISTRY_CHILD"] = "1"
    return child_env


def _decode_registry_child(stdout: str) -> dict:
    """Decode the final JSON line while tolerating profile bootstrap notices."""

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
    raise ValueError("registry child did not emit a JSON object")


def _registry_profile_child(
    profile: str,
    root_home: Path,
) -> tuple[str, dict]:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "hermes_cli.main",
            "-p",
            profile,
            "send",
            "--json",
            "--delivery-scopes-current",
        ],
        cwd=str(Path.cwd()),
        env=_registry_child_env(root_home),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if completed.returncode != _SUCCESS_EXIT:
        raise RuntimeError(
            f"profile {profile!r} registry child exited "
            f"{completed.returncode}"
        )
    return profile, _decode_registry_child(completed.stdout)


def _semantic_registry_profiles(root_home: Path) -> list[str]:
    """Enumerate only profile homes provisioning is allowed to own.

    Ordinary gateway profile multiplexing retains its existing behavior.
    Semantic authority discovery is intentionally narrower: it never follows
    a symlinked ``profiles`` root or a symlinked named profile into an
    unprovisioned/outside home.
    """

    profiles = ["default"]
    profiles_root = root_home / "profiles"
    if not profiles_root.is_dir() or profiles_root.is_symlink():
        return profiles
    try:
        profiles_root_resolved = profiles_root.resolve(strict=True)
        children = sorted(profiles_root.iterdir())
    except OSError:
        return profiles
    for child in children:
        if child.is_symlink():
            continue
        try:
            child_resolved = child.resolve(strict=True)
        except OSError:
            continue
        if (
            child.is_dir()
            and child.name != "default"
            and _DELIVERY_PROFILE.fullmatch(child.name) is not None
            and child_resolved.parent == profiles_root_resolved
        ):
            profiles.append(child.name)
    return sorted(profiles)


def _all_delivery_scope_registry() -> dict:
    """Discover every profile in a separate, credential-isolated process."""

    from hermes_constants import get_default_hermes_root
    from hermes_cli.semantic_delivery import SEMANTIC_DELIVERY_CONTRACT

    root_home = get_default_hermes_root()
    all_accounts: list[dict] = []
    seen_accounts: dict[tuple[str, str], str] = {}
    expected_keys = {
        "profile",
        "provider",
        "gateway_account_id",
        "delivery_scope_id",
    }
    profiles = _semantic_registry_profiles(root_home)
    if profiles:
        with ThreadPoolExecutor(max_workers=min(8, len(profiles))) as executor:
            child_payloads = list(
                executor.map(
                    lambda profile: _registry_profile_child(
                        profile,
                        root_home,
                    ),
                    profiles,
                )
            )
    else:
        child_payloads = []

    for profile, payload in child_payloads:
        if (
            payload.get("delivery_contract")
            != SEMANTIC_DELIVERY_CONTRACT
            or not isinstance(payload.get("accounts"), list)
        ):
            raise ValueError(
                f"profile {profile!r} emitted an invalid registry contract"
            )
        for raw_row in payload["accounts"]:
            if not isinstance(raw_row, dict) or set(raw_row) != expected_keys:
                raise ValueError(
                    f"profile {profile!r} emitted an invalid account row"
                )
            row = {
                key: str(raw_row[key] or "").strip()
                for key in expected_keys
            }
            if (
                row["profile"] != profile
                or _DELIVERY_PROVIDER.fullmatch(row["provider"]) is None
                or not _valid_gateway_account_id(row["gateway_account_id"])
                or _DELIVERY_SCOPE_ID.fullmatch(
                    row["delivery_scope_id"]
                ) is None
            ):
                raise ValueError(
                    f"profile {profile!r} emitted an incomplete account row"
                )
            identity = (row["provider"], row["gateway_account_id"])
            owner = seen_accounts.get(identity)
            if owner is not None:
                raise ValueError(
                    "duplicate semantic delivery account "
                    f"{identity[0]}:{identity[1]} in profiles "
                    f"{owner!r} and {profile!r}"
                )
            seen_accounts[identity] = profile
            all_accounts.append(row)
    all_accounts.sort(
        key=lambda row: (
            row["profile"],
            row["provider"],
            row["gateway_account_id"],
        )
    )
    return {
        "delivery_contract": SEMANTIC_DELIVERY_CONTRACT,
        "accounts": all_accounts,
    }


def _emit_delivery_registry(args: argparse.Namespace) -> None:
    try:
        payload = _all_delivery_scope_registry()
    except Exception as exc:
        from hermes_cli.semantic_delivery import SEMANTIC_DELIVERY_CONTRACT

        payload = {
            "delivery_contract": SEMANTIC_DELIVERY_CONTRACT,
            "accounts": [],
            "error": "semantic_delivery_scope_discovery_failed",
            "outcome": "rejected",
        }
        if getattr(args, "json", False):
            print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        else:
            print(f"hermes send: {payload['error']}: {exc}", file=sys.stderr)
        sys.exit(_FAILURE_EXIT)

    if getattr(args, "json", False):
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    else:
        for row in payload["accounts"]:
            print(
                f"{row['profile']} {row['provider']} "
                f"{row['gateway_account_id']} {row['delivery_scope_id']}"
            )
    sys.exit(_SUCCESS_EXIT)


def _semantic_authority_changed(
    *,
    delivery_contract: str,
    delivery_id: str,
    delivery_scope_id: str,
    provider: str,
    gateway_account_id: str,
    target: str = "",
    current_delivery_scope_id: str = "",
) -> dict:
    payload = {
        "delivery_contract": delivery_contract,
        "delivery_id": delivery_id,
        "delivery_scope_id": delivery_scope_id,
        "error": "semantic_delivery_authority_changed",
        "gateway_account_id": gateway_account_id,
        "outcome": "retryable",
        "provider": provider,
        "provider_write_attempted": False,
        "provider_write_started": False,
        "replay_strategy": "none",
        "replayed": False,
        "target": target,
    }
    if current_delivery_scope_id:
        payload["current_delivery_scope_id"] = current_delivery_scope_id
    return payload


def cmd_send(args: argparse.Namespace) -> None:
    """Entry point wired into the top-level argparse dispatcher."""

    if getattr(args, "delivery_scopes", False):
        if (
            getattr(args, "to", None)
            or getattr(args, "message", None)
            or getattr(args, "file", None)
            or getattr(args, "subject", None)
            or getattr(args, "quiet", False)
            or getattr(args, "list_targets", False)
            or getattr(args, "delivery_contract", None)
            or getattr(args, "delivery_id", None)
            or getattr(args, "delivery_scope", False)
            or getattr(args, "delivery_scope_id", None)
            or getattr(args, "delivery_status", False)
            or getattr(args, "delivery_scopes_current", False)
            or getattr(args, "gateway_account_id", None)
            or getattr(args, "delivery_provider", None)
        ):
            print(
                "hermes send: --delivery-scopes is a read-only registry "
                "lookup and cannot be combined with another send mode",
                file=sys.stderr,
            )
            sys.exit(_USAGE_EXIT)
        # This must run before `_load_hermes_env`: registry children receive a
        # strict allowlist and load only their exact `-p` profile credentials.
        _emit_delivery_registry(args)

    if getattr(args, "list_targets", False) and any(
        (
            getattr(args, "delivery_contract", None),
            getattr(args, "delivery_id", None),
            getattr(args, "delivery_scope", False),
            getattr(args, "delivery_scope_id", None),
            getattr(args, "delivery_status", False),
            getattr(args, "delivery_scopes_current", False),
            getattr(args, "gateway_account_id", None),
            getattr(args, "delivery_provider", None),
        )
    ):
        print(
            "hermes send: --list cannot be combined with semantic delivery "
            "flags",
            file=sys.stderr,
        )
        sys.exit(_USAGE_EXIT)

    # Bridge ~/.hermes/.env and ~/.hermes/config.yaml into os.environ so the
    # gateway config loader (invoked downstream by send_message_tool and by
    # the channel directory) can see platform credentials and home channels.
    _load_hermes_env()

    if getattr(args, "delivery_scopes_current", False):
        if os.environ.get("HERMES_DELIVERY_REGISTRY_CHILD") != "1":
            print(
                "hermes send: --delivery-scopes-current is reserved for an "
                "isolated registry child",
                file=sys.stderr,
            )
            sys.exit(_USAGE_EXIT)
        if (
            getattr(args, "to", None)
            or getattr(args, "message", None)
            or getattr(args, "file", None)
            or getattr(args, "subject", None)
            or getattr(args, "quiet", False)
            or getattr(args, "list_targets", False)
            or getattr(args, "delivery_contract", None)
            or getattr(args, "delivery_id", None)
            or getattr(args, "delivery_scope", False)
            or getattr(args, "delivery_scope_id", None)
            or getattr(args, "delivery_status", False)
            or getattr(args, "gateway_account_id", None)
            or getattr(args, "delivery_provider", None)
        ):
            print(
                "hermes send: --delivery-scopes-current is an isolated "
                "read-only registry child mode",
                file=sys.stderr,
            )
            sys.exit(_USAGE_EXIT)
        payload = _current_delivery_scope_registry()
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        sys.exit(_SUCCESS_EXIT)

    if getattr(args, "delivery_scope", False):
        if (
            getattr(args, "to", None)
            or getattr(args, "message", None)
            or getattr(args, "file", None)
            or getattr(args, "delivery_contract", None)
            or getattr(args, "delivery_id", None)
            or getattr(args, "delivery_scope_id", None)
            or getattr(args, "delivery_status", False)
            or getattr(args, "gateway_account_id", None)
            or getattr(args, "delivery_provider", None)
        ):
            print(
                "hermes send: --delivery-scope is a read-only lookup and "
                "cannot be combined with a target, message, or delivery claim",
                file=sys.stderr,
            )
            sys.exit(_USAGE_EXIT)
        from hermes_cli.semantic_delivery import (
            SEMANTIC_DELIVERY_CONTRACT,
            semantic_delivery_scope_id,
        )

        payload = {
            "delivery_contract": SEMANTIC_DELIVERY_CONTRACT,
            "delivery_scope_id": semantic_delivery_scope_id(),
        }
        if getattr(args, "json", False):
            print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        else:
            print(payload["delivery_scope_id"])
        sys.exit(_SUCCESS_EXIT)

    # --list short-circuits everything else.
    if getattr(args, "list_targets", False):
        # When `--list telegram` is used, argparse stores "telegram" in the
        # `message` positional (since list_targets takes no argument).
        platform_filter = getattr(args, "message", None)
        exit_code = _list_targets(platform_filter, json_mode=getattr(args, "json", False))
        sys.exit(exit_code)

    if getattr(args, "delivery_status", False):
        delivery_contract = (
            getattr(args, "delivery_contract", None) or ""
        ).strip()
        delivery_id = (
            getattr(args, "delivery_id", None) or ""
        ).strip()
        delivery_scope_id = (
            getattr(args, "delivery_scope_id", None) or ""
        ).strip()
        delivery_provider = (
            getattr(args, "delivery_provider", None) or ""
        ).strip().lower()
        gateway_account_id = (
            getattr(args, "gateway_account_id", None) or ""
        ).strip()
        if (
            getattr(args, "to", None)
            or getattr(args, "message", None)
            or getattr(args, "file", None)
            or not delivery_contract
            or not delivery_id
            or not delivery_scope_id
            or not delivery_provider
            or not gateway_account_id
        ):
            print(
                "hermes send: --delivery-status requires "
                "--delivery-contract, --delivery-id, and "
                "--delivery-scope-id, --delivery-provider, and "
                "--gateway-account-id, without a target or message",
                file=sys.stderr,
            )
            sys.exit(_USAGE_EXIT)
        if not _delivery_account_is_configured(
            delivery_provider,
            gateway_account_id,
        ):
            payload = _semantic_authority_changed(
                delivery_contract=delivery_contract,
                delivery_id=delivery_id,
                delivery_scope_id=delivery_scope_id,
                provider=delivery_provider,
                gateway_account_id=gateway_account_id,
            )
            if getattr(args, "json", False):
                print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
            else:
                print(payload["outcome"])
            sys.exit(_FAILURE_EXIT)
        from hermes_cli.semantic_delivery import (
            semantic_delivery_scope_id,
            semantic_delivery_status,
        )

        current_scope_id = semantic_delivery_scope_id()
        if current_scope_id != delivery_scope_id:
            payload = _semantic_authority_changed(
                delivery_contract=delivery_contract,
                delivery_id=delivery_id,
                delivery_scope_id=delivery_scope_id,
                provider=delivery_provider,
                gateway_account_id=gateway_account_id,
                current_delivery_scope_id=current_scope_id,
            )
            if getattr(args, "json", False):
                print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
            else:
                print(payload["outcome"])
            sys.exit(_FAILURE_EXIT)

        payload = semantic_delivery_status(
            delivery_id=delivery_id,
            contract_version=delivery_contract,
            expected_scope_id=delivery_scope_id,
            expected_provider=delivery_provider,
            gateway_account_id=gateway_account_id,
        )
        if getattr(args, "json", False):
            print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        else:
            print(payload.get("outcome", "unknown"))
        sys.exit(
            _SUCCESS_EXIT
            if payload.get("success") is True
            else _FAILURE_EXIT
        )

    target = _resolve_target(getattr(args, "to", None))
    if not target:
        print(
            "hermes send: --to PLATFORM[:channel[:thread]] is required\n"
            "Examples:\n"
            "  hermes send --to telegram \"hello\"\n"
            "  hermes send --to discord:#ops --file report.md\n"
            "  hermes send --list      # list available targets",
            file=sys.stderr,
        )
        sys.exit(_USAGE_EXIT)

    message = _read_message_body(
        getattr(args, "message", None),
        getattr(args, "file", None),
    )
    if message is None or not message.strip():
        print(
            "hermes send: no message provided. Pass text as a positional "
            "argument, use --file PATH, or pipe data via stdin.",
            file=sys.stderr,
        )
        sys.exit(_USAGE_EXIT)

    # Optional: prepend a subject line. Useful for alerting scripts that
    # want a consistent header without inlining it into every call.
    subject = getattr(args, "subject", None)
    if subject:
        message = f"{subject}\n\n{message.lstrip()}"

    delivery_contract = (
        getattr(args, "delivery_contract", None) or ""
    ).strip()
    delivery_id = (getattr(args, "delivery_id", None) or "").strip()
    delivery_scope_id = (
        getattr(args, "delivery_scope_id", None) or ""
    ).strip()
    gateway_account_id = (
        getattr(args, "gateway_account_id", None) or ""
    ).strip()
    delivery_provider = (
        getattr(args, "delivery_provider", None) or ""
    ).strip()
    if delivery_provider:
        print(
            "hermes send: --delivery-provider is valid only with "
            "--delivery-status",
            file=sys.stderr,
        )
        sys.exit(_USAGE_EXIT)
    if (
        bool(delivery_contract) != bool(delivery_id)
        or bool(delivery_contract) != bool(delivery_scope_id)
        or bool(delivery_contract) != bool(gateway_account_id)
    ):
        print(
            "hermes send: --delivery-contract, --delivery-id, and "
            "--delivery-scope-id, and --gateway-account-id must be "
            "provided together",
            file=sys.stderr,
        )
        sys.exit(_USAGE_EXIT)

    provider = target.partition(":")[0].strip().lower()
    if delivery_contract and not _delivery_account_is_configured(
        provider,
        gateway_account_id,
    ):
        payload = _semantic_authority_changed(
            delivery_contract=delivery_contract,
            delivery_id=delivery_id,
            delivery_scope_id=delivery_scope_id,
            provider=provider,
            gateway_account_id=gateway_account_id,
            target=target,
        )
        result = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        exit_code = _emit_result(
            result,
            json_mode=getattr(args, "json", False),
            quiet=getattr(args, "quiet", False),
        )
        sys.exit(exit_code)

    if delivery_contract:
        from hermes_cli.semantic_delivery import semantic_delivery_scope_id

        current_scope_id = semantic_delivery_scope_id()
        if current_scope_id != delivery_scope_id:
            result = json.dumps(
                _semantic_authority_changed(
                    delivery_contract=delivery_contract,
                    delivery_id=delivery_id,
                    delivery_scope_id=delivery_scope_id,
                    provider=provider,
                    gateway_account_id=gateway_account_id,
                    target=target,
                    current_delivery_scope_id=current_scope_id,
                ),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            exit_code = _emit_result(
                result,
                json_mode=getattr(args, "json", False),
                quiet=getattr(args, "quiet", False),
            )
            sys.exit(exit_code)

    # Import only after semantic account prevalidation so an invalid
    # profile/provider/account claim cannot enter any provider code path.
    from tools.send_message_tool import send_message_tool

    if delivery_contract:
        from hermes_cli.semantic_delivery import semantic_send

        result = json.dumps(
            semantic_send(
                delivery_id=delivery_id,
                contract_version=delivery_contract,
                target=target,
                message=message,
                send=send_message_tool,
                expected_scope_id=delivery_scope_id,
                gateway_account_id=gateway_account_id,
            ),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    else:
        result = send_message_tool(
            {
                "action": "send",
                "target": target,
                "message": message,
            }
        )
    exit_code = _emit_result(
        result,
        json_mode=getattr(args, "json", False),
        quiet=getattr(args, "quiet", False),
    )
    sys.exit(exit_code)


def register_send_subparser(subparsers) -> argparse.ArgumentParser:
    """Create the ``send`` subparser and return it.

    Kept as a standalone function so the top-level parser builder can wire
    it in next to the other messaging subcommands without cluttering
    ``_parser.py`` or ``main.py``.
    """
    parser = subparsers.add_parser(
        "send",
        help="Send a message to a configured platform (scripts, cron jobs, CI).",
        description=(
            "Pipe text from any shell script to any messaging platform Hermes "
            "is already configured for. Reuses the gateway's platform "
            "credentials (~/.hermes/.env + ~/.hermes/config.yaml) — no LLM, "
            "no agent loop, no running gateway required for bot-token "
            "platforms like Telegram/Discord/Slack/Signal."
        ),
        epilog=(
            "Examples:\n"
            "  hermes send --to telegram \"deploy finished\"\n"
            "  echo \"RAM 92%\" | hermes send --to telegram:-1001234567890\n"
            "  hermes send --to discord:#ops --file /tmp/report.md\n"
            "  hermes send --to slack:#eng --subject \"[CI]\" --file build.log\n"
            "  hermes send --to telegram \"MEDIA:/tmp/chart.png\"   # send a media attachment\n"
            "  hermes send --list                  # all platforms\n"
            "  hermes send --list telegram         # filter by platform\n"
            "\n"
            "Exit codes: 0 ok, 1 delivery/backend error, 2 usage error."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-t",
        "--to",
        metavar="TARGET",
        default=None,
        help=(
            "Delivery target. Format: 'platform' (home channel), "
            "'platform:chat_id', 'platform:chat_id:thread_id', or "
            "'platform:#channel-name'. Examples: telegram, "
            "telegram:-1001234567890:17585, discord:#ops, slack:C0123ABCD, "
            "signal:+15551234567."
        ),
    )

    parser.add_argument(
        "message",
        nargs="?",
        default=None,
        help="Message text. If omitted, read from --file or stdin.",
    )

    # Legacy / convenience positional removed — use --to for clarity.

    parser.add_argument(
        "-f",
        "--file",
        metavar="PATH",
        default=None,
        help=(
            "Read message body from PATH (text only). Use '-' to force stdin. "
            "To send an image/document as an attachment, use MEDIA:<path> in "
            "the message text instead."
        ),
    )

    parser.add_argument(
        "-s",
        "--subject",
        metavar="LINE",
        default=None,
        help="Prepend a subject/header line before the message body.",
    )

    parser.add_argument(
        "-l",
        "--list",
        dest="list_targets",
        action="store_true",
        default=False,
        help="List available targets. Optional positional filter: `hermes send --list telegram`.",
    )

    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        default=False,
        help="Suppress stdout on success (exit code only).",
    )

    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Emit raw JSON result instead of human-readable output.",
    )

    parser.add_argument(
        "--delivery-contract",
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--delivery-id",
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--delivery-scope",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--delivery-scopes",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--delivery-scopes-current",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--delivery-scope-id",
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--delivery-status",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--delivery-provider",
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--gateway-account-id",
        default=None,
        help=argparse.SUPPRESS,
    )

    parser.set_defaults(func=cmd_send)
    return parser


__all__ = ["cmd_send", "register_send_subparser"]
