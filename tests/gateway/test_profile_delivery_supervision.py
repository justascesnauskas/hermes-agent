"""Profile-home multiplexing contracts for durable gateway delivery."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from gateway.profile_delivery_supervisor import (
    ProfileDeliverySupervisionError,
    ProfileDeliverySupervisor,
    enumerate_profile_delivery_bindings,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROCESS_HARNESS = Path(__file__).with_name(
    "process_harness_profile_delivery_supervision.py"
)


def _runner(*, multiplex: bool = True) -> Any:
    return SimpleNamespace(
        config=SimpleNamespace(multiplex_profiles=multiplex),
        adapters={"provider": "default-adapter"},
        _profile_adapters={
            "secondary": {"provider": "secondary-adapter"},
        },
    )


async def _empty_semantic_pump() -> dict[str, int]:
    return {
        "claimed": 0,
        "completion_completed": 0,
    }


def test_enumeration_uses_exact_homes_and_exact_adapter_maps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    default_home = tmp_path / "hermes-home"
    secondary_home = default_home / "profiles" / "secondary"
    secondary_home.mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(default_home))
    runner = _runner()

    bindings = enumerate_profile_delivery_bindings(runner)
    by_name = {binding.profile_name: binding for binding in bindings}

    assert set(by_name) == {"default", "secondary"}
    assert by_name["default"].profile_home == default_home.resolve()
    assert by_name["secondary"].profile_home == secondary_home.resolve()
    assert by_name["default"].adapter_snapshot() == {
        "provider": "default-adapter"
    }
    assert by_name["secondary"].adapter_snapshot() == {
        "provider": "secondary-adapter"
    }

    # Reconnect replacement is observed, but remains in the same profile map.
    runner._profile_adapters["secondary"]["provider"] = (
        "replacement-secondary-adapter"
    )
    assert by_name["secondary"].adapter_snapshot() == {
        "provider": "replacement-secondary-adapter"
    }
    assert "default-adapter" not in (
        by_name["secondary"].adapter_snapshot().values()
    )


@pytest.mark.asyncio
async def test_supervisor_starts_every_profile_beyond_legacy_worker_counts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    default_home = tmp_path / "hermes-home"
    for index in range(130):
        (default_home / "profiles" / f"profile-{index:03d}").mkdir(
            parents=True
        )
    monkeypatch.setenv("HERMES_HOME", str(default_home))

    supervisor = ProfileDeliverySupervisor(
        _runner(),
        poll_seconds=60,
        semantic_dispatch=lambda **kwargs: _empty_semantic_pump(),
        preview_reconcile=lambda **kwargs: None,
        preview_dispatch=lambda **kwargs: None,
    )

    assert supervisor.start() is True
    assert len(supervisor.bindings) == 131
    assert supervisor.bindings[0].profile_name == "default"
    assert supervisor.bindings[-1].profile_name == "profile-129"
    assert supervisor.active_profile_count == 131
    await supervisor.stop(timeout=2)
    assert supervisor.active_profile_count == 0


def test_startup_mapping_failure_is_atomic(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    default_home = tmp_path / "hermes-home"
    default_home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(default_home))
    monkeypatch.setattr(
        "hermes_cli.profiles.profiles_to_serve",
        lambda multiplex: [
            ("default", default_home),
            ("secondary", tmp_path / "not-the-secondary-home"),
        ],
    )
    supervisor = ProfileDeliverySupervisor(_runner())

    with pytest.raises(
        ProfileDeliverySupervisionError,
        match="exact home",
    ):
        supervisor.start()

    assert supervisor.running is False
    assert supervisor.bindings == ()


@pytest.mark.asyncio
async def test_start_is_single_owner_and_profile_faults_are_isolated(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    default_home = tmp_path / "hermes-home"
    secondary_home = default_home / "profiles" / "secondary"
    secondary_home.mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(default_home))
    calls: list[tuple[str, str]] = []
    both_profiles = asyncio.Event()

    async def semantic_dispatch(
        *,
        bound_adapters: dict[Any, Any],
        ledger_path: Path,
    ) -> dict[str, int]:
        owner = next(iter(bound_adapters.values()))
        calls.append((str(owner), str(ledger_path)))
        if len(calls) == 1:
            raise RuntimeError("one profile is broken")
        both_profiles.set()
        return {
            "claimed": 0,
            "completion_completed": 0,
        }

    supervisor = ProfileDeliverySupervisor(
        _runner(),
        poll_seconds=60,
        semantic_dispatch=semantic_dispatch,
        preview_reconcile=lambda **kwargs: None,
        preview_dispatch=lambda **kwargs: None,
    )

    assert supervisor.start() is True
    assert supervisor.start() is False
    await asyncio.wait_for(both_profiles.wait(), timeout=5)
    await supervisor.stop(timeout=2)

    assert len(supervisor.bindings) == 2
    assert {owner for owner, _ in calls} == {
        "default-adapter",
        "secondary-adapter",
    }


@pytest.mark.asyncio
async def test_gateway_lifecycle_starts_one_delivery_owner_before_cron(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import gateway.run as gateway_run

    events: list[str] = []
    runners: list[Any] = []

    class RunningRunner:
        def __init__(self, config):
            self.config = config
            self.adapters = {}
            self._profile_adapters = {}
            self._running = True
            self._draining = False
            self._external_drain_active = False
            self._restart_requested = False
            self._restart_via_service = False
            self.should_exit_cleanly = False
            self.should_exit_with_failure = False
            self.exit_reason = None
            self.exit_code = None
            runners.append(self)

        async def start(self):
            return True

        async def wait_for_shutdown(self):
            return None

    class ExternalCron:
        name = "external-test"

        def start(self, stop_event, **kwargs):
            del stop_event, kwargs
            events.append("cron-start")

        def stop(self):
            events.append("cron-stop")

    class DeliveryOwner:
        async def stop(self, **kwargs):
            del kwargs
            events.append("delivery-stop")

    def start_delivery_owner(runner):
        assert runner is runners[0]
        events.append("delivery-start")
        return DeliveryOwner()

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(gateway_run, "GatewayRunner", RunningRunner)
    monkeypatch.setattr(
        gateway_run,
        "_run_planned_stop_watcher",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        gateway_run,
        "_start_gateway_housekeeping",
        lambda *args, **kwargs: events.append("housekeeping-start"),
    )
    monkeypatch.setattr(
        gateway_run,
        "_ensure_windows_gateway_venv_imports",
        lambda: None,
    )
    monkeypatch.setattr(
        "gateway.code_skew.record_boot_fingerprint",
        lambda: None,
    )
    monkeypatch.setattr("gateway.status.get_running_pid", lambda: None)
    monkeypatch.setattr(
        "gateway.status.acquire_gateway_runtime_lock",
        lambda: True,
    )
    monkeypatch.setattr("gateway.status.write_pid_file", lambda: None)
    monkeypatch.setattr("gateway.status.remove_pid_file", lambda: None)
    monkeypatch.setattr(
        "gateway.status.release_gateway_runtime_lock",
        lambda: None,
    )
    monkeypatch.setattr(
        "tools.skills_sync.sync_skills",
        lambda quiet=True: None,
    )
    monkeypatch.setattr(
        "hermes_logging.setup_logging",
        lambda hermes_home, mode: None,
    )
    monkeypatch.setattr(
        "hermes_cli.security_audit_startup.log_startup_security_warnings",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "hermes_cli.nous_auth_keepalive.start_nous_auth_keepalive",
        lambda: None,
    )
    monkeypatch.setattr(
        "hermes_cli.nous_auth_keepalive.stop_nous_auth_keepalive",
        lambda: None,
    )
    monkeypatch.setattr(
        "tools.mcp_tool.discover_mcp_tools",
        lambda: None,
    )
    monkeypatch.setattr(
        "tools.mcp_tool.shutdown_mcp_servers",
        lambda: None,
    )
    monkeypatch.setattr(
        "cron.scheduler_provider.resolve_cron_scheduler",
        lambda: ExternalCron(),
    )
    monkeypatch.setattr(
        "gateway.profile_delivery_supervisor."
        "start_profile_delivery_supervisor",
        start_delivery_owner,
    )

    result = await gateway_run.start_gateway(
        config=gateway_run.GatewayConfig(),
        replace=False,
        verbosity=None,
    )

    assert result is True
    assert events.count("delivery-start") == 1
    assert events.index("delivery-start") < events.index("cron-start")
    assert events.count("delivery-stop") == 1


@pytest.mark.asyncio
async def test_gateway_failure_verdict_follows_shared_runtime_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import gateway.run as gateway_run

    events: list[str] = []
    stop_events: dict[str, Any] = {}

    class FailingRunner:
        def __init__(self, config):
            self.config = config
            self.adapters = {}
            self._profile_adapters = {}
            self._running = True
            self._draining = False
            self._external_drain_active = False
            self._restart_requested = False
            self._restart_via_service = False
            self.should_exit_cleanly = False
            self.should_exit_with_failure = True
            self.exit_reason = "fatal adapter boundary"
            self.exit_code = None

        async def start(self):
            events.append("runner-start")
            return True

        async def wait_for_shutdown(self):
            deadline = asyncio.get_running_loop().time() + 2
            while (
                set(stop_events) != {"cron", "housekeeping", "planned"}
                and asyncio.get_running_loop().time() < deadline
            ):
                await asyncio.sleep(0.001)
            assert set(stop_events) == {"cron", "housekeeping", "planned"}
            events.append("runner-shutdown")

    class ExternalCron:
        name = "external-test"

        def start(self, stop_event, **kwargs):
            del kwargs
            stop_events["cron"] = stop_event
            events.append("cron-start")

        def stop(self):
            assert stop_events["cron"].is_set()
            events.append("cron-stop")

    class DeliveryOwner:
        async def stop(self, **kwargs):
            del kwargs
            events.append("delivery-stop")

    def planned_stop_watcher(stop_event, *_args):
        stop_events["planned"] = stop_event
        events.append("planned-watcher-start")

    def housekeeping(stop_event, **_kwargs):
        stop_events["housekeeping"] = stop_event
        events.append("housekeeping-start")

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(gateway_run, "GatewayRunner", FailingRunner)
    monkeypatch.setattr(
        gateway_run,
        "_run_planned_stop_watcher",
        planned_stop_watcher,
    )
    monkeypatch.setattr(
        gateway_run,
        "_start_gateway_housekeeping",
        housekeeping,
    )
    monkeypatch.setattr(
        gateway_run,
        "_ensure_windows_gateway_venv_imports",
        lambda: None,
    )
    monkeypatch.setattr(
        "gateway.code_skew.record_boot_fingerprint",
        lambda: None,
    )
    monkeypatch.setattr("gateway.status.get_running_pid", lambda: None)
    monkeypatch.setattr(
        "gateway.status.acquire_gateway_runtime_lock",
        lambda: True,
    )
    monkeypatch.setattr("gateway.status.write_pid_file", lambda: None)
    monkeypatch.setattr("gateway.status.remove_pid_file", lambda: None)
    monkeypatch.setattr(
        "gateway.status.release_gateway_runtime_lock",
        lambda: None,
    )
    monkeypatch.setattr(
        "tools.skills_sync.sync_skills",
        lambda quiet=True: None,
    )
    monkeypatch.setattr(
        "hermes_logging.setup_logging",
        lambda hermes_home, mode: None,
    )
    monkeypatch.setattr(
        "hermes_cli.security_audit_startup.log_startup_security_warnings",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "hermes_cli.nous_auth_keepalive.start_nous_auth_keepalive",
        lambda: None,
    )
    monkeypatch.setattr(
        "hermes_cli.nous_auth_keepalive.stop_nous_auth_keepalive",
        lambda: events.append("keepalive-stop"),
    )
    monkeypatch.setattr(
        "tools.mcp_tool.discover_mcp_tools",
        lambda: None,
    )
    monkeypatch.setattr(
        "tools.mcp_tool.shutdown_mcp_servers",
        lambda: events.append("mcp-stop"),
    )
    monkeypatch.setattr(
        "cron.scheduler_provider.resolve_cron_scheduler",
        lambda: ExternalCron(),
    )
    monkeypatch.setattr(
        "gateway.profile_delivery_supervisor."
        "start_profile_delivery_supervisor",
        lambda _runner: DeliveryOwner(),
    )

    result = await gateway_run.start_gateway(
        config=gateway_run.GatewayConfig(),
        replace=False,
        verbosity=None,
    )

    assert result is False
    assert stop_events["cron"].is_set()
    assert stop_events["housekeeping"].is_set()
    assert stop_events["planned"].is_set()
    assert events.index("runner-shutdown") < events.index("cron-stop")
    assert events.index("cron-stop") < events.index("delivery-stop")
    assert events.index("delivery-stop") < events.index("mcp-stop")


def _run_process(root: Path, command: str) -> dict[str, Any]:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            str(REPOSITORY_ROOT),
            environment.get("PYTHONPATH", ""),
        )
        if value
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(PROCESS_HARNESS),
            str(root),
            command,
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, {
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
    lines = [
        line for line in completed.stdout.splitlines() if line.strip()
    ]
    assert len(lines) == 1, completed.stdout
    payload = json.loads(lines[0])
    assert isinstance(payload, dict)
    return payload


def test_fresh_chronos_process_recovers_secondary_without_cross_send(
    tmp_path: Path,
) -> None:
    staged = _run_process(tmp_path, "stage")
    recovered = _run_process(tmp_path, "recover")

    assert staged["groupState"] == "pending"
    assert recovered["chronosProvider"] == "chronos"
    assert recovered["semanticState"]["state"] == "retired"
    assert recovered["semanticState"]["group_state"] == "completed"
    assert recovered["bridgeState"] == "enqueued"
    assert recovered["secondaryAckSucceeded"] == 2
    assert recovered["defaultAckSucceeded"] == 0
    assert recovered["unscopedPlanningFailedClosed"] is True

    # The default adapter deliberately advertises the same provider/account.
    # Exact profile binding still permits only the secondary adapter to write.
    assert recovered["providerCalls"] == [
        {
            "deliveryId": "secondary-profile-retry",
            "owner": "secondary",
            "profileHome": staged["secondaryHome"],
            "runnerToken": "secondary-secret",
        }
    ]

    # Both the ordinary provider-confirmed ACK and the semantic-retry
    # completion ACK use the secondary profile's path and credential scope.
    assert {
        call["idempotencyKey"] for call in recovered["ackCalls"]
    } == {
        "preview-review-normal-provider-confirmed",
        "preview-review-retry-completion",
    }
    assert all(
        call["profileHome"] == staged["secondaryHome"]
        and call["baseUrl"] == "https://secondary-hub.invalid"
        and call["runnerId"] == "secondary-runner"
        and call["runnerToken"] == "secondary-secret"
        for call in recovered["ackCalls"]
    )
