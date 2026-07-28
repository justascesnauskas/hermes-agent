"""Profile-isolated recovery for durable gateway delivery outboxes.

Cron providers only own cron triggers.  This supervisor lives with the
gateway so semantic provider retries and Planning preview acknowledgements
continue to converge when cron is externally owned (for example, Chronos).

Every served profile gets one task, one explicit pair of state paths, one
profile secret scope, and only that profile's live adapter registry.  Nothing
in this module combines adapter maps or credential scopes across profiles.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
import logging
from pathlib import Path
from typing import Any


logger = logging.getLogger("gateway.profile_delivery_supervisor")

DEFAULT_POLL_SECONDS = 1.0
DEFAULT_STOP_TIMEOUT_SECONDS = 35.0


class ProfileDeliverySupervisionError(RuntimeError):
    """The gateway cannot safely establish exact profile recovery ownership."""


@dataclass(frozen=True, slots=True)
class ProfileDeliveryBinding:
    """One exact profile home and its live, profile-owned adapter registry."""

    profile_name: str
    profile_home: Path
    is_primary: bool
    is_multiplex: bool
    _runner: Any = field(repr=False, compare=False)

    def adapter_snapshot(self) -> dict[Any, Any]:
        """Return only adapters owned by this exact profile.

        The maps are copied on every pump because reconnects replace adapter
        instances while the gateway is running.  Adapter objects are never
        logged or persisted by the supervisor.
        """

        if self.is_primary:
            adapters = getattr(self._runner, "adapters", None)
        else:
            profile_adapters = getattr(
                self._runner,
                "_profile_adapters",
                None,
            )
            adapters = (
                profile_adapters.get(self.profile_name)
                if isinstance(profile_adapters, Mapping)
                else None
            )
        return dict(adapters) if isinstance(adapters, Mapping) else {}


def semantic_ledger_path(profile_home: Path) -> Path:
    """Return the semantic retry ledger for one exact profile home."""

    return (
        Path(profile_home)
        / "state"
        / "semantic-delivery"
        / "ledger.sqlite3"
    )


def preview_ack_outbox_path(profile_home: Path) -> Path:
    """Return the Planning preview ACK outbox for one exact profile home."""

    return (
        Path(profile_home)
        / "state"
        / "planning-preview-ack"
        / "outbox.sqlite3"
    )


@contextmanager
def _profile_delivery_scope(
    profile_home: Path,
    *,
    isolate_secrets: bool,
):
    """Install one profile's home and secrets without mutating ``os.environ``."""

    from agent.secret_scope import (
        build_profile_secret_scope,
        reset_secret_scope,
        set_secret_scope,
    )
    from hermes_constants import (
        reset_hermes_home_override,
        set_hermes_home_override,
    )

    home = Path(profile_home)
    home_token = set_hermes_home_override(str(home))
    secret_token = (
        set_secret_scope(build_profile_secret_scope(home))
        if isolate_secrets
        else None
    )
    try:
        yield
    finally:
        if secret_token is not None:
            reset_secret_scope(secret_token)
        reset_hermes_home_override(home_token)


def enumerate_profile_delivery_bindings(
    runner: Any,
) -> tuple[ProfileDeliveryBinding, ...]:
    """Enumerate every exact home served by ``runner``.

    ``profiles_to_serve`` is the gateway's canonical default/named-profile
    chokepoint.  This function additionally proves every returned name maps
    back to the same on-disk home, rejects aliases between homes, and binds the
    process home to the primary adapter registry.  No arbitrary profile cap is
    applied: every profile the gateway serves receives recovery ownership.
    """

    from hermes_constants import get_hermes_home
    from hermes_cli.profiles import get_profile_dir, profiles_to_serve

    multiplex = bool(
        getattr(getattr(runner, "config", None), "multiplex_profiles", False)
    )
    served = profiles_to_serve(multiplex=multiplex)

    process_home = Path(get_hermes_home()).expanduser().resolve()
    seen_names: set[str] = set()
    seen_homes: set[Path] = set()
    bindings: list[ProfileDeliveryBinding] = []
    primary_count = 0

    for raw_name, raw_home in served:
        name = str(raw_name)
        if name in seen_names:
            raise ProfileDeliverySupervisionError(
                f"duplicate served profile identity: {name!r}"
            )
        seen_names.add(name)

        home = Path(raw_home).expanduser().absolute()
        expected = Path(get_profile_dir(name)).expanduser().absolute()
        if home != expected:
            raise ProfileDeliverySupervisionError(
                f"served profile {name!r} does not map to its exact home"
            )
        if not home.is_dir():
            raise ProfileDeliverySupervisionError(
                f"served profile {name!r} home is unavailable"
            )

        canonical_home = home.resolve()
        if canonical_home in seen_homes:
            raise ProfileDeliverySupervisionError(
                f"served profile {name!r} aliases another profile home"
            )
        seen_homes.add(canonical_home)

        is_primary = canonical_home == process_home
        primary_count += int(is_primary)
        bindings.append(
            ProfileDeliveryBinding(
                profile_name=name,
                profile_home=canonical_home,
                is_primary=is_primary,
                is_multiplex=multiplex,
                _runner=runner,
            )
        )

    if primary_count != 1:
        raise ProfileDeliverySupervisionError(
            "gateway process home does not identify exactly one served profile"
        )
    return tuple(bindings)


class ProfileDeliverySupervisor:
    """Own the sole gateway retry/ACK pump for every served profile."""

    def __init__(
        self,
        runner: Any,
        *,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        semantic_dispatch: Callable[..., Awaitable[Mapping[str, int]]] | None = None,
        preview_reconcile: Callable[..., Mapping[str, Any] | None] | None = None,
        preview_dispatch: Callable[..., Mapping[str, Any] | None] | None = None,
        ack_deliver: Callable[[Mapping[str, Any]], Any] | None = None,
    ) -> None:
        from hermes_cli.planning_preview_ack_outbox import (
            deliver_preview_ack_request,
            dispatch_one_preview_ack,
            reconcile_preview_ack_bridge,
        )
        from hermes_cli.semantic_delivery import (
            dispatch_due_semantic_delivery_retries,
        )

        self._runner = runner
        self._poll_seconds = max(0.05, float(poll_seconds))
        self._semantic_dispatch = (
            semantic_dispatch or dispatch_due_semantic_delivery_retries
        )
        self._preview_reconcile = (
            preview_reconcile or reconcile_preview_ack_bridge
        )
        self._preview_dispatch = (
            preview_dispatch or dispatch_one_preview_ack
        )
        self._ack_deliver = ack_deliver or deliver_preview_ack_request
        self._bindings: tuple[ProfileDeliveryBinding, ...] = ()
        self._tasks: tuple[asyncio.Task[None], ...] = ()
        self._stop_event: asyncio.Event | None = None

    @property
    def bindings(self) -> tuple[ProfileDeliveryBinding, ...]:
        return self._bindings

    @property
    def running(self) -> bool:
        return any(not task.done() for task in self._tasks)

    @property
    def active_profile_count(self) -> int:
        """Number of profile recovery tasks that still own their loop."""

        return sum(not task.done() for task in self._tasks)

    def start(self) -> bool:
        """Start one task per exact profile; repeated calls are a no-op."""

        if self.running:
            return False
        self._bindings = enumerate_profile_delivery_bindings(self._runner)
        self._stop_event = asyncio.Event()
        self._tasks = tuple(
            asyncio.create_task(
                self._run_profile(binding),
                name=(
                    "profile-delivery-supervisor:"
                    f"{binding.profile_name}"
                ),
            )
            for binding in self._bindings
        )
        return True

    async def stop(
        self,
        *,
        timeout: float = DEFAULT_STOP_TIMEOUT_SECONDS,
    ) -> None:
        """Stop all profile tasks without blocking the gateway event loop."""

        if self._stop_event is not None:
            self._stop_event.set()
        tasks = tuple(task for task in self._tasks if not task.done())
        if not tasks:
            return
        done, pending = await asyncio.wait(
            tasks,
            timeout=max(0.0, float(timeout)),
        )
        del done
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def _run_profile(self, binding: ProfileDeliveryBinding) -> None:
        assert self._stop_event is not None
        while not self._stop_event.is_set():
            did_work = False
            if binding.profile_home.is_dir():
                did_work = await self._pump_semantic(binding)
                did_work = await self._pump_preview_ack(binding) or did_work

            if did_work:
                await asyncio.sleep(0)
                continue
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._poll_seconds,
                )
            except TimeoutError:
                pass

    async def _pump_semantic(
        self,
        binding: ProfileDeliveryBinding,
    ) -> bool:
        try:
            with _profile_delivery_scope(
                binding.profile_home,
                isolate_secrets=binding.is_multiplex,
            ):
                counts = await self._semantic_dispatch(
                    bound_adapters=binding.adapter_snapshot(),
                    ledger_path=semantic_ledger_path(
                        binding.profile_home
                    ),
                )
        except asyncio.CancelledError:
            raise
        except BaseException:
            logger.error(
                "Semantic delivery recovery failed for profile %s",
                binding.profile_name,
                exc_info=True,
            )
            return False

        claimed = int(counts.get("claimed", 0) or 0)
        completions = int(
            counts.get("completion_completed", 0) or 0
        )
        if claimed or completions:
            logger.info(
                "Semantic delivery recovery for profile %s: %s",
                binding.profile_name,
                dict(counts),
            )
        return bool(claimed or completions)

    async def _pump_preview_ack(
        self,
        binding: ProfileDeliveryBinding,
    ) -> bool:
        try:
            bridge, outcome = await asyncio.to_thread(
                self._pump_preview_ack_sync,
                binding,
            )
        except asyncio.CancelledError:
            raise
        except BaseException:
            logger.error(
                "Planning preview ACK recovery failed for profile %s",
                binding.profile_name,
                exc_info=True,
            )
            return False

        # A pending bridge is waiting on semantic state and must not spin.
        bridge_progressed = bool(
            bridge is not None and bridge.get("state") != "pending"
        )
        return outcome is not None or bridge_progressed

    def _pump_preview_ack_sync(
        self,
        binding: ProfileDeliveryBinding,
    ) -> tuple[Mapping[str, Any] | None, Mapping[str, Any] | None]:
        ack_path = preview_ack_outbox_path(binding.profile_home)
        ledger_path = semantic_ledger_path(binding.profile_home)
        with _profile_delivery_scope(
            binding.profile_home,
            isolate_secrets=binding.is_multiplex,
        ):
            bridge = self._preview_reconcile(
                path=ack_path,
                semantic_ledger_path=ledger_path,
            )
            outcome = self._preview_dispatch(
                deliver=self._ack_deliver,
                path=ack_path,
            )
        return bridge, outcome


def start_profile_delivery_supervisor(
    runner: Any,
    **kwargs: Any,
) -> ProfileDeliverySupervisor:
    """Construct and start the gateway's profile delivery supervisor."""

    supervisor = ProfileDeliverySupervisor(runner, **kwargs)
    supervisor.start()
    return supervisor


__all__ = [
    "ProfileDeliveryBinding",
    "ProfileDeliverySupervisionError",
    "ProfileDeliverySupervisor",
    "enumerate_profile_delivery_bindings",
    "preview_ack_outbox_path",
    "semantic_ledger_path",
    "start_profile_delivery_supervisor",
]
