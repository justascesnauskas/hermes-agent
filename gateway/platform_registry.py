"""
Platform Adapter Registry

Allows platform adapters (built-in and plugin) to self-register so the gateway
can discover and instantiate them without hardcoded if/elif chains.

Built-in adapters continue to use the existing if/elif in _create_adapter()
for now.  Plugin adapters register here via PluginContext.register_platform()
and are looked up first -- if nothing is found the gateway falls through to
the legacy code path.

Usage (plugin side):

    from gateway.platform_registry import platform_registry, PlatformEntry

    platform_registry.register(PlatformEntry(
        name="irc",
        label="IRC",
        adapter_factory=lambda cfg: IRCAdapter(cfg),
        check_fn=check_requirements,
        validate_config=lambda cfg: bool(cfg.extra.get("server")),
        required_env=["IRC_SERVER"],
        install_hint="pip install irc",
    ))

Usage (gateway side):

    adapter = platform_registry.create_adapter("irc", platform_config)
"""

import logging
import importlib
import re
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SemanticExactAttemptDeclaration:
    """Provider-owned exact-attempt capability, independent of load path.

    Plugin adapters normally declare on :class:`PlatformEntry`. Built-in
    adapters that do not have an entry use :func:`declare_semantic_exact_attempt`
    from their own module. There is deliberately no central provider allowlist:
    an absent, malformed, or conflicting declaration is unsupported.
    """

    provider: str
    standalone: bool | None
    live: bool | None
    owner: str
    planning_ineligible_reason: str = ""
    standalone_sender_fn: Callable[..., Awaitable[dict]] | None = field(
        default=None,
        compare=False,
        repr=False,
    )


@dataclass(frozen=True, slots=True)
class StandaloneSemanticExactAttemptConformance:
    """One inspectable standalone-provider exact-attempt verdict."""

    provider: str
    registered: bool
    declaration: bool | None
    exact_sender: bool
    supported: bool
    conformant: bool
    owner: str
    reason: str


@dataclass(frozen=True, slots=True)
class LiveSemanticExactAttemptConformance:
    """One complete, inspectable live-provider capability verdict."""

    provider: str
    registered: bool
    bound: bool
    outbound_send: bool
    exact_attempt_method: bool
    declaration: bool | None
    supported: bool
    conformant: bool
    owner: str
    reason: str
    planning_ineligible_reason: str = ""


_semantic_exact_attempt_declarations: dict[
    str, SemanticExactAttemptDeclaration
] = {}


def _clean_provider_name(name: Any) -> str:
    return str(getattr(name, "value", name) or "").strip().lower()


def _load_builtin_semantic_declaration(provider: str) -> None:
    """Load one built-in provider-owned capability contract on first query.

    Plugin declarations arrive through their existing deferred registry entry.
    Built-ins such as Signal have no ``PlatformEntry`` and therefore need this
    narrow convention loader in a fresh standalone-send process. Unknown names
    and import failures remain unsupported; no central capability allowlist is
    introduced.
    """

    if provider in _semantic_exact_attempt_declarations:
        return
    module_component = provider.replace("-", "_")
    if re.fullmatch(r"[a-z][a-z0-9_]{0,119}", module_component) is None:
        return
    module_name = f"gateway.platforms.{module_component}"
    try:
        importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name != module_name:
            logger.warning(
                "Semantic capability load of built-in platform '%s' failed: %s",
                provider,
                exc,
                exc_info=True,
            )
    except Exception as exc:
        logger.warning(
            "Semantic capability load of built-in platform '%s' failed: %s",
            provider,
            exc,
            exc_info=True,
        )


def declare_semantic_exact_attempt(
    provider: str,
    *,
    standalone: bool | None,
    live: bool | None,
    owner: str,
    planning_ineligible_reason: str = "",
    standalone_sender_fn: Callable[..., Awaitable[dict]] | None = None,
) -> SemanticExactAttemptDeclaration:
    """Declare one built-in/provider-owned capability without an allowlist.

    Repeating the identical declaration is idempotent. A second owner cannot
    silently widen or narrow an existing provider contract: the conflict is
    retained as an unsupported declaration so all callers fail closed.
    """

    clean = _clean_provider_name(provider)
    ineligible_reason = str(planning_ineligible_reason or "").strip()
    if (
        not clean
        or (
            standalone is not None
            and type(standalone) is not bool
        )
        or (live is not None and type(live) is not bool)
        or (
            (standalone is True and not callable(standalone_sender_fn))
            or (
                standalone is not True
                and standalone_sender_fn is not None
            )
        )
        or not str(owner or "").strip()
        or (
            ineligible_reason
            and (
                live is not False
                or len(ineligible_reason) > 160
                or not all(
                    character.islower()
                    or character.isdigit()
                    or character in {"_", "-", "."}
                    for character in ineligible_reason
                )
            )
        )
    ):
        raise ValueError("invalid semantic exact-attempt declaration")
    proposed = SemanticExactAttemptDeclaration(
        provider=clean,
        standalone=standalone,
        live=live,
        owner=str(owner).strip(),
        planning_ineligible_reason=ineligible_reason,
        standalone_sender_fn=standalone_sender_fn,
    )
    existing = _semantic_exact_attempt_declarations.get(clean)
    if existing is not None and existing != proposed:
        conflict = SemanticExactAttemptDeclaration(
            provider=clean,
            standalone=None,
            live=None,
            owner=f"conflict:{existing.owner},{proposed.owner}",
            planning_ineligible_reason="",
            standalone_sender_fn=None,
        )
        _semantic_exact_attempt_declarations[clean] = conflict
        logger.error(
            "Conflicting semantic exact-attempt declarations for '%s': "
            "%s vs %s",
            clean,
            existing,
            proposed,
        )
        return conflict
    _semantic_exact_attempt_declarations[clean] = proposed
    return proposed


def planning_semantic_delivery_ineligibility(
    name: Any,
    *,
    adapter: Any | None = None,
) -> str | None:
    """Return an explicit no-provider-write Planning preflight classification.

    Response planes, ingress-only listeners, and unresolved delegated routes
    are not broken provider adapters. They deliberately cannot own Planning's
    exact provider-message receipt. The provider module must declare a stable
    reason and ``live=False``; absent, conflicting, or adapter-mismatched
    declarations fail closed as ordinary unsupported providers instead.
    """

    clean = _clean_provider_name(name)
    platform_registry.get(clean)
    _load_builtin_semantic_declaration(clean)
    declaration = _semantic_exact_attempt_declarations.get(clean)
    if (
        declaration is None
        or declaration.live is not False
        or not declaration.planning_ineligible_reason
    ):
        return None
    entry = platform_registry.get(clean)
    if (
        entry is not None
        and entry.live_semantic_exact_attempt not in {None, False}
    ):
        return None
    if adapter is not None:
        adapter_provider = _clean_provider_name(
            getattr(adapter, "platform", None)
        )
        if adapter_provider != clean:
            return None
    return declaration.planning_ineligible_reason


def unregister_semantic_exact_attempt_declaration(
    provider: str,
    *,
    owner: str | None = None,
) -> bool:
    """Test/plugin teardown helper with optional owner fencing."""

    clean = _clean_provider_name(provider)
    existing = _semantic_exact_attempt_declarations.get(clean)
    if existing is None:
        return False
    if owner is not None and existing.owner != owner:
        return False
    del _semantic_exact_attempt_declarations[clean]
    return True


@dataclass
class PlatformEntry:
    """Metadata and factory for a single platform adapter."""

    # Identifier used in config.yaml (e.g. "irc", "viber").
    name: str

    # Human-readable label (e.g. "IRC", "Viber").
    label: str

    # Factory callable: receives a PlatformConfig, returns an adapter instance.
    # Using a factory instead of a bare class lets plugins do custom init
    # (e.g. passing extra kwargs, wrapping in try/except).
    adapter_factory: Callable[[Any], Any]

    # Returns True when the platform's dependencies are available.
    check_fn: Callable[[], bool]

    # Optional: given a PlatformConfig, is it properly configured?
    # If None, the registry skips config validation and lets the adapter
    # fail at connect() time with a descriptive error.
    validate_config: Optional[Callable[[Any], bool]] = None

    # Optional: given a PlatformConfig, is the platform connected/enabled?
    # Used by ``GatewayConfig.get_connected_platforms()`` and setup UI status.
    # If None, falls back to ``validate_config`` or ``check_fn``.
    is_connected: Optional[Callable[[Any], bool]] = None

    # Env vars this platform needs (for ``hermes setup`` display).
    required_env: list = field(default_factory=list)

    # Hint shown when check_fn returns False.
    install_hint: str = ""

    # Optional setup function for interactive configuration.
    # Signature: () -> None (prompts user, saves env vars).
    # If None, falls back to _setup_standard_platform (needs token_var + vars)
    # or a generic "set these env vars" display.
    setup_fn: Optional[Callable[[], None]] = None

    # "builtin" or "plugin"
    source: str = "plugin"

    # Name of the plugin manifest that registered this entry (empty for
    # built-ins).  Used by ``hermes gateway setup`` to auto-enable the
    # owning plugin when the user configures its platform.
    plugin_name: str = ""

    # ── Auth env var names (for _is_user_authorized integration) ──
    # E.g. "IRC_ALLOWED_USERS" — checked for comma-separated user IDs.
    allowed_users_env: str = ""
    # E.g. "IRC_ALLOW_ALL_USERS" — if truthy, all users authorized.
    allow_all_env: str = ""

    # ── Message limits ──
    # Max message length for smart-chunking.  0 = no limit.
    max_message_length: int = 0

    # ── Privacy ──
    # If True, session descriptions redact PII (phone numbers, etc.)
    pii_safe: bool = False

    # ── Display ──
    # Emoji for CLI/gateway display (e.g. "💬")
    emoji: str = "🔌"

    # Whether this platform should appear in _UPDATE_ALLOWED_PLATFORMS
    # (allows /update command from this platform).
    allow_update_command: bool = True

    # ── LLM guidance ──
    # Platform hint injected into the system prompt (e.g. "You are on IRC.
    # Do not use markdown.").  Empty string = no hint.
    platform_hint: str = ""

    # ── Env-driven auto-configuration ──
    # Optional: read env vars, return a dict of ``PlatformConfig.extra`` fields
    # to seed when the platform is auto-enabled.  Called during
    # ``_apply_env_overrides`` BEFORE the adapter is constructed, so
    # ``gateway status`` etc. can reflect env-only configuration without
    # instantiating the adapter.  Return ``None`` (or an empty dict) to skip.
    # Signature: () -> Optional[dict[str, Any]]
    env_enablement_fn: Optional[Callable[[], Optional[dict]]] = None

    # ── YAML→env config bridge ──
    # Optional: translate this platform's ``config.yaml`` keys into env vars
    # and/or seed ``PlatformConfig.extra`` directly.  Lets a plugin own its
    # YAML config translation instead of forcing core ``gateway/config.py``
    # to know every platform's schema.
    #
    # Signature: (yaml_cfg: dict, platform_cfg: dict) -> Optional[dict]
    # Called from ``load_gateway_config()`` after the generic shared-key loop
    # and before ``_apply_env_overrides``.  Mutating ``os.environ`` is allowed
    # (use ``not os.getenv(...)`` guards to preserve env > YAML precedence);
    # any returned dict is merged into ``PlatformConfig.extra``.  Exceptions
    # are caught and logged at debug level.
    # See website/docs/developer-guide/adding-platform-adapters.md for the
    # full contract and a worked example.
    apply_yaml_config_fn: Optional[Callable[[dict, dict], Optional[dict]]] = None

    # Optional: home-channel env var name for cron/notification delivery
    # (e.g. ``"IRC_HOME_CHANNEL"``).  When set, ``cron.scheduler`` treats this
    # platform as a valid ``deliver=<name>`` target and reads the env var to
    # resolve the default chat/room ID.  Empty = no cron home-channel support.
    cron_deliver_env_var: str = ""

    # ── Standalone (out-of-process) sending ──
    # Optional: async coroutine that delivers a message without a live
    # gateway adapter.  Called by ``tools/send_message_tool._send_via_adapter``
    # when ``cron`` runs in a separate process from the gateway and the
    # in-process adapter weakref is therefore ``None``.
    #
    # Signature:
    #     async (pconfig, chat_id, message, *, thread_id=None,
    #            media_files=None, force_document=False) -> dict
    #
    # Returns ``{"success": True, "message_id": ...}`` on success or
    # ``{"error": str}`` on failure.  Plugin authors typically open an
    # ephemeral connection / acquire a fresh OAuth token, send, and close.
    # Without this hook, plugin platforms cannot serve as cron ``deliver=``
    # targets when the gateway is not co-resident with the cron process.
    standalone_sender_fn: Optional[Callable[..., Awaitable[dict]]] = None

    # Explicit opt-in for the versioned semantic-delivery attempt contract.
    # A capable standalone sender must make exactly one provider attempt,
    # accept the deterministic delivery_* kwargs, avoid hidden retries and
    # fallbacks, and return a real provider message receipt on success.
    # Unknown/third-party plugins remain fail-closed until they declare this.
    semantic_exact_attempt: bool | None = None

    # The actual standalone primitive that owns the exact-attempt contract.
    # A boolean declaration alone is never capability. The semantic dispatcher
    # calls this function (not the ordinary standalone fallback) so a plugin
    # cannot widen Planning delivery by flipping metadata only.
    standalone_semantic_exact_attempt_fn: Optional[
        Callable[..., Awaitable[dict]]
    ] = None

    # Explicit opt-in for the live gateway adapter path. This is intentionally
    # separate from ``semantic_exact_attempt``: a safe standalone sender does
    # not prove that a long-lived SDK adapter avoids retries, typing calls,
    # recipient probes, chunking, or formatting fallbacks.
    live_semantic_exact_attempt: bool | None = None


class PlatformRegistry:
    """Central registry of platform adapters.

    Thread-safe for reads (dict lookups are atomic under GIL).
    Writes happen at startup during sequential discovery.
    """

    def __init__(self) -> None:
        self._entries: dict[str, PlatformEntry] = {}
        # Deferred platform loaders: name -> zero-arg callable that imports the
        # owning plugin module (which calls register() and populates _entries).
        #
        # Why this exists: platform adapter modules import heavy, platform-
        # specific SDKs at module level (lark_oapi, microsoft_teams, discord.py,
        # slack_bolt, ...). Eagerly loading all ~20 bundled platform plugins at
        # plugin-discovery time added several seconds to *every* `hermes`
        # invocation -- including plain `hermes chat`, which never touches any
        # gateway platform. Discovery now registers a cheap deferred loader per
        # platform; the real module is imported only when a registry lookup
        # actually asks for that platform (gateway start, cron delivery,
        # `hermes setup`/`gateway status`, send_message).
        self._deferred: dict[str, Callable[[], None]] = {}

    # -- deferred loading ----------------------------------------------------

    def register_deferred(self, name: str, loader: Callable[[], None]) -> None:
        """Register a lazy loader for a platform that hasn't been imported yet.

        *loader* is a zero-arg callable that imports the owning plugin module,
        which is expected to call :meth:`register` with the real entry for
        *name*.  The loader runs at most once, the first time *name* is looked
        up (or when the full entry list is materialized).  A real entry that is
        registered directly (e.g. a built-in) takes precedence -- the deferred
        loader is then dropped.
        """
        if name in self._entries:
            # Already concretely registered; no need to defer.
            return
        self._deferred[name] = loader

    def _resolve(self, name: str) -> None:
        """Run the deferred loader for *name* if one is pending."""
        loader = self._deferred.pop(name, None)
        if loader is None:
            return
        try:
            loader()
        except Exception as e:
            logger.warning(
                "Deferred load of platform '%s' failed: %s",
                name,
                e,
                exc_info=True,
            )

    def _resolve_all(self) -> None:
        """Run every pending deferred loader.

        Used by the iterate-all accessors (``all_entries``/``plugin_entries``),
        which are only called by paths that genuinely need every adapter:
        gateway startup, ``hermes setup``/``gateway status``, channel
        directory.  CLI chat never iterates the full set.
        """
        if not self._deferred:
            return
        # Snapshot keys -- loaders mutate _deferred as they resolve.
        for name in list(self._deferred):
            self._resolve(name)

    def register(self, entry: PlatformEntry) -> None:
        """Register a platform adapter entry.

        If an entry with the same name exists, it is replaced (last writer
        wins -- this lets plugins override built-in adapters if desired).
        """
        # A concrete registration supersedes any pending deferred loader.
        self._deferred.pop(entry.name, None)
        if entry.name in self._entries:
            prev = self._entries[entry.name]
            logger.info(
                "Platform '%s' re-registered (was %s, now %s)",
                entry.name,
                prev.source,
                entry.source,
            )
        self._entries[entry.name] = entry
        logger.debug("Registered platform adapter: %s (%s)", entry.name, entry.source)

    def unregister(self, name: str) -> bool:
        """Remove a platform entry.  Returns True if it existed."""
        self._deferred.pop(name, None)
        return self._entries.pop(name, None) is not None

    def get(self, name: str) -> Optional[PlatformEntry]:
        """Look up a platform entry by name."""
        if name not in self._entries:
            self._resolve(name)
        return self._entries.get(name)

    def all_entries(self) -> list[PlatformEntry]:
        """Return all registered platform entries."""
        self._resolve_all()
        return list(self._entries.values())

    def plugin_entries(self) -> list[PlatformEntry]:
        """Return only plugin-registered platform entries."""
        self._resolve_all()
        return [e for e in self._entries.values() if e.source == "plugin"]

    def is_registered(self, name: str) -> bool:
        # A deferred (not-yet-imported) platform still counts as registered --
        # the loader will materialize it on first real use.  This keeps cheap
        # membership checks (toolset resolution, webhook deliver-target checks)
        # from triggering a heavy import.
        return name in self._entries or name in self._deferred

    def create_adapter(self, name: str, config: Any) -> Optional[Any]:
        """Create an adapter instance for the given platform name.

        Returns None if:
        - No entry registered for *name*
        - check_fn() returns False (missing deps)
        - validate_config() returns False (misconfigured)
        - The factory raises an exception
        """
        if name not in self._entries:
            self._resolve(name)
        entry = self._entries.get(name)
        if entry is None:
            return None

        if not entry.check_fn():
            hint = f" ({entry.install_hint})" if entry.install_hint else ""
            logger.warning(
                "Platform '%s' requirements not met%s",
                entry.label,
                hint,
            )
            return None

        if entry.validate_config is not None:
            try:
                if not entry.validate_config(config):
                    logger.warning(
                        "Platform '%s' config validation failed",
                        entry.label,
                    )
                    return None
            except Exception as e:
                logger.warning(
                    "Platform '%s' config validation error: %s",
                    entry.label,
                    e,
                )
                return None

        try:
            adapter = entry.adapter_factory(config)
            return adapter
        except Exception as e:
            logger.error(
                "Failed to create adapter for platform '%s': %s",
                entry.label,
                e,
                exc_info=True,
            )
            return None


# Module-level singleton
platform_registry = PlatformRegistry()


def supports_semantic_exact_attempt(name: str) -> bool:
    """Return whether one standalone route owns the exact-attempt contract."""

    clean = _clean_provider_name(name)
    entry = platform_registry.get(clean)
    _load_builtin_semantic_declaration(clean)
    declaration = _semantic_exact_attempt_declarations.get(clean)
    values = [
        value
        for value in (
            entry.semantic_exact_attempt if entry is not None else None,
            declaration.standalone if declaration is not None else None,
        )
        if value is not None
    ]
    if not values or not all(value is True for value in values):
        return False
    entry_claims = (
        entry is not None and entry.semantic_exact_attempt is True
    )
    declaration_claims = (
        declaration is not None and declaration.standalone is True
    )
    return bool(
        (
            not entry_claims
            or callable(entry.standalone_semantic_exact_attempt_fn)
        )
        and (
            not declaration_claims
            or callable(declaration.standalone_sender_fn)
        )
        and (entry_claims or declaration_claims)
    )


def enumerate_standalone_semantic_exact_attempt_conformance(
) -> tuple[StandaloneSemanticExactAttemptConformance, ...]:
    """Enumerate declaration and callable ownership for every standalone route."""

    entries = {
        entry.name: entry for entry in platform_registry.all_entries()
    }
    providers = set(entries) | set(_semantic_exact_attempt_declarations)
    rows: list[StandaloneSemanticExactAttemptConformance] = []
    for provider in sorted(providers):
        entry = entries.get(provider)
        _load_builtin_semantic_declaration(provider)
        declared = _semantic_exact_attempt_declarations.get(provider)
        declarations = [
            value
            for value in (
                entry.semantic_exact_attempt if entry is not None else None,
                declared.standalone if declared is not None else None,
            )
            if value is not None
        ]
        conflict = bool(
            declarations
            and any(value != declarations[0] for value in declarations[1:])
        )
        declaration = (
            None
            if conflict or not declarations
            else bool(declarations[0])
        )
        entry_claims = bool(
            entry is not None and entry.semantic_exact_attempt is True
        )
        declaration_claims = bool(
            declared is not None and declared.standalone is True
        )
        exact_sender = bool(
            (
                not entry_claims
                or callable(entry.standalone_semantic_exact_attempt_fn)
            )
            and (
                not declaration_claims
                or callable(declared.standalone_sender_fn)
            )
            and (entry_claims or declaration_claims)
        )
        supported = bool(
            declaration is True and exact_sender and not conflict
        )
        if conflict:
            reason = "conflicting_declarations"
        elif declaration is None:
            reason = "declaration_missing"
        elif declaration is True and not exact_sender:
            reason = "exact_sender_missing"
        elif supported:
            reason = "supported"
        else:
            reason = "explicitly_unsupported"
        owner = (
            declared.owner
            if declared is not None
            else (
                entry.plugin_name
                if entry is not None and entry.plugin_name
                else (entry.source if entry is not None else "")
            )
        )
        rows.append(
            StandaloneSemanticExactAttemptConformance(
                provider=provider,
                registered=entry is not None or declared is not None,
                declaration=declaration,
                exact_sender=exact_sender,
                supported=supported,
                conformant=bool(
                    not conflict
                    and declaration is not None
                    and (declaration is False or exact_sender)
                ),
                owner=owner,
                reason=reason,
            )
        )
    return tuple(rows)


def supports_live_semantic_exact_attempt(
    name: str,
    *,
    adapter: Any | None = None,
) -> bool:
    """Return whether the live adapter owns the exact-attempt contract."""

    clean = _clean_provider_name(name)
    entry = platform_registry.get(clean)
    _load_builtin_semantic_declaration(clean)
    declaration = _semantic_exact_attempt_declarations.get(clean)
    values = [
        value
        for value in (
            entry.live_semantic_exact_attempt if entry is not None else None,
            declaration.live if declaration is not None else None,
        )
        if value is not None
    ]
    declared = bool(values and all(value is True for value in values))
    if not declared or adapter is None:
        return declared
    from gateway.semantic_exact_attempt import (
        owns_live_semantic_exact_attempt,
    )

    adapter_provider = _clean_provider_name(
        getattr(adapter, "platform", None)
    )
    return bool(
        adapter_provider == clean
        and owns_live_semantic_exact_attempt(adapter)
    )


def enumerate_live_semantic_exact_attempt_conformance(
    bound_adapters: Mapping[Any, Any] | None = None,
) -> tuple[LiveSemanticExactAttemptConformance, ...]:
    """Enumerate every registry/declaration/runtime outbound provider.

    This is an audit surface, not a capability inference shortcut. Explicit
    ``False`` is conformant with a bound adapter only when the provider module
    also owns an explicit Planning-ineligible preflight classification.
    Otherwise a bound unsupported adapter, a missing/conflicting declaration,
    or one bound account without the exact primitive is non-conformant.
    """

    entries = {entry.name: entry for entry in platform_registry.all_entries()}
    bound_by_provider: dict[str, list[Any]] = {}

    def iter_adapters(
        value: Any,
        *,
        provider_hint: str = "",
    ):
        if isinstance(value, Mapping):
            for key, nested in value.items():
                yield from iter_adapters(
                    nested,
                    provider_hint=_clean_provider_name(key),
                )
            return
        if isinstance(value, (list, tuple, set, frozenset)):
            for nested in value:
                yield from iter_adapters(
                    nested,
                    provider_hint=provider_hint,
                )
            return
        if value is not None:
            yield provider_hint, value

    for key, adapter in iter_adapters(bound_adapters or {}):
        provider = _clean_provider_name(
            getattr(adapter, "platform", None) or key
        )
        if provider:
            bound_by_provider.setdefault(provider, []).append(adapter)

    providers = (
        set(entries)
        | set(_semantic_exact_attempt_declarations)
        | set(bound_by_provider)
    )
    rows: list[LiveSemanticExactAttemptConformance] = []
    for provider in sorted(providers):
        entry = entries.get(provider)
        _load_builtin_semantic_declaration(provider)
        declared = _semantic_exact_attempt_declarations.get(provider)
        declarations = [
            value
            for value in (
                entry.live_semantic_exact_attempt if entry is not None else None,
                declared.live if declared is not None else None,
            )
            if value is not None
        ]
        conflict = bool(
            declarations
            and any(value != declarations[0] for value in declarations[1:])
        )
        declaration = (
            None
            if conflict or not declarations
            else bool(declarations[0])
        )
        planning_ineligible_reason = (
            declared.planning_ineligible_reason
            if (
                declared is not None
                and declaration is False
                and not conflict
            )
            else ""
        )
        adapters = bound_by_provider.get(provider, [])
        outbound_send = bool(adapters) and all(
            callable(getattr(adapter, "send", None))
            for adapter in adapters
        )
        from gateway.semantic_exact_attempt import (
            owns_live_semantic_exact_attempt,
        )

        exact_attempt_method = bool(adapters) and all(
            owns_live_semantic_exact_attempt(adapter)
            for adapter in adapters
        )
        if conflict:
            reason = "conflicting_declarations"
        elif declaration is None:
            reason = "declaration_missing"
        elif declaration is False and planning_ineligible_reason:
            reason = "planning_ineligible"
        elif declaration is False and adapters:
            reason = "bound_adapter_unsupported"
        elif declaration is False:
            reason = "explicitly_unsupported"
        elif adapters and not outbound_send:
            reason = "bound_adapter_send_missing"
        elif adapters and not exact_attempt_method:
            reason = "bound_adapter_exact_method_missing"
        else:
            reason = "supported"
        conformant = bool(
            declaration is not None
            and not conflict
            and (
                (
                    declaration is True
                    and (
                        not adapters
                        or (
                            outbound_send
                            and exact_attempt_method
                        )
                    )
                )
                or (
                    declaration is False
                    and bool(planning_ineligible_reason)
                )
            )
        )
        supported = bool(declaration is True and conformant)
        owners = [
            owner
            for owner in (
                entry.plugin_name or entry.source if entry is not None else "",
                declared.owner if declared is not None else "",
            )
            if owner
        ]
        rows.append(
            LiveSemanticExactAttemptConformance(
                provider=provider,
                registered=entry is not None or declared is not None,
                bound=bool(adapters),
                outbound_send=outbound_send,
                exact_attempt_method=exact_attempt_method,
                declaration=declaration,
                supported=supported,
                conformant=conformant,
                owner=",".join(owners),
                reason=reason,
                planning_ineligible_reason=planning_ineligible_reason,
            )
        )
    return tuple(rows)
