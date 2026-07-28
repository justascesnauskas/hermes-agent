"""Fresh-process harness for multiplexed gateway delivery recovery."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
from types import SimpleNamespace
from typing import Any


PROVIDER = "profile_retry_test"
ACCOUNT = "shared-looking-account"
DELIVERY_ID = "secondary-profile-retry"
DELIVERY_GROUP = "secondary-profile-retry-group"
TARGET = f"{PROVIDER}:secondary-chat"
MESSAGE = "Recover only through the secondary profile adapter."


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _homes(root: Path) -> tuple[Path, Path]:
    default_home = root / "hermes-home"
    secondary_home = default_home / "profiles" / "secondary"
    return default_home, secondary_home


def _semantic_path(home: Path) -> Path:
    return home / "state" / "semantic-delivery" / "ledger.sqlite3"


def _ack_path(home: Path) -> Path:
    return home / "state" / "planning-preview-ack" / "outbox.sqlite3"


def _configure(root: Path) -> tuple[Path, Path]:
    default_home, secondary_home = _homes(root)
    secondary_home.mkdir(parents=True, exist_ok=True)
    (default_home / "config.yaml").write_text(
        "\n".join(
            (
                "gateway:",
                "  multiplex_profiles: true",
                "cron:",
                "  provider: chronos",
                "  chronos:",
                "    portal_url: https://portal.invalid",
                "    callback_url: https://agent.invalid/api/cron/fire",
                "",
            )
        ),
        encoding="utf-8",
    )
    (default_home / ".env").write_text(
        "\n".join(
            (
                "AGENT_OPS_RUNNER_TOKEN=default-secret-must-not-leak",
                "AGENT_OPS_RUNNER_ID=default-runner",
                "AGENT_OPS_API_URL=https://default-hub.invalid",
                "",
            )
        ),
        encoding="utf-8",
    )
    (secondary_home / ".env").write_text(
        "\n".join(
            (
                "AGENT_OPS_RUNNER_TOKEN=secondary-secret",
                "AGENT_OPS_RUNNER_ID=secondary-runner",
                "AGENT_OPS_API_URL=https://secondary-hub.invalid",
                "",
            )
        ),
        encoding="utf-8",
    )
    return default_home, secondary_home


def _register_provider() -> None:
    from gateway.platform_registry import PlatformEntry, platform_registry

    if not platform_registry.is_registered(PROVIDER):
        platform_registry.register(
            PlatformEntry(
                name=PROVIDER,
                label="Profile retry process test",
                adapter_factory=lambda config: None,
                check_fn=lambda: True,
                semantic_exact_attempt=False,
                live_semantic_exact_attempt=True,
            )
        )


def _success(message_id: str) -> Any:
    return SimpleNamespace(
        success=True,
        message_id=message_id,
        continuation_message_ids=(),
        error=None,
        raw_response={},
        retryable=False,
        retry_after=None,
    )


def _adapter(owner: str, observations: Path | None = None) -> Any:
    from gateway.semantic_exact_attempt import (
        LiveSemanticExactAttemptCapability,
    )

    async def send_semantic_exact_attempt(self, request):
        if observations is not None:
            from agent.secret_scope import get_secret
            from hermes_constants import get_hermes_home

            calls = _read_json(observations, [])
            calls.append(
                {
                    "owner": owner,
                    "profileHome": str(get_hermes_home().resolve()),
                    "runnerToken": get_secret("AGENT_OPS_RUNNER_TOKEN"),
                    "deliveryId": request.delivery_id,
                }
            )
            _write_json(observations, calls)
        return _success(f"{owner}-provider-message")

    adapter_type = type(
        f"{owner.title()}ProfileRetryAdapter",
        (),
        {
            "platform": PROVIDER,
            "SEMANTIC_EXACT_ATTEMPT_CAPABILITY": (
                LiveSemanticExactAttemptCapability(
                    provider=PROVIDER,
                    contract="hermes-live-semantic-exact-attempt/1",
                    segmentation_version="profile-retry-process-v1",
                    max_logical_units=4096,
                    length_semantics="unicode_codepoints",
                    wire_encoding="profile-retry-process-json-v1",
                )
            ),
            "send_semantic_exact_attempt": send_semantic_exact_attempt,
        },
    )
    instance = adapter_type()
    instance.config = SimpleNamespace(
        extra={"gateway_account_id": ACCOUNT}
    )
    return instance


def _request(label: str, *, complete: bool) -> dict[str, Any]:
    preview_id = f"preview-{label}"
    message_id = f"inbound-{label}"
    request: dict[str, Any] = {
        "schemaVersion": "planning.preview-ack-request.v1",
        "threadId": f"planning-thread-{label}",
        "previewResultId": preview_id,
        "idempotencyKey": f"preview-review-{label}",
        "expectedPreviewHash": "sha256:" + "a" * 64,
        "offset": 0,
        "count": 1,
        "pageDigest": "sha256:" + "b" * 64,
        "origin": {
            "schemaVersion": "1.0",
            "provider": PROVIDER,
            "gatewayInstanceId": "secondary-runner",
            "gatewayAccountId": ACCOUNT,
            "chatId": "secondary-chat",
            "threadId": "secondary-thread",
            "messageId": message_id,
            "senderId": "secondary-user",
            "chatType": "thread",
            "sourceTimestamp": "2026-07-28T09:00:00Z",
            "providerEventId": f"provider-event-{label}",
        },
    }
    proof_base = {
        "schemaVersion": "planning.preview-delivery-proof.v1",
        "deliveryNonce": f"delivery-nonce-{label}",
        "provider": PROVIDER,
        "gatewayInstanceId": "secondary-runner",
        "gatewayAccountId": ACCOUNT,
        "chatId": "secondary-chat",
        "previewResultId": preview_id,
        "previewResultHash": "sha256:" + "a" * 64,
        "offset": 0,
        "count": 1,
        "pageDigest": "sha256:" + "b" * 64,
    }
    if complete:
        request["deliveryProof"] = {
            **proof_base,
            "providerMessageId": f"provider-message-{label}",
            "providerMessageIds": [f"provider-message-{label}"],
            "deliveredAt": "2026-07-28T09:01:00Z",
            "deliveryPayloadDigest": "sha256:" + "c" * 64,
            "deliveryContentDigest": "sha256:" + "d" * 64,
        }
    else:
        request["deliveryProofBase"] = proof_base
        request["deliveryPayloadDigest"] = "sha256:" + "c" * 64
        request["deliveryContentDigest"] = "sha256:" + "d" * 64
    return request


def _stage(root: Path) -> None:
    default_home, secondary_home = _configure(root)
    os.environ["HERMES_HOME"] = str(default_home)
    _register_provider()

    from gateway.semantic_exact_attempt import (
        live_semantic_exact_attempt_encoding_contract,
    )
    from hermes_cli.planning_preview_ack_outbox import (
        PLANNING_PREVIEW_ACK_COMPLETION_CONTRACT,
        enqueue_preview_ack,
        stage_preview_ack_bridge,
    )
    from hermes_cli.semantic_delivery import (
        SEMANTIC_DELIVERY_CONTRACT,
        semantic_delivery_scope_id,
        stage_semantic_delivery_retry,
    )

    ledger_path = _semantic_path(secondary_home)
    ack_path = _ack_path(secondary_home)
    direct = enqueue_preview_ack(
        _request("normal-provider-confirmed", complete=True),
        path=ack_path,
    )
    scope_id = semantic_delivery_scope_id(ledger_path=ledger_path)
    bridge = stage_preview_ack_bridge(
        [_request("retry-completion", complete=False)],
        semantic_delivery_ids=[DELIVERY_ID],
        semantic_scope_id=scope_id,
        provider=PROVIDER,
        gateway_account_id=ACCOUNT,
        path=ack_path,
    )
    adapter = _adapter("staging")
    staged = stage_semantic_delivery_retry(
        delivery_id=DELIVERY_ID,
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target=TARGET,
        message=MESSAGE,
        gateway_account_id=ACCOUNT,
        adapter=adapter,
        encoding_contract=live_semantic_exact_attempt_encoding_contract(
            adapter
        ),
        chat_id="secondary-chat",
        delivery_group_id=DELIVERY_GROUP,
        unit_index=0,
        unit_count=1,
        completion_contract=PLANNING_PREVIEW_ACK_COMPLETION_CONTRACT,
        completion_ref=str(bridge["bridgeId"]),
        expected_scope_id=scope_id,
        initial_delay_seconds=0,
        ledger_path=ledger_path,
    )
    print(
        json.dumps(
            {
                "bridgeId": bridge["bridgeId"],
                "defaultHome": str(default_home.resolve()),
                "directAckId": direct["ackId"],
                "groupState": staged["group_state"],
                "secondaryHome": str(secondary_home.resolve()),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _successful_ack_count(path: Path) -> int:
    if not path.exists():
        return 0
    with sqlite3.connect(path) as connection:
        return int(
            connection.execute(
                "SELECT COUNT(*) FROM planning_preview_ack_outbox "
                "WHERE state='succeeded'"
            ).fetchone()[0]
        )


def _bridge_state(path: Path) -> str:
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT state FROM planning_preview_ack_bridge "
            "ORDER BY created_at LIMIT 1"
        ).fetchone()
    return str(row[0]) if row else ""


async def _recover_async(root: Path) -> dict[str, Any]:
    default_home, secondary_home = _configure(root)
    os.environ["HERMES_HOME"] = str(default_home)
    _register_provider()

    from agent.secret_scope import (
        set_multiplex_active,
        UnscopedSecretError,
    )
    from cron.scheduler_provider import resolve_cron_scheduler
    from gateway.profile_delivery_supervisor import (
        ProfileDeliverySupervisor,
    )
    from hermes_constants import get_hermes_home
    from hermes_cli.dev_hub_planning_v2 import PlanningV2Client
    from hermes_cli.semantic_delivery import (
        semantic_delivery_retry_status,
    )
    from plugins.cron_providers.chronos import ChronosCronScheduler

    ChronosCronScheduler._have_nous_token = lambda self: True
    cron_provider = resolve_cron_scheduler()
    if cron_provider.name != "chronos":
        raise RuntimeError(
            f"expected external Chronos mode, got {cron_provider.name!r}"
        )

    provider_observations = root / "provider-calls.json"
    ack_observations = root / "ack-calls.json"
    default_adapter = _adapter("default", provider_observations)
    secondary_adapter = _adapter("secondary", provider_observations)
    runner = SimpleNamespace(
        config=SimpleNamespace(multiplex_profiles=True),
        adapters={PROVIDER: default_adapter},
        _profile_adapters={
            "secondary": {PROVIDER: secondary_adapter},
        },
    )
    os.environ["AGENT_OPS_RUNNER_TOKEN"] = (
        "process-secret-must-not-leak"
    )
    os.environ["AGENT_OPS_RUNNER_ID"] = (
        "process-runner-must-not-leak"
    )
    os.environ["AGENT_OPS_API_URL"] = (
        "https://process-hub-must-not-leak.invalid"
    )
    set_multiplex_active(True)
    try:
        PlanningV2Client()
    except UnscopedSecretError:
        unscoped_planning_failed_closed = True
    else:
        raise RuntimeError(
            "unscoped multiplex Planning client read process identity"
        )

    def deliver_ack(request: dict[str, Any]) -> None:
        client = PlanningV2Client()
        calls = _read_json(ack_observations, [])
        calls.append(
            {
                "baseUrl": client.base_url,
                "idempotencyKey": request["idempotencyKey"],
                "profileHome": str(get_hermes_home().resolve()),
                "runnerId": client.runner_id,
                "runnerToken": client.token,
            }
        )
        _write_json(ack_observations, calls)

    supervisor = ProfileDeliverySupervisor(
        runner,
        poll_seconds=0.02,
        ack_deliver=deliver_ack,
    )
    if not supervisor.start():
        raise RuntimeError("profile delivery supervisor did not start")
    deadline = time.monotonic() + 10.0
    try:
        while time.monotonic() < deadline:
            status = semantic_delivery_retry_status(
                delivery_id=DELIVERY_ID,
                ledger_path=_semantic_path(secondary_home),
            )
            if (
                status is not None
                and status["state"] == "retired"
                and _successful_ack_count(_ack_path(secondary_home)) == 2
            ):
                break
            await asyncio.sleep(0.02)
        else:
            raise RuntimeError(
                "secondary profile delivery recovery did not converge"
            )
    finally:
        await supervisor.stop(timeout=2.0)

    return {
        "ackCalls": _read_json(ack_observations, []),
        "bridgeState": _bridge_state(_ack_path(secondary_home)),
        "chronosProvider": cron_provider.name,
        "defaultAckSucceeded": _successful_ack_count(
            _ack_path(default_home)
        ),
        "providerCalls": _read_json(provider_observations, []),
        "secondaryAckSucceeded": _successful_ack_count(
            _ack_path(secondary_home)
        ),
        "semanticState": semantic_delivery_retry_status(
            delivery_id=DELIVERY_ID,
            ledger_path=_semantic_path(secondary_home),
        ),
        "unscopedPlanningFailedClosed": (
            unscoped_planning_failed_closed
        ),
    }


def _recover(root: Path) -> None:
    print(
        json.dumps(
            asyncio.run(_recover_async(root)),
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("command", choices=("stage", "recover"))
    arguments = parser.parse_args()
    root = arguments.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    if arguments.command == "stage":
        _stage(root)
    else:
        _recover(root)


if __name__ == "__main__":
    main()
