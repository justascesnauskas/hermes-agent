"""Fresh-process child for semantic delivery crash-boundary tests."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from typing import Any


CRASH_EXIT = 86
MESSAGE = "One exact Planning notification."
PREVIEW_SESSION = "planning-preview-process-session"
PREVIEW_GENERATION = 17
PREVIEW_TARGET = "matrix:preview-target-v1:process"
PREVIEW_ACCOUNT = "matrix-preview-process-account"
PREVIEW_NONCE = "preview-delivery-process-stable"
PREVIEW_PROVIDER_MESSAGE_ID = (
    "  matrix-" + ("訊" * 2_048) + "-event  "
)
PREVIEW_CONTENT = (
    "## Implementation plan\n\n"
    "### 1. Preserve the original operation\n\n"
    "Resume after process loss without creating a duplicate."
)
LOCK_DELIVERY_ID = "delivery-process-cross-process-lock"
LOCK_TARGET = "slack:process-lock-chat"
STATUS_ACCOUNT = "status-account"
STATUS_TARGET = "slack:process-status-chat"
STATUS_NATIVE_TARGET = "matrix:process-status-room"


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _provider_effect(
    state_root: Path,
    *,
    provider: str,
    target: str,
    delivery_id: str,
) -> dict[str, Any]:
    from hermes_cli.semantic_delivery import provider_delivery_token

    path = state_root / f"{provider}-provider.json"
    state = _read_json(
        path,
        {"businessEffects": 0, "calls": [], "receipts": {}},
    )
    token = provider_delivery_token(
        delivery_id,
        provider=provider,
        target=target,
        unit=0,
    )
    state["calls"].append({"pid": os.getpid(), "token": token})
    receipt = state["receipts"].get(token)
    if receipt is None:
        state["businessEffects"] += 1
        receipt = {
            "message_id": f"{provider}-message-{state['businessEffects']}"
        }
        state["receipts"][token] = receipt
    _write_json(path, state)
    return {
        "success": True,
        "message_id": receipt["message_id"],
        "message_ids": [receipt["message_id"]],
    }


def _semantic_send(
    state_root: Path,
    *,
    provider: str,
    crash_after_provider: bool,
) -> None:
    from hermes_cli.semantic_delivery import (
        SEMANTIC_DELIVERY_CONTRACT,
        semantic_send,
    )

    delivery_id = f"delivery-process-{provider}"
    target = f"{provider}:process-chat"

    def send(_payload: dict[str, Any]) -> dict[str, Any]:
        return _provider_effect(
            state_root,
            provider=provider,
            target=target,
            delivery_id=delivery_id,
        )

    def after_provider(_result: dict[str, Any]) -> None:
        if crash_after_provider:
            os._exit(CRASH_EXIT)

    result = semantic_send(
        delivery_id=delivery_id,
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target=target,
        message=MESSAGE,
        send=send,
        after_provider_result=after_provider,
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


def _lock_provider_send(state_root: Path) -> dict[str, Any]:
    return _provider_effect(
        state_root,
        provider="slack",
        target=LOCK_TARGET,
        delivery_id=LOCK_DELIVERY_ID,
    )


def _semantic_lock_owner(state_root: Path) -> None:
    """Hold a real cross-process claim until the parent releases stdin."""

    from hermes_cli.semantic_delivery import (
        SEMANTIC_DELIVERY_CONTRACT,
        begin_semantic_delivery,
        finish_semantic_delivery,
    )

    attempt = begin_semantic_delivery(
        delivery_id=LOCK_DELIVERY_ID,
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target=LOCK_TARGET,
        message=MESSAGE,
    )
    if attempt.action != "send":
        raise RuntimeError(f"owner did not claim delivery: {attempt.action}")
    print(
        json.dumps(
            {"action": attempt.action, "pid": os.getpid()},
            sort_keys=True,
            separators=(",", ":"),
        ),
        flush=True,
    )
    release = sys.stdin.readline()
    if release.strip() != "release":
        attempt.release()
        raise RuntimeError("parent did not release delivery owner")
    result = finish_semantic_delivery(
        attempt,
        _lock_provider_send(state_root),
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")), flush=True)


def _semantic_lock_send(state_root: Path) -> None:
    from hermes_cli.semantic_delivery import (
        SEMANTIC_DELIVERY_CONTRACT,
        semantic_send,
    )

    result = semantic_send(
        delivery_id=LOCK_DELIVERY_ID,
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target=LOCK_TARGET,
        message=MESSAGE,
        send=lambda _payload: _lock_provider_send(state_root),
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


def _semantic_lock_status() -> None:
    from hermes_cli.semantic_delivery import (
        SEMANTIC_DELIVERY_CONTRACT,
        semantic_delivery_scope_id,
        semantic_delivery_status,
    )

    scope = semantic_delivery_scope_id()
    result = semantic_delivery_status(
        delivery_id=LOCK_DELIVERY_ID,
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        expected_scope_id=scope,
        expected_provider="slack",
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


def _status_settle_then_crash(kind: str) -> None:
    from hermes_cli.semantic_delivery import (
        SEMANTIC_DELIVERY_CONTRACT,
        begin_semantic_delivery,
        finish_semantic_delivery,
        semantic_delivery_scope_id,
    )

    scope = semantic_delivery_scope_id()
    provider = "matrix" if kind == "native-sending" else "slack"
    target = STATUS_NATIVE_TARGET if provider == "matrix" else STATUS_TARGET
    delivery_id = f"delivery-process-status-{kind}"
    attempt = begin_semantic_delivery(
        delivery_id=delivery_id,
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        target=target,
        message=f"status boundary {kind}",
        expected_scope_id=scope,
        gateway_account_id=STATUS_ACCOUNT,
    )
    if attempt.action != "send":
        raise RuntimeError(f"status setup did not claim: {attempt.action}")
    if kind == "native-sending":
        os._exit(CRASH_EXIT)
    provider_result = {
        "retryable": {
            "provider_write_attempted": False,
            "provider_retryable": True,
        },
        "rejected": {
            "provider_write_attempted": False,
            "provider_retryable": False,
        },
        "delivered": {
            "success": True,
            "message_id": "status-delivered-message",
        },
    }[kind]
    result = finish_semantic_delivery(attempt, provider_result)
    expected = {
        "retryable": "retryable",
        "rejected": "rejected",
        "delivered": "delivered",
    }[kind]
    if result.get("outcome") != expected:
        raise RuntimeError(f"unexpected local settlement: {result}")
    os._exit(CRASH_EXIT)


def _status_read(kind: str) -> None:
    from hermes_cli.semantic_delivery import (
        SEMANTIC_DELIVERY_CONTRACT,
        semantic_delivery_scope_id,
        semantic_delivery_status,
    )

    provider = "matrix" if kind == "native-sending" else "slack"
    result = semantic_delivery_status(
        delivery_id=f"delivery-process-status-{kind}",
        contract_version=SEMANTIC_DELIVERY_CONTRACT,
        expected_scope_id=semantic_delivery_scope_id(),
        expected_provider=provider,
        gateway_account_id=STATUS_ACCOUNT,
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


def _status_scope_only_crash() -> None:
    from hermes_cli.semantic_delivery import semantic_delivery_scope_id

    semantic_delivery_scope_id()
    os._exit(CRASH_EXIT)


def _preview_payload() -> tuple[dict[str, Any], str, str]:
    payload = {
        "schemaVersion": "planning.preview-delivery-payload.v1",
        "threadId": "planning-thread-process",
        "previewResultId": "preview-process",
        "offset": 0,
        "count": 1,
        "tasks": [
            {
                "stableTaskId": "task-process-1",
                "summary": "Preserve the original operation",
            }
        ],
    }
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    payload_digest = "sha256:" + hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()
    content_digest = "sha256:" + hashlib.sha256(
        PREVIEW_CONTENT.encode("utf-8")
    ).hexdigest()
    return payload, payload_digest, content_digest


def _register_preview(state_root: Path) -> None:
    from hermes_cli.planning_preview_delivery import (
        bind_preview_delivery_generation,
        register_preview_delivery_intent,
        reset_preview_delivery_generation,
    )

    payload, payload_digest, content_digest = _preview_payload()

    def acknowledge(receipt: Any) -> dict[str, Any]:
        ack_path = state_root / "preview-acks.json"
        acknowledgements = _read_json(ack_path, [])
        acknowledgements.append(
            {
                "deliveryNonce": receipt.delivery_nonce,
                "providerMessageIds": list(receipt.provider_message_ids),
                "deliveryPayloadDigest": receipt.delivery_payload_digest,
                "deliveryContentDigest": receipt.delivery_content_digest,
            }
        )
        _write_json(ack_path, acknowledgements)
        return {"replayed": False}

    token = bind_preview_delivery_generation(
        PREVIEW_SESSION,
        PREVIEW_GENERATION,
    )
    try:
        registered = register_preview_delivery_intent(
            thread_id="planning-thread-process",
            preview_result_id="preview-process",
            preview_result_hash="sha256:" + ("a" * 64),
            offset=0,
            count=1,
            page_digest="sha256:" + ("b" * 64),
            delivery_payload=payload,
            delivery_payload_digest=payload_digest,
            delivery_content=PREVIEW_CONTENT,
            delivery_content_digest=content_digest,
            delivery_nonce=PREVIEW_NONCE,
            delivery_target=PREVIEW_TARGET,
            acknowledge=acknowledge,
            acknowledgement_request={
                "schemaVersion": "planning.preview-ack-request.v1",
                "threadId": "planning-thread-process",
                "previewResultId": "preview-process",
                "idempotencyKey": "preview-process-ack-stable",
                "expectedPreviewHash": "sha256:" + ("a" * 64),
                "offset": 0,
                "count": 1,
                "pageDigest": "sha256:" + ("b" * 64),
                "origin": {
                    "schemaVersion": "1.0",
                    "provider": "matrix",
                    "gatewayInstanceId": "runner-process",
                    "gatewayAccountId": PREVIEW_ACCOUNT,
                    "chatId": "preview-target-v1:process",
                    "threadId": None,
                    "messageId": "preview-inbound-message",
                    "senderId": "preview-user",
                    "chatType": "direct",
                    "sourceTimestamp": "2026-07-28T07:59:00Z",
                    "providerEventId": "preview-inbound-event",
                },
                "deliveryProofBase": {
                    "schemaVersion": (
                        "planning.preview-delivery-proof.v1"
                    ),
                    "deliveryNonce": PREVIEW_NONCE,
                    "provider": "matrix",
                    "gatewayInstanceId": "runner-process",
                    "gatewayAccountId": PREVIEW_ACCOUNT,
                    "chatId": "preview-target-v1:process",
                    "previewResultId": "preview-process",
                    "previewResultHash": "sha256:" + ("a" * 64),
                    "offset": 0,
                    "count": 1,
                    "pageDigest": "sha256:" + ("b" * 64),
                },
            },
        )
    finally:
        reset_preview_delivery_generation(token)
    if not registered:
        raise RuntimeError("preview intent was not registered")


def _preview_record_then_crash(state_root: Path) -> None:
    from hermes_cli.planning_preview_delivery import (
        claim_preview_delivery,
        prepare_preview_delivery_content,
        record_preview_delivery_result,
    )

    _register_preview(state_root)
    delivered_content = prepare_preview_delivery_content(
        PREVIEW_SESSION,
        PREVIEW_GENERATION,
        "The exact plan is attached below.",
    )
    claim = claim_preview_delivery(
        PREVIEW_SESSION,
        PREVIEW_GENERATION,
        delivered_content=delivered_content,
        provider="matrix",
        target=PREVIEW_TARGET,
        gateway_account_id=PREVIEW_ACCOUNT,
    )
    if claim.action != "send":
        raise RuntimeError(f"unexpected preview claim: {claim.action}")

    provider_state_path = state_root / "preview-provider.json"
    provider_state = _read_json(
        provider_state_path,
        {"businessEffects": 0, "calls": 0},
    )
    provider_state["businessEffects"] += 1
    provider_state["calls"] += 1
    _write_json(provider_state_path, provider_state)

    digest = "sha256:" + hashlib.sha256(
        delivered_content.encode("utf-8")
    ).hexdigest()
    result = SimpleNamespace(
        success=True,
        message_id=PREVIEW_PROVIDER_MESSAGE_ID,
        continuation_message_ids=(PREVIEW_PROVIDER_MESSAGE_ID,),
        delivered_content_digest=digest,
        delivered_content_complete=True,
        error=None,
        raw_response=None,
    )
    persisted = record_preview_delivery_result(
        PREVIEW_SESSION,
        PREVIEW_GENERATION,
        delivered_content=delivered_content,
        result=result,
    )
    if not isinstance(persisted, dict) or persisted.get("outcome") != "delivered":
        raise RuntimeError("preview provider receipt did not persist")
    os._exit(CRASH_EXIT)


def _preview_ack_replay(state_root: Path) -> None:
    from hermes_cli.planning_preview_delivery import (
        claim_preview_delivery,
        complete_preview_delivery,
        prepare_preview_delivery_content,
        replayed_preview_send_result,
    )

    _register_preview(state_root)
    delivered_content = prepare_preview_delivery_content(
        PREVIEW_SESSION,
        PREVIEW_GENERATION,
        "The exact plan is attached below.",
    )
    claim = claim_preview_delivery(
        PREVIEW_SESSION,
        PREVIEW_GENERATION,
        delivered_content=delivered_content,
        provider="matrix",
        target=PREVIEW_TARGET,
        gateway_account_id=PREVIEW_ACCOUNT,
    )
    if claim.action != "delivered":
        raise RuntimeError(f"unexpected preview replay: {claim.action}")
    result = replayed_preview_send_result(claim, delivered_content)
    acknowledged = asyncio.run(
        complete_preview_delivery(
            PREVIEW_SESSION,
            PREVIEW_GENERATION,
            delivered_content=delivered_content,
            result=result,
            delivered_at="2026-07-28T08:00:00+00:00",
        )
    )
    print(
        json.dumps(
            {
                "acknowledged": acknowledged,
                "claimAction": claim.action,
                "providerMessageIds": list(claim.provider_message_ids),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _preview_bridge_recover() -> None:
    from hermes_cli.planning_preview_ack_outbox import (
        dispatch_one_preview_ack,
        reconcile_preview_ack_bridge,
    )

    reconciled = reconcile_preview_ack_bridge()
    delivered: list[dict[str, Any]] = []
    dispatched = dispatch_one_preview_ack(
        deliver=lambda request: delivered.append(dict(request)),
    )
    print(
        json.dumps(
            {
                "reconciled": reconciled,
                "dispatched": dispatched,
                "requests": delivered,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("state_root", type=Path)
    parser.add_argument(
        "command",
        choices=(
            "discord-accept-crash",
            "discord-replay",
            "slack-accept-crash",
            "slack-replay",
            "matrix-accept-crash",
            "matrix-replay",
            "lock-owner",
            "lock-contender",
            "lock-replay",
            "lock-status",
            "status-retryable-settle-crash",
            "status-retryable-read",
            "status-rejected-settle-crash",
            "status-rejected-read",
            "status-delivered-settle-crash",
            "status-delivered-read",
            "status-native-sending-settle-crash",
            "status-native-sending-read",
            "status-scope-only-crash",
            "status-missing-read",
            "preview-record-crash",
            "preview-ack-replay",
            "preview-bridge-recover",
        ),
    )
    arguments = parser.parse_args()
    state_root = arguments.state_root.resolve()
    state_root.mkdir(parents=True, exist_ok=True)
    os.environ["HERMES_HOME"] = str(state_root / "hermes-home")

    if arguments.command.endswith("-accept-crash"):
        _semantic_send(
            state_root,
            provider=arguments.command.partition("-")[0],
            crash_after_provider=True,
        )
        return
    if arguments.command.endswith("-replay") and not arguments.command.startswith(
        "preview-"
    ):
        if arguments.command == "lock-replay":
            _semantic_lock_send(state_root)
            return
        _semantic_send(
            state_root,
            provider=arguments.command.partition("-")[0],
            crash_after_provider=False,
        )
        return
    if arguments.command == "lock-owner":
        _semantic_lock_owner(state_root)
        return
    if arguments.command == "lock-contender":
        _semantic_lock_send(state_root)
        return
    if arguments.command == "lock-status":
        _semantic_lock_status()
        return
    if arguments.command == "status-scope-only-crash":
        _status_scope_only_crash()
        return
    if arguments.command == "status-missing-read":
        _status_read("missing")
        return
    if arguments.command.startswith("status-"):
        status_action = arguments.command[len("status-") :]
        if status_action.endswith("-settle-crash"):
            kind = status_action[: -len("-settle-crash")]
            _status_settle_then_crash(kind)
            return
        if status_action.endswith("-read"):
            kind = status_action[: -len("-read")]
            _status_read(kind)
            return
    if arguments.command == "preview-record-crash":
        _preview_record_then_crash(state_root)
        return
    if arguments.command == "preview-ack-replay":
        _preview_ack_replay(state_root)
        return
    if arguments.command == "preview-bridge-recover":
        _preview_bridge_recover()
        return
    raise RuntimeError("unsupported process command")


if __name__ == "__main__":
    main()
