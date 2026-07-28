"""Crash-after-each-unit harness for durable multipart preview delivery."""

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


CRASH_EXIT = 86
PROVIDER = "closurepreview"
ACCOUNT = "closure-preview-account"
CHAT = "closure-preview-chat"
THREAD = "closure-preview-thread"
SESSION = "closure-preview-session"
GENERATION = 41
TARGET_PREFIX = f"{PROVIDER}:preview-target-v1:"
OLD_ENCODING = {
    "provider": PROVIDER,
    "contract": "hermes-live-semantic-exact-attempt/1",
    "segmentation_version": "closure-preview-segmentation-v1",
    "max_logical_units": 80,
    "length_semantics": "unicode_codepoints",
    "wire_encoding": "closure-preview-wire-v1",
}
NEW_ENCODING = {
    **OLD_ENCODING,
    "segmentation_version": "closure-preview-segmentation-v2",
    "max_logical_units": 29,
    "wire_encoding": "closure-preview-wire-v2",
}
FROZEN_ROUTE = {"transport": "closure-preview-route-v1"}
CURRENT_ROUTE = {"transport": "closure-preview-route-v2"}
CONTENT = "\n".join(
    (
        f"Task {index:02d}: preserve exact ordered multipart evidence "
        "through every process restart."
    )
    for index in range(1, 9)
)


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(_canonical(value), encoding="utf-8")
    os.replace(temporary, path)


def _register_provider() -> None:
    from gateway.platform_registry import declare_semantic_exact_attempt

    declare_semantic_exact_attempt(
        PROVIDER,
        standalone=False,
        live=True,
        owner="multipart-preview-supervisor-process-test",
    )


def _result(message_id: str) -> Any:
    return SimpleNamespace(
        success=True,
        message_id=message_id,
        continuation_message_ids=(),
        error=None,
        raw_response={},
        retryable=False,
        retry_after=None,
    )


def _adapter(state_root: Path, *, upgraded: bool) -> Any:
    from gateway.semantic_exact_attempt import (
        LiveSemanticExactAttemptCapability,
    )

    capability_data = NEW_ENCODING if upgraded else OLD_ENCODING
    capability = LiveSemanticExactAttemptCapability(
        provider=PROVIDER,
        contract=capability_data["contract"],
        segmentation_version=capability_data["segmentation_version"],
        max_logical_units=capability_data["max_logical_units"],
        length_semantics=capability_data["length_semantics"],
        wire_encoding=capability_data["wire_encoding"],
    )

    def bind_route(
        self,
        *,
        chat_id: str,
        thread_id: str | None = None,
        reply_to: str | None = None,
    ):
        del self, chat_id, thread_id, reply_to
        return CURRENT_ROUTE if upgraded else FROZEN_ROUTE

    async def send_semantic_exact_attempt(self, request):
        del self
        if request.encoding_contract.as_mapping() != OLD_ENCODING:
            raise AssertionError("recovery did not use frozen encoding")
        if dict(request.provider_route) != FROZEN_ROUTE:
            raise AssertionError("recovery did not use frozen provider route")
        if not request.delivery_target.startswith(TARGET_PREFIX):
            raise AssertionError("preview target changed")

        path = state_root / "provider.json"
        state = _read_json(
            path,
            {
                "businessEffects": 0,
                "calls": [],
                "receipts": {},
            },
        )
        delivery_id = str(request.delivery_id)
        receipt = state["receipts"].get(delivery_id)
        if receipt is None:
            state["businessEffects"] += 1
            receipt = (
                f"closure-preview-message-{request.delivery_unit:03d}"
            )
            state["receipts"][delivery_id] = receipt
        state["calls"].append(
            {
                "content": request.content,
                "deliveryId": delivery_id,
                "encoding": request.encoding_contract.as_mapping(),
                "messageId": receipt,
                "pid": os.getpid(),
                "providerRoute": dict(request.provider_route),
                "unit": request.delivery_unit,
            }
        )
        _write_json(path, state)
        return _result(receipt)

    adapter_type = type(
        "MultipartPreviewRecoveryAdapter",
        (),
        {
            "platform": PROVIDER,
            "SEMANTIC_EXACT_ATTEMPT_CAPABILITY": capability,
            "SEMANTIC_EXACT_ATTEMPT_ENCODING_CONTRACTS": (
                (OLD_ENCODING, NEW_ENCODING)
                if upgraded
                else (OLD_ENCODING,)
            ),
            "bind_semantic_exact_attempt_provider_route": bind_route,
            "send_semantic_exact_attempt": (
                send_semantic_exact_attempt
            ),
        },
    )
    instance = adapter_type()
    instance.config = SimpleNamespace(
        enabled=True,
        extra={"gateway_account_id": ACCOUNT},
    )
    return instance


def _source() -> Any:
    return SimpleNamespace(
        platform=PROVIDER,
        chat_id=CHAT,
        thread_id=THREAD,
        gateway_account_id=ACCOUNT,
    )


def _ack_request() -> dict[str, Any]:
    return {
        "schemaVersion": "planning.preview-ack-request.v1",
        "threadId": "planning-thread-multipart-process",
        "previewResultId": "preview-multipart-process",
        "idempotencyKey": "preview-review-multipart-process",
        "expectedPreviewHash": "sha256:" + "a" * 64,
        "offset": 0,
        "count": 8,
        "pageDigest": "sha256:" + "b" * 64,
        "origin": {
            "schemaVersion": "1.0",
            "provider": PROVIDER,
            "gatewayInstanceId": "multipart-process-runner",
            "gatewayAccountId": ACCOUNT,
            "chatId": CHAT,
            "threadId": THREAD,
            "messageId": "multipart-process-inbound",
            "senderId": "multipart-process-user",
            "chatType": "thread",
            "sourceTimestamp": "2026-07-28T10:00:00Z",
            "providerEventId": "multipart-process-event",
        },
        "deliveryProofBase": {
            "schemaVersion": "planning.preview-delivery-proof.v1",
            "deliveryNonce": "multipart-process-delivery-nonce",
            "provider": PROVIDER,
            "gatewayInstanceId": "multipart-process-runner",
            "gatewayAccountId": ACCOUNT,
            "chatId": CHAT,
            "previewResultId": "preview-multipart-process",
            "previewResultHash": "sha256:" + "a" * 64,
            "offset": 0,
            "count": 8,
            "pageDigest": "sha256:" + "b" * 64,
        },
    }


def _register_intent() -> str:
    from hermes_cli.planning_preview_delivery import (
        bind_preview_delivery_generation,
        canonical_preview_delivery_target,
        prepare_preview_delivery_content,
        register_preview_delivery_intent,
        reset_preview_delivery_generation,
    )

    payload = {
        "schemaVersion": "planning.preview-delivery-payload.v1",
        "threadId": "planning-thread-multipart-process",
        "previewResultId": "preview-multipart-process",
        "offset": 0,
        "count": 8,
        "tasks": [
            {
                "stableTaskId": f"task-{index:02d}",
                "summary": "Preserve exact ordered multipart evidence",
            }
            for index in range(1, 9)
        ],
    }
    payload_digest = "sha256:" + hashlib.sha256(
        _canonical(payload).encode("utf-8")
    ).hexdigest()
    content_digest = "sha256:" + hashlib.sha256(
        CONTENT.encode("utf-8")
    ).hexdigest()
    delivery_target = canonical_preview_delivery_target(
        provider=PROVIDER,
        gateway_account_id=ACCOUNT,
        chat_id=CHAT,
        thread_id=THREAD,
    )
    token = bind_preview_delivery_generation(SESSION, GENERATION)
    try:
        registered = register_preview_delivery_intent(
            thread_id="planning-thread-multipart-process",
            preview_result_id="preview-multipart-process",
            preview_result_hash="sha256:" + "a" * 64,
            offset=0,
            count=8,
            page_digest="sha256:" + "b" * 64,
            delivery_payload=payload,
            delivery_payload_digest=payload_digest,
            delivery_content=CONTENT,
            delivery_content_digest=content_digest,
            delivery_nonce="multipart-process-delivery-nonce",
            delivery_target=delivery_target,
            acknowledgement_request=_ack_request(),
            acknowledge=lambda _receipt: (_ for _ in ()).throw(
                AssertionError(
                    "process-local ACK callback must not own recovery"
                )
            ),
        )
    finally:
        reset_preview_delivery_generation(token)
    if not registered:
        raise RuntimeError("multipart preview intent registration failed")
    return prepare_preview_delivery_content(
        SESSION,
        GENERATION,
        "Generated wrapper must not alter the canonical preview.",
    )


def _stage_then_crash(state_root: Path) -> None:
    _register_provider()
    adapter = _adapter(state_root, upgraded=False)
    delivered_content = _register_intent()
    from hermes_cli import planning_preview_delivery

    original_record = (
        planning_preview_delivery.record_preview_delivery_result
    )

    def record_then_crash(*args, **kwargs):
        result = original_record(*args, **kwargs)
        if (
            isinstance(result, dict)
            and result.get("outcome") == "delivered"
        ):
            os._exit(CRASH_EXIT)
        return result

    planning_preview_delivery.record_preview_delivery_result = (
        record_then_crash
    )
    asyncio.run(
        planning_preview_delivery.deliver_preview_for_source(
            SESSION,
            GENERATION,
            delivered_content=delivered_content,
            source=_source(),
            adapter=adapter,
            delivered_at="2026-07-28T10:00:01Z",
        )
    )
    raise RuntimeError("stage process did not crash after provider settlement")


def _paths(profile_home: Path) -> tuple[Path, Path]:
    return (
        profile_home
        / "state"
        / "semantic-delivery"
        / "ledger.sqlite3",
        profile_home
        / "state"
        / "planning-preview-ack"
        / "outbox.sqlite3",
    )


def _inspect(profile_home: Path, state_root: Path) -> dict[str, Any]:
    semantic_path, ack_path = _paths(profile_home)
    semantic: dict[str, Any] = {
        "group": None,
        "manifest": None,
        "rows": [],
    }
    if semantic_path.exists():
        with sqlite3.connect(semantic_path) as connection:
            connection.row_factory = sqlite3.Row
            group = connection.execute(
                "SELECT delivery_group_id,unit_count,state,"
                "completion_contract,completion_ref "
                "FROM semantic_delivery_retry_groups "
                "ORDER BY created_at LIMIT 1"
            ).fetchone()
            if group is not None:
                semantic["group"] = dict(group)
            rows = connection.execute(
                "SELECT delivery_id,unit_index,unit_count,state,last_error,"
                "route_json,encoding_contract_json "
                "FROM semantic_delivery_retry_outbox "
                "ORDER BY unit_index"
            ).fetchall()
            semantic["rows"] = [dict(row) for row in rows]
            manifest = connection.execute(
                "SELECT segmentation_contract,encoding_contract_json,"
                "boundaries_json,unit_digests_json "
                "FROM planning_preview_delivery_manifests LIMIT 1"
            ).fetchone()
            if manifest is not None:
                semantic["manifest"] = dict(manifest)

    ack: dict[str, Any] = {"bridge": None, "rows": []}
    if ack_path.exists():
        with sqlite3.connect(ack_path) as connection:
            connection.row_factory = sqlite3.Row
            bridge = connection.execute(
                "SELECT bridge_id,state,semantic_delivery_ids_json,"
                "delivered_at,last_error "
                "FROM planning_preview_ack_bridge LIMIT 1"
            ).fetchone()
            if bridge is not None:
                ack["bridge"] = dict(bridge)
            ack["rows"] = [
                dict(row)
                for row in connection.execute(
                    "SELECT ack_id,state,attempts,last_error "
                    "FROM planning_preview_ack_outbox ORDER BY created_at"
                ).fetchall()
            ]
    return {
        "semantic": semantic,
        "ack": ack,
        "ackCalls": _read_json(state_root / "ack-calls.json", []),
        "provider": _read_json(
            state_root / "provider.json",
            {
                "businessEffects": 0,
                "calls": [],
                "receipts": {},
            },
        ),
    }


async def _run_supervisor(
    profile_home: Path,
    state_root: Path,
    *,
    crash_after_settlement: bool,
) -> None:
    _register_provider()
    adapter = _adapter(state_root, upgraded=True)
    from gateway.profile_delivery_supervisor import (
        ProfileDeliverySupervisor,
    )
    from hermes_cli import semantic_delivery

    if crash_after_settlement:
        original_finish = semantic_delivery.finish_semantic_delivery

        def finish_then_crash(attempt, provider_result):
            result = original_finish(attempt, provider_result)
            if result.get("outcome") == "delivered":
                os._exit(CRASH_EXIT)
            return result

        semantic_delivery.finish_semantic_delivery = finish_then_crash

    def deliver_ack(request: dict[str, Any]) -> None:
        calls = _read_json(state_root / "ack-calls.json", [])
        calls.append(
            {
                "idempotencyKey": request["idempotencyKey"],
                "deliveryProof": dict(request["deliveryProof"]),
                "pid": os.getpid(),
            }
        )
        _write_json(state_root / "ack-calls.json", calls)

    runner = SimpleNamespace(
        config=SimpleNamespace(multiplex_profiles=False),
        adapters={PROVIDER: adapter},
        _profile_adapters={},
    )
    supervisor = ProfileDeliverySupervisor(
        runner,
        poll_seconds=0.02,
        ack_deliver=deliver_ack,
    )
    if not supervisor.start():
        raise RuntimeError("profile delivery supervisor did not start")
    if crash_after_settlement:
        await asyncio.Event().wait()

    deadline = time.monotonic() + 10
    try:
        while time.monotonic() < deadline:
            snapshot = _inspect(profile_home, state_root)
            group = snapshot["semantic"]["group"]
            ack_rows = snapshot["ack"]["rows"]
            if (
                group is not None
                and group["state"] == "completed"
                and len(ack_rows) == 1
                and ack_rows[0]["state"] == "succeeded"
            ):
                return
            await asyncio.sleep(0.02)
        raise RuntimeError("multipart supervisor recovery did not converge")
    finally:
        await supervisor.stop(timeout=2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("profile_home", type=Path)
    parser.add_argument("state_root", type=Path)
    parser.add_argument(
        "command",
        choices=("stage-crash", "recover-crash", "converge", "inspect"),
    )
    arguments = parser.parse_args()
    profile_home = arguments.profile_home.resolve()
    state_root = arguments.state_root.resolve()
    profile_home.mkdir(parents=True, exist_ok=True)
    state_root.mkdir(parents=True, exist_ok=True)
    os.environ["HERMES_HOME"] = str(profile_home)

    if arguments.command == "stage-crash":
        _stage_then_crash(state_root)
    elif arguments.command == "recover-crash":
        asyncio.run(
            _run_supervisor(
                profile_home,
                state_root,
                crash_after_settlement=True,
            )
        )
    elif arguments.command == "converge":
        asyncio.run(
            _run_supervisor(
                profile_home,
                state_root,
                crash_after_settlement=False,
            )
        )
        print(_canonical(_inspect(profile_home, state_root)))
    else:
        print(_canonical(_inspect(profile_home, state_root)))


if __name__ == "__main__":
    main()
