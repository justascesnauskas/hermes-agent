"""Fresh-process crash harness for Planning turn/outbox ownership transfer."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys


CRASH_EXIT = 86


def _canonical(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _increment(path: Path) -> None:
    current = int(path.read_text(encoding="utf-8")) if path.exists() else 0
    path.write_text(str(current + 1), encoding="utf-8")


def _register_preview(session_key: str, generation: int) -> str:
    from hermes_cli.planning_preview_delivery import (
        bind_preview_delivery_generation,
        prepare_preview_delivery_content,
        register_preview_delivery_intent,
        reset_preview_delivery_generation,
    )

    content = "\n".join(
        f"Task {index:03d}: retain exact crash-safe delivery evidence."
        for index in range(80)
    )
    payload = {
        "schemaVersion": "planning.preview-delivery-payload.v1",
        "threadId": "planning-process-thread",
        "previewResultId": "planning-process-preview",
        "offset": 0,
        "count": 80,
        "tasks": [
            {
                "stableTaskId": f"task-{index:03d}",
                "summary": "Retain exact crash-safe delivery evidence",
            }
            for index in range(80)
        ],
    }
    token = bind_preview_delivery_generation(session_key, generation)
    try:
        registered = register_preview_delivery_intent(
            thread_id="planning-process-thread",
            preview_result_id="planning-process-preview",
            preview_result_hash="sha256:" + "a" * 64,
            offset=0,
            count=80,
            page_digest=(
                "sha256:"
                + hashlib.sha256(content.encode("utf-8")).hexdigest()
            ),
            delivery_payload=payload,
            delivery_payload_digest=(
                "sha256:"
                + hashlib.sha256(
                    _canonical(payload).encode("utf-8")
                ).hexdigest()
            ),
            delivery_content=content,
            delivery_content_digest=(
                "sha256:"
                + hashlib.sha256(content.encode("utf-8")).hexdigest()
            ),
            delivery_nonce="planning-process-preview-stable-nonce",
            acknowledge=lambda _receipt: None,
            allow_process_local_ack=True,
        )
    finally:
        reset_preview_delivery_generation(token)
    if not registered:
        raise RuntimeError("preview registration failed")
    return prepare_preview_delivery_content(
        session_key,
        generation,
        "The signed preview follows.",
    )


def _adapter(provider_write_counter: Path):
    from gateway.config import Platform, PlatformConfig
    from gateway.platform_registry import declare_semantic_exact_attempt
    from gateway.semantic_exact_attempt import (
        LiveSemanticExactAttemptCapability,
    )

    declare_semantic_exact_attempt(
        "telegram",
        standalone=False,
        live=True,
        owner="planning-process-handoff-test",
    )

    async def send_semantic_exact_attempt(self, _request):
        _increment(provider_write_counter)
        raise AssertionError("claim/staging harness must never write provider")

    return type(
        "PlanningProcessExactAdapter",
        (),
        {
            "platform": Platform.TELEGRAM,
            "config": PlatformConfig(
                enabled=True,
                extra={"gateway_account_id": "telegram-account"},
            ),
            "SEMANTIC_EXACT_ATTEMPT_CAPABILITY": (
                LiveSemanticExactAttemptCapability(
                    provider="telegram",
                    contract="hermes-live-semantic-exact-attempt/1",
                    segmentation_version="planning-process-unicode-v1",
                    max_logical_units=256,
                    length_semantics="unicode_codepoints",
                    wire_encoding="telegram-process-json-v1",
                )
            ),
            "send_semantic_exact_attempt": send_semantic_exact_attempt,
        },
    )()


def main() -> None:
    profile_home = Path(sys.argv[1]).resolve()
    session_key = sys.argv[2]
    queue_id = sys.argv[3]
    mode = sys.argv[4]
    model_counter = Path(sys.argv[5])
    provider_counter = Path(sys.argv[6])
    os.environ["HERMES_HOME"] = str(profile_home)

    from hermes_cli.gateway_turn_queue import claim_turn

    claim = claim_turn(
        queue_id,
        owner=f"crashed-{mode}",
        profile_home=profile_home,
        claim_seconds=86_400,
    )
    if claim is None or claim.session_key != session_key:
        raise RuntimeError("queued turn could not be claimed")

    # This represents the one model/tool execution that produced the preview.
    _increment(model_counter)
    if mode == "before-stage":
        os._exit(CRASH_EXIT)

    from hermes_cli import planning_preview_delivery
    from hermes_cli.planning_preview_delivery import (
        claim_preview_delivery_for_source,
    )
    from hermes_cli.semantic_delivery import (
        semantic_retry_turn_execution_committed,
    )

    generation = 77
    content = _register_preview(session_key, generation)
    adapter = _adapter(provider_counter)
    if mode == "partial-stage":
        original_stage = planning_preview_delivery.stage_semantic_delivery_retry
        staged = 0

        def crash_after_first_unit(**kwargs):
            nonlocal staged
            result = original_stage(**kwargs)
            staged += 1
            if staged == 1:
                os._exit(CRASH_EXIT)
            return result

        planning_preview_delivery.stage_semantic_delivery_retry = (
            crash_after_first_unit
        )

    claim_result = claim_preview_delivery_for_source(
        session_key,
        generation,
        delivered_content=content,
        source=claim.event.source,
        adapter=adapter,
        turn_execution_ref=queue_id,
    )
    if mode != "full-stage":
        raise RuntimeError(f"unexpected mode completion: {mode}")
    if claim_result.action != "send":
        raise RuntimeError(
            f"full staging did not reach provider boundary: "
            f"{claim_result.action}"
        )
    if not semantic_retry_turn_execution_committed(queue_id):
        raise RuntimeError("full staging proof is missing")
    os._exit(CRASH_EXIT)


if __name__ == "__main__":
    main()
