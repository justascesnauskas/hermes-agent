"""Fresh-process public-facade harness for artifact upload convergence.

The file-backed Hub double models the important transport boundary: the first
upload is durably committed and then its response is lost.  Every Hermes-side
operation (public registry dispatch, facade routing, private spool replay, and
spool acknowledgement) remains production code.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _read_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "thread": None,
            "uploads": {},
            "uploadCalls": [],
            "uploadWrites": 0,
            "run": None,
            "runWrites": 0,
        }
    return json.loads(path.read_text(encoding="utf-8"))


def _write_state(path: Path, state: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(_canonical(state), encoding="utf-8")
    os.replace(temporary, path)


class ProcessHubClient:
    """Small durable Hub implementation used only at the HTTP-client seam."""

    runner_id = "artifact-process-runner"

    def __init__(self) -> None:
        self.state_path = Path(os.environ["CLOSURE_HUB_STATE"]).resolve()
        self.mode = os.environ["CLOSURE_MODE"]

    def current_origin(self):
        from hermes_cli.dev_hub_planning_v2 import (
            planning_origin_from_current_turn,
        )

        return planning_origin_from_current_turn(runner_id=self.runner_id)

    @staticmethod
    def _thread_projection(
        state: dict[str, Any],
        *,
        duplicate: bool = False,
    ) -> dict[str, Any]:
        thread = dict(state["thread"])
        thread["activeRunId"] = (
            state["run"]["runId"] if state.get("run") else None
        )
        return {
            "thread": thread,
            "bindings": [{"provider": "discord"}],
            "inputEventCount": int(thread["headInputSequence"]),
            "semanticEventHead": 0,
            "duplicate": duplicate,
        }

    def create_thread(self, *, origin, payload, input_kind, title, **_kwargs):
        from hermes_cli.dev_hub_planning_v2 import (
            PlanningV2Response,
            derive_thread_id_from_origin,
        )

        state = _read_state(self.state_path)
        duplicate = state["thread"] is not None
        if not duplicate:
            state["thread"] = {
                "threadId": derive_thread_id_from_origin(origin),
                "title": title or "Artifact replay",
                "status": "active",
                "activeRunId": None,
                "activePreviewVersionId": None,
                "headInputSequence": 1,
                "inputKind": input_kind,
                "inputPayloadDigest": "sha256:"
                + hashlib.sha256(_canonical(payload).encode()).hexdigest(),
                "updatedAt": "2026-07-28T10:00:00Z",
            }
            _write_state(self.state_path, state)
        return PlanningV2Response(
            200 if duplicate else 201,
            self._thread_projection(state, duplicate=duplicate),
        )

    def resolve_current_thread(
        self,
        *,
        origin,
        cursor=None,
        page_size=None,
    ):
        from hermes_cli.dev_hub_planning_v2 import PlanningV2Response

        del origin, cursor, page_size
        state = _read_state(self.state_path)
        threads = [dict(state["thread"])] if state["thread"] else []
        return PlanningV2Response(
            200,
            {
                "match": "one" if threads else "none",
                "matchCount": len(threads),
                "returnedCount": len(threads),
                "threads": threads,
                "hasMore": False,
                "nextCursor": None,
            },
        )

    def upload_artifact(
        self,
        thread_id: str,
        path: str,
        *,
        role: str,
        position: int,
        idempotency_key: str,
        content_type=None,
        retain_until=None,
        input_origin=None,
        required=None,
    ):
        from hermes_cli.dev_hub_planning_v2 import (
            PlanningV2Response,
            PlanningV2TransportError,
        )

        state = _read_state(self.state_path)
        raw = Path(path).read_bytes()
        checksum = "sha256:" + hashlib.sha256(raw).hexdigest()
        state["uploadCalls"].append(
            {
                "idempotencyKey": idempotency_key,
                "checksum": checksum,
                "sizeBytes": len(raw),
                "pid": os.getpid(),
            }
        )
        stored = state["uploads"].get(idempotency_key)
        if stored is None:
            blob_id = "blob-" + hashlib.sha256(raw).hexdigest()[:24]
            artifact_ref = f"planning-artifact-v1:{blob_id}"
            descriptor = {
                "schemaVersion": "1.0",
                "artifactId": blob_id,
                "artifactRef": artifact_ref,
                "sourceReference": artifact_ref,
                "referenceId": f"reference-{position}",
                "checksum": checksum,
                "sizeBytes": len(raw),
                "contentType": content_type or "application/octet-stream",
                "role": role,
                "position": position,
                "required": bool(required),
                "filename": Path(path).name,
            }
            stored = {
                "descriptor": descriptor,
                "retainUntil": retain_until,
                "origin": dict(input_origin or {}),
            }
            state["uploads"][idempotency_key] = stored
            state["uploadWrites"] += 1
            state["thread"]["headInputSequence"] += 1
            _write_state(self.state_path, state)
            if self.mode == "lose":
                raise PlanningV2TransportError(
                    "planning.hub_timeout",
                    detail="upload response lost after durable Hub commit",
                    retryable=True,
                    ambiguous=True,
                    attempts=1,
                )
            disposition = "committed"
            status = 201
        else:
            if (
                stored["descriptor"]["checksum"] != checksum
                or stored["descriptor"]["role"] != role
                or stored["descriptor"]["position"] != position
            ):
                raise AssertionError("artifact replay contract changed")
            _write_state(self.state_path, state)
            disposition = "replayed"
            status = 200

        descriptor = dict(stored["descriptor"])
        return PlanningV2Response(
            status,
            {
                "ok": True,
                "storage": {"mode": "local", "reason": "process-harness"},
                "disposition": disposition,
                "artifact": {
                    "blobId": descriptor["artifactId"],
                    "artifactRef": descriptor["artifactRef"],
                    "checksum": descriptor["checksum"],
                    "sizeBytes": descriptor["sizeBytes"],
                    "contentType": descriptor["contentType"],
                    "retentionPolicy": "reference_bound",
                    "retainUntil": stored["retainUntil"],
                    "createdAt": "2026-07-28T10:00:01Z",
                },
                "reference": {
                    "referenceId": descriptor["referenceId"],
                    "blobId": descriptor["artifactId"],
                    "artifactRef": descriptor["artifactRef"],
                    "role": descriptor["role"],
                    "position": descriptor["position"],
                    "provenance": {"filename": descriptor["filename"]},
                    "createdAt": "2026-07-28T10:00:01Z",
                    "releasedAt": None,
                },
                "artifactInput": descriptor,
                "inputStored": True,
                "inputReplayed": disposition == "replayed",
                "previewInvalidated": False,
                "inputEvent": {
                    "sequence": state["thread"]["headInputSequence"],
                    "inputKind": "artifact",
                },
            },
        )

    def get_thread_input_identity(self, thread_id: str):
        state = _read_state(self.state_path)
        assert state["thread"]["threadId"] == thread_id
        identity = {
            "threadId": thread_id,
            "basisInputSequence": state["thread"]["headInputSequence"],
            "uploads": sorted(state["uploads"]),
        }
        return {
            "threadId": thread_id,
            "basisInputSequence": identity["basisInputSequence"],
            "inputDigest": "sha256:"
            + hashlib.sha256(_canonical(identity).encode()).hexdigest(),
        }

    def create_run(
        self,
        thread_id: str,
        *,
        idempotency_key: str,
        expected_basis_input_sequence: int,
        expected_input_digest: str,
        policy,
        route_policy,
        correlation_id,
    ):
        from hermes_cli.dev_hub_planning_v2 import PlanningV2Response

        state = _read_state(self.state_path)
        replayed = state["run"] is not None
        if not replayed:
            identity = self.get_thread_input_identity(thread_id)
            assert expected_basis_input_sequence == identity[
                "basisInputSequence"
            ]
            assert expected_input_digest == identity["inputDigest"]
            state["run"] = {
                "runId": "run-artifact-process",
                "threadId": thread_id,
                "status": "queued",
                "basisInputSequence": expected_basis_input_sequence,
                "inputDigest": expected_input_digest,
                "idempotencyKey": idempotency_key,
                "policy": dict(policy),
                "routePolicy": dict(route_policy),
                "correlationId": correlation_id,
                "workItems": [],
                "progress": {},
            }
            state["runWrites"] += 1
            _write_state(self.state_path, state)
        else:
            assert state["run"]["idempotencyKey"] == idempotency_key
        return PlanningV2Response(
            200 if replayed else 201,
            dict(state["run"]),
        )

    def get_thread(self, thread_id: str):
        from hermes_cli.dev_hub_planning_v2 import PlanningV2Response

        state = _read_state(self.state_path)
        assert state["thread"]["threadId"] == thread_id
        return PlanningV2Response(200, self._thread_projection(state))

    def get_run(self, run_id: str):
        from hermes_cli.dev_hub_planning_v2 import PlanningV2Response

        state = _read_state(self.state_path)
        assert state["run"]["runId"] == run_id
        return PlanningV2Response(200, dict(state["run"]))

    def get_delivery_attention(self, thread_id: str, **_kwargs):
        from hermes_cli.dev_hub_planning_v2 import PlanningV2Response

        return PlanningV2Response(
            200,
            {
                "ok": True,
                "threadId": thread_id,
                "deliveryAttention": {
                    "status": "clear",
                    "count": 0,
                    "returnedCount": 0,
                    "hasMore": False,
                    "nextAfterResolutionToken": None,
                    "requiresExplicitToken": False,
                    "items": [],
                },
            },
        )

    def get_apply_status(self, thread_id: str, **_kwargs):
        from hermes_cli.dev_hub_planning_v2 import PlanningV2Response

        return PlanningV2Response(
            200,
            {
                "ok": True,
                "threadId": thread_id,
                "applyStatus": {
                    "count": 0,
                    "actionableCount": 0,
                    "automaticResumeCount": 0,
                    "revisionRequiredCount": 0,
                    "returnedCount": 0,
                    "hasMore": False,
                    "nextAfterRecoveryToken": None,
                    "items": [],
                },
            },
        )


def _turn_origin(
    mode: str,
    source: Path | None,
):
    from hermes_cli.turn_origin import (
        TurnAttachmentOriginV1,
        TurnOriginV1,
        turn_attachment_path_fingerprint,
    )

    attachments = ()
    if source is not None:
        attachments = (
            TurnAttachmentOriginV1(
                attachment_id="attachment-process-stable",
                ingress_ordinal=1,
                path_fingerprint=turn_attachment_path_fingerprint(str(source)),
                local_path=str(source),
            ),
        )
    return TurnOriginV1(
        provider="discord",
        gateway_account_id="artifact-account",
        chat_id="artifact-chat",
        thread_id="artifact-provider-thread",
        message_id=f"artifact-message-{mode}",
        sender_id="artifact-user",
        chat_type="channel",
        source_timestamp="2026-07-28T10:00:00Z",
        event_id=(
            "artifact-create-event"
            if mode == "lose"
            else "artifact-retry-event"
        ),
        attachments=attachments,
    )


def main() -> None:
    profile_home = Path(sys.argv[1]).resolve()
    state_path = Path(sys.argv[2]).resolve()
    mode = sys.argv[3]
    source = (
        Path(sys.argv[4]).resolve()
        if len(sys.argv) > 4 and sys.argv[4] != "-"
        else None
    )
    os.environ["HERMES_HOME"] = str(profile_home)
    os.environ["CLOSURE_HUB_STATE"] = str(state_path)
    os.environ["CLOSURE_MODE"] = mode
    os.environ["AGENT_OPS_API_URL"] = "https://hub.invalid"
    os.environ["AGENT_OPS_RUNNER_ID"] = ProcessHubClient.runner_id
    os.environ["AGENT_OPS_RUNNER_TOKEN"] = "artifact-process-token"

    import model_tools
    from agent.auxiliary_client import scoped_runtime_main
    from gateway.config import PlatformConfig
    from hermes_cli import dev_hub_planning_facade as facade
    from hermes_cli import planning_artifact_spool
    from hermes_cli.turn_origin import (
        scoped_turn_delivery_adapter,
        scoped_turn_origin,
        scoped_turn_user_text,
    )
    from plugins.platforms.discord.adapter import DiscordAdapter
    from tools import agent_ops_tasking_tool as public_tool
    from tools import dev_hub_planning_tool as private_tool

    # Replace only the network client. Registry, dispatcher, facade, spool,
    # internal handler, replay key derivation, and acknowledgement are real.
    facade.PlanningV2Client = ProcessHubClient
    private_tool.PlanningV2Client = ProcessHubClient
    public_tool.PlanningV2Client = ProcessHubClient

    origin = _turn_origin(mode, source)
    delivery_adapter = DiscordAdapter(
        PlatformConfig(
            enabled=True,
            token="artifact-process-discord-token",
            extra={"gateway_account_id": origin.gateway_account_id},
        )
    )
    text = (
        "Build this plan using the attached immutable evidence."
        if mode == "lose"
        else "Retry the same durable plan after the interrupted upload."
    )
    with (
        scoped_turn_origin(origin),
        scoped_turn_user_text(text),
        scoped_turn_delivery_adapter(delivery_adapter),
        scoped_runtime_main(
            {
                "provider": "openrouter",
                "model": "anthropic/claude-sonnet-4",
                "api_mode": "chat_completions",
                "base_url": "https://router.invalid/v1",
                "api_key": "runtime-secret",
                "auth_mode": "bearer",
            }
        ),
    ):
        result = json.loads(
            model_tools.handle_function_call(
                "agent_ops_task_plan",
                {"intent": "new" if mode == "lose" else "retry"},
                user_task=text,
                session_id="artifact-process-session",
                enabled_toolsets=["planning_v2"],
                skip_pre_tool_call_hook=True,
                skip_tool_request_middleware=True,
            )
        )
        state = _read_state(state_path)
        thread_id = (
            str(state["thread"]["threadId"])
            if state["thread"]
            else ""
        )
        live = (
            planning_artifact_spool.list_artifact_recoveries(
                current_origin=ProcessHubClient().current_origin(),
                thread_id=thread_id,
            )
            if thread_id
            else ()
        )
        token = (
            live[0].token
            if live
            else str(os.environ.get("CLOSURE_RECOVERY_TOKEN") or "")
        )
        completion = (
            planning_artifact_spool.load_artifact_recovery_completion(
                token,
                current_origin=ProcessHubClient().current_origin(),
            )
            if token
            else None
        )
    print(
        _canonical(
            {
                "result": result,
                "state": state,
                "live": [
                    {
                        "token": record.token,
                        "snapshotPath": record.snapshot_path,
                        "checksum": record.checksum,
                        "sizeBytes": record.size_bytes,
                    }
                    for record in live
                ],
                "completion": completion,
                "token": token,
            }
        )
    )


if __name__ == "__main__":
    main()
