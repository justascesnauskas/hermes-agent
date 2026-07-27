"""User-facing Planning V2 tool boundaries and semantic result tests."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

from hermes_cli.dev_hub_planning_v2 import (
    PlanningV2HTTPError,
    PlanningV2Response,
    PlanningV2TransportError,
    planning_origin_from_current_turn,
)
from hermes_cli.turn_origin import (
    TurnAttachmentOriginV1,
    TurnOriginV1,
    get_current_turn_origin,
    scoped_turn_origin,
    scoped_turn_user_text,
    turn_attachment_path_fingerprint,
)
from hermes_cli.planning_preview_delivery import (
    bind_preview_delivery_generation,
    complete_preview_delivery,
    prepare_preview_delivery_content,
    reset_preview_delivery_generation,
)
from tools import dev_hub_planning_tool as planning_tool
from toolsets import resolve_toolset, validate_toolset


def _turn_origin(provider: str, *, event_id: str) -> TurnOriginV1:
    return TurnOriginV1(
        provider=provider,
        gateway_account_id=f"{provider}-account",
        chat_id=f"{provider}-chat",
        thread_id=f"{provider}-native-thread",
        message_id=f"{provider}-message",
        sender_id=f"{provider}-user",
        chat_type="direct",
        source_timestamp="2026-07-27T12:30:00Z",
        event_id=event_id,
    )


def _thread_projection(
    *,
    providers: tuple[str, ...],
    input_count: int,
    active_run_id: str | None = None,
    active_preview_version_id: str | None = None,
    duplicate: bool = False,
) -> dict[str, Any]:
    return {
        "thread": {
            "threadId": "planning-thread-1",
            "status": "active",
            "headInputSequence": input_count,
            "activeRunId": active_run_id,
            "activePreviewVersionId": active_preview_version_id,
        },
        "bindings": [
            {
                "deliveryMode": "subscribed",
                "endpoint": {"provider": provider},
            }
            for provider in providers
        ],
        "inputEventCount": input_count,
        "semanticEventHead": input_count,
        "inputEvent": {"eventId": f"input-{input_count}"},
        "duplicate": duplicate,
    }


def _run_projection(
    *,
    run_id: str = "run-1",
    work_items: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "runId": run_id,
        "threadId": "planning-thread-1",
        "status": "running",
        "basisInputSequence": 2,
        "workItems": list(work_items or []),
    }


class _CrossProviderClient:
    runner_id = "runner-1"

    def __init__(self) -> None:
        self.create_origins: list[dict[str, Any]] = []
        self.append_origins: list[dict[str, Any]] = []
        self.append_thread_ids: list[str] = []
        self.run_keys: list[str] = []

    def current_origin(self) -> dict[str, Any]:
        return planning_origin_from_current_turn(runner_id=self.runner_id)

    def create_thread(
        self,
        *,
        origin: dict[str, Any],
        payload: dict[str, Any],
        **_kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        self.create_origins.append(origin)
        assert payload == {"text": "same planning text"}
        return PlanningV2Response(
            201,
            _thread_projection(providers=("discord",), input_count=1),
        )

    def append_thread_input(
        self,
        thread_id: str,
        *,
        origin: dict[str, Any],
        payload: dict[str, Any],
        **_kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        self.append_thread_ids.append(thread_id)
        self.append_origins.append(origin)
        assert payload == {"text": "same planning text"}
        return PlanningV2Response(
            201,
            {
                **_thread_projection(
                    providers=("discord", "slack"),
                    input_count=2,
                ),
                "previewInvalidated": True,
            },
        )

    def create_run(
        self,
        _thread_id: str,
        *,
        idempotency_key: str,
        **_kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        self.run_keys.append(idempotency_key)
        return PlanningV2Response(
            201,
            _run_projection(run_id=f"run-{len(self.run_keys)}"),
        )

    def get_thread_input_identity(
        self,
        _thread_id: str,
    ) -> dict[str, Any]:
        basis = len(self.run_keys) + 1
        return {
            "threadId": "planning-thread-1",
            "basisInputSequence": basis,
            "inputDigest": "sha256:" + f"{basis:064x}",
        }


def test_explicit_discord_to_slack_continuation_uses_only_scoped_origin(
    monkeypatch,
) -> None:
    fake = _CrossProviderClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    with scoped_turn_origin(_turn_origin("discord", event_id="shared-event-id")):
        created = json.loads(
            planning_tool._handle_planning_v2(
                {"action": "create", "message": "same planning text"},
                user_task="same planning text",
            )
        )
    assert get_current_turn_origin() is None

    with scoped_turn_origin(_turn_origin("slack", event_id="shared-event-id")):
        continued = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "continue",
                    "thread_id": created["threadId"],
                    "message": "same planning text",
                },
                user_task="same planning text",
            )
        )
    assert get_current_turn_origin() is None

    assert fake.append_thread_ids == ["planning-thread-1"]
    assert fake.create_origins[0] == {
        "schemaVersion": "1.0",
        "provider": "discord",
        "gatewayInstanceId": "runner-1",
        "gatewayAccountId": "discord-account",
        "chatId": "discord-chat",
        "threadId": "discord-native-thread",
        "messageId": "discord-message",
        "senderId": "discord-user",
        "chatType": "direct",
        "sourceTimestamp": "2026-07-27T12:30:00Z",
        "providerEventId": "shared-event-id",
    }
    assert fake.append_origins[0]["provider"] == "slack"
    assert fake.append_origins[0]["providerEventId"] == "shared-event-id"
    assert "same planning text" not in json.dumps(
        [fake.create_origins, fake.append_origins]
    )
    assert len(set(fake.run_keys)) == 2
    assert all(
        key.startswith("hermes-planning-run-v2:") for key in fake.run_keys
    )
    assert created["boundProviders"] == ["discord"]
    assert continued["boundProviders"] == ["discord", "slack"]
    assert continued["previewInvalidated"] is True

    missing_origin = json.loads(
        planning_tool._handle_planning_v2(
            {"action": "create", "message": "same planning text"},
            user_task="same planning text",
        )
    )
    assert missing_origin["code"] == "planning.origin_missing"
    assert len(fake.create_origins) == 1


def test_missing_gateway_account_id_fails_closed_before_hub_write(
    monkeypatch,
) -> None:
    fake = _CrossProviderClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)
    incomplete = TurnOriginV1(
        provider="discord",
        chat_id="discord-chat",
        message_id="discord-message",
        sender_id="discord-user",
        source_timestamp="2026-07-27T12:30:00Z",
        event_id="discord-event",
    )

    with scoped_turn_origin(incomplete):
        result = json.loads(
            planning_tool._handle_planning_v2(
                {"action": "create", "message": "must not write"},
                user_task="must not write",
            )
        )

    assert result["code"] == "planning.origin_incomplete"
    assert "gateway_account_id" in result["detail"]["missing"]
    assert fake.create_origins == []


def test_exact_runtime_turn_text_cannot_be_replaced_by_model_arguments(
    monkeypatch,
) -> None:
    class _ExactInputClient(_CrossProviderClient):
        def __init__(self) -> None:
            super().__init__()
            self.payloads: list[dict[str, Any]] = []

        def create_thread(
            self,
            *,
            origin: dict[str, Any],
            payload: dict[str, Any],
            **_kwargs: Any,
        ) -> PlanningV2Response[dict[str, Any]]:
            self.create_origins.append(origin)
            self.payloads.append(payload)
            return PlanningV2Response(
                201,
                _thread_projection(providers=("discord",), input_count=1),
            )

    fake = _ExactInputClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    with (
        scoped_turn_origin(_turn_origin("discord", event_id="exact-input")),
        scoped_turn_user_text("Exact gateway instruction"),
    ):
        result = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "create",
                    "message": "Model paraphrase",
                    "payload": {
                        "text": "Attempted replacement",
                        "audience": "SMB",
                    },
                    "start_run": False,
                }
            )
        )

    assert result["ok"] is True
    assert fake.payloads == [
        {
            "text": "Exact gateway instruction",
            "modelNormalization": {
                "message": "Model paraphrase",
                "payload": {"audience": "SMB"},
            },
        }
    ]


class _ArtifactClient:
    runner_id = "runner-1"

    def __init__(self) -> None:
        self.upload_calls: list[dict[str, Any]] = []
        self.append_calls: list[dict[str, Any]] = []

    def current_origin(self) -> dict[str, Any]:
        return planning_origin_from_current_turn(runner_id=self.runner_id)

    def upload_artifact(
        self,
        thread_id: str,
        path: str,
        **kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        call = {"threadId": thread_id, "path": path, **kwargs}
        self.upload_calls.append(call)
        position = int(kwargs["position"])
        blob_id = f"blob-{position}"
        artifact_ref = f"planning-artifact-v1:{blob_id}"
        return PlanningV2Response(
            201,
            {
                "ok": True,
                "storage": {"mode": "local", "reason": "local_fallback"},
                "disposition": "committed",
                "artifact": {
                    "blobId": blob_id,
                    "artifactRef": artifact_ref,
                    "checksum": "sha256:" + "a" * 64,
                    "sizeBytes": 2048 + position,
                    "contentType": kwargs.get("content_type") or "image/png",
                    "retentionPolicy": "reference_bound",
                    "retainUntil": kwargs.get("retain_until"),
                    "createdAt": "2026-07-27T12:30:00Z",
                },
                "reference": {
                    "referenceId": f"reference-{position}",
                    "blobId": blob_id,
                    "artifactRef": artifact_ref,
                    "role": kwargs["role"],
                    "position": position,
                    "provenance": {
                        "filename": path.rsplit("/", 1)[-1],
                    },
                    "createdAt": "2026-07-27T12:30:00Z",
                    "releasedAt": None,
                },
            },
        )

    def append_thread_input(
        self,
        thread_id: str,
        *,
        origin: dict[str, Any],
        payload: dict[str, Any],
        input_kind: str,
    ) -> PlanningV2Response[dict[str, Any]]:
        self.append_calls.append(
            {
                "threadId": thread_id,
                "origin": origin,
                "payload": payload,
                "inputKind": input_kind,
            }
        )
        return PlanningV2Response(
            201,
            {
                **_thread_projection(
                    providers=(origin["provider"],),
                    input_count=len(self.append_calls) + 1,
                ),
                "previewInvalidated": False,
            },
        )


def test_legacy_artifact_upload_falls_back_to_separate_opaque_input(
    monkeypatch,
) -> None:
    fake = _ArtifactClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)
    origin = _turn_origin("discord", event_id="attachment-event")

    with scoped_turn_origin(origin):
        result = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "upload_artifact",
                    "thread_id": "planning-thread-1",
                    "local_path": "/gateway/cache/dashboard.png",
                    "role": "design_reference",
                    "position": 1,
                    "required": True,
                }
            )
        )

    upload = fake.upload_calls[0]
    appended = fake.append_calls[0]
    descriptor = appended["payload"]
    assert upload["path"] == "/gateway/cache/dashboard.png"
    assert upload["idempotency_key"].startswith(
        "hermes-planning-artifact-v1:"
    )
    assert "/gateway/cache/dashboard.png" not in upload["idempotency_key"]
    assert appended["inputKind"] == "artifact"
    assert appended["origin"]["providerEventId"].startswith(
        "hermes-planning-artifact-input-v1:"
    )
    assert appended["origin"]["messageId"] == "discord-message"
    assert appended["origin"]["senderId"] == "discord-user"
    assert descriptor == {
        "schemaVersion": "1.0",
        "artifactId": "blob-1",
        "artifactRef": "planning-artifact-v1:blob-1",
        "sourceReference": "planning-artifact-v1:blob-1",
        "referenceId": "reference-1",
        "checksum": "sha256:" + "a" * 64,
        "sizeBytes": 2049,
        "contentType": "image/png",
        "role": "design_reference",
        "position": 1,
        "required": True,
        "filename": "dashboard.png",
    }
    assert "local_path" not in descriptor
    assert result["artifact"] == descriptor
    assert result["inputStored"] is True
    assert result["startRunAction"] == {
        "tool": planning_tool.PLANNING_V2_TOOL_NAME,
        "arguments": {
            "action": "start_run",
            "thread_id": "planning-thread-1",
        },
    }


def test_convergent_artifact_upload_skips_legacy_append(
    monkeypatch,
) -> None:
    class _ConvergentArtifactClient(_ArtifactClient):
        def upload_artifact(
            self,
            thread_id: str,
            path: str,
            **kwargs: Any,
        ) -> PlanningV2Response[dict[str, Any]]:
            response = super().upload_artifact(thread_id, path, **kwargs)
            artifact = response.payload["artifact"]
            reference = response.payload["reference"]
            descriptor = {
                "schemaVersion": "1.0",
                "artifactId": artifact["blobId"],
                "artifactRef": artifact["artifactRef"],
                "sourceReference": artifact["artifactRef"],
                "referenceId": reference["referenceId"],
                "checksum": artifact["checksum"],
                "sizeBytes": artifact["sizeBytes"],
                "contentType": artifact["contentType"],
                "role": reference["role"],
                "position": reference["position"],
                "required": kwargs["required"],
                "filename": path.rsplit("/", 1)[-1],
            }
            response.payload.update(
                {
                    "disposition": "replayed",
                    "artifactInput": descriptor,
                    "inputStored": True,
                    "inputReplayed": True,
                    "previewInvalidated": True,
                    "inputEvent": {
                        "eventId": "input-artifact-1",
                        "threadId": thread_id,
                        "inputKind": "artifact",
                        "payload": descriptor,
                        "origin": kwargs["input_origin"],
                        "causationId": kwargs["input_origin"][
                            "providerEventId"
                        ],
                    },
                }
            )
            return PlanningV2Response(
                201,
                response.payload,
                transport_attempts=2,
            )

    fake = _ConvergentArtifactClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    with scoped_turn_origin(
        _turn_origin("teams", event_id="convergent-artifact-event")
    ):
        result = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "upload_artifact",
                    "thread_id": "planning-thread-1",
                    "local_path": "/gateway/cache/research.pdf",
                    "role": "research_evidence",
                    "position": 2,
                    "required": False,
                    "content_type": "application/pdf",
                }
            )
        )

    upload = fake.upload_calls[0]
    assert upload["required"] is False
    assert upload["input_origin"]["provider"] == "teams"
    assert upload["input_origin"]["providerEventId"].startswith(
        "hermes-planning-artifact-input-v1:"
    )
    assert fake.append_calls == []
    assert result["inputStored"] is True
    assert result["inputReplayed"] is True
    assert result["previewInvalidated"] is True
    assert result["uploadReplayed"] is True
    assert result["artifact"]["artifactRef"] == "planning-artifact-v1:blob-2"
    assert result["artifact"]["role"] == "research_evidence"
    assert result["artifact"]["required"] is False


def test_artifact_retry_identity_uses_ingress_attachment_not_model_labels(
    monkeypatch,
) -> None:
    fake = _ArtifactClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)
    cached_path = "/gateway/cache/exact-upload.png"
    origin = replace(
        _turn_origin("discord", event_id="attachment-event"),
        attachments=(
            TurnAttachmentOriginV1(
                attachment_id="att_v1_provider_exact",
                ingress_ordinal=1,
                path_fingerprint=turn_attachment_path_fingerprint(cached_path),
            ),
        ),
    )

    with scoped_turn_origin(origin):
        first = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "upload_artifact",
                    "thread_id": "planning-thread-1",
                    "local_path": cached_path,
                    "role": "design_reference",
                    "position": 7,
                }
            )
        )
        second = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "upload_artifact",
                    "thread_id": "planning-thread-1",
                    "local_path": cached_path,
                    "role": "database_schema",
                    "position": 99,
                }
            )
        )

    assert first["ok"] is True
    assert second["ok"] is True
    assert fake.upload_calls[0]["idempotency_key"] == fake.upload_calls[1][
        "idempotency_key"
    ]
    assert fake.append_calls[0]["origin"]["providerEventId"] == fake.append_calls[
        1
    ]["origin"]["providerEventId"]
    assert fake.upload_calls[0]["role"] != fake.upload_calls[1]["role"]
    assert fake.upload_calls[0]["position"] != fake.upload_calls[1]["position"]


def test_artifact_upload_resolves_sandbox_cache_path_on_gateway(
    monkeypatch,
) -> None:
    from tools import credential_files

    fake = _ArtifactClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)
    host_path = "/home/gateway/.hermes/cache/documents/schema.pdf"
    sandbox_path = "/root/.hermes/cache/documents/schema.pdf"
    monkeypatch.setattr(
        credential_files,
        "from_agent_visible_cache_path",
        lambda value: host_path if value == sandbox_path else value,
    )
    origin = replace(
        _turn_origin("discord", event_id="sandbox-attachment"),
        attachments=(
            TurnAttachmentOriginV1(
                attachment_id="att_v1_sandbox",
                ingress_ordinal=1,
                path_fingerprint=turn_attachment_path_fingerprint(host_path),
            ),
        ),
    )

    with scoped_turn_origin(origin):
        result = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "upload_artifact",
                    "thread_id": "planning-thread-1",
                    "local_path": sandbox_path,
                    "role": "database_schema",
                    "position": 1,
                }
            )
        )

    assert result["ok"] is True
    assert fake.upload_calls[0]["path"] == host_path


def test_artifact_upload_has_exact_recovery_after_lost_response(
    monkeypatch,
) -> None:
    class _LostArtifactClient(_ArtifactClient):
        def __init__(self) -> None:
            super().__init__()
            self.lose_first_response = True

        def upload_artifact(
            self,
            thread_id: str,
            path: str,
            **kwargs: Any,
        ) -> PlanningV2Response[dict[str, Any]]:
            if self.lose_first_response:
                self.lose_first_response = False
                self.upload_calls.append(
                    {"threadId": thread_id, "path": path, **kwargs}
                )
                raise PlanningV2TransportError(
                    "planning.hub_timeout",
                    detail="upload response lost after commit",
                    retryable=True,
                    ambiguous=True,
                    attempts=2,
                )
            return super().upload_artifact(thread_id, path, **kwargs)

    fake = _LostArtifactClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    with scoped_turn_origin(
        _turn_origin("slack", event_id="lost-attachment-event")
    ):
        failure = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "upload_artifact",
                    "thread_id": "planning-thread-1",
                    "local_path": "/gateway/cache/schema.pdf",
                    "role": "database_schema",
                    "position": 2,
                    "content_type": "application/pdf",
                    "retain_until": "2027-07-27T00:00:00Z",
                }
            )
        )

    recovery = failure["recovery"]["nextAction"]["arguments"]
    assert failure["outcomeAmbiguous"] is True
    assert failure["recovery"]["artifactUploadStarted"] is True
    assert recovery["action"] == "upload_artifact"
    assert recovery["recovery_token"].startswith("artrec_v1_")
    assert recovery == {
        "action": "upload_artifact",
        "recovery_token": recovery["recovery_token"],
    }
    assert "/gateway/cache/schema.pdf" not in json.dumps(failure)

    with scoped_turn_origin(
        _turn_origin("slack", event_id="recovery-turn-event")
    ):
        recovered = json.loads(
            planning_tool._handle_planning_v2(recovery)
        )

    assert recovered["ok"] is True
    assert recovered["inputStored"] is True
    assert fake.upload_calls[0]["idempotency_key"] == fake.upload_calls[1][
        "idempotency_key"
    ]


def test_artifact_upload_requires_current_scoped_origin(monkeypatch) -> None:
    fake = _ArtifactClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    result = json.loads(
        planning_tool._handle_planning_v2(
            {
                "action": "upload_artifact",
                "thread_id": "planning-thread-1",
                "local_path": "/gateway/cache/schema.pdf",
                "role": "database_schema",
                "position": 1,
            }
        )
    )

    assert result["code"] == "planning.origin_missing"
    assert fake.upload_calls == []
    assert fake.append_calls == []


def test_artifact_recovery_token_rejects_model_path_or_metadata_overrides(
    monkeypatch,
) -> None:
    fake = _ArtifactClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)
    scoped = _turn_origin("discord", event_id="artifact-recovery-origin")

    with scoped_turn_origin(scoped):
        wire_origin = fake.current_origin()
        token = planning_tool._register_artifact_recovery(
            thread_id="planning-thread-1",
            local_path="/gateway/cache/private.pdf",
            origin=wire_origin,
            role="database_schema",
            position=1,
            required=True,
            idempotency_key="hermes-planning-artifact-v1:exact",
            content_type="application/pdf",
            retain_until=None,
            attachment_identity=None,
            ingress_ordinal=None,
        )
        result = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "upload_artifact",
                    "recovery_token": token,
                    "local_path": "/gateway/cache/replacement.pdf",
                }
            )
        )

    assert result["code"] == "planning.artifact_recovery_ambiguous"
    assert "/gateway/cache/private.pdf" not in json.dumps(result)
    assert fake.upload_calls == []


def test_artifact_flow_has_no_total_count_cap(monkeypatch) -> None:
    fake = _ArtifactClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    with scoped_turn_origin(
        _turn_origin("discord", event_id="many-attachments-event")
    ):
        for position in range(1, 138):
            result = json.loads(
                planning_tool._handle_planning_v2(
                    {
                        "action": "upload_artifact",
                        "thread_id": "planning-thread-1",
                        "local_path": (
                            f"/gateway/cache/reference-{position}.png"
                        ),
                        "role": "design_reference",
                        "position": position,
                    }
                )
            )
            assert result["ok"] is True

    assert len(fake.upload_calls) == 137
    assert len(fake.append_calls) == 137
    assert len(
        {call["idempotency_key"] for call in fake.upload_calls}
    ) == 137
    assert len(
        {
            call["origin"]["providerEventId"]
            for call in fake.append_calls
        }
    ) == 137


class _StatusClient:
    runner_id = "runner-1"

    def get_thread(
        self,
        _thread_id: str,
    ) -> PlanningV2Response[dict[str, Any]]:
        return PlanningV2Response(
            200,
            _thread_projection(
                providers=("discord", "slack"),
                input_count=8,
                active_run_id="run-status",
                active_preview_version_id="preview-v3",
            ),
        )

    def get_run(
        self,
        _run_id: str,
    ) -> PlanningV2Response[dict[str, Any]]:
        return PlanningV2Response(
            200,
            _run_projection(
                run_id="run-status",
                work_items=[
                    {
                        "workItemId": "research",
                        "workKind": "research",
                        "scopeKey": "market",
                        "status": "succeeded",
                    },
                    {
                        "workItemId": "architecture",
                        "workKind": "architecture",
                        "scopeKey": "system",
                        "status": "running",
                        "progress": {"phase": "failure modes", "percent": 65},
                    },
                    {
                        "workItemId": "decision",
                        "workKind": "decision",
                        "scopeKey": "storage",
                        "status": "needs_decision",
                        "terminalReason": "Choose artifact durability tier",
                        "progress": {"options": ["R2", "local durable store"]},
                    },
                    {
                        "workItemId": "preview",
                        "workKind": "preview_reduce",
                        "scopeKey": "preview",
                        "status": "succeeded",
                        "acceptedResultId": "result-preview-3",
                    },
                ],
            ),
        )


def test_status_returns_concise_progress_decision_and_preview_facts(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(
        planning_tool,
        "PlanningV2Client",
        lambda: _StatusClient(),
    )

    raw = planning_tool._handle_planning_v2(
        {"action": "status", "thread_id": "planning-thread-1"}
    )
    result = json.loads(raw)

    assert result["progress"] == {
        "total": 4,
        "completed": 2,
        "active": 1,
        "waiting": 1,
        "byStatus": {
            "needs_decision": 1,
            "running": 1,
            "succeeded": 2,
        },
        "facts": [
            {
                "workItemId": "architecture",
                "workKind": "architecture",
                "scopeKey": "system",
                "status": "running",
                "progress": {"phase": "failure modes", "percent": 65},
            },
            {
                "workItemId": "decision",
                "workKind": "decision",
                "scopeKey": "storage",
                "status": "needs_decision",
                "progress": {"options": ["R2", "local durable store"]},
                "reason": "Choose artifact durability tier",
            },
        ],
    }
    assert result["needsDecision"]["required"] is True
    assert result["needsDecision"]["items"][0]["scopeKey"] == "storage"
    assert result["preview"] == {
        "ready": True,
        "status": "ready",
        "versionId": "preview-v3",
        "workItemId": "preview",
        "resultId": "result-preview-3",
    }
    assert "workItems" not in raw


def test_status_rejects_a_run_from_another_thread(monkeypatch) -> None:
    class _MismatchedStatusClient(_StatusClient):
        def get_run(
            self,
            _run_id: str,
        ) -> PlanningV2Response[dict[str, Any]]:
            payload = _run_projection(run_id="run-from-thread-2")
            payload["threadId"] = "planning-thread-2"
            return PlanningV2Response(200, payload)

    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(
        planning_tool,
        "PlanningV2Client",
        lambda: _MismatchedStatusClient(),
    )

    result = json.loads(
        planning_tool._handle_planning_v2(
            {
                "action": "status",
                "thread_id": "planning-thread-1",
                "run_id": "run-from-thread-2",
            }
        )
    )

    assert result["code"] == "planning.run_thread_mismatch"
    assert result["detail"]["threadId"] == "planning-thread-1"
    assert result["detail"]["runThreadId"] == "planning-thread-2"


def test_run_projection_progress_survives_without_embedded_work_items() -> None:
    assert planning_tool._work_progress(
        {
            "progress": {
                "total": 5,
                "completed": 1,
                "states": {
                    "succeeded": 1,
                    "queued": 3,
                    "needs_decision": 1,
                },
            }
        }
    ) == {
        "total": 5,
        "completed": 1,
        "active": 3,
        "waiting": 1,
        "byStatus": {
            "needs_decision": 1,
            "queued": 3,
            "succeeded": 1,
        },
        "facts": [],
    }


class _LostRunResponseClient:
    runner_id = "runner-1"

    def __init__(self) -> None:
        self.run_keys: list[str] = []
        self.run_calls: list[dict[str, Any]] = []
        self.lose_first_response = True

    def current_origin(self) -> dict[str, Any]:
        return planning_origin_from_current_turn(runner_id=self.runner_id)

    def create_thread(
        self,
        **_kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        return PlanningV2Response(
            201,
            _thread_projection(providers=("discord",), input_count=1),
        )

    def create_run(
        self,
        _thread_id: str,
        *,
        idempotency_key: str,
        **kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        self.run_keys.append(idempotency_key)
        self.run_calls.append(
            {"idempotency_key": idempotency_key, **kwargs}
        )
        if self.lose_first_response:
            self.lose_first_response = False
            raise PlanningV2TransportError(
                "planning.hub_timeout",
                detail="response lost after commit",
                ambiguous=True,
                attempts=2,
            )
        return PlanningV2Response(
            200,
            _run_projection(run_id="replayed-run"),
        )

    def get_thread(
        self,
        _thread_id: str,
    ) -> PlanningV2Response[dict[str, Any]]:
        return PlanningV2Response(
            200,
            _thread_projection(
                providers=("discord",),
                input_count=1,
                active_run_id="replayed-run",
            ),
        )

    def get_thread_input_identity(
        self,
        _thread_id: str,
    ) -> dict[str, Any]:
        return {
            "threadId": "planning-thread-1",
            "basisInputSequence": 1,
            "inputDigest": "sha256:" + "1" * 64,
        }


def test_lost_run_response_returns_replay_safe_recovery_action(
    monkeypatch,
) -> None:
    fake = _LostRunResponseClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    with scoped_turn_origin(_turn_origin("discord", event_id="event-1")):
        failure = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "create",
                    "message": "persist this plan",
                    "run_policy": {"quality": "maximum"},
                    "route_policy": {"model": "frontier"},
                    "correlation_id": "correlation-exact",
                },
                user_task="persist this plan",
            )
        )

    recovery = failure["recovery"]["nextAction"]["arguments"]
    assert failure["outcomeAmbiguous"] is True
    assert failure["recovery"]["inputStored"] is True
    assert recovery["action"] == "start_run"
    assert recovery["thread_id"] == "planning-thread-1"
    assert recovery["idempotency_key"] == fake.run_keys[0]
    assert recovery["expected_basis_input_sequence"] == 1
    assert recovery["expected_input_digest"] == "sha256:" + "1" * 64

    recovered = json.loads(planning_tool._handle_planning_v2(recovery))
    assert recovered["runId"] == "replayed-run"
    assert recovered["runReplayed"] is True
    assert fake.run_keys == [
        recovery["idempotency_key"],
        recovery["idempotency_key"],
    ]
    assert fake.run_calls[0] == fake.run_calls[1]
    assert fake.run_calls[0]["expected_basis_input_sequence"] == 1
    assert fake.run_calls[0]["expected_input_digest"] == (
        "sha256:" + "1" * 64
    )
    assert recovery["run_policy"] == {"quality": "maximum"}
    assert recovery["route_policy"] == {"model": "frontier"}
    assert recovery["correlation_id"] == "correlation-exact"


def test_started_run_keeps_recovery_key_when_followup_read_times_out(
    monkeypatch,
) -> None:
    class _RunStartedReadLostClient:
        runner_id = "runner-1"

        def __init__(self) -> None:
            self.run_keys: list[str] = []
            self.lose_first_read = True

        def create_run(
            self,
            _thread_id: str,
            *,
            idempotency_key: str,
            **_kwargs: Any,
        ) -> PlanningV2Response[dict[str, Any]]:
            self.run_keys.append(idempotency_key)
            return PlanningV2Response(
                201 if len(self.run_keys) == 1 else 200,
                _run_projection(run_id="committed-run"),
            )

        def get_thread(
            self,
            _thread_id: str,
        ) -> PlanningV2Response[dict[str, Any]]:
            if self.lose_first_read:
                self.lose_first_read = False
                raise PlanningV2TransportError(
                    "planning.hub_timeout",
                    detail="follow-up read timed out",
                    retryable=True,
                    attempts=2,
                )
            return PlanningV2Response(
                200,
                _thread_projection(
                    providers=("discord",),
                    input_count=1,
                    active_run_id="committed-run",
                ),
            )

    fake = _RunStartedReadLostClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    failure = json.loads(
        planning_tool._handle_planning_v2(
            {
                "action": "start_run",
                "thread_id": "planning-thread-1",
                "idempotency_key": "operator-stable-key",
            }
        )
    )

    assert failure["recovery"]["runStarted"] is True
    recovery = failure["recovery"]["nextAction"]["arguments"]
    assert recovery["idempotency_key"] == "operator-stable-key"

    recovered = json.loads(planning_tool._handle_planning_v2(recovery))
    assert recovered["runId"] == "committed-run"
    assert recovered["runReplayed"] is True
    assert fake.run_keys == ["operator-stable-key", "operator-stable-key"]


class _ApprovalClient:
    runner_id = "runner-1"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def current_origin(self) -> dict[str, Any]:
        return planning_origin_from_current_turn(runner_id=self.runner_id)

    def approve_and_apply_preview(
        self,
        thread_id: str,
        preview_result_id: str,
        **kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        self.calls.append(
            {
                "threadId": thread_id,
                "previewResultId": preview_result_id,
                **kwargs,
            }
        )
        return PlanningV2Response(
            200,
            {
                "ok": True,
                "replayed": len(self.calls) > 1,
                "applyBindingId": "apply-binding-1",
                "threadId": thread_id,
                "runId": "run-1",
                "previewResultId": preview_result_id,
                "previewResultHash": kwargs["expected_preview_hash"],
                "planHash": kwargs["expected_plan_hash"],
                "operationId": "operation-1",
                "status": "requested",
                "operation": {"status": "queued"},
            },
        )


class _PreviewClient:
    runner_id = "runner-1"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.review_calls: list[dict[str, Any]] = []

    def get_preview_page(
        self,
        thread_id: str,
        preview_result_id: str,
        *,
        offset: int,
        limit: int,
    ) -> PlanningV2Response[dict[str, Any]]:
        self.calls.append(
            {
                "threadId": thread_id,
                "previewResultId": preview_result_id,
                "offset": offset,
                "limit": limit,
            }
        )
        tasks = [
            {"stableTaskId": f"task-{index}"}
            for index in range(offset, offset + limit)
        ]
        preview_hash = "sha256:" + "a" * 64
        page_document = {
            "schemaVersion": "planning.preview-page.v1",
            "threadId": thread_id,
            "previewResultId": preview_result_id,
            "previewResultHash": preview_hash,
            "taskCount": 137,
            "offset": offset,
            "count": len(tasks),
            "tasks": tasks,
        }
        page_digest = "sha256:" + hashlib.sha256(
            json.dumps(
                page_document,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        delivery_payload = {
            "schemaVersion": "planning.preview-delivery-payload.v1",
            "threadId": thread_id,
            "runId": "run-1",
            "previewResultId": preview_result_id,
            "previewResultHash": preview_hash,
            "planHash": "1" * 64,
            "basisInputSequence": 4,
            "title": "Planning V2 delivery",
            "objective": "Ship the accepted implementation chain",
            "summary": "Review all tasks before approval.",
            "decisions": [{"code": "ready_for_approval"}],
            "coverage": {"ready": True, "findings": []},
            "taskCount": 137,
            "offset": offset,
            "count": limit,
            "tasks": tasks,
            "pageDigest": page_digest,
            "hasMore": True,
            "nextOffset": offset + limit,
        }
        delivery_payload_digest = "sha256:" + hashlib.sha256(
            json.dumps(
                delivery_payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        delivery_content = "\n\n".join(
            [
                "## Planning V2 delivery",
                "Ship the accepted implementation chain",
                *[
                    f"### {index + 1}. Task {index + 1}"
                    for index, _task in enumerate(tasks, start=offset)
                ],
            ]
        )
        delivery_content_digest = "sha256:" + hashlib.sha256(
            delivery_content.encode()
        ).hexdigest()
        return PlanningV2Response(
            200,
            {
                "threadId": thread_id,
                "runId": "run-1",
                "previewResultId": preview_result_id,
                "previewResultHash": preview_hash,
                "planHash": "1" * 64,
                "basisInputSequence": 4,
                "taskCount": 137,
                "offset": offset,
                "limit": limit,
                "returned": limit,
                "tasks": tasks,
                "pageDigest": page_digest,
                "reviewStatus": {
                    "taskCount": 137,
                    "coveredTaskCount": 0,
                    "coveredRanges": [],
                    "missingRanges": [{"offset": 0, "count": 137}],
                    "complete": False,
                },
                "hasMore": True,
                "nextOffset": offset + limit,
                "title": "Planning V2 delivery",
                "objective": "Ship the accepted implementation chain",
                "summary": "Review all tasks before approval.",
                "decisions": [{"code": "ready_for_approval"}],
                "coverage": {"ready": True, "findings": []},
                "acceptedAt": "2026-07-27T12:30:00Z",
                "approvalEligible": False,
                "deliveryPayload": delivery_payload,
                "deliveryPayloadDigest": delivery_payload_digest,
                "deliveryContent": delivery_content,
                "deliveryContentDigest": delivery_content_digest,
            },
        )

    def current_origin(self) -> dict[str, Any]:
        return {
            "schemaVersion": "1.0",
            "provider": "discord",
            "gatewayInstanceId": "runner-1",
            "gatewayAccountId": "discord-account",
            "chatId": "discord-chat",
            "threadId": None,
            "messageId": "discord-inbound-message",
            "senderId": "discord-user",
            "chatType": "direct",
            "sourceTimestamp": "2026-07-27T12:30:00Z",
            "providerEventId": "discord-event",
        }

    def acknowledge_preview_page(
        self,
        thread_id: str,
        preview_result_id: str,
        **kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        self.review_calls.append(
            {
                "threadId": thread_id,
                "previewResultId": preview_result_id,
                **kwargs,
            }
        )
        return PlanningV2Response(
            201,
            {
                "ok": True,
                "replayed": False,
                "receipt": {
                    "reviewReceiptId": "review-receipt-1",
                    "threadId": thread_id,
                    "previewResultId": preview_result_id,
                    "previewResultHash": kwargs[
                        "expected_preview_hash"
                    ],
                    "offset": kwargs["offset"],
                    "count": kwargs["count"],
                    "pageDigest": kwargs["page_digest"],
                },
                "reviewStatus": {
                    "taskCount": 137,
                    "coveredTaskCount": 50,
                    "coveredRanges": [{"offset": 0, "count": 50}],
                    "missingRanges": [{"offset": 50, "count": 87}],
                    "complete": False,
                },
                "approvalEligible": False,
            },
        )


def test_preview_fetch_is_read_only_until_exact_provider_delivery(
    monkeypatch,
) -> None:
    fake = _PreviewClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    token = bind_preview_delivery_generation("session-1", 7)
    try:
        result = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "preview",
                    "thread_id": "planning-thread-1",
                    "preview_result_id": "preview-1",
                    "offset": 0,
                    "limit": 50,
                }
            )
        )
    finally:
        reset_preview_delivery_generation(token)

    assert fake.calls == [
        {
            "threadId": "planning-thread-1",
            "previewResultId": "preview-1",
            "offset": 0,
            "limit": 50,
        }
    ]
    assert result["previewResultHash"] == "sha256:" + "a" * 64
    assert result["planHash"] == "1" * 64
    assert result["taskCount"] == 137
    assert len(result["tasks"]) == 50
    assert result["pageReviewed"] is False
    assert result["deliveryReceiptPending"] is True
    assert result["reviewStatus"]["coveredTaskCount"] == 0
    assert result["approvalEligible"] is False
    assert fake.review_calls == []
    assert result["nextActionAfterDelivery"] == {
        "tool": planning_tool.PLANNING_V2_TOOL_NAME,
        "arguments": {
            "action": "preview",
            "thread_id": "planning-thread-1",
            "preview_result_id": "preview-1",
            "offset": 50,
            "limit": 50,
        },
    }
    outbound = prepare_preview_delivery_content(
        "session-1",
        7,
        "Here is the page.",
    )
    exact_content = fake.get_preview_page(
        "planning-thread-1",
        "preview-1",
        offset=0,
        limit=50,
    ).payload["deliveryContent"]
    assert exact_content in outbound
    assert "planning-thread-1" not in outbound
    assert '"threadId"' not in outbound
    completed = asyncio.run(
        complete_preview_delivery(
            "session-1",
            7,
            delivered_content=outbound,
            result=SimpleNamespace(
                success=True,
                message_id="discord-outbound-final",
                continuation_message_ids=(
                    "discord-outbound-first",
                    "discord-outbound-final",
                ),
            ),
            delivered_at="2026-07-27T12:30:00+00:00",
        )
    )
    assert completed is True
    assert len(fake.review_calls) == 1
    proof = fake.review_calls[0]["delivery_proof"]
    assert proof["providerMessageIds"] == [
        "discord-outbound-first",
        "discord-outbound-final",
    ]


def test_lost_preview_receipt_response_replays_exact_page_before_progressing(
    monkeypatch,
) -> None:
    class _LostReceiptClient(_PreviewClient):
        def __init__(self) -> None:
            super().__init__()
            self.receipt_keys: list[str] = []
            self.delivery_proofs: list[dict[str, Any]] = []
            self.lose_first_response = True

        def acknowledge_preview_page(
            self,
            thread_id: str,
            preview_result_id: str,
            **kwargs: Any,
        ) -> PlanningV2Response[dict[str, Any]]:
            self.receipt_keys.append(kwargs["idempotency_key"])
            self.delivery_proofs.append(kwargs["delivery_proof"])
            if self.lose_first_response:
                self.lose_first_response = False
                raise PlanningV2TransportError(
                    "planning.hub_timeout",
                    detail="review receipt response lost after commit",
                    retryable=True,
                    ambiguous=True,
                    attempts=2,
                )
            return super().acknowledge_preview_page(
                thread_id,
                preview_result_id,
                **kwargs,
            )

    fake = _LostReceiptClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)
    arguments = {
        "action": "preview",
        "thread_id": "planning-thread-1",
        "preview_result_id": "preview-1",
        "offset": 0,
        "limit": 50,
    }

    token = bind_preview_delivery_generation("session-lost-ack", 11)
    try:
        page = json.loads(planning_tool._handle_planning_v2(arguments))
    finally:
        reset_preview_delivery_generation(token)
    outbound = prepare_preview_delivery_content(
        "session-lost-ack",
        11,
        "Exact preview follows.",
    )
    completed = asyncio.run(
        complete_preview_delivery(
            "session-lost-ack",
            11,
            delivered_content=outbound,
            result=SimpleNamespace(
                success=True,
                message_id="discord-outbound-1",
                continuation_message_ids=(),
            ),
            delivered_at="2026-07-27T12:30:00+00:00",
        )
    )
    assert page["pageReviewed"] is False
    assert completed is True
    assert fake.receipt_keys[0] == fake.receipt_keys[1]
    assert fake.receipt_keys[0].startswith(
        "hermes-planning-preview-review-v1:"
    )
    assert fake.delivery_proofs[0] == fake.delivery_proofs[1]


def test_preview_tool_schema_requires_review_before_approval() -> None:
    schema = planning_tool.PLANNING_V2_SCHEMA
    properties = schema["parameters"]["properties"]
    description = schema["description"]

    assert "preview" in properties["action"]["enum"]
    assert properties["offset"]["minimum"] == 0
    assert properties["limit"]["minimum"] == 1
    assert "show/review every page in order" in description
    assert "Never approve tasks the user has not seen" in description
    assert "approvalEligible=false" in description


def test_artifact_tool_schema_enforces_upload_before_run_workflow() -> None:
    schema = planning_tool.PLANNING_V2_SCHEMA
    properties = schema["parameters"]["properties"]
    description = schema["description"]

    assert "upload_artifact" in properties["action"]["enum"]
    assert properties["position"]["minimum"] == 1
    assert "maximum" not in properties["position"]
    assert "create the thread with start_run=false" in description
    assert "there is no total artifact-count cap" in description
    assert "never put file bytes or base64" in description
    assert "upload all of them before calling start_run" in (
        properties["start_run"]["description"]
    )


def test_approval_uses_exact_current_turn_and_cross_provider_origin(
    monkeypatch,
) -> None:
    fake = _ApprovalClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)
    exact_user_turn = "Tvirtinu būtent preview-1 planą."

    with scoped_turn_origin(_turn_origin("slack", event_id="approval-event")):
        result = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "approve_apply",
                    "thread_id": "planning-thread-1",
                    "preview_result_id": "preview-1",
                    "expected_preview_hash": "sha256:preview-1",
                    "expected_plan_hash": "sha256:plan-1",
                    "approval_message": "modelio perrašytas tekstas",
                    "approval_evidence": {"verbatimClause": "Tvirtinu"},
                },
                user_task=exact_user_turn,
            )
        )

    call = fake.calls[0]
    assert call["approval_message"] == exact_user_turn
    assert call["origin"]["provider"] == "slack"
    assert call["origin"]["gatewayAccountId"] == "slack-account"
    assert call["origin"]["providerEventId"] == "approval-event"
    assert call["idempotency_key"].startswith(
        "hermes-planning-approval-v1:"
    )
    assert exact_user_turn not in call["idempotency_key"]
    assert result == {
        "ok": True,
        "action": "approve_apply",
        "threadId": "planning-thread-1",
        "runId": "run-1",
        "previewResultId": "preview-1",
        "previewResultHash": "sha256:preview-1",
        "planHash": "sha256:plan-1",
        "applyBindingId": "apply-binding-1",
        "operationId": "operation-1",
        "status": "requested",
        "replayed": False,
        "operationStatus": "queued",
    }


def test_approval_requires_current_scoped_origin_before_hub_write(
    monkeypatch,
) -> None:
    fake = _ApprovalClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    result = json.loads(
        planning_tool._handle_planning_v2(
            {
                "action": "approve_apply",
                "thread_id": "planning-thread-1",
                "preview_result_id": "preview-1",
                "expected_preview_hash": "sha256:preview-1",
                "expected_plan_hash": "sha256:plan-1",
                "approval_message": "Tvirtinu",
            }
        )
    )

    assert result["code"] == "planning.origin_missing"
    assert fake.calls == []


def test_same_turn_approval_rejection_passes_through_without_retry(
    monkeypatch,
) -> None:
    fake = _ApprovalClient()

    def _reject(*_args: Any, **_kwargs: Any):
        raise PlanningV2HTTPError(
            "planning.same_turn_approval_forbidden",
            status=409,
            detail="Approval must arrive in a later user turn.",
        )

    monkeypatch.setattr(fake, "approve_and_apply_preview", _reject)
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    with scoped_turn_origin(_turn_origin("discord", event_id="same-turn")):
        result = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "approve_apply",
                    "thread_id": "planning-thread-1",
                    "preview_result_id": "preview-1",
                    "expected_preview_hash": "sha256:preview-1",
                    "expected_plan_hash": "sha256:plan-1",
                },
                user_task="Tvirtinu",
            )
        )

    assert result["code"] == "planning.same_turn_approval_forbidden"
    assert result["httpStatus"] == 409
    assert result["detail"] == "Approval must arrive in a later user turn."
    assert "nextAction" not in result["recovery"]


def test_lost_approval_response_has_exact_replay_recovery(
    monkeypatch,
) -> None:
    class _LostApprovalClient(_ApprovalClient):
        def approve_and_apply_preview(
            self,
            thread_id: str,
            preview_result_id: str,
            **kwargs: Any,
        ) -> PlanningV2Response[dict[str, Any]]:
            if not self.calls:
                self.calls.append(
                    {
                        "threadId": thread_id,
                        "previewResultId": preview_result_id,
                        **kwargs,
                    }
                )
                raise PlanningV2TransportError(
                    "planning.hub_timeout",
                    detail="approval response lost after commit",
                    retryable=True,
                    ambiguous=True,
                    attempts=2,
                )
            return super().approve_and_apply_preview(
                thread_id,
                preview_result_id,
                **kwargs,
            )

    fake = _LostApprovalClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)
    initial = {
        "action": "approve_apply",
        "thread_id": "planning-thread-1",
        "preview_result_id": "preview-1",
        "expected_preview_hash": "sha256:preview-1",
        "expected_plan_hash": "sha256:plan-1",
        "approval_evidence": {"verbatimClause": "Tvirtinu"},
    }

    with scoped_turn_origin(_turn_origin("discord", event_id="approval-event")):
        failed = json.loads(
            planning_tool._handle_planning_v2(
                initial,
                user_task="Tvirtinu",
            )
        )
        recovery = failed["recovery"]["nextAction"]
        recovered = json.loads(
            planning_tool._handle_planning_v2(recovery["arguments"])
        )

    assert recovery["tool"] == planning_tool.PLANNING_V2_TOOL_NAME
    assert recovery["arguments"]["action"] == "approve_apply"
    assert recovery["arguments"]["approval_message"] == "Tvirtinu"
    assert recovery["arguments"]["idempotency_key"].startswith(
        "hermes-planning-approval-v1:"
    )
    assert len(fake.calls) == 2
    assert fake.calls[0]["idempotency_key"] == fake.calls[1][
        "idempotency_key"
    ]
    assert fake.calls[0]["origin"] == fake.calls[1]["origin"]
    assert recovered["replayed"] is True
    assert recovered["operationId"] == "operation-1"


def test_approval_tool_schema_requires_later_explicit_exact_preview() -> None:
    schema = planning_tool.PLANNING_V2_SCHEMA
    properties = schema["parameters"]["properties"]

    assert "approve_apply" in properties["action"]["enum"]
    assert {
        "preview_result_id",
        "expected_preview_hash",
        "expected_plan_hash",
        "approval_message",
        "approval_evidence",
    } <= set(properties)
    description = schema["description"]
    assert "exact preview id/hash and plan hash" in description
    assert "later conversation turn" in description
    assert "never auto-approve" in description


def test_permanent_run_rejection_does_not_offer_a_retry_loop(
    monkeypatch,
) -> None:
    fake = _LostRunResponseClient()

    def _reject_run(
        _thread_id: str,
        *,
        idempotency_key: str,
        **_kwargs: Any,
    ) -> PlanningV2Response[dict[str, Any]]:
        fake.run_keys.append(idempotency_key)
        raise PlanningV2HTTPError(
            "planning.idempotency_key_conflict",
            status=409,
            detail="the key belongs to a different request",
        )

    monkeypatch.setattr(fake, "create_run", _reject_run)
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: True,
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    with scoped_turn_origin(_turn_origin("discord", event_id="event-1")):
        failure = json.loads(
            planning_tool._handle_planning_v2(
                {"action": "create", "message": "persist this plan"},
                user_task="persist this plan",
            )
        )

    assert failure["code"] == "planning.idempotency_key_conflict"
    assert failure["recovery"]["inputStored"] is True
    assert "nextAction" not in failure["recovery"]


def test_tool_is_not_available_without_explicit_profile_opt_in(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda _provider=None: False,
    )

    def _must_not_construct():
        raise AssertionError("client must not be constructed while disabled")

    monkeypatch.setattr(
        planning_tool,
        "PlanningV2Client",
        _must_not_construct,
    )

    result = json.loads(
        planning_tool._handle_planning_v2(
            {"action": "status", "thread_id": "planning-thread-1"}
        )
    )

    assert result["code"] == "planning.tool_not_enabled"
    assert "normal Hermes conversation is unchanged" in result["error"]


def test_profile_opt_in_is_provider_scoped(monkeypatch) -> None:
    from hermes_cli import config

    monkeypatch.setattr(
        config,
        "load_config",
        lambda: {
            "platform_toolsets": {
                "discord": ["hermes-discord", "planning_v2"],
                "slack": ["hermes-slack"],
            }
        },
    )

    assert planning_tool._profile_opted_in() is True
    assert planning_tool._profile_opted_in("discord") is True
    assert planning_tool._profile_opted_in("slack") is False
    assert planning_tool._profile_opted_in("telegram") is False


def test_write_is_refused_when_current_provider_is_not_opted_in(
    monkeypatch,
) -> None:
    fake = _CrossProviderClient()
    monkeypatch.setattr(
        planning_tool,
        "_profile_opted_in",
        lambda provider=None: provider in {None, "discord"},
    )
    monkeypatch.setattr(planning_tool, "PlanningV2Client", lambda: fake)

    with scoped_turn_origin(_turn_origin("slack", event_id="slack-event")):
        result = json.loads(
            planning_tool._handle_planning_v2(
                {
                    "action": "continue",
                    "thread_id": "planning-thread-1",
                    "message": "do not append",
                }
            )
        )

    assert result["code"] == "planning.tool_not_enabled_for_provider"
    assert fake.append_origins == []


def test_planning_toolset_is_valid_but_not_part_of_normal_core() -> None:
    assert validate_toolset("planning_v2") is True
    assert resolve_toolset("planning_v2") == [
        planning_tool.PLANNING_V2_TOOL_NAME
    ]

    from hermes_cli.tools_config import _get_platform_tools
    from toolsets import _HERMES_CORE_TOOLS

    assert planning_tool.PLANNING_V2_TOOL_NAME not in _HERMES_CORE_TOOLS
    assert "planning_v2" not in _get_platform_tools({}, "discord")
    enabled = _get_platform_tools(
        {
            "platform_toolsets": {
                "discord": ["hermes-discord", "planning_v2"],
            }
        },
        "discord",
    )
    assert "planning_v2" in enabled
