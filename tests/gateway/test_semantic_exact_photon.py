from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.config import Platform
from gateway.semantic_exact_attempt import (
    LiveSemanticExactAttemptRequest,
    coerce_live_semantic_exact_attempt_provider_route,
    live_semantic_exact_attempt_encoding_contract,
    owns_live_semantic_exact_attempt,
)
from hermes_cli.semantic_delivery import SEMANTIC_DELIVERY_CONTRACT
from plugins.platforms.photon.adapter import (
    _PHOTON_SEMANTIC_EXACT_ENCODING,
    PhotonAdapter,
    _photon_semantic_exact_digest,
    register,
)


def _adapter(*, response: tuple[int, object] | None = None) -> PhotonAdapter:
    adapter = object.__new__(PhotonAdapter)
    adapter.platform = Platform("photon")
    adapter._http_client = object()
    adapter._sidecar_bind = "127.0.0.1"
    adapter._sidecar_port = 8789
    adapter._sidecar_token = "sidecar-secret"
    adapter._semantic_exact_sidecar_post = AsyncMock(
        return_value=response
        or (
            200,
            {
                "ok": True,
                "messageId": "photon-message-123",
                "deliveryId": "delivery-photon-1",
                "deliveryUnit": 0,
                "routeKind": "dm_phone",
                "wireEncoding": _PHOTON_SEMANTIC_EXACT_ENCODING,
                "contentDigest": _photon_semantic_exact_digest(
                    space_id="any;-;+37060000000",
                    text="**Planning preview**\nApprove or revise.",
                    delivery_id="delivery-photon-1",
                    delivery_unit=0,
                ),
            },
        )
    )
    adapter._record_sent_message = MagicMock()
    return adapter


def _request(
    adapter: PhotonAdapter,
    *,
    content: str = "**Planning preview**\nApprove or revise.",
    route: dict[str, str] | None = None,
) -> LiveSemanticExactAttemptRequest:
    return LiveSemanticExactAttemptRequest(
        chat_id="any;-;+37060000000",
        content=content,
        delivery_contract=SEMANTIC_DELIVERY_CONTRACT,
        delivery_id="delivery-photon-1",
        delivery_target="photon:any;-;+37060000000",
        delivery_unit=0,
        encoding_contract=live_semantic_exact_attempt_encoding_contract(
            adapter
        ),
        provider_route=coerce_live_semantic_exact_attempt_provider_route(
            route
            or {
                "message_mode": "flat",
                "space_kind": "dm_phone",
                "transport": "photon_sidecar_exact",
            }
        ),
        reply_to="inbound-photon-message",
    )


def test_photon_capability_and_registration_are_live() -> None:
    adapter = _adapter()
    capability = live_semantic_exact_attempt_encoding_contract(adapter)

    assert owns_live_semantic_exact_attempt(adapter) is True
    assert capability.provider == "photon"
    assert capability.max_logical_units == 8000
    assert capability.length_semantics == "unicode_codepoints"
    assert (
        capability.wire_encoding
        == _PHOTON_SEMANTIC_EXACT_ENCODING
    )

    captured = {}
    context = SimpleNamespace(
        register_platform=lambda **kwargs: captured.update(kwargs),
        register_cli_command=lambda **kwargs: None,
    )
    register(context)
    assert captured["semantic_exact_attempt"] is False
    assert captured["live_semantic_exact_attempt"] is True


def test_photon_route_binding_freezes_one_resolution_branch() -> None:
    adapter = _adapter()

    assert adapter.bind_semantic_exact_attempt_provider_route(
        chat_id="+37060000000"
    ) == {
        "message_mode": "flat",
        "space_kind": "dm_phone",
        "transport": "photon_sidecar_exact",
    }
    assert adapter.bind_semantic_exact_attempt_provider_route(
        chat_id="opaque-group-space"
    ) == {
        "message_mode": "flat",
        "space_kind": "space_id",
        "transport": "photon_sidecar_exact",
    }


@pytest.mark.asyncio
async def test_photon_exact_attempt_preserves_content_and_receipt() -> None:
    adapter = _adapter()

    result = await adapter.send_semantic_exact_attempt(_request(adapter))

    assert result.success is True
    assert result.message_id == "photon-message-123"
    adapter._semantic_exact_sidecar_post.assert_awaited_once()
    payload = adapter._semantic_exact_sidecar_post.await_args.args[0]
    assert payload == {
        "spaceId": "any;-;+37060000000",
        "text": "**Planning preview**\nApprove or revise.",
        "deliveryId": "delivery-photon-1",
        "deliveryUnit": 0,
        "routeKind": "dm_phone",
        "wireEncoding": _PHOTON_SEMANTIC_EXACT_ENCODING,
        "contentDigest": _photon_semantic_exact_digest(
            space_id="any;-;+37060000000",
            text="**Planning preview**\nApprove or revise.",
            delivery_id="delivery-photon-1",
            delivery_unit=0,
        ),
    }
    adapter._record_sent_message.assert_called_once_with(
        "photon-message-123"
    )


@pytest.mark.asyncio
async def test_photon_opaque_provider_receipt_is_preserved_exactly() -> None:
    message_id = "  photon-" + ("訊" * 2_048) + "-receipt  "
    adapter = _adapter()
    status, response = adapter._semantic_exact_sidecar_post.return_value
    response = {**response, "messageId": message_id}
    adapter._semantic_exact_sidecar_post.return_value = (status, response)

    result = await adapter.send_semantic_exact_attempt(_request(adapter))

    assert result.success is True
    assert result.message_id == message_id
    adapter._semantic_exact_sidecar_post.assert_awaited_once()
    adapter._record_sent_message.assert_called_once_with(message_id)


@pytest.mark.parametrize(
    "message_id",
    [42, "photon\tcontrol", "photon\ud800surrogate"],
)
@pytest.mark.asyncio
async def test_photon_unsafe_or_non_string_provider_receipt_is_ambiguous(
    message_id,
) -> None:
    adapter = _adapter()
    status, response = adapter._semantic_exact_sidecar_post.return_value
    response = {**response, "messageId": message_id}
    adapter._semantic_exact_sidecar_post.return_value = (status, response)

    result = await adapter.send_semantic_exact_attempt(_request(adapter))

    assert result.success is False
    assert result.error == "semantic_delivery_provider_receipt_invalid"
    assert "provider_write_attempted" not in result.raw_response
    adapter._semantic_exact_sidecar_post.assert_awaited_once()
    adapter._record_sent_message.assert_not_called()


@pytest.mark.asyncio
async def test_photon_invalid_and_oversize_are_zero_write() -> None:
    adapter = _adapter()

    oversize = await adapter.send_semantic_exact_attempt(
        _request(adapter, content="x" * 8001)
    )
    wrong_route = await adapter.send_semantic_exact_attempt(
        _request(
            adapter,
            route={
                "message_mode": "flat",
                "space_kind": "space_id",
                "transport": "photon_sidecar_exact",
            },
        )
    )

    assert oversize.success is False
    assert oversize.raw_response["provider_write_attempted"] is False
    assert wrong_route.success is False
    assert wrong_route.raw_response["provider_write_attempted"] is False
    adapter._semantic_exact_sidecar_post.assert_not_awaited()


@pytest.mark.asyncio
async def test_photon_prewrite_sidecar_rejection_is_retryable() -> None:
    adapter = _adapter(
        response=(
            503,
            {
                "ok": False,
                "error": "photon_semantic_exact_route_unavailable",
                "providerWriteAttempted": False,
                "retryable": True,
            },
        )
    )

    result = await adapter.send_semantic_exact_attempt(_request(adapter))

    assert result.success is False
    assert result.retryable is True
    assert result.raw_response["provider_write_attempted"] is False
    assert result.raw_response["provider_retryable"] is True
    adapter._semantic_exact_sidecar_post.assert_awaited_once()


@pytest.mark.asyncio
async def test_photon_sidecar_rejection_keeps_redacted_digest_evidence() -> None:
    secret = "ghp_" + ("p" * 80)
    provider_body = {
        "ok": False,
        "error": "provider rejected exact message",
        "token": secret,
        "detail": "provider failure " * 40,
        "providerWriteAttempted": False,
        "retryable": False,
    }
    adapter = _adapter(response=(422, provider_body))

    result = await adapter.send_semantic_exact_attempt(_request(adapter))

    rejection = result.raw_response["provider_rejection"]
    assert result.success is False
    assert result.raw_response["provider_write_attempted"] is False
    assert rejection["truncated"] is True
    assert rejection["redacted"] is True
    assert secret not in rejection["body_preview"]
    assert rejection["body_sha256"] in result.error
    adapter._semantic_exact_sidecar_post.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        (502, {"ok": False, "providerWriteAttempted": True}),
        (500, None),
        (200, {"ok": True, "messageId": None}),
    ],
)
async def test_photon_postwrite_or_invalid_receipt_is_ambiguous(
    response: tuple[int, object],
) -> None:
    adapter = _adapter(response=response)

    result = await adapter.send_semantic_exact_attempt(_request(adapter))

    assert result.success is False
    assert "provider_write_attempted" not in result.raw_response
    assert result.retryable is not True
    adapter._semantic_exact_sidecar_post.assert_awaited_once()


@pytest.mark.asyncio
async def test_photon_transport_response_loss_never_resends() -> None:
    adapter = _adapter()
    adapter._semantic_exact_sidecar_post = AsyncMock(
        side_effect=RuntimeError("loopback response lost")
    )

    result = await adapter.send_semantic_exact_attempt(_request(adapter))

    assert result.success is False
    assert result.raw_response == {}
    adapter._semantic_exact_sidecar_post.assert_awaited_once()


@pytest.mark.asyncio
async def test_photon_exact_http_disables_redirects() -> None:
    adapter = _adapter()
    response = MagicMock(status_code=200)
    response.json.return_value = {"ok": True}
    client = AsyncMock()
    client.post.return_value = response
    context = AsyncMock()
    context.__aenter__.return_value = client
    context.__aexit__.return_value = False

    with patch(
        "plugins.platforms.photon.adapter.httpx.AsyncClient",
        return_value=context,
    ) as constructor:
        status, data, response_body = (
            await PhotonAdapter._semantic_exact_sidecar_post(
            adapter,
            {"text": "preview"},
        )
        )

    assert status == 200
    assert data == {"ok": True}
    assert response_body == response.text
    constructor.assert_called_once_with(
        timeout=30.0,
        follow_redirects=False,
    )
    client.post.assert_awaited_once_with(
        "http://127.0.0.1:8789/send-exact",
        json={"text": "preview"},
        headers={"X-Hermes-Sidecar-Token": "sidecar-secret"},
    )


def test_photon_sidecar_exact_provider_boundary_suite() -> None:
    suite = (
        Path(__file__).parents[2]
        / "plugins/platforms/photon/sidecar/semantic_exact.test.mjs"
    )
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for the Photon sidecar")
    completed = subprocess.run(
        [node, "--test", str(suite)],
        cwd=suite.parent,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, (
        completed.stdout + "\n" + completed.stderr
    )
