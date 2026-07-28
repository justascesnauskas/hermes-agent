"""No-silent-loss contracts for exact provider rejection evidence."""

from __future__ import annotations

import hashlib

from gateway.semantic_exact_attempt import (
    provider_protocol_rejection_evidence,
    provider_rejection_error,
    provider_rejection_evidence,
)


def test_long_rejection_keeps_digest_counts_and_explicit_truncation() -> None:
    secret = "ghp_" + ("a" * 80)
    body = (
        '{"error":"invalid request","authorization":"Bearer '
        + secret
        + '","detail":"'
        + ("provider detail " * 80)
        + '"}'
    )

    evidence = provider_rejection_evidence(
        provider="Example",
        status=422,
        body=body,
    )

    assert evidence["schema_version"] == (
        "hermes.provider-rejection-evidence/1"
    )
    assert evidence["status"] == 422
    assert evidence["body_representation"] == "utf8_text"
    assert evidence["body_sha256"] == (
        "sha256:" + hashlib.sha256(body.encode("utf-8")).hexdigest()
    )
    assert evidence["body_bytes"] == len(body.encode("utf-8"))
    assert evidence["body_characters"] == len(body)
    assert evidence["truncated"] is True
    assert evidence["redacted"] is True
    assert len(evidence["body_preview"]) <= 200
    assert secret not in evidence["body_preview"]
    assert body[:200] not in provider_rejection_error(evidence)
    assert evidence["body_sha256"] in provider_rejection_error(evidence)


def test_short_rejection_is_complete_and_mapping_is_deterministic() -> None:
    evidence = provider_rejection_evidence(
        provider="Example",
        status=400,
        body={"message": "field is required", "field": "project"},
    )

    assert evidence["body_preview"] == (
        '{"field":"project","message":"field is required"}'
    )
    assert evidence["body_representation"] == "canonical_json"
    assert evidence["truncated"] is False
    assert evidence["body_bytes"] == len(
        evidence["body_preview"].encode("utf-8")
    )


def test_native_protocol_rejection_is_typed_without_fake_http_status() -> None:
    response = {
        "type": "chatCmdError",
        "chatError": {"type": "error", "message": "denied"},
    }

    evidence = provider_protocol_rejection_evidence(
        provider="SimpleX",
        protocol="simplex-json-websocket",
        response=response,
    )

    assert evidence["schema_version"] == (
        "hermes.provider-protocol-rejection-evidence/1"
    )
    assert "status" not in evidence
    assert evidence["protocol"] == "simplex-json-websocket"
    assert evidence["response_representation"] == "canonical_json"
    assert evidence["response_preview"] == (
        '{"chatError":{"message":"denied","type":"error"},'
        '"type":"chatCmdError"}'
    )
    assert evidence["response_sha256"] in provider_rejection_error(
        evidence
    )


def test_representation_discriminator_never_claims_reconstructed_json_is_wire() -> None:
    first_wire = b'{ "z": 2, "a": 1 }'
    second_wire = b'{"a":1, "z":2}'
    decoded = {"z": 2, "a": 1}

    first_evidence = provider_rejection_evidence(
        provider="Example",
        status=400,
        body=first_wire,
    )
    second_evidence = provider_rejection_evidence(
        provider="Example",
        status=400,
        body=second_wire,
    )
    decoded_evidence = provider_rejection_evidence(
        provider="Example",
        status=400,
        body=decoded,
    )

    assert first_evidence["body_representation"] == "raw_bytes"
    assert second_evidence["body_representation"] == "raw_bytes"
    assert first_evidence["body_sha256"] != second_evidence["body_sha256"]
    assert decoded_evidence["body_representation"] == "canonical_json"
    assert decoded_evidence["body_sha256"] not in {
        first_evidence["body_sha256"],
        second_evidence["body_sha256"],
    }
