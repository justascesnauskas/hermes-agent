"""Shared signal-cli send-result validation.

Both the live gateway adapter and the standalone ``hermes send`` rail must
classify recipient-level results identically before a timestamp can become a
semantic delivery receipt.
"""
from __future__ import annotations

from typing import Any


def validate_signal_send_result(
    result: Any,
) -> tuple[bool, str | None]:
    """Validate every structured recipient result returned by signal-cli."""

    if not result or not isinstance(result, dict):
        return True, None

    results = result.get("results")
    if isinstance(results, list):
        for recipient_result in results:
            if not isinstance(recipient_result, dict):
                continue
            result_type = recipient_result.get("type")
            if result_type and result_type != "SUCCESS":
                return False, str(result_type)
            if (
                "success" in recipient_result
                and not recipient_result.get("success")
            ):
                failure = recipient_result.get("failure")
                if failure:
                    return False, str(failure)
                return False, "Recipient delivery failed"
    return True, None
