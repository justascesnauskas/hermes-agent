"""Crash after leasing a preview ACK, before recording its Hub response."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

from hermes_cli.planning_preview_ack_outbox import claim_preview_ack


def main() -> None:
    ack_id, observation_path = sys.argv[1:3]
    claim = claim_preview_ack(ack_id)
    if claim is None:
        raise RuntimeError("preview acknowledgement was not claimable")
    Path(observation_path).write_text(
        json.dumps(
            {
                "ackId": claim.ack_id,
                "request": claim.request,
                "attempts": claim.attempts,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    # Model: Hub committed the idempotent request, then this process vanished
    # before its local success settlement.
    os._exit(73)


if __name__ == "__main__":
    main()
