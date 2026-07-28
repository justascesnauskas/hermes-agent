"""Fresh-process DingTalk semantic-route retry harness."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from gateway.config import PlatformConfig
from gateway.platform_registry import PlatformEntry, platform_registry
from hermes_cli.semantic_delivery import (
    dispatch_due_semantic_delivery_retries,
)
from plugins.platforms.dingtalk.adapter import DingTalkAdapter


async def _run(
    ledger: Path,
    *,
    account: str,
    mode: str,
    webhook: str,
) -> dict[str, int]:
    platform_registry.register(
        PlatformEntry(
            name="dingtalk",
            label="DingTalk",
            adapter_factory=lambda config: None,
            check_fn=lambda: True,
            semantic_exact_attempt=False,
            live_semantic_exact_attempt=True,
        )
    )
    adapter = DingTalkAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "client_id": "dt-process-app",
                "client_secret": "dt-process-secret",
                "gateway_account_id": account,
            },
        )
    )
    calls: list[str] = []

    class _Response:
        status_code = 200
        headers = {}

        @staticmethod
        def json():
            return {"errcode": 0, "errmsg": "ok"}

    async def _post(url: str, **_kwargs):
        calls.append(url)
        return _Response()

    adapter._http_client = SimpleNamespace(post=_post)
    if mode == "rebind":
        adapter._session_webhooks["chat-dt-process"] = (webhook, 0)
    result = await dispatch_due_semantic_delivery_retries(
        bound_adapters={"dingtalk": adapter},
        ledger_path=ledger,
        due_before=None if mode == "missing" else 10**20,
    )
    result["provider_calls"] = len(calls)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("ledger", type=Path)
    parser.add_argument("account")
    parser.add_argument("mode", choices=("missing", "rebind"))
    parser.add_argument("webhook")
    args = parser.parse_args()
    result = asyncio.run(
        _run(
            args.ledger,
            account=args.account,
            mode=args.mode,
            webhook=args.webhook,
        )
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
