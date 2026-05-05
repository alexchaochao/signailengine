from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from typing import cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from core.pipeline import PipelineWorker
from sentinel.onchain_listener import build_onchain_event
from sentinel.wallet_tracker import build_wallet_event


class MemoryRedis:
    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.groups: set[tuple[str, str]] = set()
        self.acked: list[tuple[str, str, str]] = []
        self.counter = 0

    def xadd(self, stream_name: str, mapping: dict[str, str]) -> str:
        self.counter += 1
        message_id = f"{self.counter}-0"
        self.streams.setdefault(stream_name, []).append((message_id, mapping))
        return message_id

    def xgroup_create(
        self,
        stream_name: str,
        group_name: str,
        id: str = "0-0",
        mkstream: bool = False,
    ) -> bool:
        _ = id
        if mkstream:
            self.streams.setdefault(stream_name, [])
        self.groups.add((stream_name, group_name))
        return True

    def xreadgroup(
        self,
        group_name: str,
        consumer_name: str,
        streams: dict[str, str],
        count: int = 100,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        _ = group_name, consumer_name, block
        stream_name = next(iter(streams))
        return [(stream_name, self.streams.get(stream_name, [])[:count])]

    def xack(self, stream_name: str, group_name: str, message_id: str) -> int:
        self.acked.append((stream_name, group_name, message_id))
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a deterministic Arbitrum paper execution smoke")
    parser.add_argument("--token", default="ARB")
    parser.add_argument("--chain", default="arbitrum")
    parser.add_argument("--json", action="store_true")
    return parser


def run_smoke(settings: AppSettings, *, token: str, chain: str) -> dict[str, object]:
    paper_settings = settings.model_copy(
        update={
            "runtime": settings.runtime.model_copy(update={"environment": "paper"}),
            "risk": settings.risk.model_copy(update={"live_trading_enabled": False}),
            "venues": settings.venues.model_copy(
                update={
                    "dex_adapter": "evm_primary",
                    "paper_execution_enabled": True,
                }
            ),
        }
    )
    worker = PipelineWorker(
        paper_settings,
        cast(Redis, MemoryRedis()),
        db_engine=create_engine("sqlite:///:memory:"),
    )
    worker.ensure_streams("paper-smoke")

    observed_at = datetime.now(UTC)
    result = worker.process_events(
        [
            build_onchain_event(
                {
                    "chain": chain,
                    "token": token,
                    "observed_at": observed_at,
                    "liquidity_usd": 180_000.0,
                    "volume_5m_usd": 60_000.0,
                    "buy_pressure": 0.82,
                    "estimated_slippage_bps": 90.0,
                }
            ),
            build_wallet_event(
                {
                    "chain": chain,
                    "token": token,
                    "observed_at": observed_at,
                    "wallet_inflow_score": 0.70,
                }
            ),
        ]
    )
    return {
        "signal_chain": result.signal.chain,
        "signal_token": result.signal.token,
        "route": result.route.route,
        "venue": None if result.route.intent is None else result.route.intent.venue,
        "risk_allowed": result.risk.allowed,
        "execution_status": None if result.execution is None else result.execution.status,
        "execution_message": None if result.execution is None else result.execution.message,
        "simulation": None if result.execution is None else result.execution.simulation,
        "executed_notional_usd": None if result.execution is None else result.execution.executed_notional_usd,
    }


def main() -> int:
    args = build_parser().parse_args()
    report = run_smoke(AppSettings.load(), token=args.token, chain=args.chain)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        for key, value in report.items():
            print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())