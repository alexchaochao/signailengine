from __future__ import annotations

import os

from datetime import UTC, datetime
from typing import cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from core.pipeline import PipelineWorker
from core.schemas import EventEnvelope, TokenState


def _load_default_settings(monkeypatch) -> AppSettings:
    for key in list(os.environ):
        if key.startswith("SIGNALENGINE_"):
            monkeypatch.delenv(key, raising=False)
    return AppSettings.load()


class FakeRedis:
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

    def xgroup_create(self, stream_name: str, group_name: str, id: str = "0-0", mkstream: bool = False) -> bool:
        _ = id
        if mkstream:
            self.streams.setdefault(stream_name, [])
        self.groups.add((stream_name, group_name))
        return True

    def xreadgroup(self, group_name: str, consumer_name: str, streams: dict[str, str], count: int = 100, block: int | None = None):
        _ = group_name, consumer_name, block
        stream_name = next(iter(streams))
        return [(stream_name, self.streams.get(stream_name, [])[:count])]

    def xack(self, stream_name: str, group_name: str, message_id: str) -> int:
        self.acked.append((stream_name, group_name, message_id))
        return 1


def test_pipeline_worker_routes_qualified_launch_candidate_into_dex_entry(monkeypatch) -> None:
    settings = _load_default_settings(monkeypatch)
    client = FakeRedis()
    engine = create_engine("sqlite:///:memory:")
    worker = PipelineWorker(settings, cast(Redis, client), db_engine=engine)
    worker.ensure_streams("signal-workers")

    result = worker.process_events(
        [
            EventEnvelope(
                event_id="launch-qual-1",
                event_type="alpha.launch_candidate",
                source="launch_alpha_backfill",
                chain="solana",
                token="NEWPAPER",
                observed_at=datetime.now(UTC),
                ingested_at=datetime.now(UTC),
                payload={
                    "launch_candidate_status": "QUALIFIED",
                    "launch_alpha_score": 0.94,
                    "liquidity_usd": 180_000.0,
                    "volume_5m_usd": 55_000.0,
                    "buy_pressure": 0.86,
                    "wallet_inflow_score": 0.74,
                    "holder_growth_15m": 0.72,
                    "estimated_slippage_bps": 70.0,
                    "feature_quality": {"launch_alpha": "ok"},
                },
            )
        ]
    )

    assert result.transition.new_state == TokenState.EARLY_LIQUIDITY
    assert result.route.route == "DEX_ENTRY"
    assert result.risk.allowed is True
    assert result.execution is not None