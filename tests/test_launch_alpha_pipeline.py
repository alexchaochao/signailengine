from __future__ import annotations

import os
from datetime import UTC, datetime
from threading import Event

from typing import cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings, LaunchAlphaLiveSourceConfig
from core.pipeline import PipelineWorker
from core.schemas import EventEnvelope, TokenState
from discovery.live_sources import HttpLaunchSnapshotSource


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


def test_launch_alpha_fetches_pair_details_concurrently(monkeypatch) -> None:
    settings = _load_default_settings(monkeypatch)
    created_at_ms = int(datetime.now(UTC).timestamp() * 1000)
    config = LaunchAlphaLiveSourceConfig(
        enabled=True,
        provider="dexscreener_latest_profiles",
        source_name="launch_alpha_main",
        chain="solana",
        source_url="https://example.invalid/seed",
        pair_detail_url="https://example.invalid/detail",
        max_seed_records=2,
        max_snapshot_age_seconds=10_000.0,
        min_initial_liquidity_usd=0.0,
        min_buy_notional_5m_usd=0.0,
        min_trade_count_5m=0,
        min_unique_wallets_5m=0,
        min_liquidity_lock_ratio=0.0,
        max_creator_hold_pct=1.0,
        dex_allowlist=[],
        quote_asset_allowlist=["USDC"],
        token_denylist=[],
        token_allowlist=[],
        retry_attempts=1,
        retry_backoff_seconds=0.0,
        min_request_interval_seconds=0.0,
        cache_ttl_seconds=0.0,
    )

    seed_payload = [
        {"chainId": "solana", "tokenAddress": "TokenA"},
        {"chainId": "solana", "tokenAddress": "TokenB"},
    ]
    started_details: list[str] = []
    all_details_started = Event()

    def transport(url: str, timeout_seconds: float):
        _ = timeout_seconds
        if url == config.source_url:
            return seed_payload
        if url.startswith(config.pair_detail_url):
            started_details.append(url)
            if len(started_details) >= 2:
                all_details_started.set()
            if not all_details_started.wait(timeout=1.0):
                raise AssertionError("pair detail requests were not started concurrently")
            token_address = url.rsplit("/", 1)[-1]
            pair_address = f"pair-{token_address}"
            return {
                "pairs": [
                    {
                        "chainId": "solana",
                        "pairAddress": pair_address,
                        "dexId": "raydium",
                        "baseToken": {"address": token_address, "symbol": token_address},
                        "quoteToken": {"address": "USDC", "symbol": "USDC"},
                        "liquidity": {"usd": 25000.0},
                        "volume": {"m5": 12000.0},
                        "txns": {"m5": {"buys": 12, "sells": 4}},
                            "pairCreatedAt": created_at_ms,
                    }
                ]
            }
        raise AssertionError(f"unexpected_url:{url}")

    source = HttpLaunchSnapshotSource(settings, config, transport=transport)

    snapshots = source.fetch_snapshots()

    assert len(snapshots) == 2
    assert sorted(snapshot.token for snapshot in snapshots) == ["TokenA", "TokenB"]
    assert len(started_details) == 2