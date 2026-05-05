from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from core.schemas import EventEnvelope
from discovery.schemas import AlphaCandidateStatus, AlphaType
from discovery.service import LaunchAlphaSyncService
from infra.postgres import count_rows, init_storage
from infra.redis_stream import read_models
from infra.repository import StorageRepository
from tests.test_event_flow import FakeRedis


def test_launch_alpha_sync_service_ingests_snapshot_and_publishes_candidate() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()
    service = LaunchAlphaSyncService(settings, cast(Redis, client), repository)

    result = service.ingest_snapshot(
        {
            "source_event_id": "launch-1",
            "chain": "solana",
            "token": "NEWTKN",
            "pool_address": "pool-1",
            "dex": "raydium",
            "quote_asset": "USDC",
            "observed_at": datetime(2026, 5, 3, 12, 0, 10, tzinfo=UTC),
            "initial_liquidity_usd": 25000.0,
            "liquidity_lock_ratio": 0.92,
            "buy_notional_5m_usd": 18000.0,
            "trade_count_5m": 16,
            "unique_wallets_5m": 11,
            "smart_money_wallets_5m": 3,
            "creator_hold_pct": 0.08,
            "metadata": {"launchpad": "pumpfun"},
        }
    )
    stored = read_models(cast(Redis, client), settings.redis.raw_events_stream, EventEnvelope)
    candidate = repository.discovery.load_candidate("solana:pool-1")

    assert result.status == AlphaCandidateStatus.QUALIFIED
    assert result.published_message_id == "1-0"
    assert count_rows(engine, "alpha_candidates") == 1
    assert count_rows(engine, "alpha_candidate_events") == 2
    assert count_rows(engine, "alpha_snapshots") == 1
    assert count_rows(engine, "raw_events") == 1
    assert candidate is not None
    assert candidate.alpha_type == AlphaType.LAUNCH
    assert candidate.score > 0.9
    assert stored[0][1].event_type == "alpha.launch_candidate"
    assert stored[1][1].event_type == "alpha.candidate_qualified"
    assert stored[1][1].payload["alpha_type"] == "LAUNCH"
    assert stored[0][1].payload["status"] == "QUALIFIED"
    assert stored[0][1].payload["liquidity_usd"] == 25000.0


def test_launch_alpha_sync_service_ingests_jsonl_records(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()
    service = LaunchAlphaSyncService(settings, cast(Redis, client), repository)
    input_path = tmp_path / "launch_backfill.jsonl"
    input_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "source_type": "launch_pool_snapshot",
                        "payload": {
                            "source_event_id": "launch-1",
                            "chain": "solana",
                            "token": "NEWTKN",
                            "pool_address": "pool-1",
                            "dex": "raydium",
                            "quote_asset": "USDC",
                            "observed_at": "2026-05-03T12:00:10Z",
                            "initial_liquidity_usd": 25000.0,
                            "liquidity_lock_ratio": 0.92,
                            "buy_notional_5m_usd": 18000.0,
                            "trade_count_5m": 16,
                            "unique_wallets_5m": 11,
                            "smart_money_wallets_5m": 3,
                            "creator_hold_pct": 0.08,
                        },
                    }
                ),
                json.dumps(
                    {
                        "source_type": "launch_pool_snapshot",
                        "payload": {
                            "source_event_id": "launch-2",
                            "chain": "base",
                            "token": "WATCHME",
                            "pool_address": "pool-2",
                            "dex": "uniswap_v3",
                            "quote_asset": "USDC",
                            "observed_at": "2026-05-03T12:01:10Z",
                            "initial_liquidity_usd": 6000.0,
                            "liquidity_lock_ratio": 0.95,
                            "buy_notional_5m_usd": 2200.0,
                            "trade_count_5m": 5,
                            "unique_wallets_5m": 3,
                            "smart_money_wallets_5m": 1,
                            "creator_hold_pct": 0.12,
                        },
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    results = service.ingest_jsonl(input_path)
    qualified = repository.discovery.list_candidates(
        status=AlphaCandidateStatus.QUALIFIED,
        alpha_type=AlphaType.LAUNCH,
    )

    assert len(results) == 2
    assert results[0].status == AlphaCandidateStatus.QUALIFIED
    assert results[1].status == AlphaCandidateStatus.OBSERVED
    assert len(qualified) == 1
    assert qualified[0].candidate_id == "solana:pool-1"