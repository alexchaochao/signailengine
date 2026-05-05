from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from core.pipeline import PipelineWorker
from core.schemas import EventEnvelope, FeatureSnapshot
from infra.postgres import init_storage
from infra.redis_stream import read_models
from infra.repository import StorageRepository
from sentinel.onchain_feature_publisher import OnchainFeaturePublisher
from tests.test_event_flow import FakeRedis as StreamFakeRedis
from tests.test_pipeline import FakeRedis as PipelineFakeRedis


def test_onchain_feature_publisher_emits_normalized_event() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = StreamFakeRedis()
    publisher = OnchainFeaturePublisher(settings, cast(Redis, client), repository)
    as_of = datetime(2026, 5, 3, 12, 5, tzinfo=UTC)

    repository.features.save_snapshot(
        FeatureSnapshot(
            chain="solana",
            token="BONK",
            feature_name="buy_pressure",
            feature_value=0.81,
            window_name=settings.features.onchain.buy_pressure_primary_window,
            as_of=as_of,
            sample_count=42,
            freshness_seconds=3.1,
            quality_flag="ok",
            formula_version="bp_v1",
            inputs={"buy_notional_usd": 120000.0, "sell_notional_usd": 28000.0},
        )
    )
    repository.features.save_snapshot(
        FeatureSnapshot(
            chain="solana",
            token="BONK",
            feature_name="estimated_slippage_bps",
            feature_value=72.0,
            window_name="latest",
            as_of=as_of,
            sample_count=3,
            freshness_seconds=2.4,
            quality_flag="ok",
            formula_version="slip_v1",
            inputs={"quote_notional_usd": 5000.0, "route_provider": "jupiter"},
        )
    )

    event, message_id = publisher.publish_latest("solana", "BONK")
    stored = read_models(cast(Redis, client), settings.redis.raw_events_stream, EventEnvelope)

    assert message_id == "1-0"
    assert event.event_type == "onchain.liquidity_snapshot"
    assert event.payload["buy_pressure"] == 0.81
    assert event.payload["estimated_slippage_bps"] == 72.0
    assert event.payload["volume_5m_usd"] == 148000.0
    assert event.payload["liquidity_usd"] > 600000.0
    assert event.payload["feature_quality"]["buy_pressure"] == "ok"
    assert stored[0][1].payload["formula_versions"]["estimated_slippage_bps"] == "slip_v1"


def test_pipeline_worker_accepts_published_onchain_feature_event() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = PipelineFakeRedis()
    publisher = OnchainFeaturePublisher(settings, cast(Redis, client), repository)
    worker = PipelineWorker(settings, cast(Redis, client))
    worker.ensure_streams("signal-workers")
    as_of = datetime(2026, 5, 3, 12, 5, tzinfo=UTC)

    repository.features.save_snapshot(
        FeatureSnapshot(
            chain="solana",
            token="BONK",
            feature_name="buy_pressure",
            feature_value=0.81,
            window_name=settings.features.onchain.buy_pressure_primary_window,
            as_of=as_of,
            sample_count=42,
            freshness_seconds=3.1,
            quality_flag="ok",
            formula_version="bp_v1",
            inputs={"buy_notional_usd": 120000.0, "sell_notional_usd": 28000.0},
        )
    )
    repository.features.save_snapshot(
        FeatureSnapshot(
            chain="solana",
            token="BONK",
            feature_name="estimated_slippage_bps",
            feature_value=72.0,
            window_name="latest",
            as_of=as_of,
            sample_count=3,
            freshness_seconds=2.4,
            quality_flag="ok",
            formula_version="slip_v1",
            inputs={"quote_notional_usd": 5000.0, "route_provider": "jupiter"},
        )
    )

    publisher.publish_latest("solana", "BONK")
    results = worker.poll_once("signal-workers", "worker-1")

    assert len(results) == 1
    assert results[0].signal.token == "BONK"
    assert results[0].signal.features["buy_pressure"] == 0.81
    assert results[0].signal.features["estimated_slippage_bps"] == 72.0
    assert results[0].signal.features["volume_5m_usd"] == 148000.0
    assert results[0].signal.features["onchain_feature_quality"] == 1.0
    assert results[0].route.route == "REJECT"
    assert results[0].risk.allowed is False
    assert results[0].execution is None


def test_onchain_feature_publisher_falls_back_to_quote_market_context() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = StreamFakeRedis()
    publisher = OnchainFeaturePublisher(settings, cast(Redis, client), repository)
    as_of = datetime(2026, 5, 3, 12, 5, tzinfo=UTC)

    repository.features.save_snapshot(
        FeatureSnapshot(
            chain="arbitrum",
            token="ARB",
            feature_name="estimated_slippage_bps",
            feature_value=26.22322,
            window_name="latest",
            as_of=as_of,
            sample_count=1,
            freshness_seconds=2.0,
            quality_flag="ok",
            formula_version="slip_v1",
            inputs={
                "quote_notional_usd": 5000.0,
                "route_provider": "odos",
                "volume_5m_usd": 4200.0,
                "buy_pressure": 0.75,
                "market_source": "dexscreener",
            },
        )
    )

    event, _ = publisher.publish_latest("arbitrum", "ARB")

    assert event.payload["volume_5m_usd"] == 4200.0
    assert event.payload["buy_pressure"] == 0.75
    assert event.payload["feature_quality"]["buy_pressure"] == "degraded"
