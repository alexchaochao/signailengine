from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine

from core.config import AppSettings
from core.schemas import RawEventRecord
from infra.postgres import count_rows, init_storage
from infra.repository import StorageRepository
from sentinel.feature_aggregator import OnchainFeatureAggregator, classify_trade


def test_classify_trade_uses_explicit_side() -> None:
    raw_event = RawEventRecord(
        source_type="onchain_trade",
        source_name="solana_ws",
        source_event_id="solana:tx-1:1:BONK",
        chain="solana",
        token="BONK",
        observed_at=datetime(2026, 5, 3, 12, 0, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 3, 12, 0, 1, tzinfo=UTC),
        cursor="1",
        payload={
            "pool_address": "pool-1",
            "wallet_address": "wallet-1",
            "token": "BONK",
            "token_amount": 10.0,
            "quote_amount": 2.0,
            "quote_amount_usd": 2.0,
            "side": "buy",
        },
    )

    trade = classify_trade(raw_event)

    assert trade.side == "buy"
    assert trade.trade_id == "solana:tx-1:1:BONK"


def test_classify_trade_rejects_unresolved_side() -> None:
    raw_event = RawEventRecord(
        source_type="onchain_trade",
        source_name="solana_ws",
        source_event_id="solana:tx-1:1:BONK",
        chain="solana",
        token="BONK",
        observed_at=datetime(2026, 5, 3, 12, 0, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 3, 12, 0, 1, tzinfo=UTC),
        cursor="1",
        payload={
            "pool_address": "pool-1",
            "wallet_address": "wallet-1",
            "token": "BONK",
            "token_amount": 10.0,
            "quote_amount": 2.0,
            "quote_amount_usd": 2.0,
        },
    )

    with pytest.raises(ValueError, match="trade_side_unresolved"):
        classify_trade(raw_event)


def test_onchain_feature_aggregator_builds_trade_windows_and_buy_pressure_snapshots() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load().model_copy(
        update={
            "features": {
                "onchain": {
                    "trade_windows": ["1m", "5m"],
                    "min_trade_count_for_buy_pressure": 2,
                    "max_trade_lag_seconds": 10_000.0,
                }
            }
        }
    )
    aggregator = OnchainFeatureAggregator(settings, repository)
    first = repository.raw_events.save(
        RawEventRecord(
            source_type="onchain_trade",
            source_name="solana_ws",
            source_event_id="solana:tx-1:1:BONK",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 3, 12, 0, 10, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 11, tzinfo=UTC),
            cursor="1",
            payload={
                "pool_address": "pool-1",
                "wallet_address": "wallet-1",
                "token": "BONK",
                "token_amount": 10.0,
                "quote_amount": 2.0,
                "quote_amount_usd": 100.0,
                "side": "buy",
            },
        )
    )
    second = repository.raw_events.save(
        RawEventRecord(
            source_type="onchain_trade",
            source_name="solana_ws",
            source_event_id="solana:tx-2:1:BONK",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 3, 12, 0, 40, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 41, tzinfo=UTC),
            cursor="2",
            payload={
                "pool_address": "pool-1",
                "wallet_address": "wallet-2",
                "token": "BONK",
                "token_amount": 8.0,
                "quote_amount": 1.0,
                "quote_amount_usd": 50.0,
                "side": "sell",
            },
        )
    )

    snapshots = aggregator.ingest_raw_trade(first)
    snapshots = aggregator.ingest_raw_trade(second)
    latest = repository.features.load_latest_snapshot("solana", "BONK", "buy_pressure", "1m")
    trade_window = repository.features.load_trade_window(
        "solana",
        "BONK",
        "1m",
        datetime(2026, 5, 3, 12, 1, tzinfo=UTC),
    )

    assert len(snapshots) == 2
    assert count_rows(engine, "dex_trade_facts") == 2
    assert count_rows(engine, "token_trade_windows") == 2
    assert count_rows(engine, "feature_snapshots") == 2
    assert trade_window is not None
    assert trade_window.buy_notional_usd == 100.0
    assert trade_window.sell_notional_usd == 50.0
    assert trade_window.trade_count == 2
    assert trade_window.unique_wallets == 2
    assert latest is not None
    assert latest.sample_count == 2
    assert latest.feature_value == 100.0 / 150.0
    assert latest.quality_flag == "ok"
    assert latest.inputs["unique_wallets"] == 2
    quality = repository.features.load_latest_quality("solana", "BONK", "buy_pressure")
    assert quality is not None
    assert quality.degraded_reason is None


def test_buy_pressure_quality_marks_low_sample() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load().model_copy(
        update={
            "features": {
                "onchain": {
                    "trade_windows": ["1m"],
                    "min_trade_count_for_buy_pressure": 2,
                    "max_trade_lag_seconds": 10_000.0,
                }
            }
        }
    )
    aggregator = OnchainFeatureAggregator(settings, repository)
    raw_trade = repository.raw_events.save(
        RawEventRecord(
            source_type="onchain_trade",
            source_name="solana_ws",
            source_event_id="solana:tx-1:1:BONK",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 3, 12, 0, 10, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 11, tzinfo=UTC),
            cursor="1",
            payload={
                "pool_address": "pool-1",
                "wallet_address": "wallet-1",
                "token": "BONK",
                "token_amount": 10.0,
                "quote_amount": 2.0,
                "quote_amount_usd": 100.0,
                "side": "buy",
            },
        )
    )

    aggregator.ingest_raw_trade(raw_trade)

    quality = repository.features.load_latest_quality("solana", "BONK", "buy_pressure")
    assert quality is not None
    assert quality.degraded_reason == "low_sample"