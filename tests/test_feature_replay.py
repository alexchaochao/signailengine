from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import create_engine

from core.config import AppSettings
from core.schemas import RawEventRecord
from infra.postgres import init_storage
from infra.repository import StorageRepository
from replay.feature_replay import replay_feature_events
from sentinel.feature_aggregator import OnchainFeatureAggregator, SlippageFeatureAggregator


def test_feature_replay_rebuilds_buy_pressure_and_slippage_snapshots() -> None:
    source_engine = create_engine("sqlite:///:memory:")
    replay_engine = create_engine("sqlite:///:memory:")
    init_storage(source_engine)
    init_storage(replay_engine)
    source_repository = StorageRepository(source_engine)
    replay_repository = StorageRepository(replay_engine)
    settings = AppSettings.load().model_copy(
        update={
            "features": {
                "onchain": {
                    "trade_windows": ["1m", "5m"],
                    "min_trade_count_for_buy_pressure": 2,
                    "max_trade_lag_seconds": 10_000.0,
                },
                "slippage": {
                    "publication_notional_usd": 5000.0,
                    "max_quote_age_seconds": 10_000.0,
                    "allow_curve_fallback": True,
                },
            }
        }
    )
    onchain_aggregator = OnchainFeatureAggregator(settings, source_repository)
    slippage_aggregator = SlippageFeatureAggregator(settings, source_repository)

    trade_one = source_repository.raw_events.save(
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
    trade_two = source_repository.raw_events.save(
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
    quote_one = source_repository.raw_events.save(
        RawEventRecord(
            source_type="dex_quote",
            source_name="jupiter_quote",
            source_event_id="q1",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 3, 12, 0, 1, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 2, tzinfo=UTC),
            cursor="req-1",
            payload={
                "quote_request_id": "req-1",
                "token": "BONK",
                "quote_notional_usd": 1000.0,
                "expected_out_usd": 980.0,
                "reference_mid_usd": 1000.0,
                "route_summary": {"provider": "jupiter", "hops": 1},
            },
        )
    )
    quote_two = source_repository.raw_events.save(
        RawEventRecord(
            source_type="dex_quote",
            source_name="jupiter_quote",
            source_event_id="q2",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 3, 12, 0, 3, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 4, tzinfo=UTC),
            cursor="req-2",
            payload={
                "quote_request_id": "req-2",
                "token": "BONK",
                "quote_notional_usd": 5000.0,
                "expected_out_usd": 4860.0,
                "reference_mid_usd": 5000.0,
                "route_summary": {"provider": "jupiter", "hops": 2},
            },
        )
    )

    onchain_aggregator.ingest_raw_trade(trade_one)
    onchain_aggregator.ingest_raw_trade(trade_two)
    slippage_aggregator.ingest_raw_quote(quote_one)
    slippage_aggregator.ingest_raw_quote(quote_two)

    summary = replay_feature_events(
        settings,
        source_repository,
        replay_repository,
        chain="solana",
        token="BONK",
    )

    source_buy_pressure = source_repository.features.load_latest_snapshot(
        "solana",
        "BONK",
        "buy_pressure",
        "1m",
    )
    replay_buy_pressure = replay_repository.features.load_latest_snapshot(
        "solana",
        "BONK",
        "buy_pressure",
        "1m",
    )
    source_slippage = source_repository.features.load_latest_snapshot(
        "solana",
        "BONK",
        "estimated_slippage_bps",
        "latest",
    )
    replay_slippage = replay_repository.features.load_latest_snapshot(
        "solana",
        "BONK",
        "estimated_slippage_bps",
        "latest",
    )

    assert summary.raw_event_count == 4
    assert summary.replayed_trade_count == 2
    assert summary.replayed_quote_count == 2
    assert summary.ignored_event_count == 0
    assert source_buy_pressure is not None
    assert replay_buy_pressure is not None
    assert source_buy_pressure.feature_value == replay_buy_pressure.feature_value
    assert source_buy_pressure.sample_count == replay_buy_pressure.sample_count
    assert source_buy_pressure.quality_flag == replay_buy_pressure.quality_flag
    assert source_slippage is not None
    assert replay_slippage is not None
    assert source_slippage.feature_value == replay_slippage.feature_value
    assert source_slippage.quality_flag == replay_slippage.quality_flag
    assert len(summary.snapshot_diffs) == 2
    assert summary.snapshot_diffs[1].feature_name == "estimated_slippage_bps"
    assert summary.snapshot_diffs[1].source_inputs["route_provider"] == "jupiter"
    assert summary.snapshot_diffs[1].target_inputs["route_provider"] == "jupiter"