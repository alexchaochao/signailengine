from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine

from core.schemas import (
    CollectorCheckpoint,
    FeatureQualityRecord,
    FeatureSnapshot,
    RawEventRecord,
)
from infra.postgres import count_rows, init_storage
from infra.repository import StorageRepository


def test_raw_event_store_is_idempotent_and_replayable() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    base_time = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)

    duplicate = RawEventRecord(
        source_type="onchain_trade",
        source_name="solana_ws",
        source_event_id="tx-1:4:BONK",
        chain="solana",
        token="BONK",
        observed_at=base_time + timedelta(seconds=5),
        ingested_at=base_time + timedelta(seconds=6),
        cursor="281234567",
        payload={"tx_hash": "tx-1", "log_index": 4},
    )
    earlier = RawEventRecord(
        source_type="onchain_trade",
        source_name="solana_ws",
        source_event_id="tx-2:1:BONK",
        chain="solana",
        token="BONK",
        observed_at=base_time + timedelta(seconds=1),
        ingested_at=base_time + timedelta(seconds=2),
        cursor="281234560",
        payload={"tx_hash": "tx-2", "log_index": 1},
    )

    first_insert = repository.raw_events.save(duplicate)
    second_insert = repository.raw_events.save(duplicate)
    repository.raw_events.save(earlier)
    replay_rows = repository.raw_events.read_events(
        source_type="onchain_trade",
        chain="solana",
        token="BONK",
    )

    assert count_rows(engine, "raw_events") == 2
    assert first_insert.id == second_insert.id
    assert replay_rows[0].source_event_id == "tx-2:1:BONK"
    assert replay_rows[1].source_event_id == "tx-1:4:BONK"
    assert replay_rows[1].payload_hash is not None


def test_checkpoint_store_round_trips_latest_cursor() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    observed_at = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)

    repository.checkpoints.save(
        CollectorCheckpoint(
            checkpoint_key="solana:onchain_collector:BONK",
            cursor="281234567",
            observed_at=observed_at,
            metadata={"backfill_active": False},
        )
    )

    loaded = repository.checkpoints.load("solana:onchain_collector:BONK")

    assert loaded is not None
    assert loaded.cursor == "281234567"
    assert loaded.observed_at == observed_at
    assert loaded.metadata == {"backfill_active": False}


def test_feature_store_persists_latest_snapshot_and_quality() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    as_of = datetime(2026, 5, 3, 12, 5, tzinfo=UTC)

    first = repository.features.save_snapshot(
        FeatureSnapshot(
            chain="solana",
            token="BONK",
            feature_name="buy_pressure",
            feature_value=0.81,
            window_name="5m",
            as_of=as_of,
            sample_count=42,
            freshness_seconds=3.1,
            quality_flag="ok",
            formula_version="bp_v1",
            inputs={"buy_notional_usd": 120000.0, "sell_notional_usd": 28000.0},
        )
    )
    second = repository.features.save_snapshot(
        FeatureSnapshot(
            chain="solana",
            token="BONK",
            feature_name="buy_pressure",
            feature_value=0.81,
            window_name="5m",
            as_of=as_of,
            sample_count=42,
            freshness_seconds=3.1,
            quality_flag="ok",
            formula_version="bp_v1",
            inputs={"buy_notional_usd": 120000.0, "sell_notional_usd": 28000.0},
        )
    )
    repository.features.save_quality(
        FeatureQualityRecord(
            chain="solana",
            token="BONK",
            feature_name="buy_pressure",
            as_of=as_of,
            freshness_seconds=3.1,
            source_lag_seconds=1.2,
            missing_sources=[],
            degraded_reason=None,
        )
    )
    latest = repository.features.load_latest_snapshot("solana", "BONK", "buy_pressure", "5m")

    assert count_rows(engine, "feature_snapshots") == 1
    assert count_rows(engine, "feature_quality") == 1
    assert first.id == second.id
    assert latest is not None
    assert latest.feature_value == 0.81
    assert latest.inputs["buy_notional_usd"] == 120000.0