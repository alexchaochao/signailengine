from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import cast
import logging

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from core.schemas import EventEnvelope
from infra.postgres import count_rows, init_storage
from infra.redis_stream import read_models
from infra.repository import StorageRepository
from sentinel.onchain_feature_sync import OnchainFeatureSyncService
from tests.test_event_flow import FakeRedis


def test_onchain_feature_sync_service_ingests_trade_and_publishes_latest_snapshot() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load().model_copy(
        update={
            "features": {
                "onchain": {
                    "trade_windows": ["1m"],
                    "buy_pressure_primary_window": "1m",
                    "min_trade_count_for_buy_pressure": 1,
                    "max_trade_lag_seconds": 10000.0,
                }
            }
        }
    )
    client = FakeRedis()
    service = OnchainFeatureSyncService(settings, cast(Redis, client), repository)

    result = service.ingest_trade(
        {
            "chain": "solana",
            "tx_hash": "tx-1",
            "log_index": 1,
            "slot": 10,
            "pool_address": "pool-1",
            "wallet_address": "wallet-1",
            "token": "BONK",
            "quote_asset": "USDC",
            "token_amount": 10.0,
            "quote_amount": 2.0,
            "quote_amount_usd": 100.0,
            "side": "buy",
            "observed_at": datetime(2026, 5, 3, 12, 0, 10, tzinfo=UTC),
        },
        source_name="evm_base_pool",
    )
    stored = read_models(cast(Redis, client), settings.redis.raw_events_stream, EventEnvelope)

    assert result.inserted is True
    assert result.snapshot_feature_names == ["buy_pressure"]
    assert result.published_message_id == "2-0"
    assert count_rows(engine, "dex_trade_facts") == 1
    assert repository.raw_events.load("evm_base_pool", result.source_event_id) is not None
    assert stored[0][1].event_type == "onchain.trade_fact"
    assert stored[0][1].source == "evm_base_pool"
    assert stored[0][1].payload["direction"] == "inflow"
    assert stored[0][1].payload["notional_usd"] == 100.0
    assert stored[1][1].event_type == "onchain.liquidity_snapshot"
    assert stored[1][1].payload["buy_pressure"] == 1.0


def test_onchain_feature_sync_service_ingests_quote_and_publishes_latest_snapshot() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load().model_copy(
        update={
            "features": {
                "slippage": {
                    "publication_notional_usd": 5000.0,
                    "max_quote_age_seconds": 10000.0,
                    "allow_curve_fallback": True,
                }
            }
        }
    )
    client = FakeRedis()
    service = OnchainFeatureSyncService(settings, cast(Redis, client), repository)

    result = service.ingest_quote(
        {
            "chain": "solana",
            "token": "BONK",
            "quote_request_id": "req-1",
            "quote_notional_usd": 5000.0,
            "expected_out_usd": 4860.0,
            "reference_mid_usd": 5000.0,
            "route_summary": {"provider": "jupiter", "hops": 2},
            "quoted_at": datetime(2026, 5, 3, 12, 0, 3, tzinfo=UTC),
        },
        source_name="evm_base_quote",
    )
    stored = read_models(cast(Redis, client), settings.redis.raw_events_stream, EventEnvelope)

    assert result.inserted is True
    assert result.snapshot_feature_names == ["estimated_slippage_bps"]
    assert result.published_message_id == "1-0"
    assert count_rows(engine, "dex_quote_samples") == 1
    assert repository.raw_events.load("evm_base_quote", result.source_event_id) is not None
    assert stored[0][1].payload["estimated_slippage_bps"] == 280.0


def test_onchain_feature_sync_service_ingests_jsonl_records(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load().model_copy(
        update={
            "features": {
                "onchain": {
                    "trade_windows": ["1m"],
                    "buy_pressure_primary_window": "1m",
                    "min_trade_count_for_buy_pressure": 1,
                    "max_trade_lag_seconds": 10000.0,
                },
                "slippage": {
                    "publication_notional_usd": 5000.0,
                    "max_quote_age_seconds": 10000.0,
                    "allow_curve_fallback": True,
                },
            }
        }
    )
    client = FakeRedis()
    service = OnchainFeatureSyncService(settings, cast(Redis, client), repository)
    input_path = tmp_path / "backfill.jsonl"
    input_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "source_type": "onchain_trade",
                        "payload": {
                            "chain": "solana",
                            "tx_hash": "tx-1",
                            "log_index": 1,
                            "slot": 10,
                            "pool_address": "pool-1",
                            "wallet_address": "wallet-1",
                            "token": "BONK",
                            "quote_asset": "USDC",
                            "token_amount": 10.0,
                            "quote_amount": 2.0,
                            "quote_amount_usd": 100.0,
                            "side": "buy",
                            "observed_at": "2026-05-03T12:00:10Z",
                        },
                    }
                ),
                json.dumps(
                    {
                        "source_type": "dex_quote",
                        "payload": {
                            "chain": "solana",
                            "token": "BONK",
                            "quote_request_id": "req-1",
                            "quote_notional_usd": 5000.0,
                            "expected_out_usd": 4860.0,
                            "reference_mid_usd": 5000.0,
                            "route_summary": {"provider": "jupiter", "hops": 2},
                            "quoted_at": "2026-05-03T12:00:03Z",
                        },
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    results = service.ingest_jsonl(input_path)

    assert len(results) == 2
    assert results[0].source_type == "onchain_trade"
    assert results[1].source_type == "dex_quote"


def test_onchain_feature_sync_service_logs_structured_result(caplog) -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load().model_copy(
        update={
            "features": {
                "onchain": {
                    "trade_windows": ["1m"],
                    "buy_pressure_primary_window": "1m",
                    "min_trade_count_for_buy_pressure": 1,
                    "max_trade_lag_seconds": 10000.0,
                }
            }
        }
    )
    client = FakeRedis()
    service = OnchainFeatureSyncService(settings, cast(Redis, client), repository)

    with caplog.at_level(logging.INFO, logger="signalengine.onchain_feature_sync"):
        service.ingest_trade(
            {
                "chain": "solana",
                "tx_hash": "tx-log-1",
                "log_index": 1,
                "slot": 10,
                "pool_address": "pool-1",
                "wallet_address": "wallet-1",
                "token": "BONK",
                "quote_asset": "USDC",
                "token_amount": 10.0,
                "quote_amount": 2.0,
                "quote_amount_usd": 100.0,
                "side": "buy",
                "observed_at": datetime(2026, 5, 3, 12, 0, 10, tzinfo=UTC),
            }
        )

    assert any(record.message == "onchain_feature_sync_result" for record in caplog.records)