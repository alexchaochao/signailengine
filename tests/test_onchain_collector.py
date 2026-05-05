from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from core.schemas import EventEnvelope
from infra.postgres import count_rows, init_storage
from infra.redis_stream import read_models
from infra.repository import StorageRepository
from sentinel.onchain_collector import OnchainTradeCollector
from tests.test_event_flow import FakeRedis


def test_onchain_collector_persists_publishes_and_checkpoints() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()
    collector = OnchainTradeCollector(settings, repository, cast(Redis, client))

    result = collector.collect_trade(
        {
            "chain": "solana",
            "tx_hash": "tx-1",
            "log_index": 12,
            "slot": 281234567,
            "pool_address": "pool-1",
            "wallet_address": "wallet-1",
            "token": "BONK",
            "quote_asset": "USDC",
            "token_amount": 12345.0,
            "quote_amount": 1520.0,
            "quote_amount_usd": 1520.0,
            "route_hint": "jupiter",
            "observed_at": datetime(2026, 5, 3, 12, 0, tzinfo=UTC),
        },
        checkpoint_key="solana:onchain_trade:BONK",
    )
    checkpoint = repository.checkpoints.load("solana:onchain_trade:BONK")
    stored = read_models(cast(Redis, client), settings.redis.raw_events_stream, EventEnvelope)

    assert result.inserted is True
    assert result.stream_message_id == "1-0"
    assert result.source_event_id == "solana:tx-1:12:BONK"
    assert result.checkpoint_cursor == "281234567"
    assert checkpoint is not None
    assert checkpoint.cursor == "281234567"
    assert checkpoint.metadata["last_source_event_id"] == "solana:tx-1:12:BONK"
    assert count_rows(engine, "raw_events") == 1
    assert stored[0][1].event_type == "onchain.trade_raw"
    assert stored[0][1].payload["quote_amount_usd"] == 1520.0


def test_onchain_collector_deduplicates_and_does_not_republish() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()
    collector = OnchainTradeCollector(settings, repository, cast(Redis, client))
    payload = {
        "chain": "solana",
        "tx_hash": "tx-1",
        "log_index": 12,
        "slot": 281234567,
        "pool_address": "pool-1",
        "wallet_address": "wallet-1",
        "token": "BONK",
        "quote_asset": "USDC",
        "token_amount": 12345.0,
        "quote_amount": 1520.0,
        "quote_amount_usd": 1520.0,
        "observed_at": datetime(2026, 5, 3, 12, 0, tzinfo=UTC),
    }

    first = collector.collect_trade(payload, checkpoint_key="solana:onchain_trade:BONK")
    second = collector.collect_trade(payload, checkpoint_key="solana:onchain_trade:BONK")
    stored = read_models(cast(Redis, client), settings.redis.raw_events_stream, EventEnvelope)

    assert first.inserted is True
    assert second.inserted is False
    assert second.stream_message_id is None
    assert count_rows(engine, "raw_events") == 1
    assert len(stored) == 1


def test_onchain_collector_checkpoint_does_not_regress_on_older_slot() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    collector = OnchainTradeCollector(settings, repository)

    collector.collect_trade(
        {
            "chain": "solana",
            "tx_hash": "tx-new",
            "log_index": 1,
            "slot": 281234600,
            "pool_address": "pool-1",
            "wallet_address": "wallet-1",
            "token": "BONK",
            "quote_asset": "USDC",
            "token_amount": 10.0,
            "quote_amount": 2.0,
            "quote_amount_usd": 2.0,
            "observed_at": datetime(2026, 5, 3, 12, 1, tzinfo=UTC),
        },
        checkpoint_key="solana:onchain_trade:BONK",
        publish=False,
    )
    result = collector.collect_trade(
        {
            "chain": "solana",
            "tx_hash": "tx-old",
            "log_index": 2,
            "slot": 281234590,
            "pool_address": "pool-1",
            "wallet_address": "wallet-2",
            "token": "BONK",
            "quote_asset": "USDC",
            "token_amount": 8.0,
            "quote_amount": 1.5,
            "quote_amount_usd": 1.5,
            "observed_at": datetime(2026, 5, 3, 12, 0, tzinfo=UTC),
        },
        checkpoint_key="solana:onchain_trade:BONK",
        publish=False,
    )
    checkpoint = repository.checkpoints.load("solana:onchain_trade:BONK")

    assert result.checkpoint_cursor == "281234600"
    assert checkpoint is not None
    assert checkpoint.cursor == "281234600"
    assert count_rows(engine, "raw_events") == 2