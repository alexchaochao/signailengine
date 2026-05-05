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
from sentinel.dex_quote_collector import DexQuoteCollector
from tests.test_event_flow import FakeRedis


def test_dex_quote_collector_persists_publishes_and_checkpoints() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()
    collector = DexQuoteCollector(settings, repository, cast(Redis, client))

    result = collector.collect_quote(
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
        checkpoint_key="solana:dex_quote:BONK",
    )
    checkpoint = repository.checkpoints.load("solana:dex_quote:BONK")
    stored = read_models(cast(Redis, client), settings.redis.raw_events_stream, EventEnvelope)

    assert result.inserted is True
    assert result.stream_message_id == "1-0"
    assert result.checkpoint_cursor == "req-1"
    assert checkpoint is not None
    assert checkpoint.cursor == "req-1"
    assert count_rows(engine, "raw_events") == 1
    assert stored[0][1].event_type == "dex.quote_raw"


def test_dex_quote_collector_deduplicates_source_event() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()
    collector = DexQuoteCollector(settings, repository, cast(Redis, client))
    payload = {
        "chain": "solana",
        "token": "BONK",
        "quote_request_id": "req-1",
        "quote_notional_usd": 5000.0,
        "expected_out_usd": 4860.0,
        "reference_mid_usd": 5000.0,
        "route_summary": {"provider": "jupiter", "hops": 2},
        "quoted_at": datetime(2026, 5, 3, 12, 0, 3, tzinfo=UTC),
    }

    first = collector.collect_quote(payload)
    second = collector.collect_quote(payload)

    assert first.inserted is True
    assert second.inserted is False
    assert count_rows(engine, "raw_events") == 1
