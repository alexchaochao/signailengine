from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from core.schemas import EventEnvelope
from discovery.schemas import AlphaCandidateStatus, AlphaType
from discovery.service import FlowAlphaSyncService
from infra.postgres import count_rows, init_storage
from infra.redis_stream import read_models
from infra.repository import StorageRepository
from tests.test_event_flow import FakeRedis


def test_flow_alpha_sync_service_ingests_snapshot() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()
    service = FlowAlphaSyncService(settings, cast(Redis, client), repository)

    result = service.ingest_snapshot(
        {
            "source_event_id": "flow-1",
            "chain": "base",
            "token": "AERO",
            "flow_type": "smart_money_rotation",
            "venue": "aerodrome",
            "observed_at": datetime(2026, 5, 3, 12, 20, 0, tzinfo=UTC),
            "netflow_15m_usd": 95_000.0,
            "smart_money_inflow_usd": 120_000.0,
            "smart_money_outflow_usd": 25_000.0,
            "unique_buyer_wallets_15m": 18,
            "unique_seller_wallets_15m": 6,
            "whale_buy_count_15m": 5,
            "exchange_outflow_usd": 80_000.0,
        }
    )

    stored = read_models(cast(Redis, client), settings.redis.raw_events_stream, EventEnvelope)
    candidate = repository.discovery.load_candidate("flow:base:AERO:aerodrome")

    assert result.status == AlphaCandidateStatus.QUALIFIED
    assert candidate is not None
    assert candidate.alpha_type == AlphaType.FLOW
    assert count_rows(engine, "alpha_candidates") == 1
    assert stored[0][1].event_type == "alpha.flow_candidate"


def test_flow_alpha_sync_service_can_run_observe_only() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()
    service = FlowAlphaSyncService(settings, cast(Redis, client), repository)

    result = service.ingest_snapshot(
        {
            "source_event_id": "flow-observe-1",
            "chain": "base",
            "token": "AERO",
            "flow_type": "smart_money_rotation",
            "venue": "aerodrome",
            "observed_at": datetime(2026, 5, 3, 12, 20, 0, tzinfo=UTC),
            "netflow_15m_usd": 95_000.0,
            "smart_money_inflow_usd": 120_000.0,
            "smart_money_outflow_usd": 25_000.0,
            "unique_buyer_wallets_15m": 18,
            "unique_seller_wallets_15m": 6,
            "whale_buy_count_15m": 5,
            "exchange_outflow_usd": 80_000.0,
        },
        publish_event=False,
    )

    stored = read_models(cast(Redis, client), settings.redis.raw_events_stream, EventEnvelope)
    candidate = repository.discovery.load_candidate("flow:base:AERO:aerodrome")

    assert result.status == AlphaCandidateStatus.QUALIFIED
    assert result.published_message_id is None
    assert candidate is not None
    assert stored == []


def test_flow_alpha_sync_service_ingests_jsonl_records(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()
    service = FlowAlphaSyncService(settings, cast(Redis, client), repository)
    input_path = tmp_path / "flow_backfill.jsonl"
    input_path.write_text(
        json.dumps(
            {
                "source_type": "flow_activity_snapshot",
                "payload": {
                    "source_event_id": "flow-1",
                    "chain": "base",
                    "token": "AERO",
                    "flow_type": "smart_money_rotation",
                    "venue": "aerodrome",
                    "observed_at": "2026-05-03T12:20:00Z",
                    "netflow_15m_usd": 95000.0,
                    "smart_money_inflow_usd": 120000.0,
                    "smart_money_outflow_usd": 25000.0,
                    "unique_buyer_wallets_15m": 18,
                    "unique_seller_wallets_15m": 6,
                    "whale_buy_count_15m": 5,
                    "exchange_outflow_usd": 80000.0
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    results = service.ingest_jsonl(input_path)

    assert len(results) == 1
    assert results[0].status == AlphaCandidateStatus.QUALIFIED


def test_flow_alpha_sync_reuses_stable_candidate_identity_for_same_market() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()
    service = FlowAlphaSyncService(settings, cast(Redis, client), repository)

    first = service.ingest_snapshot(
        {
            "source_event_id": "flow-1",
            "chain": "base",
            "token": "AERO",
            "flow_type": "smart_money_rotation",
            "venue": "aerodrome",
            "observed_at": datetime(2026, 5, 3, 12, 20, 0, tzinfo=UTC),
            "netflow_15m_usd": 95_000.0,
            "smart_money_inflow_usd": 120_000.0,
            "smart_money_outflow_usd": 25_000.0,
            "unique_buyer_wallets_15m": 18,
            "unique_seller_wallets_15m": 6,
            "whale_buy_count_15m": 5,
            "exchange_outflow_usd": 80_000.0,
        }
    )
    second = service.ingest_snapshot(
        {
            "source_event_id": "flow-2",
            "chain": "base",
            "token": "AERO",
            "flow_type": "smart_money_rotation",
            "venue": "aerodrome",
            "observed_at": datetime(2026, 5, 3, 12, 25, 0, tzinfo=UTC),
            "netflow_15m_usd": 70_000.0,
            "smart_money_inflow_usd": 90_000.0,
            "smart_money_outflow_usd": 20_000.0,
            "unique_buyer_wallets_15m": 10,
            "unique_seller_wallets_15m": 3,
            "whale_buy_count_15m": 4,
            "exchange_outflow_usd": 60_000.0,
        }
    )

    candidate = repository.discovery.load_candidate("flow:base:AERO:aerodrome")

    assert first.candidate_id == "flow:base:AERO:aerodrome"
    assert second.candidate_id == "flow:base:AERO:aerodrome"
    assert candidate is not None
    assert count_rows(engine, "alpha_candidates") == 1