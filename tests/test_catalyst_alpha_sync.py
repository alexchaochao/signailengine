from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from core.schemas import EventEnvelope
from discovery.schemas import AlphaCandidateStatus, AlphaType
from discovery.service import CatalystAlphaSyncService
from infra.postgres import count_rows, init_storage
from infra.redis_stream import read_models
from infra.repository import StorageRepository
from tests.test_event_flow import FakeRedis


def test_catalyst_alpha_sync_service_ingests_snapshot() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()
    service = CatalystAlphaSyncService(settings, cast(Redis, client), repository)

    result = service.ingest_snapshot(
        {
            "source_event_id": "cat-1",
            "chain": "base",
            "token": "AERO",
            "catalyst_type": "cex_listing_rumor",
            "headline": "Major listing rumor accelerating",
            "observed_at": datetime(2026, 5, 3, 12, 10, 0, tzinfo=UTC),
            "impact_score": 0.88,
            "credibility_score": 0.78,
            "lead_time_minutes": 30,
            "venue": "binance",
        }
    )
    stored = read_models(cast(Redis, client), settings.redis.raw_events_stream, EventEnvelope)
    candidate = repository.discovery.load_candidate("catalyst:base:AERO:cat-1")

    assert result.status == AlphaCandidateStatus.QUALIFIED
    assert candidate is not None
    assert candidate.alpha_type == AlphaType.CATALYST
    assert count_rows(engine, "alpha_candidates") == 1
    assert count_rows(engine, "alpha_candidate_events") == 2
    assert stored[0][1].event_type == "alpha.catalyst_candidate"
    assert stored[1][1].event_type == "alpha.candidate_qualified"
    assert stored[1][1].payload["alpha_type"] == "CATALYST"


def test_catalyst_alpha_sync_service_ingests_jsonl_records(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()
    service = CatalystAlphaSyncService(settings, cast(Redis, client), repository)
    input_path = tmp_path / "catalyst_backfill.jsonl"
    input_path.write_text(
        json.dumps(
            {
                "source_type": "catalyst_event_snapshot",
                "payload": {
                    "source_event_id": "cat-1",
                    "chain": "base",
                    "token": "AERO",
                    "catalyst_type": "cex_listing_rumor",
                    "headline": "Major listing rumor accelerating",
                    "observed_at": "2026-05-03T12:10:00Z",
                    "impact_score": 0.88,
                    "credibility_score": 0.78,
                    "lead_time_minutes": 30,
                    "venue": "binance"
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    results = service.ingest_jsonl(input_path)

    assert len(results) == 1
    assert results[0].status == AlphaCandidateStatus.QUALIFIED