from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from core.schemas import EventEnvelope
from discovery.schemas import AlphaCandidateStatus, AlphaType
from discovery.service import SocialConfirmationSyncService
from infra.postgres import count_rows, init_storage
from infra.repository import StorageRepository
from tests.test_event_flow import FakeRedis


def test_social_confirmation_sync_service_upserts_candidate_and_publishes_events() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()
    service = SocialConfirmationSyncService(settings, cast(Redis, client), repository)

    result = service.ingest_analysis_event(
        EventEnvelope(
            event_id="social-analysis:x_bonk_watch:req-1",
            event_type="social.analysis_completed",
            source="x_bonk_watch",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 5, 12, 0, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 5, 12, 0, 1, tzinfo=UTC),
            payload={
                "request_id": "req-1",
                "mode": "confirmation",
                "analysis_status": "matched",
                "candidate_id": "social:solana:BONK",
                "platform": "x",
                "message_count": 4,
                "unique_authors": 3,
                "engagement_score": 0.7,
                "credibility_score": 0.55,
                "social_sentiment": 0.8,
                "social_velocity": 0.65,
                "confirmation_score": 0.68,
                "snapshot_event_id": "x:event-1",
                "fsm_context": {
                    "chain": "solana",
                    "token": "BONK",
                    "previous_state": "UNKNOWN",
                    "current_state": "EARLY_LIQUIDITY",
                    "changed": True,
                    "reasons": ["volume_and_liquidity_established"],
                    "last_transition_timestamp": 1746446400,
                },
            },
        ),
        source_name="x_bonk_watch",
    )

    candidate = repository.discovery.load_candidate("social:solana:BONK")

    assert result.status == AlphaCandidateStatus.QUALIFIED
    assert result.published_message_id == "1-0"
    assert candidate is not None
    assert candidate.alpha_type == AlphaType.CATALYST
    assert candidate.score == 0.68
    assert count_rows(engine, "alpha_candidates") == 1
    assert count_rows(engine, "alpha_candidate_events") == 2
    assert count_rows(engine, "alpha_snapshots") == 1
    stored = client.streams[settings.redis.raw_events_stream]
    assert stored[0][1]["kind"] == "alpha.catalyst_candidate"
    assert stored[1][1]["kind"] == "alpha.candidate_qualified"


def test_social_confirmation_sync_service_keeps_unmatched_candidate_observed() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load()
    client = FakeRedis()
    service = SocialConfirmationSyncService(settings, cast(Redis, client), repository)

    result = service.ingest_analysis_event(
        EventEnvelope(
            event_id="social-analysis:reddit_bonk_watch:req-2",
            event_type="social.analysis_completed",
            source="reddit_bonk_watch",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 5, 12, 5, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 5, 12, 5, 1, tzinfo=UTC),
            payload={
                "request_id": "req-2",
                "mode": "confirmation",
                "analysis_status": "no_recent_social_match",
                "platform": "reddit",
                "message_count": 0,
                "unique_authors": 0,
                "engagement_score": 0.0,
                "credibility_score": 0.0,
                "social_sentiment": 0.0,
                "social_velocity": 0.0,
                "confirmation_score": 0.0,
            },
        ),
        source_name="reddit_bonk_watch",
    )

    candidate = repository.discovery.load_candidate("social:solana:BONK")

    assert result.status == AlphaCandidateStatus.OBSERVED
    assert candidate is not None
    assert candidate.status == AlphaCandidateStatus.OBSERVED
    assert count_rows(engine, "alpha_candidate_events") == 1