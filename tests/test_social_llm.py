from __future__ import annotations

from datetime import UTC, datetime

from core.config import AppSettings
from core.schemas import EventEnvelope, SocialQueryRequest
from sentinel.social_llm import build_social_llm_analyzer
from sentinel.social_live_sources import build_social_analysis_event


def test_social_llm_analyzer_heuristic_returns_structured_scores() -> None:
    settings = AppSettings.load()
    analyzer = build_social_llm_analyzer(settings)
    request = SocialQueryRequest(
        request_id="req-1",
        source_name="x_bonk_watch",
        platform="x",
        chain="solana",
        token="BONK",
        query="$BONK OR BONK",
        requested_at=datetime(2026, 5, 5, 12, 0, tzinfo=UTC),
    )
    social_event = EventEnvelope(
        event_id="x:event-1",
        event_type="social.signal_snapshot",
        source="x_bonk_watch",
        chain="solana",
        token="BONK",
        observed_at=datetime(2026, 5, 5, 12, 0, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 5, 12, 0, 1, tzinfo=UTC),
        payload={
            "source_platform": "x",
            "message_count": 3,
            "unique_authors": 3,
            "engagement_score": 0.7,
            "credibility_score": 0.5,
            "social_sentiment": 0.8,
            "social_velocity": 0.6,
            "evidence_texts": ["BONK listing rumor getting attention on X"],
        },
    )

    analysis = analyzer.analyze(request, social_event)

    assert analysis.provider == "heuristic"
    assert analysis.relevance_score > 0.5
    assert analysis.narrative_strength > 0.0
    assert isinstance(analysis.summary, str)


def test_social_analysis_event_carries_llm_fields_into_payload() -> None:
    settings = AppSettings.load()
    analyzer = build_social_llm_analyzer(settings)
    request = SocialQueryRequest(
        request_id="req-2",
        source_name="reddit_bonk_watch",
        platform="reddit",
        chain="solana",
        token="BONK",
        query="BONK",
        requested_at=datetime(2026, 5, 5, 12, 0, tzinfo=UTC),
    )
    social_event = EventEnvelope(
        event_id="reddit:event-1",
        event_type="social.signal_snapshot",
        source="reddit_bonk_watch",
        chain="solana",
        token="BONK",
        observed_at=datetime(2026, 5, 5, 12, 0, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 5, 12, 0, 1, tzinfo=UTC),
        payload={
            "source_platform": "reddit",
            "message_count": 4,
            "unique_authors": 4,
            "engagement_score": 0.4,
            "credibility_score": 0.6,
            "social_sentiment": 0.7,
            "social_velocity": 0.5,
            "evidence_texts": ["BONK community discussion is accelerating"],
        },
    )

    llm_analysis = analyzer.analyze(request, social_event)
    event = build_social_analysis_event(
        request,
        source_name="reddit_bonk_watch",
        social_event=social_event,
        llm_analysis=llm_analysis,
    )

    assert event.payload["llm_provider"] == "heuristic"
    assert event.payload["llm_summary"]
    assert event.payload["confirmation_score"] >= event.payload["base_confirmation_score"] * 0.5