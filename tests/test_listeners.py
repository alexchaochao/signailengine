from datetime import UTC, datetime

from sentinel.market_listener import build_market_event
from sentinel.onchain_listener import build_onchain_event
from sentinel.social_listener import build_reddit_event, build_social_event, build_x_event


def test_build_onchain_event_normalizes_payload() -> None:
    event = build_onchain_event(
        {
            "token": "BONK",
            "observed_at": datetime.now(UTC),
            "liquidity_usd": 120_000,
            "volume_5m_usd": 45_000,
            "buy_pressure": 0.78,
        }
    )

    assert event.event_type == "onchain.liquidity_snapshot"
    assert event.payload["liquidity_usd"] == 120_000.0


def test_build_market_event_normalizes_payload() -> None:
    event = build_market_event(
        {
            "token": "BONK",
            "observed_at": datetime.now(UTC),
            "cex_listing_confirmed": True,
            "cex_rumor_score": 0.2,
        }
    )

    assert event.event_type == "market.venue_snapshot"
    assert event.payload["cex_listing_confirmed"] is True


def test_build_social_event_normalizes_payload() -> None:
    event = build_social_event(
        {
            "token": "BONK",
            "observed_at": datetime.now(UTC),
            "social_sentiment": 0.4,
            "social_velocity": 0.6,
        }
    )

    assert event.event_type == "social.signal_snapshot"
    assert event.payload["social_velocity"] == 0.6


def test_build_x_event_projects_platform_metrics_into_social_snapshot() -> None:
    event = build_x_event(
        {
            "token": "BONK",
            "chain": "solana",
            "observed_at": datetime.now(UTC),
            "mention_count": 24,
            "unique_authors": 9,
            "viral_score": 0.8,
            "influencer_ratio": 0.65,
            "post_id": "x-1",
            "url": "https://x.example/post/1",
        }
    )

    assert event.event_type == "social.signal_snapshot"
    assert event.payload["source_platform"] == "x"
    assert event.payload["message_count"] == 24
    assert event.payload["social_velocity"] > 0.0
    assert event.payload["credibility_score"] == 0.65


def test_build_reddit_event_projects_thread_metrics_into_social_snapshot() -> None:
    event = build_reddit_event(
        {
            "token": "BONK",
            "chain": "solana",
            "observed_at": datetime.now(UTC),
            "thread_count": 12,
            "author_count": 5,
            "comment_velocity": 0.7,
            "upvote_ratio": 0.9,
            "subreddit_quality": 0.6,
            "post_id": "reddit-1",
        }
    )

    assert event.event_type == "social.signal_snapshot"
    assert event.payload["source_platform"] == "reddit"
    assert event.payload["message_count"] == 12
    assert event.payload["engagement_score"] > 0.0
    assert event.payload["social_sentiment"] > 0.0