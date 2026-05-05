from datetime import UTC, datetime

from sentinel.market_listener import build_market_event
from sentinel.onchain_listener import build_onchain_event
from sentinel.social_listener import build_social_event


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