from __future__ import annotations

from datetime import UTC, datetime, timedelta

from core.config import AppSettings, SocialLiveSourceConfig
from core.schemas import SocialQueryRequest
from sentinel.social_live_sources import (
    RedditSnapshotSource,
    XSnapshotSource,
    build_social_confirmation_source,
    build_social_live_sources,
)


def test_build_social_live_sources_returns_enabled_reddit_sources() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "social_sources": {
                    "reddit": {
                        "enabled": True,
                        "platform": "reddit",
                        "provider": "reddit_search_json",
                        "source_name": "social_reddit",
                        "query_template": "new listings",
                    },
                    "x": {
                        "enabled": True,
                        "platform": "x",
                        "provider": "x_snapshot_json",
                        "source_name": "social_x",
                        "query_template": "trending listings",
                        "source_url": "https://social-bridge.example/x/search.json",
                    },
                }
            }
        }
    )

    sources = build_social_live_sources(settings)

    assert len(sources) == 2
    assert isinstance(sources[0], RedditSnapshotSource)
    assert isinstance(sources[1], XSnapshotSource)


def test_reddit_snapshot_source_in_discovery_mode_derives_token_from_posts() -> None:
    now = datetime.now(UTC)
    config = SocialLiveSourceConfig(
        enabled=True,
        provider="reddit_search_json",
        platform="reddit",
        source_name="reddit_discovery",
        query_template="new listings",
        subreddit="CryptoMoonShots",
        min_mentions=2,
        min_unique_authors=2,
    )

    def transport(url: str, headers: dict[str, str], timeout_seconds: float):
        _ = url, headers, timeout_seconds
        return {
            "data": {
                "children": [
                    {
                        "data": {
                            "id": "post-1",
                            "author": "alice",
                            "title": "New listing rumor for $BONK",
                            "created_utc": (now - timedelta(seconds=30)).timestamp(),
                            "num_comments": 12,
                            "upvote_ratio": 0.92,
                            "score": 150,
                            "subreddit_subscribers": 800000,
                            "permalink": "/r/CryptoMoonShots/comments/post-1",
                        }
                    },
                    {
                        "data": {
                            "id": "post-2",
                            "author": "bob",
                            "title": "$BONK might be next",
                            "created_utc": (now - timedelta(seconds=60)).timestamp(),
                            "num_comments": 8,
                            "upvote_ratio": 0.88,
                            "score": 90,
                            "subreddit_subscribers": 800000,
                            "permalink": "/r/CryptoMoonShots/comments/post-2",
                        }
                    },
                ]
            }
        }

    source = RedditSnapshotSource(AppSettings.load(), config, transport=transport)

    events = source.fetch_events()

    assert len(events) == 1
    assert events[0].token == "BONK"
    assert events[0].chain == "unknown"
    assert events[0].payload["retrieval_mode"] == "discovery"


def test_reddit_snapshot_source_aggregates_recent_posts_into_social_event() -> None:
    now = datetime.now(UTC)
    config = SocialLiveSourceConfig(
        enabled=True,
        provider="reddit_search_json",
        platform="reddit",
        source_name="reddit_bonk_watch",
        token="BONK",
        chain="solana",
        query="BONK",
        subreddit="CryptoMoonShots",
        min_mentions=2,
        min_unique_authors=2,
    )

    def transport(url: str, headers: dict[str, str], timeout_seconds: float):
        _ = url, headers, timeout_seconds
        return {
            "data": {
                "children": [
                    {
                        "data": {
                            "id": "post-1",
                            "author": "alice",
                            "created_utc": (now - timedelta(seconds=30)).timestamp(),
                            "num_comments": 12,
                            "upvote_ratio": 0.92,
                            "score": 150,
                            "subreddit_subscribers": 800000,
                            "permalink": "/r/CryptoMoonShots/comments/post-1",
                        }
                    },
                    {
                        "data": {
                            "id": "post-2",
                            "author": "bob",
                            "created_utc": (now - timedelta(seconds=60)).timestamp(),
                            "num_comments": 8,
                            "upvote_ratio": 0.88,
                            "score": 90,
                            "subreddit_subscribers": 800000,
                            "permalink": "/r/CryptoMoonShots/comments/post-2",
                        }
                    },
                ]
            }
        }

    source = RedditSnapshotSource(AppSettings.load(), config, transport=transport)

    events = source.fetch_events()

    assert len(events) == 1
    assert events[0].event_type == "social.signal_snapshot"
    assert events[0].token == "BONK"
    assert events[0].payload["source_platform"] == "reddit"
    assert events[0].payload["message_count"] == 2
    assert events[0].payload["unique_authors"] == 2
    assert events[0].payload["social_sentiment"] > 0.0


def test_reddit_snapshot_source_skips_posts_below_thresholds() -> None:
    now = datetime.now(UTC)
    config = SocialLiveSourceConfig(
        enabled=True,
        provider="reddit_search_json",
        platform="reddit",
        source_name="reddit_bonk_watch",
        token="BONK",
        chain="solana",
        query="BONK",
        min_mentions=2,
        min_unique_authors=2,
    )

    def transport(url: str, headers: dict[str, str], timeout_seconds: float):
        _ = url, headers, timeout_seconds
        return {
            "data": {
                "children": [
                    {
                        "data": {
                            "id": "post-1",
                            "author": "alice",
                            "created_utc": (now - timedelta(seconds=30)).timestamp(),
                            "num_comments": 2,
                            "upvote_ratio": 0.7,
                            "score": 12,
                            "subreddit_subscribers": 50000,
                        }
                    }
                ]
            }
        }

    source = RedditSnapshotSource(AppSettings.load(), config, transport=transport)

    assert source.fetch_events() == []


def test_x_snapshot_source_aggregates_recent_posts_into_social_event() -> None:
    now = datetime.now(UTC)
    config = SocialLiveSourceConfig(
        enabled=True,
        provider="x_snapshot_json",
        platform="x",
        source_name="x_bonk_watch",
        token="BONK",
        chain="solana",
        query="$BONK",
        source_url="https://social-bridge.example/x/search.json?q=%24BONK",
        min_mentions=2,
        min_unique_authors=2,
    )

    def transport(url: str, headers: dict[str, str], timeout_seconds: float):
        _ = url, headers, timeout_seconds
        return {
            "records": [
                {
                    "id": "x-1",
                    "author_handle": "alpha_one",
                    "created_at": (now - timedelta(seconds=45)).isoformat(),
                    "like_count": 220,
                    "repost_count": 35,
                    "reply_count": 18,
                    "quote_count": 12,
                    "follower_count": 50000,
                    "is_verified": True,
                    "url": "https://x.com/alpha_one/status/x-1",
                },
                {
                    "id": "x-2",
                    "author_handle": "alpha_two",
                    "created_at": (now - timedelta(seconds=70)).isoformat(),
                    "like_count": 110,
                    "repost_count": 20,
                    "reply_count": 9,
                    "quote_count": 6,
                    "follower_count": 12000,
                    "url": "https://x.com/alpha_two/status/x-2",
                },
            ]
        }

    source = XSnapshotSource(AppSettings.load(), config, transport=transport)

    events = source.fetch_events()

    assert len(events) == 1
    assert events[0].event_type == "social.signal_snapshot"
    assert events[0].payload["source_platform"] == "x"
    assert events[0].payload["message_count"] == 2
    assert events[0].payload["unique_authors"] == 2
    assert events[0].payload["social_velocity"] > 0.0


def test_x_snapshot_source_skips_posts_below_thresholds() -> None:
    now = datetime.now(UTC)
    config = SocialLiveSourceConfig(
        enabled=True,
        provider="x_snapshot_json",
        platform="x",
        source_name="x_bonk_watch",
        token="BONK",
        chain="solana",
        query="$BONK",
        source_url="https://social-bridge.example/x/search.json?q=%24BONK",
        min_mentions=2,
        min_unique_authors=2,
    )

    def transport(url: str, headers: dict[str, str], timeout_seconds: float):
        _ = url, headers, timeout_seconds
        return {
            "records": [
                {
                    "id": "x-1",
                    "author_handle": "solo_user",
                    "created_at": (now - timedelta(seconds=20)).isoformat(),
                    "like_count": 5,
                    "repost_count": 0,
                    "reply_count": 0,
                    "quote_count": 0,
                    "follower_count": 50,
                }
            ]
        }

    source = XSnapshotSource(AppSettings.load(), config, transport=transport)

    assert source.fetch_events() == []


def test_build_social_confirmation_source_renders_query_template_for_x_requests() -> None:
    now = datetime.now(UTC)
    captured_urls: list[str] = []
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "social_sources": {
                    "x_template": {
                        "enabled": True,
                        "platform": "x",
                        "provider": "x_snapshot_json",
                        "source_name": "social_x",
                        "query_template": "${cashtag} OR ${token}",
                        "source_url": "https://social-bridge.example/x/search.json",
                    }
                }
            }
        }
    )
    request = SocialQueryRequest(
        request_id="req-1",
        source_name="x_template",
        chain="solana",
        token="BONK",
        requested_at=now,
    )

    def transport(url: str, headers: dict[str, str], timeout_seconds: float):
        _ = headers, timeout_seconds
        captured_urls.append(url)
        return {
            "records": [
                {
                    "id": "x-1",
                    "author_handle": "alpha_one",
                    "created_at": (now - timedelta(seconds=30)).isoformat(),
                    "like_count": 10,
                    "repost_count": 1,
                    "reply_count": 1,
                    "quote_count": 0,
                    "follower_count": 1000,
                    "url": "https://x.com/alpha_one/status/x-1",
                }
            ]
        }

    source = build_social_confirmation_source(settings, request, transport=transport)
    source.fetch_events()

    assert captured_urls == ["https://social-bridge.example/x/search.json?q=%24BONK+OR+BONK"]