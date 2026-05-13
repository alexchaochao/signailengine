from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx

from core.config import AppSettings, CatalystAlphaLiveSourceConfig
from discovery.catalyst_live_sources import (
  InvalidCatalystFeedError,
  RssCatalystSnapshotSource,
  _http_text_get_transport,
)


def test_catalyst_live_source_builds_snapshots_from_rss_feed() -> None:
    published_at = datetime.now(UTC) - timedelta(minutes=20)
    payload = f"""
    <rss version=\"2.0\">
      <channel>
        <item>
          <guid>listing-1</guid>
          <title>Binance Will List Aerodrome (AERO)</title>
          <description>Spot listing for Aerodrome goes live today.</description>
          <link>https://example.com/listing-1</link>
          <pubDate>{published_at.strftime('%a, %d %b %Y %H:%M:%S +0000')}</pubDate>
        </item>
        <item>
          <guid>listing-2</guid>
          <title>Maintenance notice for random pair</title>
          <description>No token catalyst here.</description>
          <link>https://example.com/listing-2</link>
          <pubDate>{published_at.strftime('%a, %d %b %Y %H:%M:%S +0000')}</pubDate>
        </item>
      </channel>
    </rss>
    """
    config = CatalystAlphaLiveSourceConfig(
        enabled=True,
        source_name="catalyst_alpha_binance",
        source_url="https://example.invalid/rss",
      venue="binance",
      default_chain="base",
    )

    source = RssCatalystSnapshotSource(
        AppSettings.load(),
        config,
        transport=lambda url, timeout_seconds: payload,
    )

    snapshots = source.fetch_snapshots()

    assert len(snapshots) == 1
    assert snapshots[0].token == "AERO"
    assert snapshots[0].venue == "binance"
    assert snapshots[0].catalyst_type == "cex_listing_announcement"
    assert snapshots[0].metadata["project_name"] == "Aerodrome"


def test_catalyst_live_source_filters_stale_and_excluded_entries() -> None:
    stale_at = datetime.now(UTC) - timedelta(days=2)
    payload = f"""
    <feed xmlns=\"http://www.w3.org/2005/Atom\">
      <entry>
        <id>entry-1</id>
        <title>Exchange will list Arbitrum (ARB)</title>
        <summary>Listing update</summary>
        <updated>{stale_at.isoformat()}</updated>
        <link href=\"https://example.com/entry-1\" />
      </entry>
      <entry>
        <id>entry-2</id>
        <title>Exchange will delist Arbitrum (ARB)</title>
        <summary>Delisting update</summary>
        <updated>{datetime.now(UTC).isoformat()}</updated>
        <link href=\"https://example.com/entry-2\" />
      </entry>
    </feed>
    """
    config = CatalystAlphaLiveSourceConfig(
        enabled=True,
        source_name="catalyst_alpha_feed",
        source_url="https://example.invalid/feed",
        max_snapshot_age_minutes=60,
      default_chain="arbitrum",
    )

    source = RssCatalystSnapshotSource(
        AppSettings.load(),
        config,
        transport=lambda url, timeout_seconds: payload,
    )

    assert source.fetch_snapshots() == []


def test_catalyst_live_source_matches_second_real_source_style() -> None:
    published_at = datetime.now(UTC) - timedelta(minutes=10)
    payload = f"""
    <rss version=\"2.0\">
      <channel>
        <item>
          <guid>coinbase-1</guid>
          <title>Coinbase adds Arbitrum (ARB) to listing roadmap</title>
          <description>Roadmap update for ARB.</description>
          <link>https://example.com/coinbase-1</link>
          <pubDate>{published_at.strftime('%a, %d %b %Y %H:%M:%S +0000')}</pubDate>
        </item>
      </channel>
    </rss>
    """
    config = CatalystAlphaLiveSourceConfig(
        enabled=True,
        source_name="catalyst_alpha_coinbase",
        source_url="https://example.invalid/feed",
        required_keywords=["list", "listing", "roadmap", "asset"],
      venue="coinbase",
      default_chain="arbitrum",
      credibility_score=0.88,
    )

    source = RssCatalystSnapshotSource(
        AppSettings.load(),
        config,
        transport=lambda url, timeout_seconds: payload,
    )

    snapshots = source.fetch_snapshots()

    assert len(snapshots) == 1
    assert snapshots[0].token == "ARB"
    assert snapshots[0].venue == "coinbase"


def test_catalyst_live_source_rejects_empty_feed_payload() -> None:
    config = CatalystAlphaLiveSourceConfig(
        enabled=True,
        source_name="catalyst_alpha_empty",
        source_url="https://example.invalid/feed",
    )

    source = RssCatalystSnapshotSource(
        AppSettings.load(),
        config,
        transport=lambda url, timeout_seconds: "   ",
    )

    try:
        source.fetch_snapshots()
    except InvalidCatalystFeedError as error:
        assert str(error) == "invalid_catalyst_feed_payload:catalyst_alpha_empty:empty_response"
    else:
        raise AssertionError("expected InvalidCatalystFeedError")


def test_catalyst_live_source_rejects_html_feed_payload() -> None:
    config = CatalystAlphaLiveSourceConfig(
        enabled=True,
        source_name="catalyst_alpha_html",
        source_url="https://example.invalid/feed",
    )

    source = RssCatalystSnapshotSource(
        AppSettings.load(),
        config,
        transport=lambda url, timeout_seconds: "<!doctype html><html><body>not rss</body></html>",
    )

    try:
        source.fetch_snapshots()
    except InvalidCatalystFeedError as error:
        assert str(error) == "invalid_catalyst_feed_payload:catalyst_alpha_html:html_response"
    else:
        raise AssertionError("expected InvalidCatalystFeedError")


def test_catalyst_live_source_builds_snapshots_from_binance_cms_payload() -> None:
    payload = {
        "code": "000000",
        "data": {
            "catalogs": [
                {
                    "catalogId": 48,
                    "articles": [
                        {
                            "id": 1,
                            "code": "abc123",
                            "title": "Binance Will List Arbitrum (ARB) with Seed Tag Applied",
                            "releaseDate": 1777540049704,
                        }
                    ],
                }
            ]
        },
    }
    config = CatalystAlphaLiveSourceConfig(
        enabled=True,
        provider="binance_cms_api",
        source_name="catalyst_alpha_binance",
        source_url="https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&pageNo=1&pageSize=20",
      max_snapshot_age_minutes=100000,
      venue="binance",
      default_chain="arbitrum",
    )

    source = RssCatalystSnapshotSource(
        AppSettings.load(),
        config,
        transport=lambda url, timeout_seconds: __import__("json").dumps(payload),
    )

    snapshots = source.fetch_snapshots()

    assert len(snapshots) == 1
    assert snapshots[0].token == "ARB"
    assert snapshots[0].venue == "binance"


def test_catalyst_live_source_builds_snapshots_from_coinbase_html_cards() -> None:
    payload = """
    <!doctype html>
    <html><body>
      <a data-testid="card-article-link-overlay" aria-label="Coinbase adds Arbitrum (ARB) to listing roadmap" href="/blog/coinbase-adds-arbitrum-arb-to-listing-roadmap"></a>
    </body></html>
    """
    config = CatalystAlphaLiveSourceConfig(
        enabled=True,
        provider="coinbase_html_page",
        source_name="catalyst_alpha_coinbase",
        source_url="https://blog.coinbase.com/feed",
      venue="coinbase",
      default_chain="arbitrum",
    )

    source = RssCatalystSnapshotSource(
        AppSettings.load(),
        config,
        transport=lambda url, timeout_seconds: payload,
    )

    snapshots = source.fetch_snapshots()

    assert len(snapshots) == 1
    assert snapshots[0].token == "ARB"
    assert snapshots[0].venue == "coinbase"


def test_catalyst_http_transport_retries_then_succeeds(monkeypatch) -> None:
  calls: list[int] = []

  class StubResponse:
    def __init__(self):
      self.status_code = 200
      self.text = "<rss version='2.0'><channel></channel></rss>"

    def raise_for_status(self) -> None:
      pass

  def stub_client_get(self, url, *, timeout, headers):
    _ = self, url, timeout, headers
    calls.append(1)
    if len(calls) < 3:
      raise httpx.RequestError("connection reset")
    return StubResponse()

  monkeypatch.setattr("httpx.Client.get", stub_client_get)
  monkeypatch.setattr("discovery.catalyst_live_sources.sleep", lambda seconds: None)

  payload = _http_text_get_transport(
    "https://example.invalid/rss",
    5.0,
    retry_attempts=3,
    retry_backoff_seconds=0.01,
  )

  assert payload == "<rss version='2.0'><channel></channel></rss>"
  assert len(calls) == 3


def test_catalyst_http_transport_raises_after_retry_budget(monkeypatch) -> None:
  calls: list[int] = []

  def stub_client_get(self, url, *, timeout, headers):
    _ = self, url, timeout, headers
    calls.append(1)
    raise httpx.RequestError("connection reset")

  monkeypatch.setattr("httpx.Client.get", stub_client_get)
  monkeypatch.setattr("discovery.catalyst_live_sources.sleep", lambda seconds: None)

  try:
    _http_text_get_transport(
      "https://example.invalid/rss",
      5.0,
      retry_attempts=2,
      retry_backoff_seconds=0.01,
    )
  except httpx.RequestError as error:
    assert "connection reset" in str(error)
  else:
    raise AssertionError("expected httpx.RequestError")

  assert len(calls) == 2