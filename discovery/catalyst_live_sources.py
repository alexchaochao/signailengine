from __future__ import annotations

import hashlib
import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from html import unescape
from html.parser import HTMLParser
from time import sleep
from typing import Callable
from urllib.error import URLError
from urllib import request

from core.config import AppSettings, CatalystAlphaLiveSourceConfig, CatalystTokenMatcherConfig
from discovery.schemas import CatalystEventSnapshot

CatalystHttpTransport = Callable[[str, float], str]


class InvalidCatalystFeedError(ValueError):
    pass


@dataclass(frozen=True)
class _FeedEntry:
    entry_id: str
    title: str
    summary: str
    link: str | None
    published_at: datetime


class RssCatalystSnapshotSource:
    def __init__(
        self,
        settings: AppSettings,
        config: CatalystAlphaLiveSourceConfig,
        *,
        transport: CatalystHttpTransport | None = None,
    ) -> None:
        self.settings = settings
        self.config = config
        self.transport = transport or (
            lambda url, timeout_seconds: _http_text_get_transport(
                url,
                timeout_seconds,
                retry_attempts=self.config.retry_attempts,
                retry_backoff_seconds=self.config.retry_backoff_seconds,
            )
        )

    def fetch_snapshots(self) -> list[CatalystEventSnapshot]:
        if self.config.provider not in {"rss_keyword_feed", "binance_cms_api", "coinbase_html_page"}:
            raise ValueError(f"unsupported_catalyst_alpha_provider:{self.config.provider}")
        payload = self.transport(self.config.source_url, self.config.timeout_seconds)
        entries = _parse_feed_entries(
            payload,
            provider=self.config.provider,
            source_name=self.config.source_name,
        )
        now = datetime.now(UTC)
        snapshots: list[CatalystEventSnapshot] = []
        for entry in entries[: self.config.max_entries]:
            if not self._passes_source_filters(entry, now=now):
                continue
            for token_config in self.config.token_configs:
                if not _entry_matches_token(entry, token_config):
                    continue
                snapshots.append(
                    CatalystEventSnapshot(
                        source_event_id=(
                            f"rss:{self.config.source_name or 'catalyst_alpha'}:{token_config.chain}:"
                            f"{token_config.token}:{entry.entry_id}"
                        ),
                        chain=token_config.chain,
                        token=token_config.token,
                        catalyst_type=token_config.catalyst_type,
                        headline=entry.title,
                        observed_at=entry.published_at,
                        impact_score=token_config.impact_score,
                        credibility_score=token_config.credibility_score,
                        lead_time_minutes=0,
                        venue=token_config.venue,
                        metadata={
                            "provider": self.config.provider,
                            "source_url": self.config.source_url,
                            "source_name": self.config.source_name,
                            "headline_summary": entry.summary,
                            "link": entry.link,
                            **token_config.metadata,
                        },
                    )
                )
        return snapshots

    def _passes_source_filters(self, entry: _FeedEntry, *, now: datetime) -> bool:
        if entry.published_at < now - timedelta(minutes=self.config.max_snapshot_age_minutes):
            return False
        haystack = f"{entry.title} {entry.summary}".lower()
        if self.config.required_keywords and not any(
            keyword.lower() in haystack for keyword in self.config.required_keywords
        ):
            return False
        if any(keyword.lower() in haystack for keyword in self.config.excluded_keywords):
            return False
        return True


def build_catalyst_live_sources(settings: AppSettings) -> list[RssCatalystSnapshotSource]:
    sources: list[RssCatalystSnapshotSource] = []
    for source_key, source_config in sorted(settings.acquisition.catalyst_alpha_sources.items()):
        config = source_config.model_copy(
            update={"source_name": source_config.source_name or f"catalyst_alpha_{source_key}"}
        )
        if not config.enabled:
            continue
        sources.append(RssCatalystSnapshotSource(settings, config))
    return sources


def _http_text_get_transport(
    url: str,
    timeout_seconds: float,
    *,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.5,
) -> str:
    http_request = request.Request(url, headers={"User-Agent": "signalengine/0.1"})
    attempts = max(retry_attempts, 1)
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            with request.urlopen(http_request, timeout=timeout_seconds) as response:  # noqa: S310
                return response.read().decode("utf-8")
        except (OSError, URLError) as error:
            last_error = error
            if attempt + 1 >= attempts:
                break
            if retry_backoff_seconds > 0:
                sleep(retry_backoff_seconds)
    if last_error is not None:
        raise last_error
    raise RuntimeError("catalyst_http_transport_failed_without_error")


class _CoinbaseArticleCardParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.entries: list[tuple[str, str]] = []
        self._active_href: str | None = None
        self._active_title: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attributes = dict(attrs)
        href = attributes.get("href")
        aria_label = attributes.get("aria-label")
        data_testid = attributes.get("data-testid")
        if data_testid != "card-article-link-overlay":
            return
        if not href or not aria_label:
            return
        self._active_href = href.strip()
        self._active_title = unescape(aria_label.strip())

    def handle_endtag(self, tag: str) -> None:
        if tag != "a":
            return
        if self._active_href and self._active_title:
            self.entries.append((self._active_href, self._active_title))
        self._active_href = None
        self._active_title = None


def _parse_feed_entries(
    payload: str,
    *,
    provider: str = "rss_keyword_feed",
    source_name: str | None = None,
) -> list[_FeedEntry]:
    if provider == "binance_cms_api":
        return _parse_binance_cms_entries(payload, source_name=source_name)
    if provider == "coinbase_html_page":
        return _parse_coinbase_html_entries(payload, source_name=source_name)
    normalized_payload = payload.lstrip()
    if not normalized_payload:
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'catalyst_alpha'}:empty_response"
        )
    lowered_payload = normalized_payload[:256].lower()
    if lowered_payload.startswith("<!doctype html") or lowered_payload.startswith("<html"):
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'catalyst_alpha'}:html_response"
        )
    try:
        root = ET.fromstring(normalized_payload)
    except ET.ParseError as error:
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'catalyst_alpha'}:xml_parse_error"
        ) from error
    channel = _first_child(root, "channel")
    if channel is not None:
        items = _children(channel, "item")
    else:
        items = _children(root, "entry")
    entries: list[_FeedEntry] = []
    for item in items:
        title = _child_text(item, "title")
        summary = (
            _child_text(item, "description")
            or _child_text(item, "summary")
            or _child_text(item, "content")
        )
        link = _child_link(item)
        published_at = _parse_datetime(
            _child_text(item, "pubDate")
            or _child_text(item, "published")
            or _child_text(item, "updated")
        )
        raw_entry_id = _child_text(item, "guid") or _child_text(item, "id") or link or title
        if not title or not raw_entry_id:
            continue
        entry_id = hashlib.sha1(raw_entry_id.encode("utf-8")).hexdigest()[:16]
        entries.append(
            _FeedEntry(
                entry_id=entry_id,
                title=title.strip(),
                summary=summary.strip(),
                link=link,
                published_at=published_at,
            )
        )
    return entries


def _parse_binance_cms_entries(payload: str, *, source_name: str | None = None) -> list[_FeedEntry]:
    normalized_payload = payload.lstrip()
    if not normalized_payload:
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'catalyst_alpha'}:empty_response"
        )
    try:
        document = json.loads(normalized_payload)
    except json.JSONDecodeError as error:
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'catalyst_alpha'}:json_parse_error"
        ) from error
    catalogs = document.get("data", {}).get("catalogs", [])
    if not isinstance(catalogs, list):
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'catalyst_alpha'}:invalid_catalogs"
        )
    entries: list[_FeedEntry] = []
    for catalog in catalogs:
        if not isinstance(catalog, dict):
            continue
        articles = catalog.get("articles", [])
        if not isinstance(articles, list):
            continue
        for article in articles:
            if not isinstance(article, dict):
                continue
            title = str(article.get("title", "")).strip()
            code = str(article.get("code", "")).strip()
            release_date = article.get("releaseDate")
            if not title or not code:
                continue
            published_at = datetime.now(UTC)
            if isinstance(release_date, (int, float)):
                published_at = datetime.fromtimestamp(float(release_date) / 1000.0, tz=UTC)
            entries.append(
                _FeedEntry(
                    entry_id=hashlib.sha1(code.encode("utf-8")).hexdigest()[:16],
                    title=title,
                    summary="",
                    link=f"https://www.binance.com/en/support/announcement/{code}",
                    published_at=published_at,
                )
            )
    return entries


def _parse_coinbase_html_entries(payload: str, *, source_name: str | None = None) -> list[_FeedEntry]:
    normalized_payload = payload.lstrip()
    if not normalized_payload:
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'catalyst_alpha'}:empty_response"
        )
    if not (normalized_payload.startswith("<!doctype html") or normalized_payload.startswith("<html")):
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'catalyst_alpha'}:unexpected_html_payload"
        )
    parser = _CoinbaseArticleCardParser()
    parser.feed(normalized_payload)
    entries: list[_FeedEntry] = []
    for href, title in parser.entries:
        absolute_href = href if href.startswith("http") else f"https://blog.coinbase.com{href}"
        entry_id = hashlib.sha1(absolute_href.encode("utf-8")).hexdigest()[:16]
        entries.append(
            _FeedEntry(
                entry_id=entry_id,
                title=title,
                summary="",
                link=absolute_href,
                published_at=datetime.now(UTC),
            )
        )
    if not entries:
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'catalyst_alpha'}:no_article_cards"
        )
    return entries


def _entry_matches_token(entry: _FeedEntry, token_config: CatalystTokenMatcherConfig) -> bool:
    haystack = f"{entry.title} {entry.summary}".lower()
    aliases = [token_config.token, *token_config.aliases]
    return any(_contains_alias(haystack, alias) for alias in aliases if alias)


def _contains_alias(haystack: str, alias: str) -> bool:
    pattern = re.compile(rf"(?<![a-z0-9]){re.escape(alias.lower())}(?![a-z0-9])")
    return pattern.search(haystack) is not None


def _parse_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.now(UTC)
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except (TypeError, ValueError):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return datetime.now(UTC)


def _local_name(tag: str) -> str:
    return tag.split("}")[-1]


def _first_child(element: ET.Element, name: str) -> ET.Element | None:
    for child in element:
        if _local_name(child.tag) == name:
            return child
    return None


def _children(element: ET.Element, name: str) -> list[ET.Element]:
    return [child for child in element.iter() if child is not element and _local_name(child.tag) == name]


def _child_text(element: ET.Element, name: str) -> str:
    child = _first_child(element, name)
    if child is None:
        return ""
    return "".join(child.itertext()).strip()


def _child_link(element: ET.Element) -> str | None:
    child = _first_child(element, "link")
    if child is None:
        return None
    href = child.attrib.get("href")
    if href:
        return href.strip()
    text = "".join(child.itertext()).strip()
    return text or None