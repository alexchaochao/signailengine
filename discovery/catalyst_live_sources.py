from __future__ import annotations

import hashlib
import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from html import unescape
from html.parser import HTMLParser
from time import sleep
from typing import Callable
import httpx

from redis import Redis

from core.config import AppSettings, CatalystAlphaLiveSourceConfig
from discovery.catalyst_common import (
    _CATALYST_DEDUP_PREFIX,
    _CATALYST_DEDUP_TTL,
    _SYMBOL_CACHE_PREFIX,
    _SYMBOL_CACHE_TTL,
    _ExchangeSymbol,
    _headline_for_symbol,
    _symbol_chain_for_provider,
)
from discovery.catalyst_entity_extractor import CatalystEntityExtractor
from discovery.catalyst_ws_sources import (
    WS_INSTRUMENT_PROVIDERS,
    WebSocketInstrumentSource,
)
from discovery.schemas import CatalystEventSnapshot
from discovery.symbol_registry import SymbolRegistry

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
        redis_client: Redis | None = None,
    ) -> None:
        self.settings = settings
        self.config = config
        self.redis_client = redis_client
        self.entity_extractor = CatalystEntityExtractor(settings, config)
        self.transport = transport or (
            lambda url, timeout_seconds: _http_text_get_transport(
                url,
                timeout_seconds,
                retry_attempts=self.config.retry_attempts,
                retry_backoff_seconds=self.config.retry_backoff_seconds,
            )
        )

    def fetch_snapshots(self) -> list[CatalystEventSnapshot]:
        if self.config.provider not in {
            "rss_keyword_feed", "binance_cms_api", "coinbase_html_page",
            "okx_cms_api", "bybit_cms_api",
        }:
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
            # Redis dedup: skip already-processed entries
            if self.redis_client is not None and self._is_deduplicated(entry.entry_id):
                continue
            if not self._passes_source_filters(entry, now=now):
                continue
            for entity in self.entity_extractor.extract(headline=entry.title, summary=entry.summary):
                if not entity.token:
                    continue
                snapshots.append(
                    CatalystEventSnapshot(
                        source_event_id=(
                            f"rss:{self.config.source_name or 'catalyst_alpha'}:{entity.chain}:"
                            f"{entity.token}:{entry.entry_id}"
                        ),
                        chain=entity.chain,
                        token=entity.token,
                        catalyst_type=entity.catalyst_type,
                        headline=entry.title,
                        observed_at=entry.published_at,
                        impact_score=self.config.impact_score,
                        credibility_score=self.config.credibility_score,
                        lead_time_minutes=0,
                        venue=self.config.venue,
                        metadata={
                            "provider": self.config.provider,
                            "source_url": self.config.source_url,
                            "source_name": self.config.source_name,
                            "headline_summary": entry.summary,
                            "link": entry.link,
                            "project_name": entity.project_name,
                            "entity_confidence": entity.confidence,
                            "extraction_mode": self.config.extraction_mode,
                        },
                    )
                )
            # Mark entry as processed in Redis
            if self.redis_client is not None:
                self._mark_deduplicated(entry.entry_id)
        return snapshots

    def _is_deduplicated(self, entry_id: str) -> bool:
        key = f"{_CATALYST_DEDUP_PREFIX}{entry_id}"
        return bool(self.redis_client.exists(key))

    def _mark_deduplicated(self, entry_id: str) -> None:
        key = f"{_CATALYST_DEDUP_PREFIX}{entry_id}"
        self.redis_client.setex(key, _CATALYST_DEDUP_TTL, "1")

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


# ── Symbol-diff based sources ──────────────────────────────────────────

# Provider types that use symbol diff (exchangeInfo polling + WS instruments)
SYMBOL_DIFF_PROVIDERS = frozenset({
    "binance_exchange_info",
    "binance_futures_info",
    "binance_alpha_api",
    "coinbase_products_api",
} | WS_INSTRUMENT_PROVIDERS)


class ExchangeInfoCatalystSource:
    """Detects new symbols/tokens by polling exchangeInfo-style APIs.

    Fetches the current symbol list, diffs against a Redis-cached baseline,
    and emits CatalystEventSnapshot for any new symbol found.
    """

    def __init__(
        self,
        settings: AppSettings,
        config: CatalystAlphaLiveSourceConfig,
        *,
        transport: CatalystHttpTransport | None = None,
        redis_client: Redis | None = None,
    ) -> None:
        self.settings = settings
        self.config = config
        self.redis_client = redis_client
        self.transport = transport or (
            lambda url, timeout_seconds: _http_text_get_transport(
                url,
                timeout_seconds,
                retry_attempts=self.config.retry_attempts,
                retry_backoff_seconds=self.config.retry_backoff_seconds,
            )
        )

    def fetch_snapshots(self) -> list[CatalystEventSnapshot]:
        provider = self.config.provider
        if provider not in SYMBOL_DIFF_PROVIDERS:
            raise ValueError(f"unsupported_symbol_diff_provider:{provider}")

        raw = self.transport(self.config.source_url, self.config.timeout_seconds)

        if provider == "binance_exchange_info":
            current_symbols = _parse_binance_spot_symbols(raw)
        elif provider == "binance_futures_info":
            current_symbols = _parse_binance_futures_symbols(raw)
        elif provider == "binance_alpha_api":
            current_symbols = _parse_binance_alpha_symbols(raw)
        elif provider == "coinbase_products_api":
            current_symbols = _parse_coinbase_products(raw)
        else:
            return []

        if not current_symbols:
            return []

        # Load cached known symbols
        cache_key = f"{_SYMBOL_CACHE_PREFIX}{self.config.source_name or provider}"
        known_raw = self.redis_client.get(cache_key) if self.redis_client else None
        known: set[str] = set()
        if known_raw:
            known = set(json.loads(known_raw))

        # Find new symbols
        new_symbols = [s for s in current_symbols if s.symbol not in known]

        if not new_symbols:
            return []

        # Update cache
        all_symbols = known | {s.symbol for s in current_symbols}
        if self.redis_client:
            self.redis_client.setex(cache_key, _SYMBOL_CACHE_TTL, json.dumps(list(all_symbols)))

        now = datetime.now(UTC)
        snapshots: list[CatalystEventSnapshot] = []
        for sym in new_symbols:
            chain = _symbol_chain_for_provider(provider)
            token = sym.base_asset
            headline = _headline_for_symbol(provider, sym)
            event_id = f"symbol:{self.config.source_name or provider}:{sym.symbol}:{int(now.timestamp())}"

            # Dedup
            if self.redis_client is not None:
                dedup_key = f"{_CATALYST_DEDUP_PREFIX}{event_id}"
                if self.redis_client.exists(dedup_key):
                    continue
                self.redis_client.setex(dedup_key, _CATALYST_DEDUP_TTL, "1")

            snapshots.append(
                CatalystEventSnapshot(
                    source_event_id=event_id,
                    chain=chain,
                    token=token,
                    catalyst_type="cex_listing_announcement",
                    headline=headline,
                    observed_at=now,
                    impact_score=self.config.impact_score,
                    credibility_score=self.config.credibility_score,
                    lead_time_minutes=0,
                    venue=self.config.venue,
                    metadata={
                        "provider": provider,
                        "source_url": self.config.source_url,
                        "source_name": self.config.source_name,
                        "symbol": sym.symbol,
                        "contract_type": sym.contract_type,
                        "status": sym.status,
                        "quote_asset": sym.quote_asset,
                    },
                )
            )
        return snapshots


def _parse_binance_spot_symbols(raw: str) -> list[_ExchangeSymbol]:
    """Parse /api/v3/exchangeInfo response."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    symbols = data.get("symbols", [])
    if not isinstance(symbols, list):
        return []
    result: list[_ExchangeSymbol] = []
    for s in symbols:
        if not isinstance(s, dict):
            continue
        status = str(s.get("status", "")).upper()
        if status not in {"TRADING", "BREAK"}:
            continue
        base = str(s.get("baseAsset", "")).strip()
        quote = str(s.get("quoteAsset", "")).strip()
        symbol = str(s.get("symbol", "")).strip()
        if not base or not symbol:
            continue
        # Only TRADING pairs that weren't previously seen
        if status == "TRADING":
            result.append(_ExchangeSymbol(
                symbol=symbol, base_asset=base, quote_asset=quote,
                status=status, contract_type="spot",
            ))
    return result


def _parse_binance_futures_symbols(raw: str) -> list[_ExchangeSymbol]:
    """Parse /fapi/v1/exchangeInfo response (USDT-M futures)."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    symbols = data.get("symbols", [])
    if not isinstance(symbols, list):
        return []
    result: list[_ExchangeSymbol] = []
    for s in symbols:
        if not isinstance(s, dict):
            continue
        status = str(s.get("status", "")).upper()
        if status not in {"TRADING", "PENDING"}:
            continue
        base = str(s.get("baseAsset", "")).strip()
        quote = str(s.get("quoteAsset", "")).strip()
        symbol = str(s.get("symbol", "")).strip()
        pair = str(s.get("pair", "")).strip()
        contract_type = str(s.get("contractType", "perpetual")).lower()
        if not base or not symbol:
            continue
        result.append(_ExchangeSymbol(
            symbol=symbol, base_asset=base, quote_asset=quote,
            status=status, contract_type=contract_type,
        ))
    return result


def _parse_binance_alpha_symbols(raw: str) -> list[_ExchangeSymbol]:
    """Parse Binance Alpha token list API response."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    # Alpha API returns: { "data": [ { "tokenName": "...", "tokenSymbol": "...", "chain": "...", "contractAddress": "..." } ] }
    items = data.get("data", [])
    if not isinstance(items, list):
        return []
    result: list[_ExchangeSymbol] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("tokenSymbol", "") or item.get("symbol", "")).strip().upper()
        token_name = str(item.get("tokenName", "") or "").strip()
        chain = str(item.get("chain", "") or "").strip().lower()
        if not symbol:
            continue
        result.append(_ExchangeSymbol(
            symbol=symbol,
            base_asset=symbol,
            quote_asset="",
            status="ALPHA",
            contract_type=f"alpha_{chain}" if chain else "alpha",
        ))
    return result


def _parse_coinbase_products(raw: str) -> list[_ExchangeSymbol]:
    """Parse Coinbase /products API response."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    products = data if isinstance(data, list) else data.get("products", data.get("data", []))
    if not isinstance(products, list):
        return []
    result: list[_ExchangeSymbol] = []
    for p in products:
        if not isinstance(p, dict):
            continue
        status = str(p.get("status", "") or p.get("trading_status", "")).upper()
        if status and status not in {"ONLINE", "TRADING"}:
            continue
        product_id = str(p.get("id", "") or p.get("product_id", "") or p.get("product_id", "")).strip()
        base = str(p.get("base_currency", "") or p.get("baseCurrency", "") or p.get("base", "")).strip()
        quote = str(p.get("quote_currency", "") or p.get("quoteCurrency", "") or p.get("quote", "")).strip()
        if not product_id or not base:
            continue
        result.append(_ExchangeSymbol(
            symbol=product_id, base_asset=base, quote_asset=quote,
            status="TRADING", contract_type="spot",
        ))
    return result


def build_catalyst_live_sources(
    settings: AppSettings,
    *,
    redis_client: Redis | None = None,
) -> list[RssCatalystSnapshotSource | ExchangeInfoCatalystSource | WebSocketInstrumentSource]:
    sources: list[RssCatalystSnapshotSource | ExchangeInfoCatalystSource | WebSocketInstrumentSource] = []
    for source_key, source_config in sorted(settings.acquisition.catalyst_alpha_sources.items()):
        config = source_config.model_copy(
            update={"source_name": source_config.source_name or f"catalyst_alpha_{source_key}"}
        )
        if not config.enabled:
            continue
        if config.provider in SYMBOL_DIFF_PROVIDERS:
            if config.provider in WS_INSTRUMENT_PROVIDERS:
                sources.append(WebSocketInstrumentSource(settings, config, redis_client=redis_client))
            else:
                sources.append(ExchangeInfoCatalystSource(settings, config, redis_client=redis_client))
        else:
            sources.append(RssCatalystSnapshotSource(settings, config, redis_client=redis_client))
    return sources


def register_catalyst_in_symbol_registry(
    snapshot: CatalystEventSnapshot,
    *,
    redis_client: Redis | None = None,
) -> None:
    """Register a CatalystEventSnapshot in the SymbolRegistry for cross-source dedup.

    Call this after processing each snapshot to build the canonical token graph.
    """
    if redis_client is None:
        return
    registry = SymbolRegistry(redis_client)
    registry.register(
        snapshot.token,
        display_name=snapshot.headline.split(":")[-1].strip() if ":" in snapshot.headline else snapshot.token,
        alias=None,
        chain=snapshot.chain,
        source_name=snapshot.metadata.get("source_name", "") or "",
    )
    venue = snapshot.venue or snapshot.metadata.get("symbol", "")
    if venue:
        registry.register_venue_listing(
            snapshot.token,
            venue,
            status=snapshot.metadata.get("status", "TRADING"),
            detected_via=snapshot.metadata.get("provider", ""),
        )


def _http_text_get_transport(
    url: str,
    timeout_seconds: float,
    *,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.5,
) -> str:
    attempts = max(retry_attempts, 1)
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            with httpx.Client() as client:
                resp = client.get(
                    url,
                    timeout=timeout_seconds,
                    headers={"User-Agent": "signalengine/0.1"},
                )
                resp.raise_for_status()
                return resp.text
        except httpx.HTTPStatusError as error:
            last_error = error
            if error.response.status_code == 429:
                if attempt + 1 >= attempts:
                    break
                sleep(max(retry_backoff_seconds, 5.0))
                continue
            if attempt + 1 >= attempts:
                break
            if retry_backoff_seconds > 0:
                sleep(retry_backoff_seconds)
        except (httpx.RequestError, OSError) as error:
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
    if provider == "okx_cms_api":
        return _parse_okx_cms_entries(payload, source_name=source_name)
    if provider == "bybit_cms_api":
        return _parse_bybit_cms_entries(payload, source_name=source_name)
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

def _parse_okx_cms_entries(payload: str, *, source_name: str | None = None) -> list[_FeedEntry]:
    """Parse OKX announcement API response.

    API: GET /api/v5/public/announcements?type=2&page=1&pageSize=20
    Response: {"code":"0","data":[{"announcement":{"title":"...","url":"...","date":"..."}}]}
    """
    normalized_payload = payload.lstrip()
    if not normalized_payload:
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'okx'}:empty_response"
        )
    try:
        document = json.loads(normalized_payload)
    except json.JSONDecodeError as error:
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'okx'}:json_parse_error"
        ) from error

    data = document.get("data", [])
    if not isinstance(data, list):
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'okx'}:invalid_data"
        )

    entries: list[_FeedEntry] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        ann = item.get("announcement", {})
        if not isinstance(ann, dict):
            continue
        title = str(ann.get("title", "")).strip()
        url = str(ann.get("url", "")).strip()
        date_str = str(ann.get("date", "")).strip()
        if not title or not url:
            continue
        entry_id = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
        published_at = datetime.now(UTC)
        try:
            published_at = datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(UTC)
        except (ValueError, TypeError):
            pass
        entries.append(
            _FeedEntry(
                entry_id=entry_id,
                title=title,
                summary="",
                link=url if url.startswith("http") else f"https://www.okx.com{url}",
                published_at=published_at,
            )
        )
    return entries


def _parse_bybit_cms_entries(payload: str, *, source_name: str | None = None) -> list[_FeedEntry]:
    """Parse Bybit announcement API response.

    API: /v5/announcements?locale=en-US&page=1&limit=20&type=listing
    Response: {"result":{"list":[{"title":"...","url":"...","dateTimestamp":"..."}]}}
    """
    normalized_payload = payload.lstrip()
    if not normalized_payload:
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'bybit'}:empty_response"
        )
    try:
        document = json.loads(normalized_payload)
    except json.JSONDecodeError as error:
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'bybit'}:json_parse_error"
        ) from error

    result = document.get("result", {})
    if not isinstance(result, dict):
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'bybit'}:invalid_result"
        )
    items = result.get("list", [])
    if not isinstance(items, list):
        raise InvalidCatalystFeedError(
            f"invalid_catalyst_feed_payload:{source_name or 'bybit'}:invalid_list"
        )

    entries: list[_FeedEntry] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        url = str(item.get("url", "")).strip()
        ts = item.get("dateTimestamp", 0)
        if not title or not url:
            continue
        entry_id = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
        published_at = datetime.now(UTC)
        if isinstance(ts, (int, float)) and ts > 0:
            published_at = datetime.fromtimestamp(ts / 1000.0, tz=UTC)
        entries.append(
            _FeedEntry(
                entry_id=entry_id,
                title=title,
                summary="",
                link=url if url.startswith("http") else f"https://announcements.bybit.com{url}",
                published_at=published_at,
            )
        )
    return entries


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