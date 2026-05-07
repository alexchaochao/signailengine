"""WebSocket-based instrument detection sources for OKX and Bybit.

Architecture — hybrid REST + WS:

1. REST bootstrap: On first run (or when Redis cache is empty), fetch the full
   instrument list via REST API to establish a baseline in Redis.
2. WS delta detection: Each poll cycle connects via WS and waits for instrument
   state-change messages. Any newly-added instrument that wasn't in the cache
   is reported as a CatalystEventSnapshot.
3. Redis cache: Known symbols stored with TTL; the cache survives restarts and
   prevents re-reporting known instruments.

OKX:  wss://ws.okx.com:8443/ws/v5/public  → channel: instruments (delta only)
       REST: GET /api/v5/public/instruments?instType=SPOT (full snapshot)
Bybit: wss://stream.bybit.com/v5/public/spot → public.instruments.info (delta)
       REST: GET /v5/market/instruments-info?category=spot (full snapshot)
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from typing import Any
from urllib.error import URLError
from urllib import request

from redis import Redis

from core.config import AppSettings, CatalystAlphaLiveSourceConfig
from discovery.catalyst_common import (
    _CATALYST_DEDUP_PREFIX,
    _CATALYST_DEDUP_TTL,
    _SYMBOL_CACHE_PREFIX,
    _SYMBOL_CACHE_TTL,
    _ExchangeSymbol,
    _headline_for_symbol,
)
from discovery.schemas import CatalystEventSnapshot

# WebSocket providers
WS_INSTRUMENT_PROVIDERS = frozenset({
    "okx_instruments_ws",
    "bybit_instruments_ws",
})


class WebSocketInstrumentSource:
    """Hybrid REST+WS source for exchange instrument detection.

    fetch_snapshots():
      1. Checks Redis cache; if empty, bootstraps via REST API.
      2. Connects via WS, subscribes, waits for delta messages.
      3. Diffs any new symbols against Redis cache.
      4. Returns CatalystEventSnapshot for each new instrument.
    """

    def __init__(
        self,
        settings: AppSettings,
        config: CatalystAlphaLiveSourceConfig,
        *,
        redis_client: Redis | None = None,
    ) -> None:
        self.settings = settings
        self.config = config
        self.redis_client = redis_client
        self._ws_read_timeout: float = 10.0

    # ── Public API ──────────────────────────────────────────────────

    def fetch_snapshots(self) -> list[CatalystEventSnapshot]:
        provider = self.config.provider
        if provider not in WS_INSTRUMENT_PROVIDERS:
            raise ValueError(f"unsupported_ws_provider:{provider}")

        cache_key = f"{_SYMBOL_CACHE_PREFIX}{self.config.source_name or provider}_ws"
        known = self._load_cache(cache_key)

        # Fetch current instruments via REST (reliable, fast)
        current_symbols = self._bootstrap_rest()
        if not current_symbols:
            return []

        # On first run, just populate cache and return nothing
        if not known:
            self._save_cache(cache_key, current_symbols)
            return []

        # Diff against cache
        new_symbols = [s for s in current_symbols if s.symbol not in known]

        if not new_symbols:
            return []

        # Update cache
        all_known = known | {s.symbol for s in current_symbols}
        self._save_cache(cache_key, list(all_known))

        return self._build_snapshots(provider, new_symbols)

    # ── REST bootstrap ──────────────────────────────────────────────

    def _bootstrap_rest(self) -> list[_ExchangeSymbol]:
        """Fetch full instrument list via REST API."""
        provider = self.config.provider
        if provider == "okx_instruments_ws":
            # Need both SPOT and SWAP
            symbols = []
            for inst_type in ("SPOT", "SWAP"):
                url = f"https://www.okx.com/api/v5/public/instruments?instType={inst_type}"
                raw = self._http_get(url)
                if raw:
                    symbols.extend(self._parse_okx_instruments(json.loads(raw).get("data", [])))
            return symbols
        elif provider == "bybit_instruments_ws":
            url = "https://api.bybit.com/v5/market/instruments-info?category=spot"
            raw = self._http_get(url)
            if raw:
                return self._parse_bybit_instruments(json.loads(raw).get("result", {}).get("list", []))
        return []

    def _http_get(self, url: str) -> str | None:
        try:
            req = request.Request(url, headers={"User-Agent": "signalengine/0.1"})
            with request.urlopen(req, timeout=10) as resp:  # noqa: S310
                return resp.read().decode("utf-8")
        except (OSError, URLError, Exception):
            return None

    # ── WS delta collection ─────────────────────────────────────────

    def _collect_ws_deltas(self, provider: str) -> list[dict[str, Any]]:
        """Connect via WS, subscribe, collect instrument delta messages."""
        try:
            import websocket  # noqa: F811
        except ImportError:
            return []

        if provider == "okx_instruments_ws":
            url = "wss://ws.okx.com:8443/ws/v5/public"
            subscribe_msg = json.dumps({
                "op": "subscribe",
                "args": [
                    {"channel": "instruments", "instType": "SPOT"},
                    {"channel": "instruments", "instType": "SWAP"},
                ],
            })
        elif provider == "bybit_instruments_ws":
            url = "wss://stream.bybit.com/v5/public/spot"
            subscribe_msg = json.dumps({
                "op": "subscribe",
                "args": ["public.instruments.info.USDT"],
            })
        else:
            return []

        collected: list[dict[str, Any]] = []

        def on_message(ws: Any, message: str) -> None:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                return
            # Only capture data-bearing messages (not subscription confirmations)
            items = data.get("data", [])
            if isinstance(items, list) and items:
                collected.extend(items)

        def on_error(ws: Any, error: Exception) -> None:
            pass

        def on_open(ws: Any) -> None:
            ws.send(subscribe_msg)

        ws = websocket.WebSocketApp(
            url,
            on_message=on_message,
            on_error=on_error,
            on_open=on_open,
        )
        import threading
        t = threading.Thread(target=ws.run_forever, kwargs={"ping_interval": 10, "ping_timeout": 5}, daemon=True)
        t.start()
        t.join(timeout=self._ws_read_timeout)
        return collected

    # ── Parsing ─────────────────────────────────────────────────────

    def _parse_instruments(self, provider: str, items: list[dict[str, Any]]) -> list[_ExchangeSymbol]:
        if provider == "okx_instruments_ws":
            return self._parse_okx_instruments(items)
        elif provider == "bybit_instruments_ws":
            return self._parse_bybit_instruments(items)
        return []

    def _parse_okx_instruments(self, items: list[dict[str, Any]]) -> list[_ExchangeSymbol]:
        result: list[_ExchangeSymbol] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            inst_id = str(item.get("instId", "")).strip()
            base = str(item.get("baseCcy", "")).strip().upper()
            quote = str(item.get("quoteCcy", "")).strip().upper()
            inst_type = str(item.get("instType", "")).upper()
            state = str(item.get("state", "")).lower()

            if not inst_id or not base:
                continue
            if state not in ("live", "open"):
                continue

            ct = "perpetual" if inst_type == "SWAP" else ("futures" if inst_type == "FUTURES" else "spot")
            result.append(_ExchangeSymbol(
                symbol=inst_id, base_asset=base, quote_asset=quote,
                status=state.upper(), contract_type=ct,
            ))
        return result

    def _parse_bybit_instruments(self, items: list[dict[str, Any]]) -> list[_ExchangeSymbol]:
        result: list[_ExchangeSymbol] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol", "")).strip()
            base = str(item.get("baseCoin", "") or item.get("baseCurrency", "")).strip().upper()
            quote = str(item.get("quoteCoin", "") or item.get("quoteCurrency", "")).strip().upper()
            status = str(item.get("status", "")).lower()
            ct_raw = str(item.get("contractType", "") or "").lower().replace(" ", "")

            if not symbol or not base:
                continue
            if status not in ("trading", "open"):
                continue

            ct_map = {"linearperpetual": "perpetual", "inverseperpetual": "perpetual", "perpetual": "perpetual"}
            ct = ct_map.get(ct_raw, "spot")

            result.append(_ExchangeSymbol(
                symbol=symbol, base_asset=base, quote_asset=quote,
                status="TRADING", contract_type=ct,
            ))
        return result

    # ── Cache helpers ───────────────────────────────────────────────

    def _load_cache(self, cache_key: str) -> set[str]:
        if not self.redis_client:
            return set()
        raw = self.redis_client.get(cache_key)
        if raw:
            try:
                return set(json.loads(raw))
            except (json.JSONDecodeError, TypeError):
                pass
        return set()

    def _save_cache(self, cache_key: str, symbols: list[_ExchangeSymbol] | set[str]) -> None:
        if not self.redis_client:
            return
        if isinstance(symbols, set):
            data = list(symbols)
        else:
            data = [s.symbol for s in symbols]
        self.redis_client.setex(cache_key, _SYMBOL_CACHE_TTL, json.dumps(data))

    # ── Snapshot building ───────────────────────────────────────────

    def _build_snapshots(self, provider: str, symbols: list[_ExchangeSymbol]) -> list[CatalystEventSnapshot]:
        now = datetime.now(UTC)
        snapshots: list[CatalystEventSnapshot] = []
        for sym in symbols:
            chain = self._chain(provider, sym.contract_type)
            event_id = f"ws:{self.config.source_name or provider}:{sym.symbol}:{int(now.timestamp())}"

            if self.redis_client is not None:
                dedup_key = f"{_CATALYST_DEDUP_PREFIX}{event_id}"
                if self.redis_client.exists(dedup_key):
                    continue
                self.redis_client.setex(dedup_key, _CATALYST_DEDUP_TTL, "1")

            snapshots.append(
                CatalystEventSnapshot(
                    source_event_id=event_id,
                    chain=chain,
                    token=sym.base_asset,
                    catalyst_type="cex_listing_announcement",
                    headline=_headline_for_symbol(provider, sym),
                    observed_at=now,
                    impact_score=self.config.impact_score,
                    credibility_score=self.config.credibility_score,
                    lead_time_minutes=0,
                    venue=self.config.venue,
                    metadata={
                        "provider": provider,
                        "source_name": self.config.source_name or provider,
                        "symbol": sym.symbol,
                        "contract_type": sym.contract_type,
                        "status": sym.status,
                        "quote_asset": sym.quote_asset,
                    },
                )
            )
        return snapshots

    @staticmethod
    def _chain(provider: str, contract_type: str) -> str:
        mapping = {"okx_instruments_ws": "okx", "bybit_instruments_ws": "bybit"}
        base = mapping.get(provider, "unknown")
        return f"{base}_futures" if contract_type == "perpetual" else base