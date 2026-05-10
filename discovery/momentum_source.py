"""Momentum Alpha — DexScreener Token-Boosts + Volume Breakout detection.

Fetches boosted tokens from DexScreener Token-Boosts API, cross-references
with pair detail for volume/price data, and scores based on:
  - Community boost amount
  - Trading volume (5m)
  - Price change (1h)
  - Unique wallet growth

Output: MomentumAlphaScanResult per token with confidence score.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any, Callable
from urllib import parse
from urllib.error import URLError
from urllib import request

from core.config import AppSettings, MomentumAlphaLiveSourceConfig
from discovery.live_sources import (
    _CachedRateLimitedLaunchTransport,
    _dexscreener_pair_snapshots,
    _dexscreener_token_address,
)
from discovery.schemas import LaunchPoolSnapshot

MomentumHttpTransport = Callable[[str, float], dict[str, Any] | list[dict[str, Any]]]

logger = logging.getLogger("signalengine.momentum_alpha")


class MomentumScanResult:
    """Result of a momentum scan for a single token."""

    def __init__(
        self,
        token: str,
        chain: str,
        boost_amount: float = 0.0,
        volume_5m_usd: float = 0.0,
        price_change_1h: float = 0.0,
        price_usd: float = 0.0,
        trade_count_5m: int = 0,
        unique_wallets_5m: int = 0,
        liquidity_usd: float = 0.0,
        fdv: float = 0.0,
        description: str = "",
        url: str = "",
    ) -> None:
        self.token = token
        self.chain = chain
        self.boost_amount = boost_amount
        self.volume_5m_usd = volume_5m_usd
        self.price_change_1h = price_change_1h
        self.price_usd = price_usd
        self.trade_count_5m = trade_count_5m
        self.unique_wallets_5m = unique_wallets_5m
        self.liquidity_usd = liquidity_usd
        self.fdv = fdv
        self.description = description
        self.url = url

    @property
    def momentum_score(self) -> float:
        """Composite momentum score: 0.0 to 1.0.

        Factors:
        - Volume score: volume_5m scaled to $50k baseline (cap at 1.0)
        - Price momentum: price_change_1h scaled to 50% baseline (cap at 1.0)
        - Wallet activity: unique_wallets scaled to 20 baseline
        - Boost bonus: 0.1 for community boost
        """
        vol_score = min(self.volume_5m_usd / 50_000.0, 1.0) * 0.4
        price_score = min(abs(self.price_change_1h) / 50.0, 1.0) * 0.25
        wallet_score = min(self.unique_wallets_5m / 20.0, 1.0) * 0.25
        boost_bonus = 0.1 if self.boost_amount > 0 else 0.0
        return round(min(vol_score + price_score + wallet_score + boost_bonus, 1.0), 4)

    def to_dict(self) -> dict[str, Any]:
        return {
            "token": self.token,
            "chain": self.chain,
            "boost_amount": self.boost_amount,
            "volume_5m_usd": self.volume_5m_usd,
            "price_change_1h": self.price_change_1h,
            "price_usd": self.price_usd,
            "trade_count_5m": self.trade_count_5m,
            "unique_wallets_5m": self.unique_wallets_5m,
            "liquidity_usd": self.liquidity_usd,
            "fdv": self.fdv,
            "momentum_score": self.momentum_score,
        }


class MomentumAlphaSource:
    """Fetches boosted tokens from DexScreener, enriches with pair detail.

    This is a discovery source — it monitors Token-Boosts API for tokens
    gaining community traction, then checks if they have real volume/price
    momentum via the pair detail API.
    """

    def __init__(
        self,
        settings: AppSettings,
        config: MomentumAlphaLiveSourceConfig,
        *,
        transport: MomentumHttpTransport | None = None,
    ) -> None:
        self.settings = settings
        self.config = config
        self.transport = transport or _CachedRateLimitedLaunchTransport(
            type("obj", (object,), {
                "cache_ttl_seconds": config.cache_ttl_seconds,
                "min_request_interval_seconds": 0.25,
                "timeout_seconds": config.timeout_seconds,
                "retry_attempts": config.retry_attempts,
                "retry_backoff_seconds": config.retry_backoff_seconds,
                "source_url": config.source_url,
            })()
        )

    def fetch_snapshots(self) -> list[MomentumScanResult]:
        """Fetch boosted tokens, enrich with pair detail, return scored results."""
        seed_payload = self._fetch_seed()
        if not seed_payload:
            return []

        # Extract token addresses from boosts
        candidates: list[dict[str, Any]] = []
        for record in seed_payload[:self.config.max_seed_records]:
            if not isinstance(record, dict):
                continue
            chain_id = str(record.get("chainId", "")).lower()
            token_address = str(record.get("tokenAddress", "") or record.get("address", "")).strip()
            boost_amount = float(record.get("amount", 0) or record.get("totalAmount", 0) or 0)
            description = str(record.get("description", "") or "")
            url = str(record.get("url", "") or "")

            if not token_address:
                continue

            candidates.append({
                "token_address": token_address,
                "chain_id": chain_id,
                "boost_amount": boost_amount,
                "description": description,
                "url": url,
            })

        if not candidates:
            return []

        # Fetch pair details and score
        results: list[MomentumScanResult] = []
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=4) as executor:
            future_map = {
                executor.submit(self._fetch_and_score, c): c
                for c in candidates
            }
            for future in as_completed(future_map):
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception:
                    continue

        return results

    def _fetch_and_score(self, candidate: dict[str, Any]) -> MomentumScanResult | None:
        """Fetch pair detail and compute momentum score for a token."""
        token_address = candidate["token_address"]
        chain_id = candidate["chain_id"]
        boost = candidate["boost_amount"]

        # Fetch pair detail
        detail_url = f"{self.config.pair_detail_url.rstrip('/')}/{parse.quote(token_address)}"
        try:
            detail_payload = self.transport(detail_url, self.config.timeout_seconds)
        except Exception:
            return None

        # Parse pair detail
        pairs = detail_payload.get("pairs") if isinstance(detail_payload, dict) else None
        if not isinstance(pairs, list) or not pairs:
            return None

        # Use best pair by volume
        best_pair = max(
            pairs,
            key=lambda p: float(p.get("volume", {}).get("h24", 0) if isinstance(p.get("volume"), dict) else p.get("volume", 0)),
        )

        token = str(best_pair.get("baseToken", {}).get("symbol", token_address))
        chain = chain_id or str(best_pair.get("chainId", "unknown"))

        # Extract metrics
        volume_5m = float(self._nested_get(best_pair, "volume", "m5", default=0))
        price_usd = float(best_pair.get("priceUsd", 0) or 0)
        price_change_1h = float(best_pair.get("priceChange", {}).get("h1", 0) if isinstance(best_pair.get("priceChange"), dict) else best_pair.get("priceChange", {}).get("h1", 0))
        txns_5m = best_pair.get("txns", {}).get("m5", {}) if isinstance(best_pair.get("txns"), dict) else {}
        buys_5m = int(txns_5m.get("buys", 0) if isinstance(txns_5m, dict) else 0)
        sells_5m = int(txns_5m.get("sells", 0) if isinstance(txns_5m, dict) else 0)
        trade_count_5m = buys_5m + sells_5m
        liquidity = float(best_pair.get("liquidity", {}).get("usd", 0) if isinstance(best_pair.get("liquidity"), dict) else best_pair.get("liquidity", 0))
        fdv = float(best_pair.get("fdv", 0) or 0)
        unique_wallets = int(best_pair.get("txnCount", 0) or 0)  # approximate

        # Filter: skip tokens below thresholds
        if volume_5m < self.config.min_volume_5m_usd:
            return None
        if trade_count_5m < self.config.min_trade_count_5m:
            return None
        if self.config.token_denylist and token.upper() in [t.upper() for t in self.config.token_denylist]:
            return None

        return MomentumScanResult(
            token=token,
            chain=chain,
            boost_amount=boost,
            volume_5m_usd=volume_5m,
            price_change_1h=price_change_1h,
            price_usd=price_usd,
            trade_count_5m=trade_count_5m,
            unique_wallets_5m=unique_wallets,
            liquidity_usd=liquidity,
            fdv=fdv,
            description=candidate["description"],
            url=candidate["url"],
        )

    def _fetch_seed(self) -> list[dict[str, Any]]:
        """Fetch Token-Boosts API seed data."""
        try:
            payload = self.transport(self.config.source_url, self.config.timeout_seconds)
        except Exception:
            return []
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            data = payload.get("data", payload.get("records", []))
            return data if isinstance(data, list) else []
        return []

    @staticmethod
    def _nested_get(d: dict[str, Any], *keys: str, default: float = 0.0) -> float:
        """Safely get nested dict value."""
        cursor = d
        for key in keys:
            if not isinstance(cursor, dict):
                return default
            cursor = cursor.get(key, {})
        if isinstance(cursor, (int, float)):
            return float(cursor)
        return default
