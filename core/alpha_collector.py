from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from logging import Logger, getLogger
from threading import Lock
from typing import Any, Callable
from urllib import parse

from redis import Redis

from core.config import AlphaCollectorConfig, AppSettings
from core.event_flow import publish_raw_events
from core.schemas import CrossDimensionSnapshot, EventEnvelope
from infra.metrics import Metrics
from infra.redis_stream import acknowledge_message, ensure_consumer_group, read_group_models
from infra.repository import StorageRepository

HttpTransport = Callable[[str, float], dict[str, Any] | list[dict[str, Any]]]

COLLECTOR_CONSUMER_GROUP = "alpha-collector"
COLLECTOR_CONSUMER_NAME = "alpha-collector-1"


def _http_json_get_transport(url: str, timeout_seconds: float) -> dict[str, Any] | list[dict[str, Any]]:
    """Generic HTTP GET transport with JSON response parsing."""
    import httpx
    response = httpx.get(url, timeout=timeout_seconds, follow_redirects=True)
    response.raise_for_status()
    return response.json()


class OnChainCollectorAdapter:
    """Collects on-chain token data via DexScreener API."""

    def __init__(self, config: AlphaCollectorConfig, *, transport: HttpTransport | None = None) -> None:
        self.config = config
        self.transport = transport or _http_json_get_transport

    def collect(self, chain: str, token_address: str) -> dict[str, Any]:
        """Fetch on-chain data for a token address via DexScreener."""
        url = f"{self.config.dexscreener_api_url.rstrip('/')}/{parse.quote(token_address)}"
        try:
            payload = self.transport(url, self.config.timeout_seconds)
        except Exception as exc:
            return {"error": f"dexscreener_fetch_failed:{exc}"}

        pairs = payload.get("pairs") if isinstance(payload, dict) else None
        if not isinstance(pairs, list):
            return {"error": "no_pairs_in_response"}

        chain_pairs = [p for p in pairs if isinstance(p, dict) and str(p.get("chainId", "")).lower() == chain.lower()]
        all_pairs = chain_pairs or pairs

        if not all_pairs:
            return {"error": "no_matching_chain_pairs"}

        total_liquidity = sum(float(p.get("liquidity", {}).get("usd", 0) or 0) for p in all_pairs)
        total_volume_5m = sum(float(p.get("volume", {}).get("m5", 0) or 0) for p in all_pairs)
        first_pair = all_pairs[0]
        price_usd = float(first_pair.get("priceUsd", 0) or 0)
        price_change_5m = float(first_pair.get("priceChange", {}).get("m5", 0) or 0)
        price_change_1h = float(first_pair.get("priceChange", {}).get("h1", 0) or 0)
        fdv = float(first_pair.get("fdv", 0) or 0)

        return {
            "liquidity_usd": total_liquidity,
            "volume_5m_usd": total_volume_5m,
            "price_usd": price_usd,
            "price_change_5m": price_change_5m,
            "price_change_1h": price_change_1h,
            "fdv": fdv,
            "pool_count": len(all_pairs),
            "chains_found": list({str(p.get("chainId", "")).lower() for p in all_pairs}),
        }


class WalletCollectorAdapter:
    """Collects wallet activity data for a token.

    Currently queries the wallet_intelligence sync data from the repository.
    Returns empty if no data is available (non-blocking).
    """

    def __init__(self, repository: StorageRepository | None = None) -> None:
        self.repository = repository

    def collect(self, chain: str, token: str) -> dict[str, Any]:
        """Query stored wallet intelligence data for this token."""
        if self.repository is None:
            return {}

        try:
            records = self.repository.raw_events.load_by_token(
                source_type="wallet_cluster_snapshot",
                chain=chain,
                token=token,
                limit=5,
            )
        except Exception:
            return {}

        if not records:
            return {}

        total_inflow = 0.0
        total_outflow = 0.0
        unique_buyers = 0
        unique_sellers = 0
        whale_buys = 0

        for record in records:
            payload = record.payload or {}
            total_inflow += float(payload.get("smart_money_inflow_usd", 0) or 0)
            total_outflow += float(payload.get("smart_money_outflow_usd", 0) or 0)
            unique_buyers = max(unique_buyers, int(payload.get("unique_buyer_wallets", 0) or 0))
            unique_sellers = max(unique_sellers, int(payload.get("unique_seller_wallets", 0) or 0))
            whale_buys += int(payload.get("whale_buy_count", 0) or 0)

        return {
            "smart_money_inflow_usd": total_inflow,
            "smart_money_outflow_usd": total_outflow,
            "unique_buyers": unique_buyers,
            "unique_sellers": unique_sellers,
            "whale_buys": whale_buys,
        }


class SocialCollectorAdapter:
    """Collects social data for a token symbol."""

    def __init__(self, config: AlphaCollectorConfig, *, transport: HttpTransport | None = None) -> None:
        self.config = config
        self.transport = transport or _http_json_get_transport

    def collect(self, token_symbol: str, chain: str) -> dict[str, Any]:
        """Quick social check for token mentions."""
        _ = chain
        if not token_symbol:
            return {}

        try:
            from sentinel.social_live_sources import RedditSnapshotSource, _http_json_get_transport as reddit_transport
        except Exception:
            return {}

        return {}


class AsyncCollectorOrchestrator:
    """Orchestrates cross-dimension data collection after alpha candidate qualification.

    Listens to alpha.candidate_qualified events on the raw-events stream,
    fires parallel collection tasks (on-chain, wallet, social), and publishes
    a unified alpha.cross_dimension_snapshot event.
    """

    def __init__(
        self,
        settings: AppSettings,
        redis_client: Redis,
        repository: StorageRepository,
        *,
        onchain_adapter: OnChainCollectorAdapter | None = None,
        wallet_adapter: WalletCollectorAdapter | None = None,
        social_adapter: SocialCollectorAdapter | None = None,
        metrics: Metrics | None = None,
        logger: Logger | None = None,
    ) -> None:
        self.settings = settings
        self.redis_client = redis_client
        self.repository = repository
        self.config = settings.alpha_collector
        self.onchain_adapter = onchain_adapter or OnChainCollectorAdapter(self.config)
        self.wallet_adapter = wallet_adapter or WalletCollectorAdapter(repository)
        self.social_adapter = social_adapter or SocialCollectorAdapter(self.config)
        self.metrics = metrics or Metrics(settings.observability.service_namespace)
        self.logger = logger or getLogger("signalengine.alpha_collector")
        self._cooldown_cache: dict[str, float] = {}
        self._cooldown_lock = Lock()

    def ensure_stream(self) -> None:
        ensure_consumer_group(
            self.redis_client,
            self.settings.redis.raw_events_stream,
            COLLECTOR_CONSUMER_GROUP,
        )

    def process_once(self, *, count: int = 20, block_ms: int | None = 1000) -> int:
        if not self.config.enabled:
            return 0

        self.metrics.mark_heartbeat(service="alpha_collector", mode="process_once")
        events = read_group_models(
            self.redis_client,
            self.settings.redis.raw_events_stream,
            COLLECTOR_CONSUMER_GROUP,
            COLLECTOR_CONSUMER_NAME,
            EventEnvelope,
            count=count,
            block_ms=block_ms,
        )
        processed = 0
        for message_id, event in events:
            try:
                if event.event_type != "alpha.candidate_qualified":
                    continue
                self._collect_and_publish(event)
            except Exception as exc:
                self.logger.exception(
                    "alpha_collector_failed",
                    extra={
                        "service": "alpha_collector",
                        "event_id": event.event_id,
                        "token": event.token,
                    },
                )
            finally:
                acknowledge_message(
                    self.redis_client,
                    self.settings.redis.raw_events_stream,
                    COLLECTOR_CONSUMER_GROUP,
                    message_id,
                )
            processed += 1
        return processed

    def _collect_and_publish(self, event: EventEnvelope) -> None:
        payload = event.payload
        alpha_type = str(payload.get("alpha_type", "UNKNOWN")).upper()
        chain = event.chain
        token = event.token
        candidate = payload.get("candidate", {})
        candidate_payload = candidate if isinstance(candidate, dict) else {}
        snapshot_payload = payload.get("snapshot", {})
        snapshot_payload = snapshot_payload if isinstance(snapshot_payload, dict) else {}

        token_address = token
        token_symbol = str(candidate_payload.get("token", token) or token)
        pool_address = str(candidate_payload.get("pool_address", "") or "")

        # Cooldown check
        cooldown_key = f"{chain}:{token}"
        with self._cooldown_lock:
            last_collected = self._cooldown_cache.get(cooldown_key, 0.0)
            now = time.monotonic()
            if now - last_collected < self.config.collection_cooldown_seconds:
                self.logger.info(
                    "alpha_collector_cooldown_skip",
                    extra={"token": token, "chain": chain},
                )
                return
            self._cooldown_cache[cooldown_key] = now

        # Parse trigger info from the candidate_qualified payload
        trigger_source = alpha_type.lower()
        trigger_score = float(payload.get("score", 0.0) or 0.0)
        trigger_reasons = payload.get("reasons", [])

        # Resolve multi-chain targets
        chains_to_collect = self._resolve_chains(chain, token_address)

        # Fire parallel collections
        start_ms = int(time.monotonic() * 1000)
        results: dict[str, Any] = {}
        errors: dict[str, str] = {}

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {}

            futures["onchain"] = executor.submit(
                self._collect_onchain_all_chains, chains_to_collect, token_address
            )
            futures["wallet"] = executor.submit(
                self.wallet_adapter.collect, chain, token
            )
            futures["social"] = executor.submit(
                self.social_adapter.collect, token_symbol, chain
            )

            for name, future in futures.items():
                try:
                    results[name] = future.result(timeout=self.config.timeout_seconds)
                except TimeoutError:
                    errors[name] = "timeout"
                except Exception as exc:
                    errors[name] = str(exc)

        elapsed_ms = int(time.monotonic() * 1000) - start_ms

        # Merge on-chain results
        onchain_result = results.get("onchain", {})
        wallet_result = results.get("wallet", {})
        social_result = results.get("social", {})

        snapshot = CrossDimensionSnapshot(
            snapshot_id=f"{event.event_id}:cross_dim",
            alpha_type=alpha_type,
            chain=chain,
            token=token,
            trigger_source=trigger_source,
            trigger_score=trigger_score,
            trigger_reasons=trigger_reasons,
            onchain_liquidity_usd=onchain_result.get("liquidity_usd"),
            onchain_volume_5m_usd=onchain_result.get("volume_5m_usd"),
            onchain_price_usd=onchain_result.get("price_usd"),
            onchain_price_change_5m=onchain_result.get("price_change_5m"),
            onchain_price_change_1h=onchain_result.get("price_change_1h"),
            onchain_fdv=onchain_result.get("fdv"),
            onchain_pool_count=onchain_result.get("pool_count", 0),
            wallet_smart_money_inflow_usd=wallet_result.get("smart_money_inflow_usd"),
            wallet_smart_money_outflow_usd=wallet_result.get("smart_money_outflow_usd"),
            wallet_unique_buyers=wallet_result.get("unique_buyers"),
            wallet_unique_sellers=wallet_result.get("unique_sellers"),
            wallet_whale_buys=wallet_result.get("whale_buys", 0),
            social_sentiment=social_result.get("sentiment"),
            social_velocity=social_result.get("velocity"),
            social_mention_count=social_result.get("mention_count", 0),
            social_unique_authors=social_result.get("unique_authors", 0),
            collected_chains=onchain_result.get("chains_found", [chain]),
            timed_out=bool(errors),
            collection_latency_ms=elapsed_ms,
            errors=errors,
        )

        # Publish as raw event
        envelope = EventEnvelope(
            event_id=snapshot.snapshot_id,
            event_type="alpha.cross_dimension_snapshot",
            source="alpha_collector",
            chain=chain,
            token=token,
            observed_at=datetime.now(UTC),
            ingested_at=datetime.now(UTC),
            payload=snapshot.model_dump(mode="json"),
        )
        publish_raw_events(self.redis_client, self.settings, envelope)

        self.logger.info(
            "alpha_cross_dimension_published",
            extra={
                "service": "alpha_collector",
                "token": token,
                "chain": chain,
                "alpha_type": alpha_type,
                "onchain_pools": onchain_result.get("pool_count", 0),
                "wallet_inflow": wallet_result.get("smart_money_inflow_usd"),
                "errors": errors,
                "latency_ms": elapsed_ms,
            },
        )
        self.metrics.notification_deliveries.labels(
            channel="alpha_cross_dimension", status="published"
        ).inc()

    def _resolve_chains(self, primary_chain: str, token_address: str) -> list[str]:
        """Resolve which chains to collect data for.

        If the primary chain is unknown, fall back to priority chains.
        Otherwise, just use the primary chain (plus any additional chains
        found during DexScreener lookup).
        """
        if primary_chain and primary_chain != "unknown":
            return [primary_chain]
        return self.config.priority_chains[: self.config.max_chains_per_token]

    def _collect_onchain_all_chains(self, chains: list[str], token_address: str) -> dict[str, Any]:
        """Collect on-chain data across all target chains."""
        merged: dict[str, Any] = {"pool_count": 0, "chains_found": []}
        for chain in chains:
            result = self.onchain_adapter.collect(chain, token_address)
            if "error" in result:
                continue
            merged["chains_found"].extend(result.get("chains_found", []))
            merged["pool_count"] += result.get("pool_count", 0)
            if merged.get("liquidity_usd") is None or (result.get("liquidity_usd", 0) > merged.get("liquidity_usd", 0)):
                merged.update(result)
        merged["chains_found"] = list(set(merged["chains_found"]))
        return merged
