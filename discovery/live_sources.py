from __future__ import annotations

import copy
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from time import monotonic, sleep
from typing import Any, Callable
import httpx
from urllib import parse

from core.config import AppSettings, LaunchAlphaLiveSourceConfig
from core.schemas import CollectorCheckpoint
from discovery.schemas import LaunchPoolSnapshot
from infra.repository import StorageRepository

LaunchHttpTransport = Callable[[str, float], dict[str, Any] | list[dict[str, Any]]]


class HttpLaunchSnapshotSource:
    def __init__(
        self,
        settings: AppSettings,
        config: LaunchAlphaLiveSourceConfig,
        *,
        transport: LaunchHttpTransport | None = None,
    ) -> None:
        self.settings = settings
        self.config = config
        self.transport = transport or _CachedRateLimitedLaunchTransport(config)

    def fetch_snapshots(self) -> list[LaunchPoolSnapshot]:
        if self.config.provider == "http_snapshot_json":
            return self._fetch_generic_snapshots()
        if self.config.provider == "dexscreener_latest_profiles":
            return self._fetch_dexscreener_snapshots()
        raise ValueError(f"unsupported_launch_alpha_provider:{self.config.provider}")

    def _fetch_generic_snapshots(self) -> list[LaunchPoolSnapshot]:
        payload = self._fetch_json_with_fallback(
            [self.config.source_url, *self.config.fallback_source_urls]
        )
        records = payload.get("records") if isinstance(payload, dict) else None
        if not isinstance(records, list):
            raise ValueError("invalid_launch_snapshot_source_payload")
        snapshots: list[LaunchPoolSnapshot] = []
        now = datetime.now(UTC)
        for record in records:
            if not isinstance(record, dict):
                continue
            snapshot = LaunchPoolSnapshot.model_validate(record)
            if self._passes_filters(snapshot, now=now):
                snapshots.append(snapshot)
        return snapshots

    def _fetch_dexscreener_snapshots(self) -> list[LaunchPoolSnapshot]:
        seed_payload = self._fetch_json_with_fallback(
            [self.config.source_url, *self.config.fallback_source_urls]
        )
        if isinstance(seed_payload, dict) and isinstance(seed_payload.get("records"), list):
            return self._snapshots_from_records(seed_payload["records"])
        if not isinstance(seed_payload, list):
            raise ValueError("invalid_dexscreener_launch_seed_payload")
        now = datetime.now(UTC)
        candidates: list[str] = []
        for record in seed_payload[: self.config.max_seed_records]:
            if not isinstance(record, dict):
                continue
            if str(record.get("chainId", "")).lower() != self.config.chain.lower():
                continue
            token_address = _dexscreener_token_address(record)
            if not token_address:
                continue
            candidates.append(token_address)

        if not candidates:
            return []

        snapshots: list[LaunchPoolSnapshot] = []
        max_workers = min(len(candidates), 4)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_token_address = {
                executor.submit(self._fetch_dexscreener_pair_snapshots, token_address): token_address
                for token_address in candidates
            }
            for future in as_completed(future_to_token_address):
                try:
                    for snapshot in future.result():
                        if self._passes_filters(snapshot, now=now):
                            snapshots.append(snapshot)
                            break
                except Exception:
                    continue
        return snapshots

    def _fetch_dexscreener_pair_snapshots(self, token_address: str) -> list[LaunchPoolSnapshot]:
        detail_url = f"{self.config.pair_detail_url.rstrip('/')}/{parse.quote(token_address)}"
        detail_payload = self.transport(detail_url, self.config.timeout_seconds)
        return _dexscreener_pair_snapshots(
            detail_payload,
            chain=self.config.chain,
            token_address=token_address,
        )

    def _fetch_json_with_fallback(self, urls: list[str]) -> dict[str, Any] | list[dict[str, Any]]:
        last_error: Exception | None = None
        for url in urls:
            try:
                return self.transport(url, self.config.timeout_seconds)
            except Exception as error:
                last_error = error
        if last_error is not None:
            raise last_error
        raise RuntimeError("launch_source_urls_missing")

    def _snapshots_from_records(self, records: list[dict[str, Any]]) -> list[LaunchPoolSnapshot]:
        snapshots: list[LaunchPoolSnapshot] = []
        now = datetime.now(UTC)
        for record in records:
            if not isinstance(record, dict):
                continue
            snapshot = LaunchPoolSnapshot.model_validate(record)
            if self._passes_filters(snapshot, now=now):
                snapshots.append(snapshot)
        return snapshots

    def _passes_filters(self, snapshot: LaunchPoolSnapshot, *, now: datetime) -> bool:
        age_seconds = max((now - snapshot.observed_at.astimezone(UTC)).total_seconds(), 0.0)
        if age_seconds > self.config.max_snapshot_age_seconds:
            return False
        if self.config.dex_allowlist and snapshot.dex not in self.config.dex_allowlist:
            return False
        if self.config.quote_asset_allowlist and snapshot.quote_asset not in self.config.quote_asset_allowlist:
            return False
        if self.config.token_allowlist and snapshot.token not in self.config.token_allowlist:
            return False
        if snapshot.token in self.config.token_denylist:
            return False
        if snapshot.initial_liquidity_usd < self.config.min_initial_liquidity_usd:
            return False
        if snapshot.buy_notional_5m_usd < self.config.min_buy_notional_5m_usd:
            return False
        if snapshot.trade_count_5m < self.config.min_trade_count_5m:
            return False
        if snapshot.unique_wallets_5m < self.config.min_unique_wallets_5m:
            return False
        if (
            snapshot.liquidity_lock_ratio is not None
            and snapshot.liquidity_lock_ratio < self.config.min_liquidity_lock_ratio
        ):
            return False
        if (
            snapshot.creator_hold_pct is not None
            and snapshot.creator_hold_pct > self.config.max_creator_hold_pct
        ):
            return False
        return True


def build_launch_live_sources(
    settings: AppSettings,
    repository: StorageRepository | None = None,
) -> list[HttpLaunchSnapshotSource]:
    sources: list[HttpLaunchSnapshotSource] = []
    for source_key, source_config in sorted(settings.acquisition.launch_alpha_sources.items()):
        config = source_config.model_copy(
            update={"source_name": source_config.source_name or f"launch_alpha_{source_key}"}
        )
        if not config.enabled:
            continue
        transport: LaunchHttpTransport | None = None
        if repository is not None:
            transport = _PersistentCheckpointLaunchTransport(config, repository)
        sources.append(HttpLaunchSnapshotSource(settings, config, transport=transport))
    return sources


class _CachedRateLimitedLaunchTransport:
    def __init__(self, config: LaunchAlphaLiveSourceConfig) -> None:
        self.config = config
        self._cache: dict[str, tuple[float, dict[str, Any] | list[dict[str, Any]]]] = {}
        self._last_request_at: float | None = None

    def __call__(self, url: str, timeout_seconds: float) -> dict[str, Any] | list[dict[str, Any]]:
        now = monotonic()
        cached = self._cache.get(url)
        if cached is not None and cached[0] > now:
            return copy.deepcopy(cached[1])

        attempts = max(self.config.retry_attempts, 1)
        last_error: Exception | None = None
        for attempt in range(attempts):
            self._respect_rate_limit()
            try:
                payload = _http_json_get_transport(url, timeout_seconds)
                self._last_request_at = monotonic()
                if self.config.cache_ttl_seconds > 0:
                    self._cache[url] = (
                        monotonic() + self.config.cache_ttl_seconds,
                        copy.deepcopy(payload),
                    )
                return payload
            except (httpx.HTTPError, OSError) as error:
                last_error = error
                self._last_request_at = monotonic()
                if attempt + 1 >= attempts:
                    break
                if self.config.retry_backoff_seconds > 0:
                    sleep(self.config.retry_backoff_seconds)
        if last_error is not None:
            raise last_error
        raise RuntimeError("launch_http_transport_failed_without_error")

    def _respect_rate_limit(self) -> None:
        if self._last_request_at is None:
            return
        min_interval = self.config.min_request_interval_seconds
        if min_interval <= 0:
            return
        elapsed = monotonic() - self._last_request_at
        if elapsed < min_interval:
            sleep(min_interval - elapsed)


class _PersistentCheckpointLaunchTransport(_CachedRateLimitedLaunchTransport):
    def __init__(self, config: LaunchAlphaLiveSourceConfig, repository: StorageRepository) -> None:
        super().__init__(config)
        self.repository = repository

    def __call__(self, url: str, timeout_seconds: float) -> dict[str, Any] | list[dict[str, Any]]:
        now = datetime.now(UTC)
        checkpoint = self.repository.checkpoints.load(self._checkpoint_key(url))
        if checkpoint is not None:
            expires_at = checkpoint.metadata.get("expires_at")
            payload = checkpoint.metadata.get("payload")
            if (
                isinstance(expires_at, str)
                and isinstance(payload, (dict, list))
                and datetime.fromisoformat(expires_at.replace("Z", "+00:00")).astimezone(UTC) > now
            ):
                return copy.deepcopy(payload)
        payload = super().__call__(url, timeout_seconds)
        if self.config.cache_ttl_seconds > 0:
            self.repository.checkpoints.save(
                CollectorCheckpoint(
                    checkpoint_key=self._checkpoint_key(url),
                    cursor=url,
                    observed_at=now,
                    metadata={
                        "expires_at": (
                            now.astimezone(UTC)
                            + timedelta(seconds=self.config.cache_ttl_seconds)
                        ).isoformat(),
                        "payload": payload,
                    },
                )
            )
        return payload

    def _checkpoint_key(self, url: str) -> str:
        digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
        return f"acquisition:launch_alpha_cache:{self.config.source_name}:{digest}"


def _http_json_get_transport(
    url: str,
    timeout_seconds: float,
    *,
    retry_attempts: int = 3,
    retry_backoff_seconds: float = 0.5,
) -> dict[str, Any] | list[dict[str, Any]]:
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
                body = resp.json()
            if isinstance(body, (dict, list)):
                return body
            raise ValueError("invalid_launch_snapshot_http_payload")
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
    if isinstance(last_error, httpx.HTTPStatusError):
        raise RuntimeError(
            f"launch_http_{last_error.response.status_code}:{url[:120]}"
        ) from last_error
    if last_error is not None:
        raise last_error
    raise RuntimeError("http_json_transport_failed_without_error")


def _dexscreener_token_address(record: dict[str, Any]) -> str | None:
    token_address = record.get("tokenAddress") or record.get("address")
    if isinstance(token_address, str) and token_address:
        return token_address
    return None


def _dexscreener_pair_snapshots(
    payload: dict[str, Any] | list[dict[str, Any]],
    *,
    chain: str,
    token_address: str,
) -> list[LaunchPoolSnapshot]:
    pairs = payload.get("pairs") if isinstance(payload, dict) else payload
    if not isinstance(pairs, list):
        return []
    snapshots: list[LaunchPoolSnapshot] = []
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        if str(pair.get("chainId", "")).lower() != chain.lower():
            continue
        base_token = pair.get("baseToken")
        quote_token = pair.get("quoteToken")
        if not isinstance(base_token, dict) or not isinstance(quote_token, dict):
            continue
        base_address = str(base_token.get("address", ""))
        if base_address.lower() != token_address.lower():
            continue
        pair_address = str(pair.get("pairAddress", "")).strip()
        dex = str(pair.get("dexId", "")).strip()
        token_symbol = str(base_token.get("symbol", "")).strip()
        quote_symbol = str(quote_token.get("symbol", "")).strip()
        if not pair_address or not dex or not token_symbol or not quote_symbol:
            continue
        liquidity_usd = _as_nested_float(pair, "liquidity", "usd")
        volume_5m_usd = _as_nested_float(pair, "volume", "m5")
        txns_buys = _as_nested_int(pair, "txns", "m5", "buys")
        txns_sells = _as_nested_int(pair, "txns", "m5", "sells")
        total_trades = txns_buys + txns_sells
        buy_ratio = (txns_buys / total_trades) if total_trades > 0 else 0.0
        pair_created_at = _pair_created_at(pair)
        snapshots.append(
            LaunchPoolSnapshot(
                source_event_id=f"dexscreener:{chain}:{pair_address}:{int(pair_created_at.timestamp())}",
                chain=chain,
                token=token_symbol,
                pool_address=pair_address,
                dex=dex,
                quote_asset=quote_symbol,
                observed_at=pair_created_at,
                initial_liquidity_usd=liquidity_usd,
                buy_notional_5m_usd=round(volume_5m_usd * buy_ratio, 6),
                trade_count_5m=total_trades,
                unique_wallets_5m=max(txns_buys, txns_sells),
                smart_money_wallets_5m=0,
                metadata={
                    "provider": "dexscreener",
                    "pair_url": pair.get("url"),
                    "fdv": pair.get("fdv"),
                    "market_cap": pair.get("marketCap"),
                    "pair_created_at_ms": pair.get("pairCreatedAt"),
                },
            )
        )
    return snapshots


def _pair_created_at(pair: dict[str, Any]) -> datetime:
    created_at = pair.get("pairCreatedAt")
    if isinstance(created_at, (int, float)):
        return datetime.fromtimestamp(float(created_at) / 1000.0, tz=UTC)
    return datetime.now(UTC)


def _as_nested_float(mapping: dict[str, Any], *keys: str) -> float:
    value: Any = mapping
    for key in keys:
        if not isinstance(value, dict):
            return 0.0
        value = value.get(key)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _as_nested_int(mapping: dict[str, Any], *keys: str) -> int:
    return int(round(_as_nested_float(mapping, *keys)))