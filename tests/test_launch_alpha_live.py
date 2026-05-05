from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.error import URLError

from core.config import AppSettings, LaunchAlphaLiveSourceConfig
from core.schemas import CollectorCheckpoint
from discovery.live_sources import (
    HttpLaunchSnapshotSource,
    _CachedRateLimitedLaunchTransport,
    _PersistentCheckpointLaunchTransport,
)


def test_launch_live_source_filters_stale_and_low_quality_snapshots() -> None:
    now = datetime.now(UTC)
    config = LaunchAlphaLiveSourceConfig(
        enabled=True,
        source_name="launch_alpha_solana",
        chain="solana",
        source_url="https://example.invalid/launches",
        dex_allowlist=["raydium"],
        quote_asset_allowlist=["USDC"],
        min_initial_liquidity_usd=20_000.0,
        min_buy_notional_5m_usd=10_000.0,
        min_trade_count_5m=10,
        min_unique_wallets_5m=8,
        min_liquidity_lock_ratio=0.85,
        max_creator_hold_pct=0.15,
        token_denylist=["SCAM"],
    )

    source = HttpLaunchSnapshotSource(
        AppSettings.load(),
        config,
        transport=lambda url, timeout_seconds: {
            "records": [
                {
                    "source_event_id": "good-1",
                    "chain": "solana",
                    "token": "GOOD",
                    "pool_address": "pool-good",
                    "dex": "raydium",
                    "quote_asset": "USDC",
                    "observed_at": now.isoformat(),
                    "initial_liquidity_usd": 40_000.0,
                    "liquidity_lock_ratio": 0.9,
                    "buy_notional_5m_usd": 15_000.0,
                    "trade_count_5m": 14,
                    "unique_wallets_5m": 9,
                    "smart_money_wallets_5m": 2,
                    "creator_hold_pct": 0.1,
                },
                {
                    "source_event_id": "stale-1",
                    "chain": "solana",
                    "token": "OLD",
                    "pool_address": "pool-old",
                    "dex": "raydium",
                    "quote_asset": "USDC",
                    "observed_at": (now - timedelta(minutes=20)).isoformat(),
                    "initial_liquidity_usd": 40_000.0,
                    "liquidity_lock_ratio": 0.9,
                    "buy_notional_5m_usd": 15_000.0,
                    "trade_count_5m": 14,
                    "unique_wallets_5m": 9,
                    "smart_money_wallets_5m": 2,
                    "creator_hold_pct": 0.1,
                },
                {
                    "source_event_id": "denied-1",
                    "chain": "solana",
                    "token": "SCAM",
                    "pool_address": "pool-scam",
                    "dex": "raydium",
                    "quote_asset": "USDC",
                    "observed_at": now.isoformat(),
                    "initial_liquidity_usd": 40_000.0,
                    "liquidity_lock_ratio": 0.9,
                    "buy_notional_5m_usd": 15_000.0,
                    "trade_count_5m": 14,
                    "unique_wallets_5m": 9,
                    "smart_money_wallets_5m": 2,
                    "creator_hold_pct": 0.1,
                },
                {
                    "source_event_id": "thin-1",
                    "chain": "solana",
                    "token": "THIN",
                    "pool_address": "pool-thin",
                    "dex": "raydium",
                    "quote_asset": "USDC",
                    "observed_at": now.isoformat(),
                    "initial_liquidity_usd": 8_000.0,
                    "liquidity_lock_ratio": 0.9,
                    "buy_notional_5m_usd": 2_000.0,
                    "trade_count_5m": 3,
                    "unique_wallets_5m": 2,
                    "smart_money_wallets_5m": 0,
                    "creator_hold_pct": 0.1,
                },
            ]
        },
    )

    snapshots = source.fetch_snapshots()

    assert len(snapshots) == 1
    assert snapshots[0].token == "GOOD"


def test_launch_live_source_builds_dexscreener_snapshots_from_realistic_payloads() -> None:
    now = datetime.now(UTC)
    token_address = "So11111111111111111111111111111111111111112"
    config = LaunchAlphaLiveSourceConfig(
        enabled=True,
        provider="dexscreener_latest_profiles",
        source_name="launch_alpha_solana",
        chain="solana",
        source_url="https://api.dexscreener.com/token-profiles/latest/v1",
        pair_detail_url="https://api.dexscreener.com/latest/dex/tokens",
        max_seed_records=5,
        dex_allowlist=["raydium"],
        quote_asset_allowlist=["USDC"],
        min_initial_liquidity_usd=20_000.0,
        min_buy_notional_5m_usd=10_000.0,
        min_trade_count_5m=10,
        min_unique_wallets_5m=8,
    )

    def transport(url: str, timeout_seconds: float):
        _ = timeout_seconds
        if url == config.source_url:
            return [
                {
                    "chainId": "solana",
                    "tokenAddress": token_address,
                }
            ]
        return {
            "pairs": [
                {
                    "chainId": "solana",
                    "dexId": "raydium",
                    "pairAddress": "pair-1",
                    "url": "https://dexscreener.com/solana/pair-1",
                    "pairCreatedAt": int(now.timestamp() * 1000),
                    "baseToken": {"address": token_address, "symbol": "NEWTKN"},
                    "quoteToken": {"address": "usdc-1", "symbol": "USDC"},
                    "liquidity": {"usd": 45000.0},
                    "volume": {"m5": 22000.0},
                    "txns": {"m5": {"buys": 14, "sells": 4}},
                }
            ]
        }

    source = HttpLaunchSnapshotSource(AppSettings.load(), config, transport=transport)

    snapshots = source.fetch_snapshots()

    assert len(snapshots) == 1
    assert snapshots[0].pool_address == "pair-1"
    assert snapshots[0].buy_notional_5m_usd > 15000.0
    assert snapshots[0].trade_count_5m == 18


def test_launch_live_source_uses_fallback_source_url() -> None:
    now = datetime.now(UTC)
    calls: list[str] = []
    config = LaunchAlphaLiveSourceConfig(
        enabled=True,
        provider="http_snapshot_json",
        source_name="launch_alpha_solana",
        chain="solana",
        source_url="https://primary.invalid/launches",
        fallback_source_urls=["https://fallback.invalid/launches"],
    )

    def transport(url: str, timeout_seconds: float):
        _ = timeout_seconds
        calls.append(url)
        if url == config.source_url:
            raise URLError("primary down")
        return {
            "records": [
                {
                    "source_event_id": "good-1",
                    "chain": "solana",
                    "token": "GOOD",
                    "pool_address": "pool-good",
                    "dex": "raydium",
                    "quote_asset": "USDC",
                    "observed_at": now.isoformat(),
                    "initial_liquidity_usd": 40_000.0,
                    "buy_notional_5m_usd": 15_000.0,
                    "trade_count_5m": 14,
                    "unique_wallets_5m": 9,
                }
            ]
        }

    source = HttpLaunchSnapshotSource(AppSettings.load(), config, transport=transport)

    snapshots = source.fetch_snapshots()

    assert len(snapshots) == 1
    assert calls == [config.source_url, config.fallback_source_urls[0]]


def test_launch_transport_caches_repeated_requests(monkeypatch) -> None:
    config = LaunchAlphaLiveSourceConfig(enabled=True, cache_ttl_seconds=30.0)
    calls: list[str] = []

    monkeypatch.setattr(
        "discovery.live_sources._http_json_get_transport",
        lambda url, timeout_seconds: calls.append(url) or {"records": []},
    )

    transport = _CachedRateLimitedLaunchTransport(config)

    first = transport("https://example.invalid/a", 5.0)
    second = transport("https://example.invalid/a", 5.0)

    assert first == {"records": []}
    assert second == {"records": []}
    assert calls == ["https://example.invalid/a"]


def test_launch_transport_retries_then_succeeds(monkeypatch) -> None:
    config = LaunchAlphaLiveSourceConfig(
        enabled=True,
        retry_attempts=3,
        retry_backoff_seconds=0.01,
        cache_ttl_seconds=0.0,
    )
    calls: list[str] = []

    def stub_get(url: str, timeout_seconds: float):
        _ = timeout_seconds
        calls.append(url)
        if len(calls) < 3:
            raise URLError("temporary reset")
        return {"records": []}

    monkeypatch.setattr("discovery.live_sources._http_json_get_transport", stub_get)
    monkeypatch.setattr("discovery.live_sources.sleep", lambda seconds: None)

    transport = _CachedRateLimitedLaunchTransport(config)

    payload = transport("https://example.invalid/a", 5.0)

    assert payload == {"records": []}
    assert len(calls) == 3


def test_launch_transport_uses_persistent_checkpoint_cache(monkeypatch) -> None:
    config = LaunchAlphaLiveSourceConfig(enabled=True, source_name="launch_alpha_solana")
    calls: list[str] = []

    class StubCheckpointStore:
        def __init__(self) -> None:
            self.data: dict[str, CollectorCheckpoint] = {}

        def load(self, checkpoint_key):
            return self.data.get(checkpoint_key)

        def save(self, checkpoint):
            self.data[checkpoint.checkpoint_key] = checkpoint

    class StubRepository:
        def __init__(self) -> None:
            self.checkpoints = StubCheckpointStore()

    monkeypatch.setattr(
        "discovery.live_sources._http_json_get_transport",
        lambda url, timeout_seconds: calls.append(url) or {"records": []},
    )

    repository = StubRepository()
    transport = _PersistentCheckpointLaunchTransport(config, repository)

    first = transport("https://example.invalid/a", 5.0)
    second = transport("https://example.invalid/a", 5.0)

    assert first == {"records": []}
    assert second == {"records": []}
    assert calls == ["https://example.invalid/a"]