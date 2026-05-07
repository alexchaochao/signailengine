from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic import field_validator, model_validator


class RuntimeConfig(BaseModel):
    app_name: str = "signalengine"
    environment: str = "local"
    log_level: str = "INFO"


class RedisConfig(BaseModel):
    url: str = "redis://localhost:6379/0"
    raw_events_stream: str = "raw-events"
    signals_stream: str = "signals"
    decisions_stream: str = "decisions"
    executions_stream: str = "executions"
    dead_letter_stream: str = "dead-letters"


class PostgresConfig(BaseModel):
    url: str = "postgresql+psycopg://signalengine:signalengine@localhost:5432/signalengine"
    echo: bool = False
    pool_size: int = 1
    max_overflow: int = 0


class ObservabilityConfig(BaseModel):
    service_namespace: str = "signalengine"
    metrics_host: str = "0.0.0.0"
    metrics_port: int = 9000
    max_event_lag_seconds: float = 30.0
    max_pipeline_latency_seconds: float = 5.0
    max_consecutive_adapter_failures: int = 3
    max_risk_rejections: int = 5
    max_consecutive_live_source_failures: int = 3


class RiskConfig(BaseModel):
    max_token_exposure: float = 0.10
    max_chain_exposure: float = 0.40
    max_concurrent_positions: int = 5
    max_daily_loss: float = 0.03
    cooldown_minutes: int = 30
    min_liquidity_usd: float = 100000
    min_volume_5m_usd: float = 25000
    max_slippage_bps: int = 150
    live_trading_enabled: bool = False
    # Minimum seconds between two consecutive state transitions for the same
    # token.  A higher value reduces state-flapping but also delays reaction
    # to fast-moving tokens.  New-launch tokens need a lower value (≈30s).
    min_transition_interval_seconds: int = 30


class ExecutionConfig(BaseModel):
    max_retries: int = 2
    retry_backoff_seconds: float = 1.0
    recover_pending_on_startup: bool = True


class ReplayRegressionThresholds(BaseModel):
    fill_rate: float = 0.01
    rejection_rate: float = 0.01
    average_executed_notional_usd: float = 1.0
    route_count: float = 1.0
    risk_rejection_reason_count: float = 1.0
    strategy_executed_notional_usd: float = 1.0


class ReplayConfig(BaseModel):
    regression_thresholds: ReplayRegressionThresholds = Field(
        default_factory=ReplayRegressionThresholds
    )


class OnchainFeatureConfig(BaseModel):
    trade_windows: list[str] = Field(default_factory=lambda: ["1m", "5m", "15m"])
    buy_pressure_primary_window: str = "5m"
    min_trade_count_for_buy_pressure: int = 10
    max_trade_lag_seconds: float = 30.0
    # Trades below this notional (in USD) still produce raw events + aggregator
    # features but skip publishing an onchain.trade_fact stream event, reducing
    # noise in the wallet intelligence projection pipeline.
    min_trade_notional_usd: float = 10.0


class SlippageFeatureConfig(BaseModel):
    quote_notional_usd: list[float] = Field(default_factory=lambda: [1000.0, 5000.0, 10000.0])
    publication_notional_usd: float = 5000.0
    max_quote_age_seconds: float = 5.0
    allow_curve_fallback: bool = True


class FeatureConfig(BaseModel):
    onchain: OnchainFeatureConfig = Field(default_factory=OnchainFeatureConfig)
    slippage: SlippageFeatureConfig = Field(default_factory=SlippageFeatureConfig)


class TelegramNotificationConfig(BaseModel):
    enabled: bool = False
    bot_token: str | None = None
    chat_id: str | None = None
    publish_alpha_types: list[str] = Field(default_factory=lambda: ["LAUNCH", "CATALYST"])
    min_score: float = 0.0
    message_template_version: str = "v1"
    consumer_group: str = "telegram-publisher"
    consumer_name: str = "telegram-publisher-1"

    @field_validator("bot_token", "chat_id", "message_template_version", mode="before")
    @classmethod
    def coerce_string_fields(cls, value: object) -> str | None:
        if value is None:
            return None
        return str(value)

    @field_validator("publish_alpha_types", mode="before")
    @classmethod
    def normalize_alpha_types(cls, value: object) -> list[str]:
        if value is None:
            return ["LAUNCH", "CATALYST"]
        if isinstance(value, list):
            return [str(item).upper() for item in value]
        return [str(value).upper()]


class NotificationsConfig(BaseModel):
    telegram: TelegramNotificationConfig = Field(default_factory=TelegramNotificationConfig)


class SolanaWalletTradeSourceConfig(BaseModel):
    enabled: bool = False
    provider: str = "solana_rpc_wallet_watch"
    source_name: str = "solana_rpc_wallet"
    checkpoint_key: str = "acquisition:solana_wallet_trades"
    chain: str = "solana"
    source_kind: str = "wallet"
    signature_address: str | None = None
    owner_address: str | None = None
    wallet_address: str | None = None
    token: str = "BONK"
    token_mint: str | None = None
    quote_asset: str = "USDC"
    quote_mint: str | None = None
    quote_asset_usd_rate: float = 1.0
    pool_address: str = "wallet-watch"
    poll_limit: int = 20


class JupiterQuoteSourceConfig(BaseModel):
    enabled: bool = False
    provider: str = "jupiter_quote_api"
    source_name: str = "jupiter_quote_api"
    chain: str = "solana"
    token: str = "BONK"
    input_mint: str | None = None
    output_mint: str | None = None
    input_decimals: int = 6
    output_decimals: int = 5
    quote_notional_usd: float = 5000.0
    slippage_bps: int = 100
    quote_url: str = "https://lite-api.jup.ag/swap/v1/quote"
    swap_url: str = "https://lite-api.jup.ag/swap/v1/swap"
    price_url: str = "https://lite-api.jup.ag/price/v2"


class EvmTransferTradeSourceConfig(BaseModel):
    enabled: bool = False
    provider: str = "evm_rpc_transfer_watch"
    source_name: str = "evm_rpc_transfer"
    checkpoint_key: str = "acquisition:evm_transfer_trades"
    chain: str = "base"
    wallet_address: str | None = None
    token: str = "AERO"
    token_contract: str | None = None
    quote_asset: str = "USDC"
    quote_contract: str | None = None
    token_decimals: int = 18
    quote_decimals: int = 6
    quote_asset_usd_rate: float = 1.0
    pool_address: str = "wallet-watch"
    initial_lookback_blocks: int = 100


class EvmChainConfig(BaseModel):
    chain_id: int | None = None
    provider: str | None = None
    api_provider: str | None = None
    quote_api_url: str | None = None
    price_url: str | None = None


class EvmLiveSourceConfig(BaseModel):
    enabled: bool = False
    source_type: str = "transfer_trade"
    provider: str | None = None
    source_name: str | None = None
    checkpoint_key: str | None = None
    chain: str = "base"
    chain_id: int | None = None
    wallet_address: str | None = None
    token: str = "AERO"
    token_contract: str | None = None
    quote_asset: str = "USDC"
    quote_contract: str | None = None
    token_decimals: int = 18
    quote_decimals: int = 6
    quote_asset_usd_rate: float = 1.0
    pool_address: str | None = None
    pool_protocol: str = "uniswap_v2"
    token_is_token0: bool = True
    initial_lookback_blocks: int = 100
    quote_notional_usd: float = 5000.0
    quote_slippage_bps: int = 100
    quote_api_url: str | None = None
    price_url: str | None = None
    api_provider: str | None = None


class LaunchAlphaLiveSourceConfig(BaseModel):
    enabled: bool = False
    provider: str = "dexscreener_latest_profiles"
    source_name: str | None = None
    chain: str = "solana"
    source_url: str = "https://api.dexscreener.com/token-profiles/latest/v1"
    fallback_source_urls: list[str] = Field(default_factory=list)
    pair_detail_url: str = "https://api.dexscreener.com/latest/dex/tokens"
    timeout_seconds: float = 5.0
    retry_attempts: int = 3
    retry_backoff_seconds: float = 0.5
    cache_ttl_seconds: float = 15.0
    min_request_interval_seconds: float = 0.25
    max_seed_records: int = 30
    max_snapshot_age_seconds: float = 120.0
    dex_allowlist: list[str] = Field(default_factory=list)
    quote_asset_allowlist: list[str] = Field(
        default_factory=lambda: ["USDC", "USDT", "SOL", "WETH"]
    )
    token_allowlist: list[str] = Field(default_factory=list)
    token_denylist: list[str] = Field(default_factory=list)
    min_initial_liquidity_usd: float = 7_500.0
    min_buy_notional_5m_usd: float = 3_500.0
    min_trade_count_5m: int = 6
    min_unique_wallets_5m: int = 4
    min_liquidity_lock_ratio: float = 0.8
    max_creator_hold_pct: float = 0.2
    # ── Rug / pool-quality filters ────────────────────────────────────────
    # Minimum deployer age in days to filter out fresh-created scam pools
    min_deployer_age_days: int = 30
    # Require honeypot simulation result to be False (token is tradeable)
    require_honeypot_sim: bool = True
    # Require mint authority to be revoked (no further token minting)
    require_mint_revoked: bool = True
    # Require deployer has transferred LP tokens (liquidity not fully held by creator)
    require_creator_lp_transfer: bool = True


class CatalystAlphaLiveSourceConfig(BaseModel):
    enabled: bool = False
    provider: str = "rss_keyword_feed"
    source_name: str | None = None
    source_url: str = ""
    timeout_seconds: float = 5.0
    retry_attempts: int = 3
    retry_backoff_seconds: float = 0.5
    # Per-source polling interval. Fast sources (exchangeInfo, WS) use 0.5~2s;
    # announcement/CMS sources use 30~60s. Falls back to acquisition.sync_interval_seconds.
    sync_interval_seconds: float | None = None
    max_entries: int = 25
    max_snapshot_age_minutes: int = 360
    required_keywords: list[str] = Field(
        default_factory=lambda: ["list", "listing", "launch", "roadmap"]
    )
    excluded_keywords: list[str] = Field(
        default_factory=lambda: ["delist", "removal", "maintenance", "suspend"]
    )
    venue: str | None = None
    default_chain: str = "unknown"
    default_catalyst_type: str = "cex_listing_announcement"
    impact_score: float = 0.82
    credibility_score: float = 0.9
    extraction_mode: str = "llm_with_heuristic_fallback"
    extraction_max_entities: int = 3
    # Redis dedup: mark processed entries to avoid repeats
    dedup_enabled: bool = True
    dedup_ttl_hours: int = 24 * 7  # 7 days
    # Multi-chain address resolution
    resolve_multi_chain: bool = False


class FlowAlphaBackfillConfig(BaseModel):
    enabled: bool = False
    source_name: str = "flow_alpha_backfill"


class FlowAlphaLiveSourceConfig(BaseModel):
    enabled: bool = False
    observe_only: bool = True
    provider: str = "wallet_intelligence_store"
    source_name: str | None = None
    chain: str = "solana"
    token: str = "BONK"
    venue: str | None = None
    window_minutes: int = 15
    min_whale_buy_usd: float = 10_000.0
    min_netflow_15m_usd: float = 25_000.0
    min_smart_money_inflow_usd: float = 40_000.0
    min_unique_buyer_wallets_15m: int = 4
    min_exchange_outflow_usd: float = 0.0


class SocialLiveSourceConfig(BaseModel):
    enabled: bool = False
    provider: str = "reddit_search_json"
    source_name: str | None = None
    platform: str = "x"
    chain: str | None = None
    token: str | None = None
    query: str | None = None
    query_template: str | None = None
    query_param_name: str = "q"
    source_url: str | None = None
    subreddit: str = "CryptoMoonShots"
    sort: str = "new"
    limit: int = 25
    timeout_seconds: float = 5.0
    retry_attempts: int = 2
    retry_backoff_seconds: float = 0.5
    max_snapshot_age_seconds: float = 300.0
    min_sentiment_score: float = 0.0
    min_velocity_score: float = 0.0
    min_mentions: int = 1
    min_unique_authors: int = 1
    user_agent: str = "signalengine/0.1"


class AcquisitionConfig(BaseModel):
    sync_interval_seconds: float = 5.0
    failure_backoff_seconds: float = 5.0
    source_cooldown_seconds: float = 60.0
    solana_wallet_trade: SolanaWalletTradeSourceConfig = Field(
        default_factory=SolanaWalletTradeSourceConfig
    )
    jupiter_quote: JupiterQuoteSourceConfig = Field(default_factory=JupiterQuoteSourceConfig)
    jupiter_quote_routes: dict[str, JupiterQuoteSourceConfig] = Field(default_factory=dict)
    evm_transfer_trade: EvmTransferTradeSourceConfig = Field(
        default_factory=EvmTransferTradeSourceConfig
    )
    evm_chains: dict[str, EvmChainConfig] = Field(default_factory=dict)
    evm_routes: dict[str, EvmLiveSourceConfig] = Field(default_factory=dict)
    evm_sources: dict[str, EvmLiveSourceConfig] = Field(default_factory=dict)
    launch_alpha_sources: dict[str, LaunchAlphaLiveSourceConfig] = Field(default_factory=dict)
    catalyst_alpha_sources: dict[str, CatalystAlphaLiveSourceConfig] = Field(default_factory=dict)
    flow_alpha_sources: dict[str, FlowAlphaLiveSourceConfig] = Field(default_factory=dict)
    social_sources: dict[str, SocialLiveSourceConfig] = Field(default_factory=dict)

    @model_validator(mode="after")
    def synchronize_evm_route_registries(self) -> "AcquisitionConfig":
        legacy_route = _legacy_evm_transfer_route(self.evm_transfer_trade)
        if legacy_route is not None:
            if "evm_transfer_trade" not in self.evm_routes:
                self.evm_routes["evm_transfer_trade"] = legacy_route
            if "evm_transfer_trade" not in self.evm_sources:
                self.evm_sources["evm_transfer_trade"] = legacy_route
        if self.evm_routes and self.evm_sources:
            merged = dict(self.evm_sources)
            merged.update(self.evm_routes)
            self.evm_routes = dict(merged)
            self.evm_sources = dict(merged)
            return self
        if self.evm_routes:
            self.evm_sources = dict(self.evm_routes)
            return self
        if self.evm_sources:
            self.evm_routes = dict(self.evm_sources)
        return self

    def resolved_evm_routes(self) -> dict[str, EvmLiveSourceConfig]:
        return resolve_evm_routes(self)


class LiveRolloutConfig(BaseModel):
    global_kill_switch_enabled: bool = False
    global_kill_switch_reason: str = ""
    capped_notional_usd: float = 100.0
    min_available_balance_usd: float = 500.0
    enforce_position_preflight: bool = True
    enforce_balance_preflight: bool = True


class ChainWalletCredentialsConfig(BaseModel):
    wallet_address: str | None = None
    private_key: str | None = None

    @field_validator("wallet_address", "private_key", mode="before")
    @classmethod
    def coerce_string_fields(cls, value: object) -> str | None:
        if value is None:
            return None
        return str(value)


class DexProviderCredentialsConfig(BaseModel):
    api_key: str | None = None
    secret_key: str | None = None
    api_passphrase: str | None = None
    project_id: str | None = None

    @field_validator("api_key", "secret_key", "api_passphrase", "project_id", mode="before")
    @classmethod
    def coerce_string_fields(cls, value: object) -> str | None:
        if value is None:
            return None
        return str(value)


class CexProviderCredentialsConfig(BaseModel):
    api_key: str | None = None
    api_secret: str | None = None

    @field_validator("api_key", "api_secret", mode="before")
    @classmethod
    def coerce_string_fields(cls, value: object) -> str | None:
        if value is None:
            return None
        return str(value)


class LiveCredentialsConfig(BaseModel):
    chain_wallets: dict[str, ChainWalletCredentialsConfig] = Field(default_factory=dict)
    dex_providers: dict[str, DexProviderCredentialsConfig] = Field(default_factory=dict)
    cex_providers: dict[str, CexProviderCredentialsConfig] = Field(default_factory=dict)


class NativeAssetBalanceSourceConfig(BaseModel):
    provider: str


class NativeAssetRpcConfig(BaseModel):
    url: str
    timeout_seconds: float = 5.0
    max_retries: int = 2


class NativeAssetPriceSourceConfig(BaseModel):
    provider: str
    asset: str
    quote_currency: str = "USD"
    lookup_key: str
    url: str


class LiveBalanceConfig(BaseModel):
    native_asset_sources: dict[str, NativeAssetBalanceSourceConfig] = Field(
        default_factory=lambda: {
            "solana": NativeAssetBalanceSourceConfig(provider="solana_rpc"),
            "ethereum": NativeAssetBalanceSourceConfig(provider="evm_rpc"),
        }
    )


class LivePricingConfig(BaseModel):
    timeout_seconds: float = 3.0
    max_retries: int = 1
    native_asset_sources: dict[str, NativeAssetPriceSourceConfig] = Field(
        default_factory=lambda: {
            "solana": NativeAssetPriceSourceConfig(
                provider="coingecko_simple_price",
                asset="SOL",
                quote_currency="USD",
                lookup_key="solana",
                url=(
                    "https://api.coingecko.com/api/v3/simple/price?ids=solana"
                    "&vs_currencies=usd"
                ),
            ),
            "ethereum": NativeAssetPriceSourceConfig(
                provider="binance_ticker_price",
                asset="ETH",
                quote_currency="USD",
                lookup_key="ETHUSDT",
                url="https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT",
            ),
        }
    )


class WalletIntelligenceSyncConfig(BaseModel):
    enabled: bool = False
    chain: str = "solana"
    chain_index: str = "501"
    measurement_token: str = "BONK"
    time_frame: str = "3"
    sort_by: str = "1"
    wallet_type: str = "3"
    registry_version: str = "okx_registry_v1"
    refresh_limit: int = 20
    raw_event_batch_size: int = 1000
    sync_interval_seconds: float = 300.0

    @field_validator("chain_index", "time_frame", "sort_by", "wallet_type", mode="before")
    @classmethod
    def coerce_string_fields(cls, value: object) -> str:
        return str(value)


class AlphaCollectorConfig(BaseModel):
    """Configuration for the async cross-dimension data collector.

    Controls how the system collects on-chain, wallet, and social data
    after an alpha.candidate_qualified event is emitted.
    """
    enabled: bool = False
    timeout_seconds: float = 30.0
    max_chains_per_token: int = 1
    priority_chains: list[str] = Field(
        default_factory=lambda: ["solana", "base", "ethereum", "bsc"]
    )
    collection_cooldown_seconds: int = 300
    dexscreener_api_url: str = "https://api.dexscreener.com/latest/dex/tokens"


class LiveConfig(BaseModel):
    require_environment_separation: bool = True
    rollout: LiveRolloutConfig = Field(default_factory=LiveRolloutConfig)
    credentials: LiveCredentialsConfig = Field(default_factory=LiveCredentialsConfig)
    balance: LiveBalanceConfig = Field(default_factory=LiveBalanceConfig)
    pricing: LivePricingConfig = Field(default_factory=LivePricingConfig)
    wallet_intelligence: WalletIntelligenceSyncConfig = Field(
        default_factory=WalletIntelligenceSyncConfig
    )


class LlmConfig(BaseModel):
    enabled: bool = False
    provider: str = "heuristic"
    model: str = "gpt-5.4"
    api_key: str | None = None
    base_url: str | None = None
    timeout_seconds: float = 8.0
    temperature: float = 0.0
    max_evidence_texts: int = 6
    max_summary_chars: int = 280

    @field_validator("provider", "model", "api_key", "base_url", mode="before")
    @classmethod
    def coerce_llm_strings(cls, value: object) -> str | None:
        if value is None:
            return None
        return str(value)


class VenueConfig(BaseModel):
    dex_adapter: str = "solana_primary"
    cex_adapter: str = "binance_paper"
    paper_execution_enabled: bool = True
    solana_rpc_url: str = "https://api.mainnet-beta.solana.com"
    solana_rpc_timeout_seconds: float = 5.0
    solana_rpc_max_retries: int = 2
    solana_quote_slippage_bps: int = 100
    solana_jito_enabled: bool = False
    native_asset_rpc: dict[str, NativeAssetRpcConfig] = Field(
        default_factory=lambda: {
            "ethereum": NativeAssetRpcConfig(
                url="https://ethereum.publicnode.com",
                timeout_seconds=5.0,
                max_retries=2,
            ),
            # "base": NativeAssetRpcConfig(
            #     url="https://base.publicnode.com",
            #     timeout_seconds=5.0,
            #     max_retries=2,
            # ),
            "base": NativeAssetRpcConfig(
                url="https://base.publicnode.com",
                timeout_seconds=5.0,
                max_retries=2,
            ),
        }
    )


class AppSettings(BaseModel):
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    replay: ReplayConfig = Field(default_factory=ReplayConfig)
    features: FeatureConfig = Field(default_factory=FeatureConfig)
    notifications: NotificationsConfig = Field(default_factory=NotificationsConfig)
    acquisition: AcquisitionConfig = Field(default_factory=AcquisitionConfig)
    alpha_collector: AlphaCollectorConfig = Field(default_factory=AlphaCollectorConfig)
    live: LiveConfig = Field(default_factory=LiveConfig)
    llm: LlmConfig = Field(default_factory=LlmConfig)
    venues: VenueConfig = Field(default_factory=VenueConfig)

    @classmethod
    def default_settings_path(cls) -> Path:
        return Path(__file__).resolve().parents[1] / "infra" / "settings.yaml"

    @classmethod
    def load(cls, path: str | Path | None = None) -> "AppSettings":
        settings_path = Path(path) if path else cls.default_settings_path()
        data = _read_yaml(settings_path)
        _validate_no_yaml_live_credentials(data)
        env_data = _read_environment(prefix="SIGNALENGINE_")
        data = _deep_merge(data, env_data)
        return cls(**data)


def _validate_no_yaml_live_credentials(data: dict[str, Any]) -> None:
    live_config = data.get("live")
    if not isinstance(live_config, dict):
        return

    credentials = live_config.get("credentials")
    if not isinstance(credentials, dict):
        return

    if _contains_non_empty_credential_value(credentials):
        raise ValueError("live credentials must be provided via environment variables")


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    if not isinstance(data, dict):
        raise ValueError(f"Settings file must contain a mapping: {path}")

    return data


def _read_environment(prefix: str) -> dict[str, Any]:
    result: dict[str, Any] = {}

    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue

        path = key.removeprefix(prefix).lower().split("__")
        cursor = result

        for part in path[:-1]:
            cursor = cursor.setdefault(part, {})

        cursor[path[-1]] = _coerce_env_value(value)

    return result


def _coerce_env_value(value: str) -> Any:
    stripped = value.strip()
    if stripped.startswith("[") or stripped.startswith("{"):
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass

    lowered = value.lower()

    if lowered in {"true", "false"}:
        return lowered == "true"

    if len(value) > 1 and value.startswith("0") and value.isdigit():
        return value

    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        pass

    return value


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)

    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value

    return merged


def _contains_non_empty_credential_value(payload: Any) -> bool:
    if isinstance(payload, dict):
        return any(_contains_non_empty_credential_value(value) for value in payload.values())
    return payload not in {None, ""}


def resolve_evm_routes(acquisition: AcquisitionConfig) -> dict[str, EvmLiveSourceConfig]:
    resolved: dict[str, EvmLiveSourceConfig] = {}
    raw_routes = dict(acquisition.evm_sources)
    raw_routes.update(acquisition.evm_routes)

    for route_key, raw_route in raw_routes.items():
        resolved[route_key] = resolve_evm_route_config(acquisition, raw_route)

    return resolved


def resolve_evm_route_config(
    acquisition: AcquisitionConfig,
    raw_route: EvmLiveSourceConfig | dict[str, Any],
) -> EvmLiveSourceConfig:
    route = (
        raw_route
        if isinstance(raw_route, EvmLiveSourceConfig)
        else EvmLiveSourceConfig.model_validate(raw_route)
    )
    chain_defaults = acquisition.evm_chains.get(route.chain)
    merged_payload = _default_evm_route_payload(route.chain, route.source_type)
    if isinstance(chain_defaults, EvmChainConfig):
        merged_payload = _deep_merge(
            merged_payload,
            chain_defaults.model_dump(exclude_none=True),
        )
    elif isinstance(chain_defaults, dict):
        merged_payload = _deep_merge(merged_payload, chain_defaults)
    merged_payload = _deep_merge(
        merged_payload,
        route.model_dump(exclude_none=True),
    )
    return EvmLiveSourceConfig.model_validate(merged_payload)


def _default_evm_route_payload(chain: str, source_type: str) -> dict[str, Any]:
    default_provider = {
        "transfer_trade": "evm_rpc_transfer_watch",
        "pool_swap_trade": "evm_rpc_pool_swap_watch",
        "quote": "evm_quote_api",
    }.get(source_type, "evm_rpc_transfer_watch")
    default_chain_ids = {
        "ethereum": 1,
        "base": 8453,
        "arbitrum": 42161,
    }
    return {
        "chain": chain,
        "chain_id": default_chain_ids.get(chain),
        "provider": default_provider,
        "api_provider": "zeroex",
        "quote_api_url": "https://api.0x.org/swap/permit2/price",
        "price_url": "https://api.dexscreener.com/latest/dex/tokens",
    }


def _legacy_evm_transfer_route(
    config: EvmTransferTradeSourceConfig,
) -> EvmLiveSourceConfig | None:
    if not config.enabled:
        return None
    payload = config.model_dump()
    payload["source_type"] = "transfer_trade"
    return EvmLiveSourceConfig.model_validate(payload)