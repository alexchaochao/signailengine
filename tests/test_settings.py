import os
from pathlib import Path

from core.config import AcquisitionConfig, AppSettings, resolve_evm_routes


def test_default_settings_file_exists() -> None:
    assert AppSettings.default_settings_path().exists()


def test_env_example_includes_live_acquisition_and_api_variables() -> None:
    env_example = Path("/home/alex/Desktop/signalengine/.env.example").read_text(encoding="utf-8")

    assert "SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__OKX__API_KEY=" in env_example
    assert "SIGNALENGINE_VENUES__SOLANA_RPC_URL=" in env_example
    assert "SIGNALENGINE_OBSERVABILITY__MAX_CONSECUTIVE_LIVE_SOURCE_FAILURES=" in env_example
    assert "SIGNALENGINE_ACQUISITION__SOLANA_WALLET_TRADE__ENABLED=" in env_example
    assert "SIGNALENGINE_ACQUISITION__FAILURE_BACKOFF_SECONDS=" in env_example
    assert "SIGNALENGINE_ACQUISITION__SOURCE_COOLDOWN_SECONDS=" in env_example
    assert "SIGNALENGINE_ACQUISITION__SOLANA_WALLET_TRADE__SOURCE_KIND=" in env_example
    assert "SIGNALENGINE_ACQUISITION__SOLANA_WALLET_TRADE__SIGNATURE_ADDRESS=" in env_example
    assert "SIGNALENGINE_ACQUISITION__JUPITER_QUOTE__INPUT_MINT=" in env_example
    assert "SIGNALENGINE_ACQUISITION__JUPITER_QUOTE_ROUTES__SOLANA_BONK__INPUT_MINT=" in env_example
    assert "SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__ZEROEX__API_KEY=" in env_example
    assert "SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__ODOS__API_KEY=" in env_example
    assert "SIGNALENGINE_ACQUISITION__EVM_CHAINS__BASE__CHAIN_ID=" in env_example
    assert "SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__SOURCE_TYPE=" in env_example
    assert "SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__POOL_PROTOCOL=" in env_example


def test_settings_loads_yaml_defaults(monkeypatch) -> None:
    for key in list(os.environ):
        if key.startswith("SIGNALENGINE_"):
            monkeypatch.delenv(key, raising=False)
    settings = AppSettings.load()

    assert settings.runtime.environment == "local"
    assert settings.risk.live_trading_enabled is False
    assert settings.execution.max_retries == 2
    assert settings.observability.max_event_lag_seconds == 30.0
    assert settings.observability.max_consecutive_adapter_failures == 3
    assert settings.replay.regression_thresholds.fill_rate == 0.01
    assert settings.replay.regression_thresholds.rejection_rate == 0.01
    assert settings.replay.regression_thresholds.average_executed_notional_usd == 1.0
    assert settings.observability.max_consecutive_live_source_failures == 3
    assert settings.features.onchain.trade_windows == ["1m", "5m", "15m"]
    assert settings.features.onchain.buy_pressure_primary_window == "5m"
    assert settings.features.onchain.min_trade_count_for_buy_pressure == 10
    assert settings.features.slippage.quote_notional_usd == [1000.0, 5000.0, 10000.0]
    assert settings.features.slippage.publication_notional_usd == 5000.0
    assert settings.acquisition.sync_interval_seconds == 5.0
    assert settings.acquisition.failure_backoff_seconds == 5.0
    assert settings.acquisition.source_cooldown_seconds == 60.0
    assert settings.acquisition.solana_wallet_trade.provider == "solana_rpc_wallet_watch"
    assert settings.acquisition.solana_wallet_trade.source_kind == "wallet"
    assert settings.acquisition.jupiter_quote.provider == "jupiter_quote_api"
    assert settings.acquisition.jupiter_quote_routes == {}
    assert set(settings.acquisition.evm_chains.keys()) == {"arbitrum", "base", "ethereum"}
    assert set(settings.acquisition.evm_routes.keys()) == {
        "arbitrum_transfer",
        "base_quote",
        "ethereum_pool",
    }
    assert set(settings.acquisition.evm_sources.keys()) == {
        "arbitrum_transfer",
        "base_quote",
        "ethereum_pool",
    }
    assert settings.acquisition.evm_chains["base"].chain_id == 8453
    assert settings.acquisition.evm_sources["base_quote"].source_type == "quote"
    assert settings.acquisition.evm_sources["base_quote"].quote_slippage_bps == 100
    assert settings.acquisition.evm_sources["ethereum_pool"].pool_protocol == "uniswap_v3"
    assert settings.acquisition.evm_sources["arbitrum_transfer"].chain == "arbitrum"
    resolved_evm_routes = resolve_evm_routes(settings.acquisition)
    assert resolved_evm_routes["base_quote"].chain_id == 8453
    assert resolved_evm_routes["base_quote"].api_provider == "zeroex"
    assert resolved_evm_routes["ethereum_pool"].provider == "evm_rpc_pool_swap_watch"
    assert settings.acquisition.flow_alpha_sources["base_aero_wallet_flow"].observe_only is False
    assert settings.live.require_environment_separation is True
    assert settings.live.rollout.global_kill_switch_enabled is False
    assert settings.live.rollout.capped_notional_usd == 100.0
    assert settings.live.balance.native_asset_sources["solana"].provider == "solana_rpc"
    assert settings.live.balance.native_asset_sources["ethereum"].provider == "evm_rpc"
    assert settings.live.pricing.native_asset_sources["solana"].provider == "coingecko_simple_price"
    assert settings.live.pricing.native_asset_sources["ethereum"].provider == "binance_ticker_price"
    assert settings.live.pricing.native_asset_sources["solana"].asset == "SOL"
    assert settings.live.pricing.timeout_seconds == 3.0
    assert settings.live.wallet_intelligence.chain == "solana"
    assert settings.live.wallet_intelligence.chain_index == "501"
    assert settings.live.wallet_intelligence.raw_event_batch_size == 1000
    assert settings.live.wallet_intelligence.sync_interval_seconds == 300.0
    assert settings.venues.solana_rpc_timeout_seconds == 5.0
    assert settings.venues.solana_rpc_max_retries == 2
    assert settings.venues.solana_quote_slippage_bps == 100
    assert settings.venues.native_asset_rpc["ethereum"].timeout_seconds == 5.0
    assert settings.venues.native_asset_rpc["ethereum"].max_retries == 2
    assert settings.venues.native_asset_rpc["base"].url == "https://base.publicnode.com"
    assert settings.venues.native_asset_rpc["arbitrum"].url == "https://arbitrum.publicnode.com"


def test_legacy_evm_transfer_trade_is_projected_into_evm_registry() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "evm_transfer_trade": {
                    "enabled": True,
                    "chain": "base",
                    "wallet_address": "0x00000000000000000000000000000000000000aa",
                    "token": "AERO",
                    "token_contract": "0x00000000000000000000000000000000000000bb",
                    "quote_contract": "0x00000000000000000000000000000000000000cc",
                }
            }
        }
    )

    acquisition = AcquisitionConfig.model_validate(settings.acquisition)

    assert acquisition.evm_routes["evm_transfer_trade"].source_type == "transfer_trade"
    assert acquisition.evm_sources["evm_transfer_trade"].wallet_address == "0x00000000000000000000000000000000000000aa"
    resolved_evm_routes = resolve_evm_routes(acquisition)
    assert resolved_evm_routes["evm_transfer_trade"].provider == "evm_rpc_transfer_watch"


def test_environment_overrides_yaml(monkeypatch) -> None:
    monkeypatch.setenv("SIGNALENGINE_RUNTIME__ENVIRONMENT", "paper")
    monkeypatch.setenv("SIGNALENGINE_RISK__MAX_CONCURRENT_POSITIONS", "7")
    monkeypatch.setenv("SIGNALENGINE_EXECUTION__MAX_RETRIES", "4")
    monkeypatch.setenv("SIGNALENGINE_OBSERVABILITY__MAX_EVENT_LAG_SECONDS", "45.0")
    monkeypatch.setenv("SIGNALENGINE_OBSERVABILITY__MAX_CONSECUTIVE_LIVE_SOURCE_FAILURES", "2")
    monkeypatch.setenv("SIGNALENGINE_LIVE__ROLLOUT__GLOBAL_KILL_SWITCH_ENABLED", "true")
    monkeypatch.setenv(
        "SIGNALENGINE_LIVE__BALANCE__NATIVE_ASSET_SOURCES__ETHEREUM__PROVIDER",
        "evm_rpc",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__OKX__API_KEY",
        "test-okx-key",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_LIVE__CREDENTIALS__CHAIN_WALLETS__SOLANA__WALLET_ADDRESS",
        "wallet-solana",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_LIVE__CREDENTIALS__CHAIN_WALLETS__ETHEREUM__WALLET_ADDRESS",
        "wallet-eth",
    )
    monkeypatch.setenv("SIGNALENGINE_LIVE__PRICING__TIMEOUT_SECONDS", "7.5")
    monkeypatch.setenv(
        "SIGNALENGINE_LIVE__PRICING__NATIVE_ASSET_SOURCES__SOLANA__URL",
        "https://prices.example/solana-usd",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_LIVE__PRICING__NATIVE_ASSET_SOURCES__ETHEREUM__PROVIDER",
        "binance_ticker_price",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_LIVE__PRICING__NATIVE_ASSET_SOURCES__ETHEREUM__ASSET",
        "ETH",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_LIVE__PRICING__NATIVE_ASSET_SOURCES__ETHEREUM__QUOTE_CURRENCY",
        "USD",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_LIVE__PRICING__NATIVE_ASSET_SOURCES__ETHEREUM__LOOKUP_KEY",
        "ETHUSDT",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_LIVE__PRICING__NATIVE_ASSET_SOURCES__ETHEREUM__URL",
        "https://prices.example/eth-usd",
    )
    monkeypatch.setenv("SIGNALENGINE_LIVE__WALLET_INTELLIGENCE__CHAIN", "base")
    monkeypatch.setenv("SIGNALENGINE_LIVE__WALLET_INTELLIGENCE__CHAIN_INDEX", "8453")
    monkeypatch.setenv("SIGNALENGINE_LIVE__WALLET_INTELLIGENCE__TOKEN", "AERO")
    monkeypatch.setenv("SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__OKX__PROJECT_ID", "01")
    monkeypatch.setenv("SIGNALENGINE_LIVE__WALLET_INTELLIGENCE__RAW_EVENT_BATCH_SIZE", "250")
    monkeypatch.setenv("SIGNALENGINE_FEATURES__ONCHAIN__MIN_TRADE_COUNT_FOR_BUY_PRESSURE", "3")
    monkeypatch.setenv("SIGNALENGINE_FEATURES__SLIPPAGE__PUBLICATION_NOTIONAL_USD", "2500.0")
    monkeypatch.setenv("SIGNALENGINE_ACQUISITION__SYNC_INTERVAL_SECONDS", "12.5")
    monkeypatch.setenv("SIGNALENGINE_ACQUISITION__FAILURE_BACKOFF_SECONDS", "9.0")
    monkeypatch.setenv("SIGNALENGINE_ACQUISITION__SOURCE_COOLDOWN_SECONDS", "30.0")
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__SOLANA_WALLET_TRADE__ENABLED",
        "true",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__SOLANA_WALLET_TRADE__WALLET_ADDRESS",
        "wallet-watch-1",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__SOLANA_WALLET_TRADE__SOURCE_KIND",
        "address",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__SOLANA_WALLET_TRADE__SIGNATURE_ADDRESS",
        "pool-address-1",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__JUPITER_QUOTE__INPUT_MINT",
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__JUPITER_QUOTE_ROUTES__SOLANA_BONK__CHAIN",
        "solana",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__JUPITER_QUOTE_ROUTES__SOLANA_BONK__TOKEN",
        "BONK",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__JUPITER_QUOTE_ROUTES__SOLANA_BONK__INPUT_MINT",
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__EVM_CHAINS__BASE__CHAIN_ID",
        "8453",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__EVM_CHAINS__BASE__API_PROVIDER",
        "odos",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__EVM_CHAINS__BASE__QUOTE_API_URL",
        "https://api.odos.xyz/sor/quote/v2",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__CHAIN",
        "base",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__SOURCE_TYPE",
        "quote",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__TOKEN",
        "AERO",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__TOKEN_CONTRACT",
        "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__QUOTE_CONTRACT",
        "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__JUPITER_QUOTE_ROUTES__SOLANA_BONK__OUTPUT_MINT",
        "DezXAZ8z7PnrnRJjz3wXBoRGnDYyWmM7RaTvfZfM7r7E",
    )
    monkeypatch.setenv("SIGNALENGINE_REPLAY__REGRESSION_THRESHOLDS__FILL_RATE", "2.5")
    monkeypatch.setenv(
        "SIGNALENGINE_REPLAY__REGRESSION_THRESHOLDS__STRATEGY_EXECUTED_NOTIONAL_USD",
        "25.0",
    )
    monkeypatch.setenv("SIGNALENGINE_VENUES__DEX_ADAPTER", "solana_stub")
    monkeypatch.setenv("SIGNALENGINE_VENUES__SOLANA_RPC_TIMEOUT_SECONDS", "8.5")
    monkeypatch.setenv("SIGNALENGINE_VENUES__SOLANA_RPC_MAX_RETRIES", "6")
    monkeypatch.setenv(
        "SIGNALENGINE_VENUES__NATIVE_ASSET_RPC__ETHEREUM__URL",
        "https://rpc.ethereum.example",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_VENUES__NATIVE_ASSET_RPC__ETHEREUM__TIMEOUT_SECONDS",
        "6.5",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_VENUES__NATIVE_ASSET_RPC__ETHEREUM__MAX_RETRIES",
        "3",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__FLOW_ALPHA_SOURCES__BASE_AERO_WALLET_FLOW__ENABLED",
        "true",
    )
    monkeypatch.setenv(
        "SIGNALENGINE_ACQUISITION__FLOW_ALPHA_SOURCES__BASE_AERO_WALLET_FLOW__OBSERVE_ONLY",
        "true",
    )

    settings = AppSettings.load(Path("/home/alex/Desktop/signalengine/infra/settings.yaml"))

    assert settings.runtime.environment == "paper"
    assert settings.risk.max_concurrent_positions == 7
    assert settings.execution.max_retries == 4
    assert settings.observability.max_event_lag_seconds == 45.0
    assert settings.observability.max_consecutive_live_source_failures == 2
    assert settings.replay.regression_thresholds.fill_rate == 2.5
    assert settings.replay.regression_thresholds.strategy_executed_notional_usd == 25.0
    assert settings.live.rollout.global_kill_switch_enabled is True
    assert settings.live.balance.native_asset_sources["ethereum"].provider == "evm_rpc"
    assert settings.live.credentials.dex_providers["okx"].api_key == "test-okx-key"
    assert settings.live.credentials.dex_providers["okx"].project_id == "01"
    assert settings.live.credentials.chain_wallets["solana"].wallet_address == "wallet-solana"
    assert settings.live.credentials.chain_wallets["ethereum"].wallet_address == "wallet-eth"
    assert settings.live.pricing.timeout_seconds == 7.5
    assert settings.live.pricing.native_asset_sources["solana"].url == "https://prices.example/solana-usd"
    assert settings.live.pricing.native_asset_sources["ethereum"].provider == "binance_ticker_price"
    assert settings.live.pricing.native_asset_sources["ethereum"].lookup_key == "ETHUSDT"
    assert settings.live.wallet_intelligence.chain == "base"
    assert settings.live.wallet_intelligence.chain_index == "8453"
    assert settings.live.wallet_intelligence.token == "AERO"
    assert settings.live.wallet_intelligence.raw_event_batch_size == 250
    assert settings.features.onchain.min_trade_count_for_buy_pressure == 3
    assert settings.features.slippage.publication_notional_usd == 2500.0
    assert settings.acquisition.sync_interval_seconds == 12.5
    assert settings.acquisition.failure_backoff_seconds == 9.0
    assert settings.acquisition.source_cooldown_seconds == 30.0
    assert settings.acquisition.solana_wallet_trade.enabled is True
    assert settings.acquisition.solana_wallet_trade.source_kind == "address"
    assert settings.acquisition.solana_wallet_trade.signature_address == "pool-address-1"
    assert settings.acquisition.solana_wallet_trade.wallet_address == "wallet-watch-1"
    assert settings.acquisition.flow_alpha_sources["base_aero_wallet_flow"].enabled is True
    assert settings.acquisition.flow_alpha_sources["base_aero_wallet_flow"].observe_only is True
    assert (
        settings.acquisition.jupiter_quote.input_mint
        == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    )
    assert settings.acquisition.jupiter_quote_routes["solana_bonk"].chain == "solana"
    assert settings.acquisition.jupiter_quote_routes["solana_bonk"].token == "BONK"
    assert (
        settings.acquisition.jupiter_quote_routes["solana_bonk"].output_mint
        == "DezXAZ8z7PnrnRJjz3wXBoRGnDYyWmM7RaTvfZfM7r7E"
    )
    assert settings.acquisition.evm_chains["base"].api_provider == "odos"
    assert settings.acquisition.evm_routes["base_quote"].token == "AERO"
    assert settings.acquisition.evm_sources["base_quote"].token_contract == "0x940181a94A35A4569E4529A3CDfB74e38FD98631"
    resolved_evm_routes = resolve_evm_routes(settings.acquisition)
    assert resolved_evm_routes["base_quote"].chain_id == 8453
    assert resolved_evm_routes["base_quote"].api_provider == "odos"
    assert resolved_evm_routes["base_quote"].quote_api_url == "https://api.odos.xyz/sor/quote/v2"
    assert settings.venues.dex_adapter == "solana_stub"
    assert settings.venues.solana_rpc_timeout_seconds == 8.5
    assert settings.venues.solana_rpc_max_retries == 6
    assert settings.venues.native_asset_rpc["ethereum"].url == "https://rpc.ethereum.example"
    assert settings.venues.native_asset_rpc["ethereum"].timeout_seconds == 6.5
    assert settings.venues.native_asset_rpc["ethereum"].max_retries == 3


def test_settings_reject_yaml_live_credentials(tmp_path) -> None:
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(
        "live:\n  credentials:\n    dex_providers:\n      okx:\n        api_key: leaked\n",
        encoding="utf-8",
    )

    try:
        AppSettings.load(settings_path)
    except ValueError as error:
        assert str(error) == "live credentials must be provided via environment variables"
    else:
        raise AssertionError("expected YAML live credentials to be rejected")