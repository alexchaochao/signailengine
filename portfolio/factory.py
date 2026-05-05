from __future__ import annotations

from typing import Callable

from core.config import AppSettings
from execution.evm_rpc import EvmRpcClient
from execution.solana_rpc import SolanaRpcClient
from portfolio.balance_provider import (
    BalanceProvider,
    EvmRpcBalanceProvider,
    NullBalanceProvider,
    RoutedBalanceProvider,
    SolanaRpcBalanceProvider,
)
from portfolio.price_provider import PriceProvider, build_price_provider

BalanceProviderBuilder = Callable[[AppSettings, str], BalanceProvider]


def build_balance_provider(settings: AppSettings) -> BalanceProvider:
    if (
        settings.runtime.environment != "live"
        or not settings.live.rollout.enforce_balance_preflight
    ):
        return NullBalanceProvider()
    return RoutedBalanceProvider(settings, providers=_build_native_asset_providers(settings))


def _build_native_asset_providers(settings: AppSettings) -> dict[tuple[str, str], BalanceProvider]:
    price_provider = build_price_provider(settings)
    registry = _balance_provider_registry(settings, price_provider)
    providers: dict[tuple[str, str], BalanceProvider] = {}

    for chain, raw_source_config in settings.live.balance.native_asset_sources.items():
        provider_name = _source_provider_name(raw_source_config)
        builder = registry.get(provider_name)
        if builder is None:
            continue
        providers[(chain, provider_name)] = builder(settings, chain)

    return providers


def _balance_provider_registry(
    settings: AppSettings,
    price_provider: PriceProvider,
) -> dict[str, BalanceProviderBuilder]:
    return {
        "solana_rpc": lambda app_settings, chain: _build_solana_balance_provider(
            app_settings,
            chain,
            price_provider,
        ),
        "evm_rpc": lambda app_settings, chain: _build_evm_balance_provider(
            app_settings,
            chain,
            price_provider,
        ),
    }


def _build_solana_balance_provider(
    settings: AppSettings,
    chain: str,
    price_provider: PriceProvider,
) -> BalanceProvider:
    if chain != "solana":
        raise ValueError(f"unsupported_solana_balance_chain:{chain}")
    return SolanaRpcBalanceProvider(
        settings,
        rpc_client=SolanaRpcClient(settings.venues),
        price_provider=price_provider,
    )


def _build_evm_balance_provider(
    settings: AppSettings,
    chain: str,
    price_provider: PriceProvider,
) -> BalanceProvider:
    return EvmRpcBalanceProvider(
        settings,
        chain=chain,
        rpc_client=EvmRpcClient(settings.venues, chain=chain),
        price_provider=price_provider,
    )


def _source_provider_name(raw_source_config: object) -> str:
    if isinstance(raw_source_config, dict):
        provider_name = raw_source_config.get("provider")
    else:
        provider_name = getattr(raw_source_config, "provider", None)
    if not isinstance(provider_name, str):
        raise ValueError("invalid_live_balance_source_config")
    return provider_name
