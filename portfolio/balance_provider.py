from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from core.config import AppSettings, NativeAssetBalanceSourceConfig
from core.schemas import ExecutionIntent, VenueType
from execution.evm_rpc import WEI_PER_ETH, EvmBalanceState
from execution.solana_rpc import SolanaBalanceState, SolanaRpcClient
from portfolio.price_provider import PriceProvider

LAMPORTS_PER_SOL = 1_000_000_000


@dataclass(frozen=True)
class BalanceSnapshot:
    available_balance_usd: float
    account_address: str
    source: str
    native_balance: float


class BalanceProvider:
    def get_available_balance(self, intent: ExecutionIntent) -> BalanceSnapshot | None:
        raise NotImplementedError


class SolanaBalanceRpc(Protocol):
    def wallet_balance(self, wallet_address: str) -> SolanaBalanceState:
        ...


class EvmBalanceRpc(Protocol):
    def wallet_balance(self, wallet_address: str) -> EvmBalanceState:
        ...


class NullBalanceProvider(BalanceProvider):
    def get_available_balance(self, intent: ExecutionIntent) -> BalanceSnapshot | None:
        del intent
        return None


class SolanaRpcBalanceProvider(BalanceProvider):
    def __init__(
        self,
        settings: AppSettings,
        rpc_client: SolanaBalanceRpc | None = None,
        price_provider: PriceProvider | None = None,
    ) -> None:
        self.settings = settings
        self.rpc_client = rpc_client or SolanaRpcClient(settings.venues)
        if price_provider is None:
            raise ValueError("price_provider_required")
        self.price_provider = price_provider

    def get_available_balance(self, intent: ExecutionIntent) -> BalanceSnapshot | None:
        if intent.venue_type != VenueType.DEX or intent.chain != "solana":
            return None

        wallet_address = _wallet_address_for_chain(self.settings, intent.chain)
        if not wallet_address:
            return None

        balance_state = self.rpc_client.wallet_balance(wallet_address)
        price_snapshot = self.price_provider.get_native_token_price_usd(intent.chain)
        if price_snapshot is None:
            return None
        native_balance = balance_state.lamports / LAMPORTS_PER_SOL
        available_balance_usd = native_balance * price_snapshot.price
        return BalanceSnapshot(
            available_balance_usd=round(available_balance_usd, 6),
            account_address=wallet_address,
            source=f"solana_rpc+{price_snapshot.source}",
            native_balance=round(native_balance, 9),
        )


class EvmRpcBalanceProvider(BalanceProvider):
    def __init__(
        self,
        settings: AppSettings,
        chain: str,
        rpc_client: EvmBalanceRpc | None = None,
        price_provider: PriceProvider | None = None,
    ) -> None:
        self.settings = settings
        self.chain = chain
        if rpc_client is None:
            raise ValueError("evm_rpc_client_required")
        if price_provider is None:
            raise ValueError("price_provider_required")
        self.rpc_client = rpc_client
        self.price_provider = price_provider

    def get_available_balance(self, intent: ExecutionIntent) -> BalanceSnapshot | None:
        if intent.venue_type != VenueType.DEX or intent.chain != self.chain:
            return None

        wallet_address = _wallet_address_for_chain(self.settings, intent.chain)
        if not wallet_address:
            return None

        balance_state = self.rpc_client.wallet_balance(wallet_address)
        price_snapshot = self.price_provider.get_native_token_price_usd(intent.chain)
        if price_snapshot is None:
            return None
        native_balance = balance_state.wei_balance / WEI_PER_ETH
        available_balance_usd = native_balance * price_snapshot.price
        return BalanceSnapshot(
            available_balance_usd=round(available_balance_usd, 6),
            account_address=wallet_address,
            source=f"evm_rpc+{price_snapshot.source}",
            native_balance=round(native_balance, 9),
        )


class RoutedBalanceProvider(BalanceProvider):
    def __init__(
        self,
        settings: AppSettings,
        providers: dict[tuple[str, str], BalanceProvider],
    ) -> None:
        self.settings = settings
        self.providers = providers

    def get_available_balance(self, intent: ExecutionIntent) -> BalanceSnapshot | None:
        if intent.venue_type != VenueType.DEX:
            return None

        raw_source_config = self.settings.live.balance.native_asset_sources.get(intent.chain)
        if raw_source_config is None:
            return None
        source_config = self._coerce_source_config(raw_source_config)

        provider = self.providers.get((intent.chain, source_config.provider))
        if provider is None:
            raise ValueError(
                f"unsupported_live_balance_provider:{intent.chain}:{source_config.provider}"
            )
        return provider.get_available_balance(intent)

    def _coerce_source_config(
        self,
        source_config: NativeAssetBalanceSourceConfig | dict[str, Any],
    ) -> NativeAssetBalanceSourceConfig:
        if isinstance(source_config, NativeAssetBalanceSourceConfig):
            return source_config
        if isinstance(source_config, dict):
            return NativeAssetBalanceSourceConfig.model_validate(source_config)
        raise ValueError("invalid_live_balance_source_config")


def build_balance_provider(settings: AppSettings) -> BalanceProvider:
    from portfolio.factory import build_balance_provider as build_routed_balance_provider

    return build_routed_balance_provider(settings)


def _wallet_address_for_chain(settings: AppSettings, chain: str) -> str | None:
    chain_wallet = settings.live.credentials.chain_wallets.get(chain)
    if chain_wallet is None:
        return None
    if isinstance(chain_wallet, dict):
        value = chain_wallet.get("wallet_address")
        return str(value) if value else None
    return chain_wallet.wallet_address