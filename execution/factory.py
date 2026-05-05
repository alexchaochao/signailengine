from __future__ import annotations

from dataclasses import dataclass

from core.config import AppSettings
from execution.base import ExecutionAdapter
from execution.cex_bridge import CexPaperExecutor
from execution.dex_executor import DexPaperExecutor
from execution.evm_adapter import EvmDexAdapter
from execution.solana_adapter import SolanaDexAdapter


@dataclass(frozen=True)
class AdapterCapabilities:
    name: str
    supports_live_execution: bool
    requires_solana_rpc: bool = False
    supports_jito: bool = False


def build_dex_adapter(settings: AppSettings) -> ExecutionAdapter:
    adapter, capabilities = _select_dex_adapter(settings)
    _validate_dex_selection(settings, capabilities)
    return adapter


def _select_dex_adapter(settings: AppSettings) -> tuple[ExecutionAdapter, AdapterCapabilities]:
    if settings.venues.dex_adapter == "solana_stub":
        return SolanaDexAdapter(settings), AdapterCapabilities(
            name="solana_stub",
            supports_live_execution=True,
            requires_solana_rpc=True,
            supports_jito=True,
        )
    if settings.venues.dex_adapter == "solana_primary":
        if settings.venues.paper_execution_enabled:
            return DexPaperExecutor(), AdapterCapabilities(
                name="solana_dex_paper",
                supports_live_execution=False,
            )
        return SolanaDexAdapter(settings), AdapterCapabilities(
            name="solana_primary",
            supports_live_execution=True,
            requires_solana_rpc=True,
            supports_jito=True,
        )
    if settings.venues.dex_adapter == "evm_primary":
        if settings.venues.paper_execution_enabled:
            return DexPaperExecutor(), AdapterCapabilities(
                name="evm_dex_paper",
                supports_live_execution=False,
            )
        return EvmDexAdapter(settings), AdapterCapabilities(
            name="evm_primary",
            supports_live_execution=True,
        )
    raise ValueError(f"unsupported_dex_adapter:{settings.venues.dex_adapter}")


def build_cex_adapter(settings: AppSettings) -> ExecutionAdapter:
    adapter, capabilities = _select_cex_adapter(settings)
    _validate_cex_selection(settings, capabilities)
    return adapter


def _select_cex_adapter(settings: AppSettings) -> tuple[ExecutionAdapter, AdapterCapabilities]:
    if settings.venues.cex_adapter == "binance_paper":
        return CexPaperExecutor(), AdapterCapabilities(
            name="binance_paper",
            supports_live_execution=False,
        )
    raise ValueError(f"unsupported_cex_adapter:{settings.venues.cex_adapter}")


def _validate_dex_selection(settings: AppSettings, capabilities: AdapterCapabilities) -> None:
    if settings.venues.solana_quote_slippage_bps <= 0:
        raise ValueError("invalid_solana_quote_slippage_bps")
    if settings.venues.solana_rpc_timeout_seconds <= 0:
        raise ValueError("invalid_solana_rpc_timeout_seconds")
    if settings.venues.solana_rpc_max_retries < 0:
        raise ValueError("invalid_solana_rpc_max_retries")
    if capabilities.requires_solana_rpc and not settings.venues.solana_rpc_url.startswith(
        ("http://", "https://")
    ):
        raise ValueError("invalid_solana_rpc_url")
    if settings.venues.solana_jito_enabled and not capabilities.supports_jito:
        raise ValueError("solana_jito_requires_live_dex_adapter")


def _validate_cex_selection(settings: AppSettings, capabilities: AdapterCapabilities) -> None:
    del settings
    del capabilities