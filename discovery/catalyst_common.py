"""Shared types and constants for catalyst source modules.

Avoids circular imports between catalyst_live_sources and catalyst_ws_sources.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# Redis dedup key prefix and TTL (7 days)
_CATALYST_DEDUP_PREFIX = "catalyst_dedup:"
_CATALYST_DEDUP_TTL = 7 * 24 * 3600

# Redis symbol cache for exchangeInfo diff / WS source diff
_SYMBOL_CACHE_PREFIX = "catalyst_symbols:"
_SYMBOL_CACHE_TTL = 3600  # 1 hour


@dataclass(frozen=True)
class _ExchangeSymbol:
    symbol: str
    base_asset: str
    quote_asset: str
    status: str
    contract_type: str = "spot"  # spot, perpetual, futures
    metadata: dict[str, Any] = field(default_factory=dict)


def _headline_for_symbol(provider: str, sym: _ExchangeSymbol) -> str:
    """Generate a human-readable headline for a new symbol detection."""
    if provider == "binance_exchange_info":
        return f"Binance Spot New Listing: {sym.base_asset} ({sym.symbol})"
    if provider == "binance_futures_info":
        ct = sym.contract_type.upper()
        return f"Binance Futures New {ct}: {sym.base_asset} ({sym.symbol})"
    if provider == "binance_alpha_api":
        return f"Binance Alpha New Token: {sym.base_asset}"
    if provider == "coinbase_products_api":
        return f"Coinbase New Trading Pair: {sym.base_asset} ({sym.symbol})"
    if provider == "okx_instruments_ws":
        ct = sym.contract_type.upper()
        return f"OKX New {ct}: {sym.base_asset} ({sym.symbol})"
    if provider == "bybit_instruments_ws":
        ct = sym.contract_type.upper()
        return f"Bybit New {ct}: {sym.base_asset} ({sym.symbol})"
    return f"New Symbol Detected: {sym.symbol}"


def _symbol_chain_for_provider(provider: str) -> str:
    """Map a provider string to a canonical chain name."""
    mapping = {
        "binance_exchange_info": "binance_spot",
        "binance_futures_info": "binance_futures",
        "binance_alpha_api": "binance_alpha",
        "coinbase_products_api": "coinbase",
        "okx_instruments_ws": "okx",
        "bybit_instruments_ws": "bybit",
    }
    return mapping.get(provider, "unknown")
