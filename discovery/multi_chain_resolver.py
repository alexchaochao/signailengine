from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable
from urllib import parse

from core.config import AppSettings

DexScreenerHttpTransport = Callable[[str, float], dict[str, Any] | list[dict[str, Any]]]


@dataclass(frozen=True)
class ChainAddress:
    chain: str
    token_address: str
    dex: str
    liquidity_usd: float
    pool_address: str


@dataclass(frozen=True)
class ResolvedToken:
    symbol: str
    name: str
    addresses: list[ChainAddress]
    url: str | None = None


def _http_json_get_transport(url: str, timeout_seconds: float) -> dict[str, Any] | list[dict[str, Any]]:
    import httpx
    response = httpx.get(url, timeout=timeout_seconds, follow_redirects=True)
    response.raise_for_status()
    return response.json()


def resolve_token_multi_chain(
    symbol: str,
    *,
    settings: AppSettings | None = None,
    primary_chain: str = "",
    transport: Callable[[str, float], dict[str, Any] | list[dict[str, Any]]] | None = None,
    dexscreener_api_url: str = "https://api.dexscreener.com/latest/dex/search",
    timeout_seconds: float = 10.0,
) -> ResolvedToken:
    """Resolve a token symbol to its addresses across multiple chains via DexScreener.

    Args:
        symbol: Token symbol (e.g. "MEGA", "BONK").
        settings: AppSettings for timeout defaults.
        primary_chain: Prefer this chain's address if available.
        transport: HTTP transport override.
        dexscreener_api_url: DexScreener search endpoint.
        timeout_seconds: HTTP timeout.

    Returns:
        ResolvedToken with all found chain addresses, sorted by liquidity desc.
        If the symbol cannot be resolved, returns an empty addresses list.
    """
    if settings is not None:
        # Use the pair_detail_url from the first launch source as base for search
        launch_sources = settings.acquisition.launch_alpha_sources
        for src in launch_sources.values():
            if src.pair_detail_url:
                base = src.pair_detail_url.rstrip("/").rsplit("/", 1)[0]
                dexscreener_api_url = base + "/search"
                break

    _transport = transport or _http_json_get_transport
    url = f"{dexscreener_api_url.rstrip('/')}?q={parse.quote(symbol)}"

    try:
        payload = _transport(url, timeout_seconds)
    except Exception:
        return ResolvedToken(symbol=symbol, name="", addresses=[])

    pairs = payload.get("pairs") if isinstance(payload, dict) else None
    if not isinstance(pairs, list):
        return ResolvedToken(symbol=symbol, name="", addresses=[])

    name = ""
    seen: dict[str, ChainAddress] = {}
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        base_token = pair.get("baseToken", {})
        if not isinstance(base_token, dict):
            continue
        token_symbol = str(base_token.get("symbol", "")).upper()
        if token_symbol != symbol.upper():
            continue
        chain = str(pair.get("chainId", "")).lower()
        token_address = str(base_token.get("address", ""))
        if not chain or not token_address:
            continue
        # Keep the one with highest liquidity per chain
        liquidity = float(pair.get("liquidity", {}).get("usd", 0) or 0)
        if chain in seen and seen[chain].liquidity_usd >= liquidity:
            continue
        dex = str(pair.get("dexId", ""))
        pool_address = str(pair.get("pairAddress", ""))
        seen[chain] = ChainAddress(
            chain=chain,
            token_address=token_address,
            dex=dex,
            liquidity_usd=liquidity,
            pool_address=pool_address,
        )
        if not name:
            name = str(base_token.get("name", ""))

    addresses = sorted(seen.values(), key=lambda a: a.liquidity_usd, reverse=True)

    # Reorder: primary_chain first if found
    if primary_chain:
        primary_idx = next((i for i, a in enumerate(addresses) if a.chain == primary_chain), -1)
        if primary_idx > 0:
            addresses.insert(0, addresses.pop(primary_idx))

    return ResolvedToken(
        symbol=symbol,
        name=name,
        addresses=addresses,
        url=f"https://dexscreener.com/search?q={parse.quote(symbol)}",
    )
