"""Venue status resolver — queries SymbolRegistry for real-time venue readiness.

Orchestration layer that answers "is this token ready to trade on DEX / CEX"
by consulting the SymbolRegistry (Redis-backed cross-source entity registry).

Usage in pipeline:

    resolver = VenueStatusResolver(redis_client)
    venue_status = resolver.resolve(token, chain)
    route = router.route(signal, transition, position_state, venue_status)
"""

from __future__ import annotations

from redis import Redis

from core.schemas import VenueStatus
from discovery.symbol_registry import SymbolRegistry

# Venues considered "CEX" for routing decisions
CEX_VENUES = frozenset({
    "binance", "coinbase", "okx", "bybit", "kucoin", "gateio", "kraken", "upbit",
})

# Venues considered "DEX" for routing decisions
DEX_VENUES = frozenset({
    "raydium", "orca", "uniswap_v3", "aerodrome", "jupiter",
    # DexScreener returns dexId like "raydium", "orca", etc.
})


class VenueStatusResolver:
    """Resolve venue readiness for a token by querying SymbolRegistry."""

    def __init__(self, redis_client: Redis) -> None:
        self._registry = SymbolRegistry(redis_client)

    def resolve(self, token: str, chain: str | None = None) -> VenueStatus:
        """Return VenueStatus with real dex/cex readiness for *token*.

        Args:
            token: Token symbol or canonical name (case-insensitive).
            chain: Optional chain filter.

        Returns:
            VenueStatus populated from SymbolRegistry data.
        """
        lifecycle = self._registry.get_venue_lifecycle(token)

        if not lifecycle:
            # No venue data yet → assume DEX is ready (likely a new pool),
            # CEX is not ready, not degraded.
            return VenueStatus(dex_ready=True, cex_ready=False, degraded=False)

        has_cex = any(venue.lower() in CEX_VENUES for venue in lifecycle)
        has_dex = any(venue.lower() in DEX_VENUES for venue in lifecycle)

        # Also check canonical entry for chain-level DEX presence
        if not has_dex and chain:
            canonical = self._registry.get_canonical(token)
            if canonical and chain in canonical.chains:
                # Token has chain data → DEX is potentially ready
                has_dex = True

        return VenueStatus(
            dex_ready=has_dex,
            cex_ready=has_cex,
            degraded=False,
        )

    def is_listed_on_cex(self, token: str) -> bool:
        """Quick check: is this token listed on any tracked CEX?"""
        lifecycle = self._registry.get_venue_lifecycle(token)
        return any(venue.lower() in CEX_VENUES for venue in lifecycle)

    def first_cex_venue(self, token: str) -> str | None:
        """Return the first CEX that listed this token, if any."""
        best = self._registry.first_listed_on(token)
        if best is None:
            return None
        venue, _ = best
        if venue.lower() in CEX_VENUES:
            return venue
        return None
