"""Cross-source entity dedup and canonical asset registry.

SymbolRegistry provides a lightweight Redis-backed mapping of token symbols
to their canonical representation across multiple venues and chains.

Purpose:
  - Cross-source entity dedup: "BONK" from Binance Alpha = "BONK" from Jupiter = "Bonk" from Coinbase
  - Chain & contract mapping: token → [(chain, address), ...]
  - Venue lifecycle tracking: which venue listed which token first
  - Pre-listing state: track tokens detected in announcements before they're tradeable
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from redis import Redis

# ── Redis key prefixes ──────────────────────────────────────────────────
_CANONICAL_PREFIX = "symreg:canonical:"       # symbol → CanonicalEntry
_VENUE_PREFIX = "symreg:venue:"               # venue:symbol → VenueStatus
_CHAIN_PREFIX = "symreg:chain:"               # chain:symbol → ChainAddress
_ALIAS_PREFIX = "symreg:alias:"               # alias → canonical_symbol
_PRE_LISTING_PREFIX = "symreg:prelisting:"    # symbol → PreListingInfo
_LIFECYCLE_PREFIX = "symreg:lifecycle:"       # symbol → {venue: timestamp}

_DEFAULT_TTL = 7 * 24 * 3600  # 7 days


@dataclass(frozen=True)
class ChainAddress:
    chain: str
    address: str | None = None
    token_program: str | None = None  # SPL token program for Solana


@dataclass
class CanonicalEntry:
    """Canonical representation of a token across all sources."""
    canonical_symbol: str          # e.g. "BONK"
    display_name: str = ""
    aliases: set[str] = field(default_factory=set)
    chains: dict[str, ChainAddress] = field(default_factory=dict)  # chain → ChainAddress
    first_seen_at: float | None = None  # unix ts
    first_seen_via: str = ""           # source name that first reported this token
    last_updated_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "canonical_symbol": self.canonical_symbol,
            "display_name": self.display_name,
            "aliases": list(self.aliases),
            "chains": {k: {"chain": v.chain, "address": v.address, "token_program": v.token_program}
                       for k, v in self.chains.items()},
            "first_seen_at": self.first_seen_at,
            "first_seen_via": self.first_seen_via,
            "last_updated_at": self.last_updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CanonicalEntry:
        chains = {}
        for k, v in data.get("chains", {}).items():
            chains[k] = ChainAddress(
                chain=v.get("chain", k),
                address=v.get("address"),
                token_program=v.get("token_program"),
            )
        return cls(
            canonical_symbol=data["canonical_symbol"],
            display_name=data.get("display_name", ""),
            aliases=set(data.get("aliases", [])),
            chains=chains,
            first_seen_at=data.get("first_seen_at"),
            first_seen_via=data.get("first_seen_via", ""),
            last_updated_at=data.get("last_updated_at"),
        )


@dataclass
class PreListingInfo:
    """Tracks a token detected before it's tradeable on a venue."""
    symbol: str
    venue: str
    detected_at: float
    detected_via: str          # e.g. "binance_announcement_ws", "exchangeInfo"
    expected_listing_type: str = "spot"  # spot, perpetual, futures
    confirmed_listed: bool = False
    confirmed_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "venue": self.venue,
            "detected_at": self.detected_at,
            "detected_via": self.detected_via,
            "expected_listing_type": self.expected_listing_type,
            "confirmed_listed": self.confirmed_listed,
            "confirmed_at": self.confirmed_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PreListingInfo:
        return cls(
            symbol=data["symbol"],
            venue=data["venue"],
            detected_at=data["detected_at"],
            detected_via=data.get("detected_via", "unknown"),
            expected_listing_type=data.get("expected_listing_type", "spot"),
            confirmed_listed=data.get("confirmed_listed", False),
            confirmed_at=data.get("confirmed_at"),
        )


class SymbolRegistry:
    """Redis-backed cross-source token entity registry."""

    def __init__(self, redis_client: Redis, ttl: int = _DEFAULT_TTL) -> None:
        self._redis = redis_client
        self._ttl = ttl

    # ── Canonical resolution ─────────────────────────────────────────

    def get_canonical(self, symbol: str) -> CanonicalEntry | None:
        """Look up the canonical entry for a symbol (case-insensitive)."""
        key = _canonical_key(symbol)
        raw = self._redis.get(key)
        if raw is None:
            # Try alias resolution
            canon = self._resolve_alias(symbol)
            if canon is None:
                return None
            key = _canonical_key(canon)
            raw = self._redis.get(key)
            if raw is None:
                return None
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
        return CanonicalEntry.from_dict(data)

    def register(
        self,
        canonical_symbol: str,
        *,
        display_name: str = "",
        alias: str | None = None,
        chain: str | None = None,
        chain_address: str | None = None,
        token_program: str | None = None,
        source_name: str = "",
    ) -> CanonicalEntry:
        """Register or update a canonical token entry.

        Returns the updated CanonicalEntry.
        """
        now = datetime.now(UTC).timestamp()
        symbol_upper = canonical_symbol.upper()

        existing = self.get_canonical(symbol_upper)
        if existing is not None:
            entry = existing
            if alias and alias.upper() not in entry.aliases:
                entry.aliases.add(alias.upper())
                self._set_alias(alias.upper(), symbol_upper)
            entry.last_updated_at = now
        else:
            entry = CanonicalEntry(
                canonical_symbol=symbol_upper,
                display_name=display_name or symbol_upper,
                first_seen_at=now,
                first_seen_via=source_name,
                last_updated_at=now,
            )
            if alias and alias.upper() != symbol_upper:
                entry.aliases.add(alias.upper())
                self._set_alias(alias.upper(), symbol_upper)

        if chain and chain_address:
            entry.chains[chain] = ChainAddress(
                chain=chain,
                address=chain_address,
                token_program=token_program,
            )

        self._redis.setex(_canonical_key(symbol_upper), self._ttl, json.dumps(entry.to_dict()))
        return entry

    def resolve_aliases(self, symbol: str) -> str:
        """Resolve an alias to its canonical symbol."""
        canon = self._resolve_alias(symbol)
        return canon or symbol.upper()

    def _resolve_alias(self, symbol: str) -> str | None:
        raw = self._redis.get(_alias_key(symbol))
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None

    def _set_alias(self, alias: str, canonical: str) -> None:
        self._redis.setex(_alias_key(alias), self._ttl, json.dumps(canonical))

    # ── Venue lifecycle ──────────────────────────────────────────────

    def register_venue_listing(
        self,
        symbol: str,
        venue: str,
        *,
        status: str = "TRADING",
        detected_via: str = "",
    ) -> None:
        """Record a token listing event on a specific venue."""
        now = datetime.now(UTC).timestamp()
        key = _lifecycle_key(symbol)
        self._redis.hset(key, venue, now)
        self._redis.expire(key, self._ttl)

        # Also update venue-specific status
        venue_key = _venue_key(venue, symbol)
        self._redis.setex(
            venue_key,
            self._ttl,
            json.dumps({"status": status, "detected_at": now, "detected_via": detected_via}),
        )

    def get_venue_lifecycle(self, symbol: str) -> dict[str, float]:
        """Get {venue: first_detected_timestamp} for a symbol."""
        key = _lifecycle_key(symbol)
        raw = self._redis.hgetall(key)
        return {k.decode() if isinstance(k, bytes) else k:
                float(v.decode() if isinstance(v, bytes) else v)
                for k, v in raw.items()}

    def first_listed_on(self, symbol: str) -> tuple[str, float] | None:
        """Find which venue listed this symbol first."""
        lifecycle = self.get_venue_lifecycle(symbol)
        if not lifecycle:
            return None
        best_venue, best_ts = min(lifecycle.items(), key=lambda x: x[1])
        return best_venue, best_ts

    # ── Pre-listing state ────────────────────────────────────────────

    def set_pre_listing(
        self,
        symbol: str,
        venue: str,
        *,
        detected_via: str = "",
        expected_type: str = "spot",
    ) -> PreListingInfo:
        """Mark a token as detected but not yet tradeable."""
        now = datetime.now(UTC).timestamp()
        info = PreListingInfo(
            symbol=symbol.upper(),
            venue=venue,
            detected_at=now,
            detected_via=detected_via,
            expected_listing_type=expected_type,
        )
        key = _pre_listing_key(symbol.upper())
        self._redis.setex(key, self._ttl, json.dumps(info.to_dict()))
        return info

    def confirm_listing(self, symbol: str, venue: str) -> PreListingInfo | None:
        """Mark a pre-listed token as confirmed tradeable."""
        key = _pre_listing_key(symbol.upper())
        raw = self._redis.get(key)
        if raw is None:
            return None
        try:
            info = PreListingInfo.from_dict(json.loads(raw))
        except (json.JSONDecodeError, TypeError):
            return None
        info.confirmed_listed = True
        info.confirmed_at = datetime.now(UTC).timestamp()
        self._redis.setex(key, self._ttl, json.dumps(info.to_dict()))
        return info

    def get_pre_listing(self, symbol: str) -> PreListingInfo | None:
        """Check if a token is in pre-listing state."""
        key = _pre_listing_key(symbol.upper())
        raw = self._redis.get(key)
        if raw is None:
            return None
        try:
            return PreListingInfo.from_dict(json.loads(raw))
        except (json.JSONDecodeError, TypeError):
            return None

    # ── Chain / address mapping ──────────────────────────────────────

    def map_chain_address(
        self,
        symbol: str,
        chain: str,
        address: str,
        *,
        token_program: str | None = None,
    ) -> None:
        """Record a chain-specific contract address for a symbol."""
        key = _chain_key(chain, symbol.upper())
        self._redis.setex(
            key,
            self._ttl,
            json.dumps({"address": address, "token_program": token_program}),
        )

    def get_chain_address(self, symbol: str, chain: str) -> ChainAddress | None:
        """Look up a contract address for a symbol on a specific chain."""
        key = _chain_key(chain, symbol.upper())
        raw = self._redis.get(key)
        if raw is None:
            return None
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
        return ChainAddress(
            chain=chain,
            address=data.get("address"),
            token_program=data.get("token_program"),
        )


# ── Key helpers ────────────────────────────────────────────────────────

def _canonical_key(symbol: str) -> str:
    return f"{_CANONICAL_PREFIX}{symbol.upper()}"


def _alias_key(alias: str) -> str:
    return f"{_ALIAS_PREFIX}{alias.upper()}"


def _venue_key(venue: str, symbol: str) -> str:
    return f"{_VENUE_PREFIX}{venue}:{symbol.upper()}"


def _chain_key(chain: str, symbol: str) -> str:
    return f"{_CHAIN_PREFIX}{chain}:{symbol.upper()}"


def _pre_listing_key(symbol: str) -> str:
    return f"{_PRE_LISTING_PREFIX}{symbol.upper()}"


def _lifecycle_key(symbol: str) -> str:
    return f"{_LIFECYCLE_PREFIX}{symbol.upper()}"
