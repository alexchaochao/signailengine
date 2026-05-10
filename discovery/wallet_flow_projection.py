"""Generic Wallet Flow Projection — not bound to BONK.

Projects smart wallet flows for any token on any chain.
Used after launch-alpha or momentum-alpha discovers a candidate:
  1. Load OKX wallet registry for the chain
  2. Scan raw-events for trades from tracked wallets on this token
  3. Return inflow/outflow metrics
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from redis import Redis

from core.config import AppSettings
from core.schemas import EventEnvelope
from infra.redis_stream import read_models
from infra.repository import StorageRepository

logger = logging.getLogger("signalengine.wallet_flow_projection")

_LOOKBACK_SECONDS = 3600  # 1 hour


@dataclass
class WalletFlowProjection:
    chain: str
    token: str
    smart_money_inflow_usd: float = 0.0
    smart_money_outflow_usd: float = 0.0
    unique_buyer_wallets: int = 0
    unique_seller_wallets: int = 0
    whale_buy_count: int = 0
    total_trade_count: int = 0
    tracked_wallets_count: int = 0
    wallet_addresses: list[str] | None = None

    @property
    def netflow_usd(self) -> float:
        return self.smart_money_inflow_usd - self.smart_money_outflow_usd

    def to_dict(self) -> dict[str, Any]:
        return {
            "chain": self.chain,
            "token": self.token,
            "smart_money_inflow_usd": self.smart_money_inflow_usd,
            "smart_money_outflow_usd": self.smart_money_outflow_usd,
            "netflow_usd": self.netflow_usd,
            "unique_buyer_wallets": self.unique_buyer_wallets,
            "unique_seller_wallets": self.unique_seller_wallets,
            "whale_buy_count": self.whale_buy_count,
            "total_trade_count": self.total_trade_count,
            "tracked_wallets_count": self.tracked_wallets_count,
        }


def project_wallet_flow(
    settings: AppSettings,
    redis_client: Redis,
    repository: StorageRepository,
    *,
    chain: str,
    token: str,
    lookback_seconds: int = _LOOKBACK_SECONDS,
) -> WalletFlowProjection:
    """Project smart wallet flow for ANY token (not just BONK)."""
    # Load tracked wallets from registry
    registry_entries = repository.wallet_intelligence.list_active_registry_entries(chain)
    tracked_wallets = {entry.wallet_address for entry in registry_entries}

    if not tracked_wallets:
        logger.info("wallet_flow_no_registry", extra={"chain": chain, "token": token})
        return WalletFlowProjection(chain=chain, token=token)

    # Scan recent trade events
    inflow = 0.0
    outflow = 0.0
    buyer_wallets: set[str] = set()
    seller_wallets: set[str] = set()
    whale_count = 0
    total_trades = 0
    seen_wallets: set[str] = set()
    cutoff = (datetime.now(UTC) - timedelta(seconds=lookback_seconds)).timestamp()

    for message_id, event in read_models(
        redis_client,
        settings.redis.raw_events_stream,
        EventEnvelope,
        last_id="-",
        count=5000,
    ):
        _ = message_id
        if event.event_type != "onchain.trade_fact":
            continue
        if event.chain != chain or event.token != token:
            continue
        if event.observed_at.timestamp() < cutoff:
            continue

        wallet = str(event.payload.get("wallet_address", "")).strip()
        if not wallet or wallet not in tracked_wallets:
            continue

        direction = str(event.payload.get("direction", "")).lower()
        notional = float(event.payload.get("notional_usd", 0.0) or 0.0)
        total_trades += 1

        if direction == "inflow":
            inflow += notional
            if wallet not in seen_wallets:
                buyer_wallets.add(wallet)
                seen_wallets.add(wallet)
            if notional >= 10_000:
                whale_count += 1
        elif direction == "outflow":
            outflow += notional
            seller_wallets.add(wallet)

    result = WalletFlowProjection(
        chain=chain,
        token=token,
        smart_money_inflow_usd=inflow,
        smart_money_outflow_usd=outflow,
        unique_buyer_wallets=len(buyer_wallets),
        unique_seller_wallets=len(seller_wallets),
        whale_buy_count=whale_count,
        total_trade_count=total_trades,
        tracked_wallets_count=len(tracked_wallets),
    )

    logger.info(
        "wallet_flow_projected",
        extra={
            "token": token,
            "chain": chain,
            "inflow_usd": inflow,
            "outflow_usd": outflow,
            "buyers": len(buyer_wallets),
            "sellers": len(seller_wallets),
            "whales": whale_count,
        },
    )
    return result
