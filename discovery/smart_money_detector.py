"""Smart Money Inflow Detection for newly discovered tokens.

Detects whether known "smart money" wallets (from OKX wallet registry)
have bought into a newly discovered token, and produces a confidence score.

Architecture:
  1. launch-alpha discovers a new pool and qualifies it
  2. SmartMoneyDetector checks the OKX registry for tracked wallets
  3. Scans recent onchain.trade_fact events from Redis raw-events stream
  4. Filters trades by tracked wallets, counts unique smart money buyers
  5. Returns a score: 0.0 (no smart money) to 1.0 (multiple smart wallets)
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from redis import Redis

from core.config import AppSettings
from core.schemas import EventEnvelope
from infra.redis_stream import read_models
from infra.repository import StorageRepository

logger = logging.getLogger("signalengine.smart_money_detector")

# Redis stream name for trade facts
_TRADE_FACT_STREAM = "raw-events"

# Default minimum smart wallets threshold for alpha signal
_DEFAULT_MIN_SMART_WALLETS = 1

# Lookback window for trade facts (seconds)
_DEFAULT_LOOKBACK_SECONDS = 3600  # 1 hour


class SmartMoneyInflowResult:
    """Result of a smart money inflow check for a token."""

    def __init__(
        self,
        token: str,
        chain: str,
        smart_wallet_buyers: int,
        total_smart_wallets_checked: int,
        total_inflow_usd: float,
        wallet_addresses: list[str] | None = None,
    ) -> None:
        self.token = token
        self.chain = chain
        self.smart_wallet_buyers = smart_wallet_buyers
        self.total_smart_wallets_checked = total_smart_wallets_checked
        self.total_inflow_usd = total_inflow_usd
        self.wallet_addresses = wallet_addresses or []
        self.timestamp = datetime.now(UTC)

    @property
    def has_smart_money(self) -> bool:
        return self.smart_wallet_buyers > 0

    @property
    def confidence_score(self) -> float:
        """Compute a confidence score based on smart wallet participation.

        - 1 smart wallet buying → 0.5 (weak signal)
        - 2 smart wallets → 0.7 (moderate)
        - 3+ smart wallets → 0.85+ (strong)
        - Large inflow amounts boost the score further.
        """
        if self.smart_wallet_buyers <= 0:
            return 0.0

        base_score = min(0.4 + (self.smart_wallet_buyers - 1) * 0.2, 0.85)
        inflow_boost = min(self.total_inflow_usd / 50_000.0, 0.15)
        return round(min(base_score + inflow_boost, 1.0), 4)

    def to_dict(self) -> dict[str, Any]:
        return {
            "token": self.token,
            "chain": self.chain,
            "smart_wallet_buyers": self.smart_wallet_buyers,
            "total_smart_wallets_checked": self.total_smart_wallets_checked,
            "total_inflow_usd": self.total_inflow_usd,
            "wallet_addresses": self.wallet_addresses,
            "has_smart_money": self.has_smart_money,
            "confidence_score": self.confidence_score,
        }


class SmartMoneyDetector:
    """Detects smart money inflow for a newly discovered token.

    Usage:
        detector = SmartMoneyDetector(settings, redis_client, repository)
        result = detector.check_token("SOL", "So11111111111111111111111111111111111111112")
    """

    def __init__(
        self,
        settings: AppSettings,
        redis_client: Redis,
        repository: StorageRepository,
        *,
        min_smart_wallets: int = _DEFAULT_MIN_SMART_WALLETS,
        lookback_seconds: int = _DEFAULT_LOOKBACK_SECONDS,
    ) -> None:
        self.settings = settings
        self.redis_client = redis_client
        self.repository = repository
        self.min_smart_wallets = min_smart_wallets
        self.lookback_seconds = lookback_seconds

    def check_token(self, chain: str, token: str) -> SmartMoneyInflowResult:
        """Check if any tracked smart wallets have bought this token recently."""
        # 1. Load tracked smart wallets from registry
        registry_entries = self.repository.wallet_intelligence.list_active_registry_entries(chain)
        tracked_wallets = {entry.wallet_address for entry in registry_entries}

        if not tracked_wallets:
            logger.info(
                "smart_money_check_no_registry",
                extra={"chain": chain, "token": token},
            )
            return SmartMoneyInflowResult(
                token=token, chain=chain,
                smart_wallet_buyers=0, total_smart_wallets_checked=0,
                total_inflow_usd=0.0,
            )

        # 2. Scan recent onchain.trade_fact events for this token
        smart_wallet_addresses: list[str] = []
        total_inflow = 0.0
        cutoff = (datetime.now(UTC) - timedelta(seconds=self.lookback_seconds)).timestamp()
        seen_wallets: set[str] = set()

        for message_id, event in read_models(
            self.redis_client,
            _TRADE_FACT_STREAM,
            EventEnvelope,
            last_id="-",
            count=5000,
        ):
            _ = message_id
            if event.event_type != "onchain.trade_fact":
                continue
            if event.chain != chain or event.token != token:
                continue

            # Check timeliness
            observed_ts = event.observed_at.timestamp()
            if observed_ts < cutoff:
                continue

            wallet = str(event.payload.get("wallet_address", "")).strip()
            if not wallet or wallet not in tracked_wallets:
                continue

            direction = str(event.payload.get("direction", "")).lower()
            notional = float(event.payload.get("notional_usd", 0.0) or 0.0)

            if direction == "inflow" and wallet not in seen_wallets:
                seen_wallets.add(wallet)
                smart_wallet_addresses.append(wallet)
                total_inflow += notional

        result = SmartMoneyInflowResult(
            token=token,
            chain=chain,
            smart_wallet_buyers=len(smart_wallet_addresses),
            total_smart_wallets_checked=len(tracked_wallets),
            total_inflow_usd=total_inflow,
            wallet_addresses=smart_wallet_addresses,
        )

        logger.info(
            "smart_money_check_result",
            extra={
                "token": token,
                "chain": chain,
                "smart_buyers": result.smart_wallet_buyers,
                "total_registry": result.total_smart_wallets_checked,
                "inflow_usd": result.total_inflow_usd,
                "confidence": result.confidence_score,
            },
        )
        return result
