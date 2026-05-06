from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from discovery.schemas import AlphaCandidate, AlphaCandidateStatus, AlphaType, LaunchPoolSnapshot

if TYPE_CHECKING:
    from core.config import LaunchAlphaLiveSourceConfig


@dataclass(frozen=True)
class LaunchAlphaThresholds:
    min_initial_liquidity_usd: float = 10_000.0
    min_buy_notional_5m_usd: float = 5_000.0
    min_trade_count_5m: int = 8
    min_unique_wallets_5m: int = 5
    min_liquidity_lock_ratio: float = 0.8
    max_creator_hold_pct: float = 0.2

    @classmethod
    def from_source_config(
        cls, source_config: LaunchAlphaLiveSourceConfig
    ) -> LaunchAlphaThresholds:
        """Create thresholds from the env-configurable source config layer.

        Respects the values defined in LaunchAlphaLiveSourceConfig, which is
        populated by SIGNALENGINE_ACQUISITION__LAUNCH_ALPHA_SOURCES__* env vars.
        """
        return cls(
            min_initial_liquidity_usd=source_config.min_initial_liquidity_usd,
            min_buy_notional_5m_usd=source_config.min_buy_notional_5m_usd,
            min_trade_count_5m=source_config.min_trade_count_5m,
            min_unique_wallets_5m=source_config.min_unique_wallets_5m,
            min_liquidity_lock_ratio=source_config.min_liquidity_lock_ratio,
            max_creator_hold_pct=source_config.max_creator_hold_pct,
        )


class LaunchAlphaScanner:
    def __init__(self, thresholds: LaunchAlphaThresholds | None = None) -> None:
        self.thresholds = thresholds or LaunchAlphaThresholds()

    def evaluate(self, snapshot: LaunchPoolSnapshot) -> AlphaCandidate:
        reasons: list[str] = []
        status = AlphaCandidateStatus.OBSERVED

        if (
            snapshot.liquidity_lock_ratio is not None
            and snapshot.liquidity_lock_ratio < self.thresholds.min_liquidity_lock_ratio
        ):
            reasons.append("liquidity_lock_ratio_below_minimum")
            status = AlphaCandidateStatus.REJECTED

        if (
            snapshot.creator_hold_pct is not None
            and snapshot.creator_hold_pct > self.thresholds.max_creator_hold_pct
        ):
            reasons.append("creator_hold_pct_above_maximum")
            status = AlphaCandidateStatus.REJECTED

        metrics = {
            "initial_liquidity_usd": snapshot.initial_liquidity_usd,
            "buy_notional_5m_usd": snapshot.buy_notional_5m_usd,
            "trade_count_5m": float(snapshot.trade_count_5m),
            "unique_wallets_5m": float(snapshot.unique_wallets_5m),
            "smart_money_wallets_5m": float(snapshot.smart_money_wallets_5m),
        }
        thresholds = {
            "initial_liquidity_usd": self.thresholds.min_initial_liquidity_usd,
            "buy_notional_5m_usd": self.thresholds.min_buy_notional_5m_usd,
            "trade_count_5m": float(self.thresholds.min_trade_count_5m),
            "unique_wallets_5m": float(self.thresholds.min_unique_wallets_5m),
            "smart_money_wallets_5m": 3.0,
        }
        score = self._score(metrics, thresholds)

        if status != AlphaCandidateStatus.REJECTED:
            if snapshot.initial_liquidity_usd >= self.thresholds.min_initial_liquidity_usd:
                reasons.append("initial_liquidity_ready")
            else:
                reasons.append("initial_liquidity_below_threshold")
            if snapshot.buy_notional_5m_usd >= self.thresholds.min_buy_notional_5m_usd:
                reasons.append("buy_notional_ready")
            else:
                reasons.append("buy_notional_below_threshold")
            if snapshot.trade_count_5m >= self.thresholds.min_trade_count_5m:
                reasons.append("trade_count_ready")
            else:
                reasons.append("trade_count_below_threshold")
            if snapshot.unique_wallets_5m >= self.thresholds.min_unique_wallets_5m:
                reasons.append("unique_wallets_ready")
            else:
                reasons.append("unique_wallets_below_threshold")

            qualifies = (
                snapshot.initial_liquidity_usd >= self.thresholds.min_initial_liquidity_usd
                and snapshot.buy_notional_5m_usd >= self.thresholds.min_buy_notional_5m_usd
                and snapshot.trade_count_5m >= self.thresholds.min_trade_count_5m
                and snapshot.unique_wallets_5m >= self.thresholds.min_unique_wallets_5m
            )
            if qualifies:
                status = AlphaCandidateStatus.QUALIFIED

        return AlphaCandidate(
            candidate_id=f"{snapshot.chain}:{snapshot.pool_address}",
            alpha_type=AlphaType.LAUNCH,
            chain=snapshot.chain,
            token=snapshot.token,
            pool_address=snapshot.pool_address,
            dex=snapshot.dex,
            quote_asset=snapshot.quote_asset,
            status=status,
            score=score,
            first_seen_at=snapshot.observed_at,
            last_seen_at=snapshot.observed_at,
            initial_liquidity_usd=snapshot.initial_liquidity_usd,
            liquidity_lock_ratio=snapshot.liquidity_lock_ratio,
            buy_notional_5m_usd=snapshot.buy_notional_5m_usd,
            trade_count_5m=snapshot.trade_count_5m,
            unique_wallets_5m=snapshot.unique_wallets_5m,
            smart_money_wallets_5m=snapshot.smart_money_wallets_5m,
            creator_hold_pct=snapshot.creator_hold_pct,
            reasons=reasons,
            metadata=snapshot.metadata,
        )

    def _score(self, metrics: dict[str, float], thresholds: dict[str, float]) -> float:
        weighted = (
            min(metrics["initial_liquidity_usd"] / thresholds["initial_liquidity_usd"], 1.0) * 0.35
            + min(metrics["buy_notional_5m_usd"] / thresholds["buy_notional_5m_usd"], 1.0) * 0.25
            + min(metrics["trade_count_5m"] / thresholds["trade_count_5m"], 1.0) * 0.2
            + min(metrics["unique_wallets_5m"] / thresholds["unique_wallets_5m"], 1.0) * 0.1
            + min(metrics["smart_money_wallets_5m"] / thresholds["smart_money_wallets_5m"], 1.0) * 0.1
        )
        return round(min(weighted, 1.0), 4)