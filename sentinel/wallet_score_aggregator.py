from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sentinel.okx_wallet_registry_importer import TrackedWalletRegistryEntry


@dataclass(frozen=True)
class WalletTokenFlow:
    chain: str
    token: str
    wallet_address: str
    direction: str
    notional_usd: float
    observed_at: datetime
    trade_count: int = 1
    flow_id: str | None = None


@dataclass(frozen=True)
class WalletScoreSnapshot:
    chain: str
    token: str
    window_end: datetime
    wallet_inflow_score: float
    wallet_outflow_score: float
    tracked_wallet_count: int
    sample_count: int
    quality_flag: str
    registry_version: str
    freshness_seconds: float


class WalletScoreAggregator:
    def __init__(
        self,
        window_seconds: int = 15 * 60,
        min_flow_count: int = 1,
        min_wallet_count: int = 1,
    ) -> None:
        self.window_seconds = window_seconds
        self.min_flow_count = min_flow_count
        self.min_wallet_count = min_wallet_count

    def build_snapshot(
        self,
        chain: str,
        token: str,
        registry_entries: list[TrackedWalletRegistryEntry],
        flows: list[WalletTokenFlow],
        window_end: datetime | None = None,
    ) -> WalletScoreSnapshot:
        effective_window_end = (window_end or datetime.now(UTC)).astimezone(UTC)
        window_start = effective_window_end - timedelta(seconds=self.window_seconds)
        active_entries = {
            entry.wallet_address: entry
            for entry in registry_entries
            if entry.chain == chain and entry.status == "active"
        }
        filtered_flows = [
            flow
            for flow in flows
            if flow.chain == chain
            and flow.token == token
            and window_start <= flow.observed_at.astimezone(UTC) <= effective_window_end
            and flow.wallet_address in active_entries
        ]
        weighted_inflow_usd = 0.0
        weighted_outflow_usd = 0.0
        sample_count = 0
        latest_observed_at: datetime | None = None
        participating_wallets: set[str] = set()
        for flow in filtered_flows:
            entry = active_entries[flow.wallet_address]
            weighted_notional = max(flow.notional_usd, 0.0) * max(entry.weight, 0.0)
            if flow.direction == "inflow":
                weighted_inflow_usd += weighted_notional
            elif flow.direction == "outflow":
                weighted_outflow_usd += weighted_notional
            sample_count += max(flow.trade_count, 1)
            participating_wallets.add(flow.wallet_address)
            if latest_observed_at is None or flow.observed_at > latest_observed_at:
                latest_observed_at = flow.observed_at
        total_weighted = weighted_inflow_usd + weighted_outflow_usd
        if total_weighted <= 0:
            wallet_inflow_score = 0.0
            wallet_outflow_score = 0.0
        else:
            wallet_inflow_score = round(weighted_inflow_usd / total_weighted, 6)
            wallet_outflow_score = round(weighted_outflow_usd / total_weighted, 6)
        if latest_observed_at is None:
            freshness_seconds = float(self.window_seconds)
        else:
            freshness_seconds = max(
                0.0,
                (effective_window_end - latest_observed_at.astimezone(UTC)).total_seconds(),
            )
        registry_version = max(
            (entry.version for entry in active_entries.values()),
            default="unversioned",
        )
        quality_flag = "ok"
        if not active_entries:
            quality_flag = "missing_registry"
        elif sample_count < self.min_flow_count or len(participating_wallets) < self.min_wallet_count:
            quality_flag = "low_sample"
        return WalletScoreSnapshot(
            chain=chain,
            token=token,
            window_end=effective_window_end,
            wallet_inflow_score=wallet_inflow_score,
            wallet_outflow_score=wallet_outflow_score,
            tracked_wallet_count=len(active_entries),
            sample_count=sample_count,
            quality_flag=quality_flag,
            registry_version=registry_version,
            freshness_seconds=freshness_seconds,
        )