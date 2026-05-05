from __future__ import annotations

from datetime import UTC, datetime

from redis import Redis

from core.config import AppSettings, FeatureConfig
from core.event_flow import publish_raw_events
from core.schemas import EventEnvelope, FeatureSnapshot
from infra.repository import StorageRepository
from sentinel.onchain_listener import build_onchain_event


class OnchainFeaturePublisher:
    def __init__(
        self,
        settings: AppSettings,
        redis_client: Redis,
        repository: StorageRepository,
    ) -> None:
        self.settings = settings
        self.redis_client = redis_client
        self.repository = repository

    def publish_latest(self, chain: str, token: str) -> tuple[EventEnvelope, str]:
        feature_settings = FeatureConfig.model_validate(self.settings.features)
        buy_pressure_window = feature_settings.onchain.buy_pressure_primary_window
        buy_pressure_snapshot = self.repository.features.load_latest_snapshot(
            chain,
            token,
            "buy_pressure",
            buy_pressure_window,
        )
        slippage_snapshot = self.repository.features.load_latest_snapshot(
            chain,
            token,
            "estimated_slippage_bps",
            "latest",
        )
        event = build_onchain_feature_event(
            chain,
            token,
            buy_pressure_snapshot=buy_pressure_snapshot,
            slippage_snapshot=slippage_snapshot,
            buy_pressure_window=buy_pressure_window,
        )
        message_id = publish_raw_events(self.redis_client, self.settings, event)[0]
        return event, message_id


def build_onchain_feature_event(
    chain: str,
    token: str,
    *,
    buy_pressure_snapshot: FeatureSnapshot | None,
    slippage_snapshot: FeatureSnapshot | None,
    buy_pressure_window: str,
    source: str = "feature_aggregator",
) -> EventEnvelope:
    observed_at = _latest_as_of(buy_pressure_snapshot, slippage_snapshot)
    volume_5m_usd = _derive_volume_5m_usd(buy_pressure_snapshot, slippage_snapshot)
    liquidity_usd = _derive_liquidity_proxy_usd(slippage_snapshot, volume_5m_usd=volume_5m_usd)
    payload = {
        "chain": chain,
        "token": token,
        "observed_at": observed_at,
        "liquidity_usd": liquidity_usd,
        "volume_5m_usd": volume_5m_usd,
        "buy_pressure": _derive_buy_pressure(buy_pressure_snapshot, slippage_snapshot),
        "estimated_slippage_bps": (
            float(slippage_snapshot.feature_value)
            if slippage_snapshot is not None
            else 0.0
        ),
        "buy_pressure_window": buy_pressure_window,
        "feature_quality": {
            "buy_pressure": _derive_buy_pressure_quality(buy_pressure_snapshot, slippage_snapshot),
            "estimated_slippage_bps": (
                slippage_snapshot.quality_flag if slippage_snapshot is not None else "missing"
            ),
        },
        "formula_versions": {
            "buy_pressure": (
                buy_pressure_snapshot.formula_version if buy_pressure_snapshot is not None else ""
            ),
            "estimated_slippage_bps": (
                slippage_snapshot.formula_version if slippage_snapshot is not None else ""
            ),
        },
    }
    return build_onchain_event(payload, source=source)


def _latest_as_of(*snapshots: FeatureSnapshot | None) -> datetime:
    observed_times = [snapshot.as_of.astimezone(UTC) for snapshot in snapshots if snapshot is not None]
    if not observed_times:
        return datetime.now(UTC)
    return max(observed_times)


def _derive_volume_5m_usd(
    buy_pressure_snapshot: FeatureSnapshot | None,
    slippage_snapshot: FeatureSnapshot | None,
) -> float:
    if buy_pressure_snapshot is not None:
        buy_notional = float(buy_pressure_snapshot.inputs.get("buy_notional_usd", 0.0))
        sell_notional = float(buy_pressure_snapshot.inputs.get("sell_notional_usd", 0.0))
        return round(max(buy_notional + sell_notional, 0.0), 6)
    if slippage_snapshot is None:
        return 0.0
    return round(float(slippage_snapshot.inputs.get("volume_5m_usd", 0.0) or 0.0), 6)


def _derive_buy_pressure(
    buy_pressure_snapshot: FeatureSnapshot | None,
    slippage_snapshot: FeatureSnapshot | None,
) -> float:
    if buy_pressure_snapshot is not None:
        return float(buy_pressure_snapshot.feature_value)
    if slippage_snapshot is None:
        return 0.0
    return round(float(slippage_snapshot.inputs.get("buy_pressure", 0.0) or 0.0), 6)


def _derive_buy_pressure_quality(
    buy_pressure_snapshot: FeatureSnapshot | None,
    slippage_snapshot: FeatureSnapshot | None,
) -> str:
    if buy_pressure_snapshot is not None:
        return buy_pressure_snapshot.quality_flag
    if slippage_snapshot is None:
        return "missing"
    fallback_volume = float(slippage_snapshot.inputs.get("volume_5m_usd", 0.0) or 0.0)
    fallback_market_source = str(slippage_snapshot.inputs.get("market_source", "")).strip()
    if fallback_volume > 0 and fallback_market_source:
        return "degraded"
    return "missing"


def _derive_liquidity_proxy_usd(
    slippage_snapshot: FeatureSnapshot | None,
    *,
    volume_5m_usd: float,
) -> float:
    if slippage_snapshot is None:
        return round(volume_5m_usd, 6)
    quote_notional_usd = float(slippage_snapshot.inputs.get("quote_notional_usd", 0.0))
    slippage_bps = max(float(slippage_snapshot.feature_value), 1.0)
    curve_liquidity_proxy = quote_notional_usd * 10000.0 / slippage_bps if quote_notional_usd > 0 else 0.0
    return round(max(curve_liquidity_proxy, volume_5m_usd), 6)