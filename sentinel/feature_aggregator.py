from __future__ import annotations

from datetime import UTC, datetime, timedelta

from core.config import AppSettings, FeatureConfig
from core.schemas import (
    DexQuoteSample,
    DexTradeFact,
    FeatureQualityRecord,
    FeatureSnapshot,
    RawEventRecord,
    SlippageCurve,
    TokenTradeWindow,
)
from infra.metrics import Metrics
from infra.repository import StorageRepository

TRADE_CLASSIFICATION_VERSION = "trade_classification_v1"
BUY_PRESSURE_FORMULA_VERSION = "bp_v1"
SLIPPAGE_CURVE_VERSION = "slippage_curve_v1"
SLIPPAGE_FORMULA_VERSION = "slip_v1"


class OnchainFeatureAggregator:
    def __init__(
        self,
        settings: AppSettings,
        repository: StorageRepository,
        metrics: Metrics | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.metrics = metrics

    def ingest_raw_trade(self, raw_event: RawEventRecord) -> list[FeatureSnapshot]:
        trade_fact = classify_trade(raw_event)
        self.repository.features.upsert_trade_fact(trade_fact)
        onchain_settings = FeatureConfig.model_validate(self.settings.features).onchain
        self._record_aggregator_metrics("buy_pressure", raw_event.chain or "", raw_event.token or "", raw_event.observed_at)

        snapshots: list[FeatureSnapshot] = []
        for window_name in onchain_settings.trade_windows:
            trade_window = self._rebuild_trade_window(
                trade_fact.chain,
                trade_fact.token,
                window_name,
                trade_fact.observed_at,
            )
            self.repository.features.upsert_trade_window(trade_window)
            snapshots.append(self._build_buy_pressure_snapshot(trade_window, onchain_settings))

        return [snapshot for snapshot in snapshots if snapshot.window_name]

    def _record_aggregator_metrics(
        self,
        feature: str,
        chain: str,
        token: str,
        watermark: datetime,
    ) -> None:
        if self.metrics is None:
            return
        self.metrics.aggregator_runs.labels(feature=feature, outcome="processed").inc()
        lag_seconds = max(0.0, (datetime.now(UTC) - watermark.astimezone(UTC)).total_seconds())
        self.metrics.aggregator_source_lag.labels(feature=feature, chain=chain, token=token).set(
            lag_seconds
        )
        self.metrics.aggregator_last_watermark.labels(
            feature=feature,
            chain=chain,
            token=token,
        ).set(watermark.astimezone(UTC).timestamp())

    def _rebuild_trade_window(
        self,
        chain: str,
        token: str,
        window_name: str,
        observed_at: datetime,
    ) -> TokenTradeWindow:
        window_end = _window_end(observed_at)
        window_size = _window_delta(window_name)
        trades = self.repository.features.load_trade_facts(
            chain,
            token,
            start_at=window_end - window_size,
            end_at=window_end,
        )
        buy_notional_usd = sum(trade.quote_amount_usd for trade in trades if trade.side == "buy")
        sell_notional_usd = sum(trade.quote_amount_usd for trade in trades if trade.side == "sell")
        unique_wallets = len({trade.wallet_address for trade in trades if trade.wallet_address})
        return TokenTradeWindow(
            chain=chain,
            token=token,
            window_name=window_name,
            window_end=window_end,
            buy_notional_usd=buy_notional_usd,
            sell_notional_usd=sell_notional_usd,
            trade_count=len(trades),
            unique_wallets=unique_wallets,
        )

    def _build_buy_pressure_snapshot(
        self,
        trade_window: TokenTradeWindow,
        onchain_settings,
    ) -> FeatureSnapshot:
        total_notional = trade_window.buy_notional_usd + trade_window.sell_notional_usd
        value = trade_window.buy_notional_usd / total_notional if total_notional > 0 else 0.0
        freshness_seconds = max(
            0.0,
            (datetime.now(UTC) - trade_window.window_end.astimezone(UTC)).total_seconds(),
        )
        quality_flag = "ok"
        if trade_window.trade_count < onchain_settings.min_trade_count_for_buy_pressure:
            quality_flag = "low_sample"
        elif freshness_seconds > onchain_settings.max_trade_lag_seconds:
            quality_flag = "stale"
        self.repository.features.save_quality(
            FeatureQualityRecord(
                chain=trade_window.chain,
                token=trade_window.token,
                feature_name="buy_pressure",
                as_of=trade_window.window_end,
                freshness_seconds=freshness_seconds,
                source_lag_seconds=freshness_seconds,
                missing_sources=[],
                degraded_reason=(quality_flag if quality_flag != "ok" else None),
            )
        )
        snapshot = FeatureSnapshot(
            chain=trade_window.chain,
            token=trade_window.token,
            feature_name="buy_pressure",
            feature_value=value,
            window_name=trade_window.window_name,
            as_of=trade_window.window_end,
            sample_count=trade_window.trade_count,
            freshness_seconds=freshness_seconds,
            quality_flag=quality_flag,
            formula_version=BUY_PRESSURE_FORMULA_VERSION,
            inputs={
                "buy_notional_usd": trade_window.buy_notional_usd,
                "sell_notional_usd": trade_window.sell_notional_usd,
                "unique_wallets": trade_window.unique_wallets,
            },
        )
        return self.repository.features.save_snapshot(snapshot)


class SlippageFeatureAggregator:
    def __init__(
        self,
        settings: AppSettings,
        repository: StorageRepository,
        metrics: Metrics | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.metrics = metrics

    def ingest_raw_quote(self, raw_event: RawEventRecord) -> FeatureSnapshot:
        slippage_settings = FeatureConfig.model_validate(self.settings.features).slippage
        sample = classify_quote(raw_event)
        self.repository.features.append_quote_sample(sample)
        samples = self.repository.features.load_quote_samples(sample.chain, sample.token, limit=100)
        curve = self.repository.features.upsert_slippage_curve(
            build_slippage_curve(sample.chain, sample.token, samples)
        )
        self._record_aggregator_metrics(
            "estimated_slippage_bps",
            sample.chain,
            sample.token,
            sample.quoted_at,
        )
        return self._build_slippage_snapshot(sample.chain, sample.token, samples, curve, slippage_settings)

    def _record_aggregator_metrics(
        self,
        feature: str,
        chain: str,
        token: str,
        watermark: datetime,
    ) -> None:
        if self.metrics is None:
            return
        self.metrics.aggregator_runs.labels(feature=feature, outcome="processed").inc()
        lag_seconds = max(0.0, (datetime.now(UTC) - watermark.astimezone(UTC)).total_seconds())
        self.metrics.aggregator_source_lag.labels(feature=feature, chain=chain, token=token).set(
            lag_seconds
        )
        self.metrics.aggregator_last_watermark.labels(
            feature=feature,
            chain=chain,
            token=token,
        ).set(watermark.astimezone(UTC).timestamp())

    def _build_slippage_snapshot(self, chain, token, samples, curve, slippage_settings) -> FeatureSnapshot:
        chosen_sample = next(
            (sample for sample in samples if sample.quote_notional_usd == slippage_settings.publication_notional_usd),
            None,
        )
        quality_flag = "ok"
        estimated_slippage_bps = 0.0
        route_provider = "curve"
        missing_sources: list[str] = []
        degraded_reason: str | None = None
        if chosen_sample is not None:
            estimated_slippage_bps = chosen_sample.slippage_bps
            route_provider = str(chosen_sample.route_summary.get("provider", "unknown"))
        elif slippage_settings.allow_curve_fallback:
            estimated_slippage_bps = _estimate_from_curve(curve, slippage_settings.publication_notional_usd)
            quality_flag = "degraded"
            missing_sources = ["exact_quote_sample"]
            degraded_reason = "curve_fallback"
        freshness_seconds = max(0.0, (datetime.now(UTC) - curve.curve_as_of).total_seconds())
        if freshness_seconds > slippage_settings.max_quote_age_seconds:
            quality_flag = "stale"
            degraded_reason = "stale_quote"
        self.repository.features.save_quality(
            FeatureQualityRecord(
                chain=chain,
                token=token,
                feature_name="estimated_slippage_bps",
                as_of=curve.curve_as_of,
                freshness_seconds=freshness_seconds,
                source_lag_seconds=freshness_seconds,
                missing_sources=missing_sources,
                degraded_reason=degraded_reason,
            )
        )
        snapshot = FeatureSnapshot(
            chain=chain,
            token=token,
            feature_name="estimated_slippage_bps",
            feature_value=estimated_slippage_bps,
            window_name="latest",
            as_of=curve.curve_as_of,
            sample_count=len(samples),
            freshness_seconds=freshness_seconds,
            quality_flag=quality_flag,
            formula_version=SLIPPAGE_FORMULA_VERSION,
            inputs={
                "quote_notional_usd": slippage_settings.publication_notional_usd,
                "route_provider": route_provider,
                "curve_version": curve.curve_version,
                "missing_sources": missing_sources,
                "volume_5m_usd": float(
                    (chosen_sample.route_summary if chosen_sample is not None else {}).get(
                        "volume_5m_usd", 0.0
                    )
                    or 0.0
                ),
                "buy_pressure": float(
                    (chosen_sample.route_summary if chosen_sample is not None else {}).get(
                        "buy_pressure", 0.0
                    )
                    or 0.0
                ),
                "market_source": str(
                    (chosen_sample.route_summary if chosen_sample is not None else {}).get(
                        "market_source", ""
                    )
                ),
            },
        )
        return self.repository.features.save_snapshot(snapshot)


def classify_trade(raw_event: RawEventRecord) -> DexTradeFact:
    payload = raw_event.payload
    side = _infer_trade_side(payload)
    return DexTradeFact(
        trade_id=str(payload.get("trade_id") or raw_event.source_event_id),
        chain=str(raw_event.chain or payload.get("chain") or "solana"),
        token=str(raw_event.token or payload["token"]),
        pool_address=str(payload["pool_address"]),
        wallet_address=str(payload.get("wallet_address")) if payload.get("wallet_address") else None,
        side=side,
        token_amount=abs(float(payload.get("token_amount", 0.0))),
        quote_amount_usd=abs(float(payload.get("quote_amount_usd", 0.0))),
        observed_at=raw_event.observed_at.astimezone(UTC),
        source_event_id=raw_event.source_event_id,
        classification_version=TRADE_CLASSIFICATION_VERSION,
    )


def _infer_trade_side(payload: dict[str, object]) -> str:
    explicit = str(payload.get("side", "")).lower().strip()
    if explicit in {"buy", "sell"}:
        return explicit

    token_amount = _as_float(payload.get("token_amount"))
    quote_amount = _as_float(payload.get("quote_amount"))
    if token_amount > 0 and quote_amount < 0:
        return "buy"
    if token_amount < 0 and quote_amount > 0:
        return "sell"

    raise ValueError("trade_side_unresolved")


def _window_end(observed_at: datetime) -> datetime:
    normalized = observed_at.astimezone(UTC)
    bucket_start = normalized.replace(second=0, microsecond=0)
    if normalized == bucket_start:
        return bucket_start
    return bucket_start + timedelta(minutes=1)


def _window_delta(window_name: str) -> timedelta:
    if not window_name.endswith("m"):
        raise ValueError("unsupported_trade_window")
    return timedelta(minutes=int(window_name[:-1]))


def _as_float(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value)
    return 0.0


def classify_quote(raw_event: RawEventRecord) -> DexQuoteSample:
    payload = raw_event.payload
    reference_mid_usd = abs(float(payload["reference_mid_usd"]))
    expected_out_usd = abs(float(payload["expected_out_usd"]))
    if reference_mid_usd <= 0:
        raise ValueError("invalid_reference_mid_usd")
    return DexQuoteSample(
        quote_id=str(payload.get("quote_id") or payload["quote_request_id"]),
        chain=str(raw_event.chain or payload.get("chain") or "solana"),
        token=str(raw_event.token or payload["token"]),
        quote_notional_usd=abs(float(payload["quote_notional_usd"])),
        expected_out_usd=expected_out_usd,
        reference_mid_usd=reference_mid_usd,
        slippage_bps=10000.0 * (reference_mid_usd - expected_out_usd) / reference_mid_usd,
        route_summary=dict(payload.get("route_summary", {})),
        quoted_at=raw_event.observed_at.astimezone(UTC),
        source_event_id=raw_event.source_event_id,
    )


def build_slippage_curve(chain: str, token: str, samples: list[DexQuoteSample]) -> SlippageCurve:
    ordered = sorted(samples, key=lambda sample: sample.quote_notional_usd)
    curve_as_of = max(sample.quoted_at for sample in ordered)
    freshness_seconds = max(0.0, (datetime.now(UTC) - curve_as_of).total_seconds())
    return SlippageCurve(
        chain=chain,
        token=token,
        curve_as_of=curve_as_of,
        sample_points=[
            {
                "quote_notional_usd": sample.quote_notional_usd,
                "slippage_bps": sample.slippage_bps,
            }
            for sample in ordered
        ],
        curve_version=SLIPPAGE_CURVE_VERSION,
        freshness_seconds=freshness_seconds,
    )


def _estimate_from_curve(curve: SlippageCurve, target_notional_usd: float) -> float:
    points = sorted(curve.sample_points, key=lambda point: float(point["quote_notional_usd"]))
    if not points:
        return 0.0
    if len(points) == 1:
        return float(points[0]["slippage_bps"])

    lower = points[0]
    upper = points[-1]
    for point in points:
        if float(point["quote_notional_usd"]) <= target_notional_usd:
            lower = point
        if float(point["quote_notional_usd"]) >= target_notional_usd:
            upper = point
            break

    lower_x = float(lower["quote_notional_usd"])
    upper_x = float(upper["quote_notional_usd"])
    lower_y = float(lower["slippage_bps"])
    upper_y = float(upper["slippage_bps"])
    if upper_x == lower_x:
        return lower_y
    ratio = (target_notional_usd - lower_x) / (upper_x - lower_x)
    return lower_y + (upper_y - lower_y) * ratio