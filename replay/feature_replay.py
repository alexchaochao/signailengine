from __future__ import annotations

from dataclasses import dataclass, field

from core.config import AppSettings, FeatureConfig
from infra.repository import StorageRepository
from sentinel.feature_aggregator import OnchainFeatureAggregator, SlippageFeatureAggregator


@dataclass(frozen=True)
class FeatureReplaySnapshotDiff:
    feature_name: str
    window_name: str
    source_value: float | None
    target_value: float | None
    delta: float | None
    source_as_of: str | None
    target_as_of: str | None
    status: str
    source_inputs: dict[str, str | float | int | bool | None] = field(default_factory=dict)
    target_inputs: dict[str, str | float | int | bool | None] = field(default_factory=dict)

    def to_dict(self) -> dict[str, str | float | None]:
        return {
            "feature_name": self.feature_name,
            "window_name": self.window_name,
            "source_value": self.source_value,
            "target_value": self.target_value,
            "delta": self.delta,
            "source_as_of": self.source_as_of,
            "target_as_of": self.target_as_of,
            "source_inputs": self.source_inputs,
            "target_inputs": self.target_inputs,
            "status": self.status,
        }


@dataclass(frozen=True)
class FeatureReplaySummary:
    raw_event_count: int
    replayed_trade_count: int
    replayed_quote_count: int
    ignored_event_count: int
    snapshot_diffs: list[FeatureReplaySnapshotDiff] = field(default_factory=list)

    def to_dict(self) -> dict[str, int | list[dict[str, str | float | None]]]:
        return {
            "raw_event_count": self.raw_event_count,
            "replayed_trade_count": self.replayed_trade_count,
            "replayed_quote_count": self.replayed_quote_count,
            "ignored_event_count": self.ignored_event_count,
            "snapshot_diffs": [snapshot_diff.to_dict() for snapshot_diff in self.snapshot_diffs],
        }


def replay_feature_events(
    settings: AppSettings,
    source_repository: StorageRepository,
    target_repository: StorageRepository,
    *,
    chain: str | None = None,
    token: str | None = None,
) -> FeatureReplaySummary:
    raw_events = source_repository.raw_events.read_events(chain=chain, token=token, limit=10_000)
    onchain_aggregator = OnchainFeatureAggregator(settings, target_repository)
    slippage_aggregator = SlippageFeatureAggregator(settings, target_repository)
    replayed_trade_count = 0
    replayed_quote_count = 0
    ignored_event_count = 0

    for raw_event in raw_events:
        target_repository.raw_events.save(raw_event)
        if raw_event.source_type == "onchain_trade":
            onchain_aggregator.ingest_raw_trade(raw_event)
            replayed_trade_count += 1
        elif raw_event.source_type == "dex_quote":
            slippage_aggregator.ingest_raw_quote(raw_event)
            replayed_quote_count += 1
        else:
            ignored_event_count += 1

    snapshot_diffs = _build_snapshot_diffs(
        settings,
        source_repository,
        target_repository,
        chain=chain,
        token=token,
    )

    return FeatureReplaySummary(
        raw_event_count=len(raw_events),
        replayed_trade_count=replayed_trade_count,
        replayed_quote_count=replayed_quote_count,
        ignored_event_count=ignored_event_count,
        snapshot_diffs=snapshot_diffs,
    )


def _build_snapshot_diffs(
    settings: AppSettings,
    source_repository: StorageRepository,
    target_repository: StorageRepository,
    *,
    chain: str | None,
    token: str | None,
) -> list[FeatureReplaySnapshotDiff]:
    if chain is None or token is None:
        return []
    feature_settings = FeatureConfig.model_validate(settings.features)
    comparisons = [
        ("buy_pressure", feature_settings.onchain.buy_pressure_primary_window),
        ("estimated_slippage_bps", "latest"),
    ]
    diffs: list[FeatureReplaySnapshotDiff] = []
    for feature_name, window_name in comparisons:
        source_snapshot = source_repository.features.load_latest_snapshot(
            chain,
            token,
            feature_name,
            window_name,
        )
        target_snapshot = target_repository.features.load_latest_snapshot(
            chain,
            token,
            feature_name,
            window_name,
        )
        source_value = source_snapshot.feature_value if source_snapshot is not None else None
        target_value = target_snapshot.feature_value if target_snapshot is not None else None
        delta = (
            round(target_value - source_value, 6)
            if source_value is not None and target_value is not None
            else None
        )
        if source_snapshot is None and target_snapshot is None:
            status = "missing_both"
        elif source_snapshot is None:
            status = "missing_source"
        elif target_snapshot is None:
            status = "missing_target"
        elif delta == 0:
            status = "match"
        else:
            status = "delta"
        diffs.append(
            FeatureReplaySnapshotDiff(
                feature_name=feature_name,
                window_name=window_name,
                source_value=source_value,
                target_value=target_value,
                delta=delta,
                source_as_of=(source_snapshot.as_of.isoformat() if source_snapshot is not None else None),
                target_as_of=(target_snapshot.as_of.isoformat() if target_snapshot is not None else None),
                source_inputs=(
                    _summarize_snapshot_inputs(source_snapshot.inputs)
                    if source_snapshot is not None
                    else {}
                ),
                target_inputs=(
                    _summarize_snapshot_inputs(target_snapshot.inputs)
                    if target_snapshot is not None
                    else {}
                ),
                status=status,
            )
        )
    return diffs


def _summarize_snapshot_inputs(
    inputs: dict[str, object],
) -> dict[str, str | float | int | bool | None]:
    summary: dict[str, str | float | int | bool | None] = {}
    for key, value in sorted(inputs.items()):
        if isinstance(value, (str, float, int, bool)) or value is None:
            summary[key] = value
    return summary