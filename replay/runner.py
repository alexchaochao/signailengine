from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings, ReplayRegressionThresholds
from core.pipeline import PipelineResult, PipelineWorker, signal_timestamp
from core.schemas import EventEnvelope
from infra.postgres import init_storage
from infra.repository import StorageRepository
from replay.feature_replay import FeatureReplaySummary, replay_feature_events


@dataclass(frozen=True)
class ReplayRunSummary:
	source_path: Path
	event_count: int
	batch_count: int
	pipeline_run_count: int
	token_count: int
	intent_count: int
	execution_count: int
	rejected_count: int
	first_observed_at: str | None
	last_observed_at: str | None

	def to_dict(self) -> dict[str, str | int | None]:
		return {
			"source_path": str(self.source_path),
			"event_count": self.event_count,
			"batch_count": self.batch_count,
			"pipeline_run_count": self.pipeline_run_count,
			"token_count": self.token_count,
			"intent_count": self.intent_count,
			"execution_count": self.execution_count,
			"rejected_count": self.rejected_count,
			"first_observed_at": self.first_observed_at,
			"last_observed_at": self.last_observed_at,
		}


@dataclass(frozen=True)
class ReplayFilterSet:
	tokens: list[str]
	start_observed_at: str | None
	end_observed_at: str | None
	source_event_count: int
	selected_event_count: int

	def to_dict(self) -> dict[str, str | int | list[str] | None]:
		return {
			"tokens": self.tokens,
			"start_observed_at": self.start_observed_at,
			"end_observed_at": self.end_observed_at,
			"source_event_count": self.source_event_count,
			"selected_event_count": self.selected_event_count,
		}


@dataclass(frozen=True)
class ReplaySignalSummary:
	state_candidate: str
	alpha_score: float
	reasons: list[str]
	features: dict[str, float | int | bool]
	sub_scores: dict[str, float]

	def to_dict(self) -> dict[str, Any]:
		return {
			"state_candidate": self.state_candidate,
			"alpha_score": self.alpha_score,
			"reasons": self.reasons,
			"features": self.features,
			"sub_scores": self.sub_scores,
		}


@dataclass(frozen=True)
class ReplayIntentSummary:
	venue_type: str
	venue: str
	action: str
	confidence: float
	target_notional_usd: float
	max_slippage_bps: int
	state: str
	strategy: str
	reasons: list[str]

	def to_dict(self) -> dict[str, Any]:
		return {
			"venue_type": self.venue_type,
			"venue": self.venue,
			"action": self.action,
			"confidence": self.confidence,
			"target_notional_usd": self.target_notional_usd,
			"max_slippage_bps": self.max_slippage_bps,
			"state": self.state,
			"strategy": self.strategy,
			"reasons": self.reasons,
		}


@dataclass(frozen=True)
class ReplayRiskSummary:
	allowed: bool
	adjusted_notional_usd: float
	violations: list[str]
	warnings: list[str]

	def to_dict(self) -> dict[str, Any]:
		return {
			"allowed": self.allowed,
			"adjusted_notional_usd": self.adjusted_notional_usd,
			"violations": self.violations,
			"warnings": self.warnings,
		}


@dataclass(frozen=True)
class ReplayExecutionSummary:
	adapter_name: str
	status: str
	executed_notional_usd: float
	external_order_id: str | None
	quote_id: str | None
	simulation: bool
	message: str

	def to_dict(self) -> dict[str, Any]:
		return {
			"adapter_name": self.adapter_name,
			"status": self.status,
			"executed_notional_usd": self.executed_notional_usd,
			"external_order_id": self.external_order_id,
			"quote_id": self.quote_id,
			"simulation": self.simulation,
			"message": self.message,
		}


@dataclass(frozen=True)
class ReplayReconciliationSummary:
	applied: bool
	reasons: list[str]
	position_is_open: bool
	position_venue_type: str
	portfolio_open_positions: int
	portfolio_daily_pnl_fraction: float

	def to_dict(self) -> dict[str, Any]:
		return {
			"applied": self.applied,
			"reasons": self.reasons,
			"position_is_open": self.position_is_open,
			"position_venue_type": self.position_venue_type,
			"portfolio_open_positions": self.portfolio_open_positions,
			"portfolio_daily_pnl_fraction": self.portfolio_daily_pnl_fraction,
		}


@dataclass(frozen=True)
class ReplayBatchDiagnostics:
	event_count: int
	decision_window_seconds: float
	average_ingest_latency_seconds: float
	max_ingest_latency_seconds: float
	signal_to_risk_latency_seconds: float
	signal_to_execution_latency_seconds: float | None
	signal_to_reconciliation_latency_seconds: float | None
	estimated_slippage_bps: float
	slippage_budget_bps: int | None
	slippage_headroom_bps: float | None

	def to_dict(self) -> dict[str, float | int | None]:
		return {
			"event_count": self.event_count,
			"decision_window_seconds": self.decision_window_seconds,
			"average_ingest_latency_seconds": self.average_ingest_latency_seconds,
			"max_ingest_latency_seconds": self.max_ingest_latency_seconds,
			"signal_to_risk_latency_seconds": self.signal_to_risk_latency_seconds,
			"signal_to_execution_latency_seconds": self.signal_to_execution_latency_seconds,
			"signal_to_reconciliation_latency_seconds": (
				self.signal_to_reconciliation_latency_seconds
			),
			"estimated_slippage_bps": self.estimated_slippage_bps,
			"slippage_budget_bps": self.slippage_budget_bps,
			"slippage_headroom_bps": self.slippage_headroom_bps,
		}


@dataclass(frozen=True)
class ReplayBatchResult:
	batch_index: int
	token: str
	observed_at: str
	event_ids: list[str]
	event_types: list[str]
	route: str
	intent_id: str | None
	signal: ReplaySignalSummary
	intent: ReplayIntentSummary | None
	risk: ReplayRiskSummary
	execution: ReplayExecutionSummary | None
	reconciliation: ReplayReconciliationSummary | None
	diagnostics: ReplayBatchDiagnostics
	ledger_statuses: list[str]

	def to_dict(self) -> dict[str, Any]:
		return {
			"batch_index": self.batch_index,
			"token": self.token,
			"observed_at": self.observed_at,
			"event_ids": self.event_ids,
			"event_types": self.event_types,
			"route": self.route,
			"intent_id": self.intent_id,
			"signal": self.signal.to_dict(),
			"intent": self.intent.to_dict() if self.intent is not None else None,
			"risk": self.risk.to_dict(),
			"execution": self.execution.to_dict() if self.execution is not None else None,
			"reconciliation": (
				self.reconciliation.to_dict() if self.reconciliation is not None else None
			),
			"diagnostics": self.diagnostics.to_dict(),
			"ledger_statuses": self.ledger_statuses,
		}


@dataclass(frozen=True)
class ReplayAttributionMetrics:
	route_counts: dict[str, int]
	risk_rejection_reason_counts: dict[str, int]
	execution_status_counts: dict[str, int]
	fill_rate: float
	rejection_rate: float
	average_signal_alpha_score: float
	average_requested_notional_usd: float
	average_executed_notional_usd: float
	strategy_requested_notional_usd: dict[str, float]
	strategy_executed_notional_usd: dict[str, float]

	def to_dict(self) -> dict[str, Any]:
		return {
			"route_counts": self.route_counts,
			"risk_rejection_reason_counts": self.risk_rejection_reason_counts,
			"execution_status_counts": self.execution_status_counts,
			"fill_rate": self.fill_rate,
			"rejection_rate": self.rejection_rate,
			"average_signal_alpha_score": self.average_signal_alpha_score,
			"average_requested_notional_usd": self.average_requested_notional_usd,
			"average_executed_notional_usd": self.average_executed_notional_usd,
			"strategy_requested_notional_usd": self.strategy_requested_notional_usd,
			"strategy_executed_notional_usd": self.strategy_executed_notional_usd,
		}


@dataclass(frozen=True)
class ReplayRunReport:
	summary: ReplayRunSummary
	filters: ReplayFilterSet
	attribution: ReplayAttributionMetrics
	batches: list[ReplayBatchResult]

	def to_dict(self) -> dict[str, Any]:
		return {
			"summary": self.summary.to_dict(),
			"filters": self.filters.to_dict(),
			"attribution": self.attribution.to_dict(),
			"batches": [batch.to_dict() for batch in self.batches],
		}


@dataclass(frozen=True)
class ReplayComparisonRow:
	dataset_path: str
	event_count: int
	batch_count: int
	intent_count: int
	execution_count: int
	rejected_count: int
	fill_rate: float
	rejection_rate: float
	average_signal_alpha_score: float
	average_executed_notional_usd: float
	average_signal_to_risk_latency_seconds: float
	average_signal_to_execution_latency_seconds: float
	average_estimated_slippage_bps: float

	def to_dict(self) -> dict[str, str | int | float]:
		return {
			"dataset_path": self.dataset_path,
			"event_count": self.event_count,
			"batch_count": self.batch_count,
			"intent_count": self.intent_count,
			"execution_count": self.execution_count,
			"rejected_count": self.rejected_count,
			"fill_rate": self.fill_rate,
			"rejection_rate": self.rejection_rate,
			"average_signal_alpha_score": self.average_signal_alpha_score,
			"average_executed_notional_usd": self.average_executed_notional_usd,
			"average_signal_to_risk_latency_seconds": self.average_signal_to_risk_latency_seconds,
			"average_signal_to_execution_latency_seconds": (
				self.average_signal_to_execution_latency_seconds
			),
			"average_estimated_slippage_bps": self.average_estimated_slippage_bps,
		}


@dataclass(frozen=True)
class ReplayComparisonReport:
	runs: list[ReplayRunReport]
	rows: list[ReplayComparisonRow]

	def to_dict(self) -> dict[str, Any]:
		return {
			"rows": [row.to_dict() for row in self.rows],
			"runs": [run.to_dict() for run in self.runs],
		}


@dataclass(frozen=True)
class ReplayRegressionJudgment:
	metric: str
	scope: str
	baseline_value: float
	candidate_value: float
	delta: float
	threshold: float
	direction: str
	status: str

	def to_dict(self) -> dict[str, str | float]:
		return {
			"metric": self.metric,
			"scope": self.scope,
			"baseline_value": self.baseline_value,
			"candidate_value": self.candidate_value,
			"delta": self.delta,
			"threshold": self.threshold,
			"direction": self.direction,
			"status": self.status,
		}


@dataclass(frozen=True)
class ReplayRegressionReport:
	baseline: ReplayRunReport
	candidate: ReplayRunReport
	summary_deltas: dict[str, float | int]
	attribution_deltas: dict[str, Any]
	judgments: list[ReplayRegressionJudgment]
	regressions: list[str]
	improvements: list[str]

	def to_dict(self) -> dict[str, Any]:
		return {
			"baseline": self.baseline.to_dict(),
			"candidate": self.candidate.to_dict(),
			"summary_deltas": self.summary_deltas,
			"attribution_deltas": self.attribution_deltas,
			"judgments": [judgment.to_dict() for judgment in self.judgments],
			"regressions": self.regressions,
			"improvements": self.improvements,
		}


class ReplayRedis:
	def __init__(self) -> None:
		self.streams: dict[str, list[tuple[str, dict[str, str]]]] = defaultdict(list)
		self.counter = 0

	def xadd(self, stream_name: str, mapping: dict[str, str]) -> str:
		self.counter += 1
		message_id = f"{self.counter}-0"
		self.streams[stream_name].append((message_id, mapping))
		return message_id


def load_replay_events(path: str | Path) -> list[EventEnvelope]:
	source_path = Path(path)
	events: list[EventEnvelope] = []

	with source_path.open("r", encoding="utf-8") as handle:
		for line in handle:
			stripped = line.strip()
			if not stripped:
				continue
			events.append(EventEnvelope.model_validate_json(stripped))

	return sorted(events, key=lambda event: (event.observed_at, event.event_id))


def build_replay_batches(events: list[EventEnvelope]) -> list[list[EventEnvelope]]:
	batches: list[list[EventEnvelope]] = []
	current_batch: list[EventEnvelope] = []
	current_key: tuple[datetime, str] | None = None

	for event in events:
		batch_key = (event.observed_at, event.token)
		if current_key != batch_key:
			if current_batch:
				batches.append(current_batch)
			current_batch = [event]
			current_key = batch_key
			continue
		current_batch.append(event)

	if current_batch:
		batches.append(current_batch)

	return batches


def parse_observed_at_filter(value: str | None) -> datetime | None:
	if value is None:
		return None
	parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
	if parsed.tzinfo is None:
		return parsed.replace(tzinfo=UTC)
	return parsed.astimezone(UTC)


def filter_replay_events(
	events: list[EventEnvelope],
	*,
	tokens: list[str] | None = None,
	start_observed_at: str | None = None,
	end_observed_at: str | None = None,
) -> tuple[list[EventEnvelope], ReplayFilterSet]:
	token_filter = set(tokens or [])
	start_filter = parse_observed_at_filter(start_observed_at)
	end_filter = parse_observed_at_filter(end_observed_at)
	filtered = [
		event
		for event in events
		if (not token_filter or event.token in token_filter)
		and (start_filter is None or event.observed_at >= start_filter)
		and (end_filter is None or event.observed_at <= end_filter)
	]
	return filtered, ReplayFilterSet(
		tokens=sorted(token_filter),
		start_observed_at=start_filter.isoformat() if start_filter is not None else None,
		end_observed_at=end_filter.isoformat() if end_filter is not None else None,
		source_event_count=len(events),
		selected_event_count=len(filtered),
	)


def replay_events(
	events: list[EventEnvelope],
	*,
	settings: AppSettings | None = None,
	worker: PipelineWorker | None = None,
) -> list[PipelineResult]:
	replay_settings = settings or AppSettings.load()
	replay_worker = worker or PipelineWorker(
		replay_settings,
		cast(Redis, ReplayRedis()),
		db_engine=create_engine("sqlite:///:memory:"),
	)
	if replay_worker.db_engine is not None:
		init_storage(replay_worker.db_engine)
	return [replay_worker.process_events(batch) for batch in build_replay_batches(events)]


def _build_signal_summary(result: PipelineResult) -> ReplaySignalSummary:
	return ReplaySignalSummary(
		state_candidate=result.signal.state_candidate.value,
		alpha_score=result.signal.alpha_score,
		reasons=list(result.signal.reasons),
		features=dict(result.signal.features),
		sub_scores=dict(result.signal.sub_scores),
	)


def _build_intent_summary(result: PipelineResult) -> ReplayIntentSummary | None:
	if result.route.intent is None:
		return None
	intent = result.route.intent
	return ReplayIntentSummary(
		venue_type=intent.venue_type.value,
		venue=intent.venue,
		action=intent.action.value,
		confidence=intent.confidence,
		target_notional_usd=intent.target_notional_usd,
		max_slippage_bps=intent.max_slippage_bps,
		state=intent.state.value,
		strategy=intent.strategy,
		reasons=list(intent.reasons),
	)


def _build_risk_summary(result: PipelineResult) -> ReplayRiskSummary:
	return ReplayRiskSummary(
		allowed=result.risk.allowed,
		adjusted_notional_usd=result.risk.adjusted_notional_usd,
		violations=list(result.risk.violations),
		warnings=list(result.risk.warnings),
	)


def _build_execution_summary(result: PipelineResult) -> ReplayExecutionSummary | None:
	if result.execution is None:
		return None
	execution = result.execution
	return ReplayExecutionSummary(
		adapter_name=execution.adapter_name,
		status=execution.status,
		executed_notional_usd=execution.executed_notional_usd,
		external_order_id=execution.external_order_id,
		quote_id=execution.quote_id,
		simulation=execution.simulation,
		message=execution.message,
	)


def _build_reconciliation_summary(result: PipelineResult) -> ReplayReconciliationSummary | None:
	if result.reconciliation is None:
		return None
	reconciliation = result.reconciliation
	return ReplayReconciliationSummary(
		applied=reconciliation.applied,
		reasons=list(reconciliation.reasons),
		position_is_open=reconciliation.position.is_open,
		position_venue_type=reconciliation.position.venue_type.value,
		portfolio_open_positions=reconciliation.portfolio.open_positions,
		portfolio_daily_pnl_fraction=reconciliation.portfolio.daily_pnl_fraction,
	)


def _build_batch_diagnostics(
	batch: list[EventEnvelope],
	result: PipelineResult,
) -> ReplayBatchDiagnostics:
	signal_time = signal_timestamp(result.signal)
	ingest_latencies = [
		(event.ingested_at - event.observed_at).total_seconds() for event in batch
	]
	decision_window_seconds = (
		max(event.observed_at for event in batch) - min(event.observed_at for event in batch)
	).total_seconds()
	estimated_slippage_bps = float(result.signal.features.get("estimated_slippage_bps", 0.0))
	slippage_budget_bps = (
		result.route.intent.max_slippage_bps if result.route.intent is not None else None
	)
	slippage_headroom_bps = (
		float(slippage_budget_bps - estimated_slippage_bps)
		if slippage_budget_bps is not None
		else None
	)
	return ReplayBatchDiagnostics(
		event_count=len(batch),
		decision_window_seconds=round(decision_window_seconds, 6),
		average_ingest_latency_seconds=round(sum(ingest_latencies) / len(ingest_latencies), 6),
		max_ingest_latency_seconds=round(max(ingest_latencies), 6),
		signal_to_risk_latency_seconds=round(
			(result.risk.timestamp - signal_time).total_seconds(),
			6,
		),
		signal_to_execution_latency_seconds=(
			round((result.execution.timestamp - signal_time).total_seconds(), 6)
			if result.execution is not None
			else None
		),
		signal_to_reconciliation_latency_seconds=(
			round((result.reconciliation.timestamp - signal_time).total_seconds(), 6)
			if result.reconciliation is not None
			else None
		),
		estimated_slippage_bps=estimated_slippage_bps,
		slippage_budget_bps=slippage_budget_bps,
		slippage_headroom_bps=slippage_headroom_bps,
	)


def _build_attribution_metrics(results: list[PipelineResult]) -> ReplayAttributionMetrics:
	route_counts = Counter(result.route.route for result in results)
	risk_rejections = Counter(
		violation
		for result in results
		for violation in result.risk.violations
		if not result.risk.allowed
	)
	execution_status_counts = Counter(
		result.execution.status for result in results if result.execution is not None
	)
	signal_alpha_scores = [result.signal.alpha_score for result in results]
	requested_intents = [
		result.route.intent for result in results if result.route.intent is not None
	]
	executions = [result.execution for result in results if result.execution is not None]
	strategy_requested_notional: dict[str, float] = {
		intent.strategy: 0.0 for intent in requested_intents
	}
	for intent in requested_intents:
		strategy_requested_notional[intent.strategy] = (
			strategy_requested_notional.get(intent.strategy, 0.0) + intent.target_notional_usd
		)
	strategy_executed_notional: dict[str, float] = {
		intent.strategy: 0.0 for intent in requested_intents
	}
	for result in results:
		if result.route.intent is None or result.execution is None:
			continue
		strategy_executed_notional[result.route.intent.strategy] = (
			strategy_executed_notional.get(result.route.intent.strategy, 0.0)
			+ result.execution.executed_notional_usd
		)

	intent_count = len(requested_intents)
	rejected_count = sum(
		1
		for result in results
		if result.route.intent is not None and not result.risk.allowed
	)
	execution_count = len(executions)
	return ReplayAttributionMetrics(
		route_counts=dict(sorted(route_counts.items())),
		risk_rejection_reason_counts=dict(sorted(risk_rejections.items())),
		execution_status_counts=dict(sorted(execution_status_counts.items())),
		fill_rate=round(execution_count / intent_count, 6) if intent_count else 0.0,
		rejection_rate=round(rejected_count / intent_count, 6) if intent_count else 0.0,
		average_signal_alpha_score=(
			round(sum(signal_alpha_scores) / len(signal_alpha_scores), 6)
			if signal_alpha_scores
			else 0.0
		),
		average_requested_notional_usd=(
			round(sum(intent.target_notional_usd for intent in requested_intents) / intent_count, 6)
			if intent_count
			else 0.0
		),
		average_executed_notional_usd=(
			round(
				sum(execution.executed_notional_usd for execution in executions) / execution_count,
				6,
			)
			if execution_count
			else 0.0
		),
		strategy_requested_notional_usd={
			strategy: round(notional, 6)
			for strategy, notional in sorted(strategy_requested_notional.items())
		},
		strategy_executed_notional_usd={
			strategy: round(notional, 6)
			for strategy, notional in sorted(strategy_executed_notional.items())
		},
	)


def build_replay_report(
	source_path: str | Path,
	events: list[EventEnvelope],
	results: list[PipelineResult],
	filters: ReplayFilterSet,
) -> ReplayRunReport:
	batches = build_replay_batches(events)
	if not events:
		return ReplayRunReport(
			summary=ReplayRunSummary(
				source_path=Path(source_path),
				event_count=0,
				batch_count=0,
				pipeline_run_count=0,
				token_count=0,
				intent_count=0,
				execution_count=0,
				rejected_count=0,
				first_observed_at=None,
				last_observed_at=None,
			),
			filters=filters,
			attribution=_build_attribution_metrics([]),
			batches=[],
		)

	unique_tokens = {event.token for event in events}
	intent_count = sum(1 for result in results if result.route.intent is not None)
	execution_count = sum(1 for result in results if result.execution is not None)
	rejected_count = sum(
		1 for result in results if result.route.intent is not None and not result.risk.allowed
	)
	batch_results = [
		ReplayBatchResult(
			batch_index=index,
			token=batch[0].token,
			observed_at=batch[0].observed_at.isoformat(),
			event_ids=[event.event_id for event in batch],
			event_types=[event.event_type for event in batch],
			route=result.route.route,
			intent_id=result.route.intent.intent_id if result.route.intent is not None else None,
			signal=_build_signal_summary(result),
			intent=_build_intent_summary(result),
			risk=_build_risk_summary(result),
			execution=_build_execution_summary(result),
			reconciliation=_build_reconciliation_summary(result),
			diagnostics=_build_batch_diagnostics(batch, result),
			ledger_statuses=[entry.status for entry in result.execution_ledger],
		)
		for index, (batch, result) in enumerate(zip(batches, results, strict=True), start=1)
	]
	return ReplayRunReport(
		summary=ReplayRunSummary(
			source_path=Path(source_path),
			event_count=len(events),
			batch_count=len(batches),
			pipeline_run_count=len(results),
			token_count=len(unique_tokens),
			intent_count=intent_count,
			execution_count=execution_count,
			rejected_count=rejected_count,
			first_observed_at=events[0].observed_at.isoformat(),
			last_observed_at=events[-1].observed_at.isoformat(),
		),
		filters=filters,
		attribution=_build_attribution_metrics(results),
		batches=batch_results,
	)


def run_replay_report(
	path: str | Path,
	*,
	settings: AppSettings | None = None,
	worker: PipelineWorker | None = None,
	tokens: list[str] | None = None,
	start_observed_at: str | None = None,
	end_observed_at: str | None = None,
) -> ReplayRunReport:
	source_path = Path(path)
	source_events = load_replay_events(source_path)
	filtered_events, filters = filter_replay_events(
		source_events,
		tokens=tokens,
		start_observed_at=start_observed_at,
		end_observed_at=end_observed_at,
	)
	results = (
		replay_events(filtered_events, settings=settings, worker=worker)
		if filtered_events
		else []
	)
	return build_replay_report(source_path, filtered_events, results, filters)


def run_replay(
	path: str | Path,
	*,
	settings: AppSettings | None = None,
	tokens: list[str] | None = None,
	start_observed_at: str | None = None,
	end_observed_at: str | None = None,
) -> ReplayRunSummary:
	return run_replay_report(
		path,
		settings=settings,
		tokens=tokens,
		start_observed_at=start_observed_at,
		end_observed_at=end_observed_at,
	).summary


def _build_comparison_rows(runs: list[ReplayRunReport]) -> list[ReplayComparisonRow]:
	def average_batch_metric(
		run: ReplayRunReport,
		accessor: Any,
	) -> float:
		if not run.batches:
			return 0.0
		values = [float(accessor(batch)) for batch in run.batches]
		return round(sum(values) / len(values), 6)

	def average_execution_latency(run: ReplayRunReport) -> float:
		if not run.batches:
			return 0.0
		values = [
			batch.diagnostics.signal_to_execution_latency_seconds
			for batch in run.batches
			if batch.diagnostics.signal_to_execution_latency_seconds is not None
		]
		if not values:
			return 0.0
		return round(sum(values) / len(values), 6)

	return [
		ReplayComparisonRow(
			dataset_path=str(run.summary.source_path),
			event_count=run.summary.event_count,
			batch_count=run.summary.batch_count,
			intent_count=run.summary.intent_count,
			execution_count=run.summary.execution_count,
			rejected_count=run.summary.rejected_count,
			fill_rate=run.attribution.fill_rate,
			rejection_rate=run.attribution.rejection_rate,
			average_signal_alpha_score=run.attribution.average_signal_alpha_score,
			average_executed_notional_usd=run.attribution.average_executed_notional_usd,
			average_signal_to_risk_latency_seconds=average_batch_metric(
				run,
				lambda batch: batch.diagnostics.signal_to_risk_latency_seconds,
			),
			average_signal_to_execution_latency_seconds=average_execution_latency(run),
			average_estimated_slippage_bps=average_batch_metric(
				run,
				lambda batch: batch.diagnostics.estimated_slippage_bps,
			),
		)
		for run in runs
	]


def run_replay_comparison(
	paths: list[str | Path],
	*,
	settings: AppSettings | None = None,
	tokens: list[str] | None = None,
	start_observed_at: str | None = None,
	end_observed_at: str | None = None,
) -> ReplayComparisonReport:
	runs = [
		run_replay_report(
			path,
			settings=settings,
			tokens=tokens,
			start_observed_at=start_observed_at,
			end_observed_at=end_observed_at,
		)
		for path in paths
	]
	return ReplayComparisonReport(runs=runs, rows=_build_comparison_rows(runs))


def _diff_numeric_dict(
	baseline: dict[str, int] | dict[str, float],
	candidate: dict[str, int] | dict[str, float],
) -> dict[str, float]:
	keys = sorted(set(baseline) | set(candidate))
	return {
		key: round(float(candidate.get(key, 0)) - float(baseline.get(key, 0)), 6)
		for key in keys
	}


def _build_judgment(
	metric: str,
	scope: str,
	baseline_value: float,
	candidate_value: float,
	*,
	threshold: float,
	direction: str,
) -> ReplayRegressionJudgment:
	delta = round(candidate_value - baseline_value, 6)
	if abs(delta) < threshold:
		status = "neutral"
	elif direction == "higher_is_better":
		status = "improvement" if delta > 0 else "regression"
	else:
		status = "improvement" if delta < 0 else "regression"
	return ReplayRegressionJudgment(
		metric=metric,
		scope=scope,
		baseline_value=round(baseline_value, 6),
		candidate_value=round(candidate_value, 6),
		delta=delta,
		threshold=threshold,
		direction=direction,
		status=status,
	)


def _collect_regression_judgments(
	baseline: ReplayRunReport,
	candidate: ReplayRunReport,
	thresholds: ReplayRegressionThresholds,
) -> list[ReplayRegressionJudgment]:
	judgments = [
		_build_judgment(
			"fill_rate",
			"global",
			baseline.attribution.fill_rate,
			candidate.attribution.fill_rate,
			threshold=thresholds.fill_rate,
			direction="higher_is_better",
		),
		_build_judgment(
			"rejection_rate",
			"global",
			baseline.attribution.rejection_rate,
			candidate.attribution.rejection_rate,
			threshold=thresholds.rejection_rate,
			direction="lower_is_better",
		),
		_build_judgment(
			"average_executed_notional_usd",
			"global",
			baseline.attribution.average_executed_notional_usd,
			candidate.attribution.average_executed_notional_usd,
			threshold=thresholds.average_executed_notional_usd,
			direction="higher_is_better",
		),
	]
	for route in sorted(
		set(baseline.attribution.route_counts) | set(candidate.attribution.route_counts)
	):
		judgments.append(
			_build_judgment(
				"route_count",
				route,
				float(baseline.attribution.route_counts.get(route, 0)),
				float(candidate.attribution.route_counts.get(route, 0)),
				threshold=thresholds.route_count,
				direction="higher_is_better",
			)
		)
	for reason in sorted(
		set(baseline.attribution.risk_rejection_reason_counts)
		| set(candidate.attribution.risk_rejection_reason_counts)
	):
		judgments.append(
			_build_judgment(
				"risk_rejection_reason_count",
				reason,
				float(baseline.attribution.risk_rejection_reason_counts.get(reason, 0)),
				float(candidate.attribution.risk_rejection_reason_counts.get(reason, 0)),
				threshold=thresholds.risk_rejection_reason_count,
				direction="lower_is_better",
			)
		)
	for strategy in sorted(
		set(baseline.attribution.strategy_executed_notional_usd)
		| set(candidate.attribution.strategy_executed_notional_usd)
	):
		judgments.append(
			_build_judgment(
				"strategy_executed_notional_usd",
				strategy,
				baseline.attribution.strategy_executed_notional_usd.get(strategy, 0.0),
				candidate.attribution.strategy_executed_notional_usd.get(strategy, 0.0),
				threshold=thresholds.strategy_executed_notional_usd,
				direction="higher_is_better",
			)
		)
	return judgments


def _build_regression_report(
	baseline: ReplayRunReport,
	candidate: ReplayRunReport,
	thresholds: ReplayRegressionThresholds,
) -> ReplayRegressionReport:
	summary_deltas: dict[str, float | int] = {
		"event_count": candidate.summary.event_count - baseline.summary.event_count,
		"batch_count": candidate.summary.batch_count - baseline.summary.batch_count,
		"intent_count": candidate.summary.intent_count - baseline.summary.intent_count,
		"execution_count": candidate.summary.execution_count - baseline.summary.execution_count,
		"rejected_count": candidate.summary.rejected_count - baseline.summary.rejected_count,
	}
	attribution_deltas: dict[str, Any] = {
		"fill_rate": round(candidate.attribution.fill_rate - baseline.attribution.fill_rate, 6),
		"rejection_rate": round(
			candidate.attribution.rejection_rate - baseline.attribution.rejection_rate,
			6,
		),
		"average_signal_alpha_score": round(
			candidate.attribution.average_signal_alpha_score
			- baseline.attribution.average_signal_alpha_score,
			6,
		),
		"average_requested_notional_usd": round(
			candidate.attribution.average_requested_notional_usd
			- baseline.attribution.average_requested_notional_usd,
			6,
		),
		"average_executed_notional_usd": round(
			candidate.attribution.average_executed_notional_usd
			- baseline.attribution.average_executed_notional_usd,
			6,
		),
		"route_counts": _diff_numeric_dict(
			baseline.attribution.route_counts,
			candidate.attribution.route_counts,
		),
		"risk_rejection_reason_counts": _diff_numeric_dict(
			baseline.attribution.risk_rejection_reason_counts,
			candidate.attribution.risk_rejection_reason_counts,
		),
		"execution_status_counts": _diff_numeric_dict(
			baseline.attribution.execution_status_counts,
			candidate.attribution.execution_status_counts,
		),
		"strategy_requested_notional_usd": _diff_numeric_dict(
			baseline.attribution.strategy_requested_notional_usd,
			candidate.attribution.strategy_requested_notional_usd,
		),
		"strategy_executed_notional_usd": _diff_numeric_dict(
			baseline.attribution.strategy_executed_notional_usd,
			candidate.attribution.strategy_executed_notional_usd,
		),
	}
	judgments = _collect_regression_judgments(baseline, candidate, thresholds)
	regressions = [
		f"{judgment.metric}:{judgment.scope}"
		for judgment in judgments
		if judgment.status == "regression"
	]
	improvements = [
		f"{judgment.metric}:{judgment.scope}"
		for judgment in judgments
		if judgment.status == "improvement"
	]
	if summary_deltas["execution_count"] < 0:
		regressions.append("execution_count:global")
	elif summary_deltas["execution_count"] > 0:
		improvements.append("execution_count:global")
	return ReplayRegressionReport(
		baseline=baseline,
		candidate=candidate,
		summary_deltas=summary_deltas,
		attribution_deltas=attribution_deltas,
		judgments=judgments,
		regressions=regressions,
		improvements=improvements,
	)


def run_replay_regression(
	baseline_path: str | Path,
	candidate_path: str | Path,
	*,
	settings: AppSettings | None = None,
	tokens: list[str] | None = None,
	start_observed_at: str | None = None,
	end_observed_at: str | None = None,
) -> ReplayRegressionReport:
	replay_settings = settings or AppSettings.load()
	baseline = run_replay_report(
		baseline_path,
		settings=replay_settings,
		tokens=tokens,
		start_observed_at=start_observed_at,
		end_observed_at=end_observed_at,
	)
	candidate = run_replay_report(
		candidate_path,
		settings=replay_settings,
		tokens=tokens,
		start_observed_at=start_observed_at,
		end_observed_at=end_observed_at,
	)
	return _build_regression_report(
		baseline,
		candidate,
		replay_settings.replay.regression_thresholds,
	)


def render_replay_summary(summary: ReplayRunSummary, *, as_json: bool) -> str:
	if as_json:
		return json.dumps(summary.to_dict(), sort_keys=True)

	return "\n".join(
		[
			f"source_path={summary.source_path}",
			f"event_count={summary.event_count}",
			f"batch_count={summary.batch_count}",
			f"pipeline_run_count={summary.pipeline_run_count}",
			f"token_count={summary.token_count}",
			f"intent_count={summary.intent_count}",
			f"execution_count={summary.execution_count}",
			f"rejected_count={summary.rejected_count}",
			f"first_observed_at={summary.first_observed_at}",
			f"last_observed_at={summary.last_observed_at}",
		]
	)


def render_replay_comparison(report: ReplayComparisonReport, *, as_json: bool) -> str:
	if as_json:
		return json.dumps(report.to_dict(), sort_keys=True)
	lines = [
		f"dataset_count={len(report.rows)}",
		(
			"dataset | events | batches | intents | execs | fill_rate | reject_rate | "
			"risk_latency_s | exec_latency_s | slippage_bps"
		),
	]
	lines.extend(
		[
			(
				f"{Path(row.dataset_path).name} | {row.event_count} | {row.batch_count} | "
				f"{row.intent_count} | {row.execution_count} | {row.fill_rate:.6f} | "
				f"{row.rejection_rate:.6f} | {row.average_signal_to_risk_latency_seconds:.6f} | "
				f"{row.average_signal_to_execution_latency_seconds:.6f} | "
				f"{row.average_estimated_slippage_bps:.2f}"
			)
			for row in report.rows
		]
	)
	return "\n".join(lines)


def render_replay_regression(report: ReplayRegressionReport, *, as_json: bool) -> str:
	if as_json:
		return json.dumps(report.to_dict(), sort_keys=True)
	judgment_lines = [
		(
			f"{judgment.metric}[{judgment.scope}] status={judgment.status} "
			f"baseline={judgment.baseline_value:.6f} "
			f"candidate={judgment.candidate_value:.6f} "
			f"delta={judgment.delta:.6f} threshold={judgment.threshold:.6f}"
		)
		for judgment in report.judgments
		if judgment.status != "neutral"
	]
	return "\n".join(
		[
			f"baseline={report.baseline.summary.source_path}",
			f"candidate={report.candidate.summary.source_path}",
			"summary_deltas:",
			f"  execution_count={report.summary_deltas['execution_count']}",
			f"  rejected_count={report.summary_deltas['rejected_count']}",
			"attribution_deltas:",
			f"fill_rate_delta={report.attribution_deltas['fill_rate']}",
			f"rejection_rate_delta={report.attribution_deltas['rejection_rate']}",
			f"average_executed_notional_delta={report.attribution_deltas['average_executed_notional_usd']}",
			"judgments:",
			*(judgment_lines or ["none"]),
			f"regressions={','.join(report.regressions) or 'none'}",
			f"improvements={','.join(report.improvements) or 'none'}",
		]
	)


def run_feature_replay_from_db(
	db_url: str,
	*,
	settings: AppSettings | None = None,
	chain: str | None = None,
	token: str | None = None,
) -> FeatureReplaySummary:
	replay_settings = settings or AppSettings.load()
	source_engine = create_engine(db_url)
	target_engine = create_engine("sqlite:///:memory:")
	init_storage(source_engine)
	init_storage(target_engine)
	return replay_feature_events(
		replay_settings,
		StorageRepository(source_engine),
		StorageRepository(target_engine),
		chain=chain,
		token=token,
	)


def render_feature_replay_summary(summary: FeatureReplaySummary, *, as_json: bool) -> str:
	if as_json:
		return json.dumps(summary.to_dict(), sort_keys=True)
	return "\n".join(
		[
			f"raw_event_count={summary.raw_event_count}",
			f"replayed_trade_count={summary.replayed_trade_count}",
			f"replayed_quote_count={summary.replayed_quote_count}",
			f"ignored_event_count={summary.ignored_event_count}",
			"snapshot_diffs:",
			*(
				[
					(
						f"  {snapshot_diff.feature_name}[{snapshot_diff.window_name}] "
						f"status={snapshot_diff.status} source={snapshot_diff.source_value} "
						f"target={snapshot_diff.target_value} delta={snapshot_diff.delta} "
						f"source_inputs={json.dumps(snapshot_diff.source_inputs, sort_keys=True)} "
						f"target_inputs={json.dumps(snapshot_diff.target_inputs, sort_keys=True)}"
					)
					for snapshot_diff in summary.snapshot_diffs
				]
				or ["  none"]
			),
		]
	)


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Run an offline SignalEngine replay")
	parser.add_argument("dataset_paths", nargs="*")
	parser.add_argument("--baseline")
	parser.add_argument("--candidate")
	parser.add_argument("--feature-replay-db-url")
	parser.add_argument("--feature-chain")
	parser.add_argument("--feature-token")
	parser.add_argument("--json", action="store_true")
	parser.add_argument("--output")
	parser.add_argument("--token", action="append")
	parser.add_argument("--start-observed-at")
	parser.add_argument("--end-observed-at")
	return parser


def write_replay_report(report: Any, output_path: str | Path) -> Path:
	target_path = Path(output_path)
	target_path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
	return target_path


def main(argv: list[str] | None = None) -> int:
	args = build_parser().parse_args(argv)
	if args.feature_replay_db_url:
		feature_summary = run_feature_replay_from_db(
			args.feature_replay_db_url,
			settings=AppSettings.load(),
			chain=args.feature_chain,
			token=args.feature_token,
		)
		output = render_feature_replay_summary(feature_summary, as_json=args.json)
		report_payload: Any = feature_summary
	elif args.baseline or args.candidate:
		if not args.baseline or not args.candidate:
			raise SystemExit("both --baseline and --candidate are required")
		regression_report = run_replay_regression(
			args.baseline,
			args.candidate,
			settings=AppSettings.load(),
			tokens=args.token,
			start_observed_at=args.start_observed_at,
			end_observed_at=args.end_observed_at,
		)
		output = render_replay_regression(regression_report, as_json=args.json)
		report_payload: ReplayRunReport | ReplayComparisonReport | ReplayRegressionReport = (
			regression_report
		)
	elif len(args.dataset_paths) > 1:
		comparison_report = run_replay_comparison(
			args.dataset_paths,
			settings=AppSettings.load(),
			tokens=args.token,
			start_observed_at=args.start_observed_at,
			end_observed_at=args.end_observed_at,
		)
		output = render_replay_comparison(comparison_report, as_json=args.json)
		report_payload = comparison_report
	elif len(args.dataset_paths) == 1:
		run_report = run_replay_report(
			args.dataset_paths[0],
			settings=AppSettings.load(),
			tokens=args.token,
			start_observed_at=args.start_observed_at,
			end_observed_at=args.end_observed_at,
		)
		output = render_replay_summary(run_report.summary, as_json=args.json)
		report_payload = run_report
	else:
		raise SystemExit("provide dataset paths or --baseline/--candidate")
	if args.output:
		write_replay_report(report_payload, args.output)
	print(output)
	return 0


if __name__ == "__main__":
	raise SystemExit(main())