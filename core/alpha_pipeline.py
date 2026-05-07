from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter
from logging import Logger, getLogger
from typing import Any

from redis import Redis
from sqlalchemy.engine import Engine

from core.config import AppSettings
from core.event_flow import build_and_publish_signal, publish_decision_bundle
from core.router import RouteDecision, Router
from core.schemas import (
    CrossDimensionSnapshot,
    EventEnvelope,
    ExecutionIntent,
    ExecutionLedgerEntry,
    ExecutionReport,
    FsmContext,
    PortfolioSnapshot,
    PositionState,
    ReconciliationResult,
    RiskDecision,
    SocialQueryRequest,
    StateTransition,
    TokenSignal,
    TokenState,
    VenueStatus,
)
from core.signal_engine import SignalEngine
from core.state_engine import StateEngine
from execution.base import ExecutionAdapter
from execution.factory import build_cex_adapter, build_dex_adapter
from execution.reconciliation import reconcile_execution
from infra.alerts import AlertManager
from infra.logging import get_logger
from infra.metrics import Metrics
from infra.postgres import init_storage
from infra.redis_stream import (
    acknowledge_message,
    ensure_consumer_group,
    publish_dead_letter,
    read_group_models,
)
from infra.repository import StorageRepository, CollectorCheckpoint
from portfolio.balance_provider import BalanceProvider
from portfolio.factory import build_balance_provider
from portfolio.risk_engine import RiskEngine

ALPHA_PIPELINE_GROUP = "alpha-pipeline"
ALPHA_PIPELINE_CONSUMER = "alpha-pipeline-1"

TERMINAL_ORDER_STATUSES = {"FILLED", "RECONCILED", "REJECTED"}


@dataclass
class AlphaPipelineResult:
    signal: TokenSignal
    transition: StateTransition
    route: RouteDecision
    risk: RiskDecision
    execution: ExecutionReport | None
    reconciliation: ReconciliationResult | None
    execution_ledger: list[ExecutionLedgerEntry]


class AlphaPipelineWorker:
    """Pipeline worker dedicated to cross-dimension snapshots.

    Runs as a separate process with its own consumer group on the
    raw-events stream.  Only processes alpha.cross_dimension_snapshot
    events.  Shares PositionState and PortfolioSnapshot with the
    existing PipelineWorker via the storage repository.

    This avoids state conflicts: the existing pipeline handles raw
    on-chain/wallet/social events, this pipeline handles discovery-born
    candidates that have been enriched with cross-dimension data.
    """

    def __init__(
        self,
        settings: AppSettings,
        redis_client: Redis,
        *,
        signal_engine: SignalEngine | None = None,
        state_engine: StateEngine | None = None,
        router: Router | None = None,
        risk_engine: RiskEngine | None = None,
        dex_executor: ExecutionAdapter | None = None,
        cex_executor: ExecutionAdapter | None = None,
        db_engine: Engine | None = None,
        metrics: Metrics | None = None,
        alert_manager: AlertManager | None = None,
        balance_provider: BalanceProvider | None = None,
    ) -> None:
        self.settings = settings
        self.redis_client = redis_client
        self.signal_engine = signal_engine or SignalEngine()
        self.state_engine = state_engine or StateEngine(
            min_transition_interval_seconds=settings.risk.min_transition_interval_seconds,
        )
        self.router = router or Router()
        self.risk_engine = risk_engine or RiskEngine()
        self.dex_executor = dex_executor or build_dex_adapter(settings)
        self.cex_executor = cex_executor or build_cex_adapter(settings)
        self.dex_executors: dict[str, ExecutionAdapter] = {
            settings.venues.dex_adapter: self.dex_executor,
        }
        self.db_engine = db_engine
        self.position_state = PositionState()
        self.portfolio_snapshot = PortfolioSnapshot()
        self.logger = get_logger("signalengine.alpha_pipeline")
        self.metrics = metrics or Metrics(settings.observability.service_namespace)
        self.alert_manager = alert_manager or AlertManager(self.metrics, self.logger)
        self.balance_provider = balance_provider or build_balance_provider(settings)
        self.repository = StorageRepository(db_engine) if db_engine is not None else None
        self.consecutive_adapter_failures = 0
        self.consecutive_risk_rejections = 0

    def ensure_streams(self, group_name: str = ALPHA_PIPELINE_GROUP) -> None:
        ensure_consumer_group(
            self.redis_client, self.settings.redis.raw_events_stream, group_name,
        )
        if self.db_engine is not None:
            init_storage(self.db_engine)
            if self.repository is not None:
                self.portfolio_snapshot = self.repository.state.load_portfolio()

    def poll_once(
        self,
        group_name: str = ALPHA_PIPELINE_GROUP,
        consumer_name: str = ALPHA_PIPELINE_CONSUMER,
        *,
        count: int = 10,
    ) -> list[AlphaPipelineResult]:
        messages = read_group_models(
            self.redis_client,
            self.settings.redis.raw_events_stream,
            group_name,
            consumer_name,
            EventEnvelope,
            count=count,
        )
        if not messages:
            return []

        results: list[AlphaPipelineResult] = []

        for message_id, event in messages:
            if event.event_type != "alpha.cross_dimension_snapshot":
                acknowledge_message(
                    self.redis_client,
                    self.settings.redis.raw_events_stream,
                    group_name,
                    message_id,
                )
                continue

            try:
                result = self._process_cross_dimension_event(event)
                results.append(result)
            except Exception as error:
                self.logger.exception(
                    "alpha_pipeline_processing_failed",
                    extra={
                        "service": "alpha_pipeline",
                        "token": event.token,
                        "chain": event.chain,
                        "event_id": event.event_id,
                    },
                )
                publish_dead_letter(
                    self.redis_client,
                    self.settings,
                    source_stream=self.settings.redis.raw_events_stream,
                    message_id=message_id,
                    kind=event.event_type,
                    payload=event.model_dump(mode="json"),
                    reason=str(error),
                )
            finally:
                acknowledge_message(
                    self.redis_client,
                    self.settings.redis.raw_events_stream,
                    group_name,
                    message_id,
                )

        return results

    def _process_cross_dimension_event(self, event: EventEnvelope) -> AlphaPipelineResult:
        started_at = perf_counter()
        self.metrics.mark_heartbeat(service="alpha_pipeline", mode="process_cross_dimension")

        snapshot = CrossDimensionSnapshot(**event.payload)
        token = snapshot.token
        chain = snapshot.chain

        if self.repository is not None:
            self.position_state = self.repository.state.load_position(token)
            self.portfolio_snapshot = self.repository.state.load_portfolio()

        self.logger.info(
            "alpha_pipeline_processing",
            extra={
                "token": token,
                "chain": chain,
                "alpha_type": snapshot.alpha_type,
                "service": "alpha_pipeline",
                "outcome": "started",
            },
        )
        self.metrics.events_ingested.labels(source=event.source).inc()

        # Build signal from the cross-dimension snapshot payload
        signal, _ = build_and_publish_signal(
            self.redis_client,
            self.settings,
            self.signal_engine,
            event,
        )
        previous_state, seconds_since_last_transition, last_transition_timestamp = (
            self._load_fsm_checkpoint(chain, token, signal.timestamp)
        )
        transition = self.state_engine.transition(
            previous_state,
            signal,
            seconds_since_last_transition=seconds_since_last_transition,
        )
        self._save_fsm_checkpoint(chain, token, transition, signal.timestamp)

        # Route
        route = self.router.route(
            signal,
            transition,
            self.position_state,
            VenueStatus(),
        )
        self._log_route_decision(route, token)

        if route.intent is None:
            self.metrics.pipeline_runs.labels(outcome="no_route").inc()
            return AlphaPipelineResult(
                signal=signal,
                transition=transition,
                route=route,
                risk=RiskDecision(
                    intent_id="",
                    allowed=True,
                    adjusted_notional_usd=0.0,
                    timestamp=datetime.now(UTC),
                ),
                execution=None,
                reconciliation=None,
                execution_ledger=[],
            )

        # Risk
        risk = self.risk_engine.evaluate(
            route.intent,
            self.position_state,
            self.portfolio_snapshot,
            fsm_context=FsmContext(
                chain=chain,
                token=token,
                previous_state=transition.previous_state,
                current_state=transition.new_state,
                changed=transition.changed,
                reasons=transition.reasons,
                last_transition_timestamp=last_transition_timestamp,
            ),
        )
        self._log_risk_decision(risk, token)

        if not risk.allowed:
            self.metrics.pipeline_runs.labels(outcome="risk_rejected").inc()
            return AlphaPipelineResult(
                signal=signal,
                transition=transition,
                route=route,
                risk=risk,
                execution=None,
                reconciliation=None,
                execution_ledger=[],
            )

        # Execute
        execution = self._execute_intent(route.intent, risk, recorded_at=datetime.now(UTC))
        reconciliation = None
        ledger_entries: list[ExecutionLedgerEntry] = []
        intent_id = route.intent.intent_id

        if execution is not None:
            reconciliation = reconcile_execution(
                self.position_state,
                self.portfolio_snapshot,
                route.intent,
                risk,
                execution,
            )
            if reconciliation is not None and reconciliation.applied:
                self.position_state = reconciliation.position
                self.portfolio_snapshot = reconciliation.portfolio
                if self.repository is not None:
                    self.repository.state.save_position(token, reconciliation.position)
                    self.repository.state.save_portfolio(reconciliation.portfolio)

        elapsed = perf_counter() - started_at
        self.metrics.decision_latency.labels(stage="alpha_pipeline_full").observe(elapsed)
        self.metrics.pipeline_runs.labels(outcome="executed").inc()

        return AlphaPipelineResult(
            signal=signal,
            transition=transition,
            route=route,
            risk=risk,
            execution=execution,
            reconciliation=reconciliation,
            execution_ledger=ledger_entries,
        )

    def _execute_intent(
        self,
        intent: ExecutionIntent,
        risk: RiskDecision,
        *,
        recorded_at: datetime | None = None,
    ) -> ExecutionReport | None:
        executor = self.dex_executors.get(
            intent.venue,
            self.dex_executor if intent.venue_type.value == "DEX" else self.cex_executor,
        )
        try:
            return executor.execute(intent, risk, recorded_at=recorded_at)
        except Exception as exc:
            self.consecutive_adapter_failures += 1
            self.alert_manager.raise_alert(
                severity="warning",
                title="execution_adapter_failure",
                message=str(exc),
                labels={"venue": intent.venue, "token": intent.token},
            )
            return None

    def _load_fsm_checkpoint(
        self,
        chain: str,
        token: str,
        signal_timestamp: int,
    ) -> tuple[TokenState | None, int | None, int | None]:
        if self.repository is None:
            return None, None, None
        checkpoint = self.repository.checkpoints.load(f"fsm_state:{chain}:{token}")
        if checkpoint is None:
            return None, None, None
        metadata = checkpoint.metadata
        previous_state = TokenState(metadata.get("previous_state", "UNKNOWN"))
        last_transition = metadata.get("last_transition_timestamp")
        seconds_since = signal_timestamp - last_transition if last_transition else None
        return previous_state, seconds_since, last_transition

    def _save_fsm_checkpoint(
        self,
        chain: str,
        token: str,
        transition: StateTransition,
        signal_timestamp: int,
    ) -> None:
        if self.repository is None:
            return
        self.repository.checkpoints.save(
            infra.repository.CollectorCheckpoint(
                checkpoint_key=f"fsm_state:{chain}:{token}",
                cursor=str(signal_timestamp),
                observed_at=datetime.fromtimestamp(signal_timestamp, UTC),
                metadata={
                    "previous_state": transition.previous_state.value,
                    "new_state": transition.new_state.value,
                    "changed": transition.changed,
                    "reasons": transition.reasons,
                    "last_transition_timestamp": signal_timestamp,
                },
            )
        )

    def _log_route_decision(self, route: RouteDecision, token: str) -> None:
        self.logger.info(
            "route_decision",
            extra={
                "service": "alpha_pipeline",
                "token": token,
                "route": route.route,
                "reasons": route.reasons,
            },
        )

    def _log_risk_decision(self, risk: RiskDecision, token: str) -> None:
        if not risk.allowed:
            self.consecutive_risk_rejections += 1
            self.logger.info(
                "risk_rejected",
                extra={
                    "service": "alpha_pipeline",
                    "token": token,
                    "intent_id": risk.intent_id,
                    "violations": risk.violations,
                },
            )
        else:
            self.consecutive_risk_rejections = 0

    def update_stream_backlog(self) -> None:
        stream_name = self.settings.redis.raw_events_stream
        try:
            info = self.redis_client.xinfo_stream(stream_name)
            groups = self.redis_client.xinfo_groups(stream_name)
            for group in groups:
                if group[b"name"].decode() == ALPHA_PIPELINE_GROUP:
                    pending = group.get(b"pending", 0)
                    lag = group.get(b"lag", 0)
                    self.metrics.redis_stream_backlog.labels(
                        stream=stream_name).set(pending + lag)
        except Exception:
            pass
