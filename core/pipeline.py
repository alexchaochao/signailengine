from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter

from redis import Redis
from sqlalchemy.engine import Engine

from core.config import AcquisitionConfig, AppSettings
from core.event_flow import build_and_publish_signal, publish_decision_bundle, publish_raw_events
from core.router import RouteDecision, Router
from core.schemas import (
    CollectorCheckpoint,
    EventEnvelope,
    ExecutionIntent,
    ExecutionLedgerEntry,
    ExecutionReport,
    FsmContext,
    PortfolioSnapshot,
    PositionState,
    RawEventRecord,
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
from infra.repository import RecoverableIntent, StorageRepository
from portfolio.balance_provider import BalanceProvider
from portfolio.factory import build_balance_provider
from portfolio.risk_engine import RiskEngine
from sentinel.social_live_sources import build_social_query_requested_event

TERMINAL_ORDER_STATUSES = {"FILLED", "RECONCILED", "REJECTED"}
SOCIAL_CONFIRMATION_TRIGGER_STATES = {
    TokenState.PRE_LAUNCH,
    TokenState.EARLY_LIQUIDITY,
    TokenState.NARRATIVE_EXPLOSION,
}
# Only these event types are fed into signal building.  All others (discovery
# events, trade raw/quote, confirmation requests etc.) are acknowledged and
# discarded — they either follow a separate processing path or are irrelevant
# to the pipeline's signal → route → execute cycle.
SIGNAL_TRIGGER_EVENT_TYPES = frozenset({
    "onchain.liquidity_snapshot",
    "wallet.cluster_snapshot",
    "social.signal_snapshot",
})


@dataclass
class PipelineResult:
    signal: TokenSignal
    transition: StateTransition
    route: RouteDecision
    risk: RiskDecision
    execution: ExecutionReport | None
    reconciliation: ReconciliationResult | None
    execution_ledger: list[ExecutionLedgerEntry]


class PipelineWorker:
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
            settings.venues.dex_adapter: self.dex_executor
        }
        self.db_engine = db_engine
        self.position_state = PositionState()
        self.portfolio_snapshot = PortfolioSnapshot()
        self.logger = get_logger("signalengine.pipeline")
        self.metrics = metrics or Metrics(settings.observability.service_namespace)
        self.alert_manager = alert_manager or AlertManager(self.metrics, self.logger)
        self.balance_provider = balance_provider or build_balance_provider(settings)
        self.repository = StorageRepository(db_engine) if db_engine is not None else None
        self.consecutive_adapter_failures = 0
        self.consecutive_risk_rejections = 0

    def ensure_streams(self, group_name: str) -> None:
        ensure_consumer_group(self.redis_client, self.settings.redis.raw_events_stream, group_name)
        if self.db_engine is not None:
            init_storage(self.db_engine)
            if self.repository is not None:
                self.portfolio_snapshot = self.repository.state.load_portfolio()
                if self.settings.execution.recover_pending_on_startup:
                    self.recover_pending_executions()

    def recover_pending_executions(self) -> list[str]:
        if self.repository is None:
            return []

        recovered_intents: list[str] = []
        for recoverable in self.repository.load_recoverable_intents():
            self.position_state = self.repository.state.load_position(recoverable.intent.token)
            self.portfolio_snapshot = self.repository.state.load_portfolio()
            self.repository.audit.append_execution_ledger([_build_recovery_ledger_entry(recoverable)])

            if recoverable.status == "FILLED":
                execution = self.repository.audit.load_latest_execution_report(
                    recoverable.intent.intent_id
                )
            else:
                execution = self._execute_intent(
                    recoverable.intent,
                    _recovery_risk_decision(recoverable),
                    recorded_at=None,
                )

            if execution is None:
                continue

            reconciliation = reconcile_execution(
                self.position_state,
                self.portfolio_snapshot,
                recoverable.intent,
                _recovery_risk_decision(
                    recoverable,
                    executed_notional=execution.executed_notional_usd,
                ),
                execution,
            )
            if not reconciliation.applied:
                continue

            self.position_state = reconciliation.position
            self.portfolio_snapshot = reconciliation.portfolio
            self.repository.audit.save_reconciliation_result(reconciliation)
            self.repository.state.save_position(recoverable.intent.token, reconciliation.position)
            self.repository.state.save_portfolio(reconciliation.portfolio)
            self.repository.orders.mark_order_status(recoverable.intent.intent_id, "RECONCILED")
            self.repository.audit.append_execution_ledger(
                [
                    ExecutionLedgerEntry(
                        intent_id=reconciliation.intent_id,
                        token=recoverable.intent.token,
                        venue_type=recoverable.intent.venue_type,
                        venue=recoverable.intent.venue,
                        stage="RECONCILIATION",
                        status="RECONCILED",
                        notional_usd=execution.executed_notional_usd,
                        message="execution_reconciled",
                        timestamp=reconciliation.timestamp,
                    )
                ]
            )
            recovered_intents.append(recoverable.intent.intent_id)

        return recovered_intents

    def poll_once(
        self,
        group_name: str,
        consumer_name: str,
        *,
        count: int = 10,
    ) -> list[PipelineResult]:
        messages = read_group_models(
            self.redis_client,
            self.settings.redis.raw_events_stream,
            group_name,
            consumer_name,
            EventEnvelope,
            count=count,
        )
        if not messages:
            self._update_stream_backlog(self.settings.redis.raw_events_stream)
            return []

        results: list[PipelineResult] = []
        grouped: dict[str, list[tuple[str, EventEnvelope]]] = {}

        for message_id, event in messages:
            # Discard events that are not signal triggers.  Discovery events,
            # raw trades, quotes, analysis reports etc. are consumed by other
            # services (measurement bridge, social confirmation, etc.) and
            # should not enter the pipeline's signal → route → execute cycle.
            if event.event_type not in SIGNAL_TRIGGER_EVENT_TYPES:
                acknowledge_message(
                    self.redis_client,
                    self.settings.redis.raw_events_stream,
                    group_name,
                    message_id,
                )
                continue
            if event.event_type == "social.signal_snapshot" and str(
                event.payload.get("retrieval_mode", "")
            ).lower() == "discovery":
                acknowledge_message(
                    self.redis_client,
                    self.settings.redis.raw_events_stream,
                    group_name,
                    message_id,
                )
                continue
            grouped.setdefault(event.token, []).append((message_id, event))

        for token_messages in grouped.values():
            message_ids = [message_id for message_id, _ in token_messages]
            events = [event for _, event in token_messages]
            try:
                result = self.process_events(events)
                results.append(result)
            except Exception as error:  # noqa: BLE001
                for message_id, event in token_messages:
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
                for message_id in message_ids:
                    acknowledge_message(
                        self.redis_client,
                        self.settings.redis.raw_events_stream,
                        group_name,
                        message_id,
                    )

        return results

    def process_events(self, events: list[EventEnvelope]) -> PipelineResult:
        started_at = perf_counter()
        self.metrics.mark_heartbeat(service="pipeline", mode="process_events")
        token = events[0].token
        chain = events[0].chain
        self._check_event_lag(events)
        if self.repository is not None:
            self.position_state = self.repository.state.load_position(token)
            self.portfolio_snapshot = self.repository.state.load_portfolio()

        self.logger.info(
            "processing_events",
            extra={
                "token": token,
                "chain": events[0].chain,
                "service": "pipeline",
                "outcome": "started",
            },
        )
        for event in events:
            self.metrics.events_ingested.labels(source=event.source).inc()

        signal, _ = build_and_publish_signal(
            self.redis_client,
            self.settings,
            self.signal_engine,
            *events,
        )
        previous_state, seconds_since_last_transition, last_transition_timestamp = (
            self._load_fsm_checkpoint(chain, token, signal.timestamp)
        )
        transition = self.state_engine.transition(
            previous_state,
            signal,
            seconds_since_last_transition=seconds_since_last_transition,
        )
        fsm_context = _build_fsm_context(
            signal,
            transition,
            last_transition_timestamp=last_transition_timestamp,
        )
        route = self.router.route(signal, transition, self.position_state, VenueStatus()).model_copy(
            update={"fsm_context": fsm_context}
        )
        risk = self._evaluate_risk(signal, route).model_copy(update={"fsm_context": fsm_context})
        self._track_risk_alerts(signal, risk, route)

        duplicate_skipped = False
        if route.intent is not None and self.repository is not None:
            existing_status = self.repository.orders.get_order_status(route.intent.intent_id)
            if existing_status in TERMINAL_ORDER_STATUSES:
                duplicate_skipped = True

        execution: ExecutionReport | None = None
        reconciliation: ReconciliationResult | None = None
        execution_ledger: list[ExecutionLedgerEntry] = []

        if duplicate_skipped:
            reconciliation = ReconciliationResult(
                intent_id=route.intent.intent_id if route.intent is not None else risk.intent_id,
                position=self.position_state,
                portfolio=self.portfolio_snapshot,
                applied=False,
                reasons=["duplicate_intent_skipped"],
                timestamp=signal_timestamp(signal),
                fsm_context=fsm_context,
            )
        else:
            execution = self._execute(route, risk, signal)
            if execution is not None:
                execution = execution.model_copy(update={"fsm_context": fsm_context})
            reconciliation = reconcile_execution(
                self.position_state,
                self.portfolio_snapshot,
                route.intent,
                risk,
                execution,
            )
            reconciliation = reconciliation.model_copy(update={"fsm_context": fsm_context})
            if reconciliation.applied:
                self.position_state = reconciliation.position
                self.portfolio_snapshot = reconciliation.portfolio

            execution_ledger = _build_execution_ledger(
                signal,
                route,
                risk,
                execution,
                reconciliation,
                fsm_context=fsm_context,
            )

        publish_decision_bundle(self.redis_client, self.settings, transition, route, risk)
        self._emit_social_confirmation_requests(signal, transition, fsm_context)
        if execution is not None:
            self.redis_client.xadd(
                self.settings.redis.executions_stream,
                {
                    "kind": "execution_report",
                    "payload": execution.model_dump_json(),
                },
            )
            self._update_stream_backlog(self.settings.redis.raw_events_stream)
            self._update_stream_backlog(self.settings.redis.signals_stream)
            self._update_stream_backlog(self.settings.redis.decisions_stream)
            self._update_stream_backlog(self.settings.redis.executions_stream)

        result = PipelineResult(
            signal=signal,
            transition=transition,
            route=route,
            risk=risk,
            execution=execution,
            reconciliation=reconciliation,
            execution_ledger=execution_ledger,
        )

        if self.repository is not None and not duplicate_skipped:
            self.repository.persist_pipeline_result(result)
        if self.repository is not None:
            self._save_fsm_checkpoint(
                signal,
                transition,
                last_transition_timestamp=last_transition_timestamp,
            )

        elapsed = perf_counter() - started_at
        self.metrics.decision_latency.labels(stage="pipeline").observe(elapsed)
        if elapsed > self.settings.observability.max_pipeline_latency_seconds:
            self.alert_manager.emit(
                "pipeline_latency_threshold_exceeded",
                token=signal.token,
                chain=signal.chain,
                details={"outcome": f"latency_{elapsed:.6f}"},
            )
        self.metrics.pipeline_runs.labels(
            outcome="executed" if execution is not None else "no_fill"
        ).inc()
        if execution is not None:
            self.metrics.execution_reports.labels(
                venue=execution.venue_type.value,
                status=execution.status,
            ).inc()
        self.logger.info(
            "processed_events",
            extra={
                "token": signal.token,
                "chain": signal.chain,
                "service": "pipeline",
                "outcome": route.route,
                "event_id": events[0].event_id,
            },
        )

        return result

    def _emit_social_confirmation_requests(
        self,
        signal: TokenSignal,
        transition: StateTransition,
        fsm_context: FsmContext,
    ) -> None:
        if not transition.changed or transition.new_state not in SOCIAL_CONFIRMATION_TRIGGER_STATES:
            return

        acquisition = AcquisitionConfig.model_validate(self.settings.acquisition)
        for source_key, source_config in sorted(acquisition.social_sources.items()):
            if not source_config.enabled:
                continue

            source_name = source_config.source_name or f"social_{source_key}"
            request = SocialQueryRequest(
                request_id=(
                    f"fsm:{signal.chain}:{signal.token}:{transition.new_state.value}:"
                    f"{transition.timestamp}:{source_name}"
                ),
                source_name=source_name,
                platform=source_config.platform,
                chain=signal.chain,
                token=signal.token,
                requested_at=signal_timestamp(signal),
                candidate_id=f"social:{signal.chain}:{signal.token}",
                fsm_context=fsm_context,
                metadata={
                    "trigger": "fsm_transition",
                    "target_state": transition.new_state.value,
                    "transition_timestamp": transition.timestamp,
                },
            )
            event = build_social_query_requested_event(request, source_name=source_name)
            if self.repository is not None:
                existing = self.repository.raw_events.load(event.source, event.event_id)
                if existing is not None:
                    continue
                self.repository.raw_events.save(
                    RawEventRecord(
                        source_type="social_query_request",
                        source_name=event.source,
                        source_event_id=event.event_id,
                        chain=event.chain,
                        token=event.token,
                        observed_at=event.observed_at,
                        ingested_at=event.ingested_at,
                        payload=event.model_dump(mode="json"),
                    )
                )
            publish_raw_events(self.redis_client, self.settings, event)

    def _load_fsm_checkpoint(
        self,
        chain: str,
        token: str,
        signal_timestamp: int,
    ) -> tuple[TokenState | None, int | None, int | None]:
        if self.repository is None:
            return None, 120, None

        checkpoint = self.repository.checkpoints.load(self._fsm_checkpoint_key(chain, token))
        if checkpoint is None:
            return None, 120, None

        previous_state = _coerce_token_state(checkpoint.cursor)
        last_transition_timestamp = _coerce_int(checkpoint.metadata.get("last_transition_timestamp"))
        if last_transition_timestamp is None:
            return previous_state, 120, None

        seconds_since_last_transition = max(signal_timestamp - last_transition_timestamp, 0)
        return previous_state, seconds_since_last_transition, last_transition_timestamp

    def _save_fsm_checkpoint(
        self,
        signal: TokenSignal,
        transition: StateTransition,
        *,
        last_transition_timestamp: int | None,
    ) -> None:
        if self.repository is None:
            return

        persisted_transition_timestamp = (
            transition.timestamp if transition.changed else last_transition_timestamp
        )
        self.repository.checkpoints.save(
            CollectorCheckpoint(
                checkpoint_key=self._fsm_checkpoint_key(signal.chain, signal.token),
                cursor=transition.new_state.value,
                observed_at=datetime.fromtimestamp(signal.timestamp, UTC),
                metadata={
                    "state": transition.new_state.value,
                    "last_transition_timestamp": persisted_transition_timestamp,
                    "changed": transition.changed,
                    "reasons": transition.reasons,
                },
            )
        )

    @staticmethod
    def _fsm_checkpoint_key(chain: str, token: str) -> str:
        return f"fsm_state:{chain}:{token}"

    def _evaluate_risk(self, signal: TokenSignal, route: RouteDecision) -> RiskDecision:
        if route.intent is None:
            return RiskDecision(
                intent_id="none",
                allowed=False,
                adjusted_notional_usd=0.0,
                violations=["no_execution_intent"],
                warnings=[],
                timestamp=signal_timestamp(signal),
            )

        try:
            balance_snapshot = self.balance_provider.get_available_balance(route.intent)
        except Exception as error:  # noqa: BLE001
            self.alert_manager.emit(
                "balance_provider_error",
                token=route.intent.token,
                chain=route.intent.chain,
                details={"outcome": str(error)},
            )
            balance_snapshot = None

        return self.risk_engine.evaluate(
            self.settings,
            signal,
            route.intent,
            self.position_state,
            self.portfolio_snapshot,
            balance_snapshot,
        )

    def _execute(
        self,
        route: RouteDecision,
        risk: RiskDecision,
        signal: TokenSignal,
    ) -> ExecutionReport | None:
        if route.intent is None or not risk.allowed:
            return None

        if self.repository is not None:
            self._record_submission(route.intent, risk, signal_timestamp(signal))

        execution = self._execute_intent(route.intent, risk, recorded_at=signal_timestamp(signal))
        if execution is None:
            raise RuntimeError(f"execution_exhausted:{route.intent.intent_id}")
        return execution

    def _execute_intent(
        self,
        intent: ExecutionIntent,
        risk: RiskDecision,
        *,
        recorded_at,
    ) -> ExecutionReport | None:
        max_attempts = self.settings.execution.max_retries + 1
        current_attempts = (
            self.repository.orders.get_execution_attempts(intent.intent_id)
            if self.repository is not None
            else 0
        )

        for _ in range(current_attempts, max_attempts):
            if self.repository is not None:
                self.repository.orders.increment_execution_attempts(intent.intent_id)
            try:
                execution = self._dispatch_execution(intent, risk)
            except Exception as error:  # noqa: BLE001
                self.consecutive_adapter_failures += 1
                if (
                    self.consecutive_adapter_failures
                    == self.settings.observability.max_consecutive_adapter_failures
                ):
                    self.alert_manager.emit(
                        "adapter_failure_threshold_exceeded",
                        token=intent.token,
                        chain=intent.chain,
                        details={"outcome": str(error)},
                    )
                if self.repository is not None:
                    attempts = self.repository.orders.get_execution_attempts(intent.intent_id)
                    status = "REJECTED" if attempts >= max_attempts else "RETRY"
                    self.repository.orders.mark_order_status(intent.intent_id, status)
                    self.repository.audit.append_execution_ledger(
                        [
                            ExecutionLedgerEntry(
                                intent_id=intent.intent_id,
                                token=intent.token,
                                venue_type=intent.venue_type,
                                venue=intent.venue,
                                stage="EXECUTION",
                                status="RETRYABLE_ERROR",
                                notional_usd=risk.adjusted_notional_usd,
                                message=str(error),
                                timestamp=recorded_at or risk.timestamp,
                            )
                        ]
                    )
                continue

            self.consecutive_adapter_failures = 0
            if self.repository is not None:
                self.repository.orders.mark_order_status(intent.intent_id, execution.status)
                self.repository.audit.save_execution_report(execution)
            return execution

        return None

    def _dispatch_execution(self, intent: ExecutionIntent, risk: RiskDecision) -> ExecutionReport:
        adapter: ExecutionAdapter
        if intent.venue_type.value == "DEX":
            adapter = self._dex_adapter_for_intent(intent)
        elif intent.venue_type.value == "CEX":
            adapter = self.cex_executor
        else:
            raise ValueError(f"unsupported_venue_type:{intent.venue_type.value}")

        prepared = adapter.prepare(intent, risk)
        return adapter.execute(prepared)

    def _dex_adapter_for_intent(self, intent: ExecutionIntent) -> ExecutionAdapter:
        adapter = self.dex_executors.get(intent.venue)
        if adapter is not None:
            return adapter

        venue_settings = self.settings.venues.model_copy(update={"dex_adapter": intent.venue})
        adapter = build_dex_adapter(self.settings.model_copy(update={"venues": venue_settings}))
        self.dex_executors[intent.venue] = adapter
        return adapter

    def _record_submission(
        self,
        intent: ExecutionIntent,
        risk: RiskDecision,
        recorded_at,
    ) -> None:
        if self.repository is None:
            return

        existing_status = self.repository.orders.get_order_status(intent.intent_id)
        if existing_status in TERMINAL_ORDER_STATUSES:
            return

        execution_attempts = self.repository.orders.get_execution_attempts(intent.intent_id)
        self.repository.orders.upsert_order(
            intent,
            risk.adjusted_notional_usd,
            existing_status or "SUBMITTED",
            execution_attempts=execution_attempts,
        )
        self.repository.audit.append_execution_ledger(
            [
                ExecutionLedgerEntry(
                    intent_id=intent.intent_id,
                    token=intent.token,
                    venue_type=intent.venue_type,
                    venue=intent.venue,
                    stage="SUBMISSION",
                    status="SUBMITTED",
                    notional_usd=risk.adjusted_notional_usd,
                    message="intent_created",
                    timestamp=recorded_at,
                )
            ]
        )

    def _check_event_lag(self, events: list[EventEnvelope]) -> None:
        oldest_observed_at = min(event.observed_at for event in events)
        lag_seconds = (datetime.now(UTC) - oldest_observed_at).total_seconds()
        if lag_seconds > self.settings.observability.max_event_lag_seconds:
            self.alert_manager.emit(
                "event_lag_threshold_exceeded",
                token=events[0].token,
                chain=events[0].chain,
                details={"outcome": f"lag_{lag_seconds:.6f}"},
            )

    def _track_risk_alerts(
        self,
        signal: TokenSignal,
        risk: RiskDecision,
        route: RouteDecision,
    ) -> None:
        if route.intent is not None and not risk.allowed:
            self.consecutive_risk_rejections += 1
            if (
                self.consecutive_risk_rejections
                == self.settings.observability.max_risk_rejections
            ):
                self.alert_manager.emit(
                    "risk_rejection_threshold_exceeded",
                    token=signal.token,
                    chain=signal.chain,
                    details={"outcome": ",".join(risk.violations) or "risk_rejected"},
                )
            return

        self.consecutive_risk_rejections = 0

    def _update_stream_backlog(self, stream_name: str) -> None:
        xlen = getattr(self.redis_client, "xlen", None)
        if not callable(xlen):
            return
        try:
            backlog = int(xlen(stream_name))
        except Exception:  # noqa: BLE001
            return
        self.metrics.redis_stream_backlog.labels(stream=stream_name).set(float(backlog))


def signal_timestamp(signal: TokenSignal):
    from datetime import UTC, datetime

    return datetime.fromtimestamp(signal.timestamp, tz=UTC)


def _coerce_token_state(value: object) -> TokenState | None:
    if not isinstance(value, str):
        return None

    try:
        return TokenState(value)
    except ValueError:
        return None


def _coerce_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value:
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _build_execution_ledger(
    signal: TokenSignal,
    route: RouteDecision,
    risk: RiskDecision,
    execution: ExecutionReport | None,
    reconciliation: ReconciliationResult | None,
    *,
    fsm_context: FsmContext,
) -> list[ExecutionLedgerEntry]:
    if route.intent is None:
        return []

    ledger: list[ExecutionLedgerEntry] = [
        ExecutionLedgerEntry(
            intent_id=route.intent.intent_id,
            token=signal.token,
            venue_type=route.intent.venue_type,
            venue=route.intent.venue,
            stage="SUBMISSION",
            status="SUBMITTED",
            notional_usd=risk.adjusted_notional_usd,
            message="intent_created",
            timestamp=signal_timestamp(signal),
            fsm_context=fsm_context,
        )
    ]

    if not risk.allowed:
        ledger.append(
            ExecutionLedgerEntry(
                intent_id=route.intent.intent_id,
                token=signal.token,
                venue_type=route.intent.venue_type,
                venue=route.intent.venue,
                stage="RISK",
                status="REJECTED",
                notional_usd=0.0,
                message=",".join(risk.violations) or "risk_rejected",
                timestamp=risk.timestamp,
                fsm_context=fsm_context,
            )
        )
        return ledger

    if execution is not None:
        ledger.append(
            ExecutionLedgerEntry(
                intent_id=execution.intent_id,
                token=signal.token,
                venue_type=execution.venue_type,
                venue=execution.venue,
                stage="EXECUTION",
                status=execution.status,
                notional_usd=execution.executed_notional_usd,
                message=execution.message,
                timestamp=execution.timestamp,
                fsm_context=fsm_context,
            )
        )

    if reconciliation is not None and reconciliation.applied:
        ledger.append(
            ExecutionLedgerEntry(
                intent_id=reconciliation.intent_id,
                token=signal.token,
                venue_type=route.intent.venue_type,
                venue=route.intent.venue,
                stage="RECONCILIATION",
                status="RECONCILED",
                notional_usd=risk.adjusted_notional_usd,
                message="execution_reconciled",
                timestamp=reconciliation.timestamp,
                fsm_context=fsm_context,
            )
        )

    return ledger


def _build_fsm_context(
    signal: TokenSignal,
    transition: StateTransition,
    *,
    last_transition_timestamp: int | None,
) -> FsmContext:
    effective_last_transition_timestamp = (
        transition.timestamp if transition.changed else last_transition_timestamp
    )
    return FsmContext(
        chain=signal.chain,
        token=signal.token,
        previous_state=transition.previous_state,
        current_state=transition.new_state,
        changed=transition.changed,
        reasons=list(transition.reasons),
        last_transition_timestamp=effective_last_transition_timestamp,
    )


def _build_recovery_ledger_entry(recoverable: RecoverableIntent) -> ExecutionLedgerEntry:
    from datetime import UTC, datetime

    return ExecutionLedgerEntry(
        intent_id=recoverable.intent.intent_id,
        token=recoverable.intent.token,
        venue_type=recoverable.intent.venue_type,
        venue=recoverable.intent.venue,
        stage="RECOVERY",
        status="RECOVERED",
        notional_usd=recoverable.adjusted_notional_usd,
        message=f"recovered_{recoverable.status.lower()}",
        timestamp=datetime.now(UTC),
    )


def _recovery_risk_decision(
    recoverable: RecoverableIntent,
    *,
    executed_notional: float | None = None,
) -> RiskDecision:
    from datetime import UTC, datetime

    return RiskDecision(
        intent_id=recoverable.intent.intent_id,
        allowed=True,
        adjusted_notional_usd=(
            recoverable.adjusted_notional_usd
            if executed_notional is None
            else executed_notional
        ),
        warnings=["recovered_pending_intent"],
        timestamp=datetime.now(UTC),
    )
