from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import pytest
from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings, ChainWalletCredentialsConfig
from core.pipeline import PipelineWorker
from core.schemas import (
    ActionType,
    EventEnvelope,
    ExecutionIntent,
    ExecutionLedgerEntry,
    ExecutionReport,
    PortfolioSnapshot,
    PositionState,
    PreparedExecution,
    StateTransition,
    TokenSignal,
    TokenState,
    VenueType,
)
from execution.base import ExecutionAdapter
from execution.dex_executor import DexPaperExecutor
from execution.solana_adapter import SolanaDexAdapter
from infra.postgres import count_rows
from infra.repository import StorageRepository
from portfolio.balance_provider import BalanceProvider, BalanceSnapshot
from portfolio.factory import build_balance_provider
from portfolio.price_provider import PriceProvider, PriceSnapshot
from sentinel.onchain_listener import build_onchain_event
from sentinel.wallet_tracker import build_wallet_event


class FakeRedis:
    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.groups: set[tuple[str, str]] = set()
        self.acked: list[tuple[str, str, str]] = []
        self.counter = 0

    def xadd(self, stream_name: str, mapping: dict[str, str]) -> str:
        self.counter += 1
        message_id = f"{self.counter}-0"
        self.streams.setdefault(stream_name, []).append((message_id, mapping))
        return message_id

    def xgroup_create(
        self,
        stream_name: str,
        group_name: str,
        id: str = "0-0",
        mkstream: bool = False,
    ) -> bool:
        _ = id
        if mkstream:
            self.streams.setdefault(stream_name, [])
        if (stream_name, group_name) in self.groups:
            raise Exception("BUSYGROUP Consumer Group name already exists")
        self.groups.add((stream_name, group_name))
        return True

    def xreadgroup(
        self,
        group_name: str,
        consumer_name: str,
        streams: dict[str, str],
        count: int = 100,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        _ = group_name, consumer_name, block
        stream_name = next(iter(streams))
        return [(stream_name, self.streams.get(stream_name, [])[:count])]

    def xack(self, stream_name: str, group_name: str, message_id: str) -> int:
        self.acked.append((stream_name, group_name, message_id))
        return 1


class RetryOnceExecutor(DexPaperExecutor):
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, prepared: PreparedExecution):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("transient_dex_error")
        return ExecutionReport(
            intent_id=prepared.intent.intent_id,
            venue_type=prepared.intent.venue_type,
            venue=prepared.intent.venue,
            adapter_name=self.adapter_name,
            external_order_id=f"dex-paper:{prepared.intent.intent_id}",
            quote_id=prepared.quote.quote_id,
            status="FILLED",
            executed_notional_usd=prepared.requested_notional_usd,
            message="paper_dex_execution",
            simulation=True,
            timestamp=datetime.now(UTC),
        )


class AlwaysFailExecutor(DexPaperExecutor):
    def execute(self, prepared: PreparedExecution):
        _ = prepared
        raise RuntimeError("dex_down")


class FakeBalanceProvider(BalanceProvider):
    def __init__(self, available_balance_usd: float | None) -> None:
        self.available_balance_usd = available_balance_usd

    def get_available_balance(self, intent: ExecutionIntent) -> BalanceSnapshot | None:
        if self.available_balance_usd is None:
            return None
        return BalanceSnapshot(
            available_balance_usd=self.available_balance_usd,
            account_address=f"wallet:{intent.token}",
            source="test",
            native_balance=0.0,
        )


class FakePriceProvider(PriceProvider):
    def __init__(self, price: float | None) -> None:
        self.price = price

    def get_native_token_price_usd(self, chain: str) -> PriceSnapshot | None:
        if self.price is None:
            return None
        return PriceSnapshot(
            asset=chain.upper(),
            quote_currency="USD",
            price=self.price,
            source="test_price",
        )




    def test_pipeline_worker_exits_open_position_on_onchain_distribution() -> None:
        settings = AppSettings.load()
        client = FakeRedis()
        engine = create_engine("sqlite:///:memory:")
        worker = PipelineWorker(settings, cast(Redis, client), db_engine=engine)
        worker.ensure_streams("signal-workers")
        repository = StorageRepository(engine)
        repository.state.save_position(
            "BONK",
            PositionState(
                is_open=True,
                venue_type=VenueType.DEX,
                token_exposure=0.025,
            ),
        )
        repository.state.save_portfolio(
            PortfolioSnapshot(
                total_portfolio_usd=10000.0,
                token_exposure=0.025,
                chain_exposure=0.025,
                open_positions=1,
                daily_pnl_fraction=0.0,
            )
        )

        result = worker.process_events(
            [
                build_onchain_event(
                    {
                        "token": "BONK",
                        "observed_at": datetime.now(UTC),
                        "liquidity_usd": 42_000,
                        "volume_5m_usd": 12_000,
                        "buy_pressure": 0.18,
                        "estimated_slippage_bps": 260,
                        "feature_quality": {
                            "buy_pressure": "stale",
                            "estimated_slippage_bps": "ok",
                        },
                    }
                )
            ]
        )

        assert result.transition.new_state == TokenState.DISTRIBUTION
        assert result.route.route == "DEX_EXIT"
        assert result.route.intent is not None
        assert result.route.intent.action == ActionType.EXIT
        assert result.risk.allowed is True
        assert result.execution is not None
        assert result.reconciliation is not None
        assert result.reconciliation.applied is True
        assert repository.state.load_position("BONK").is_open is False
        assert repository.state.load_portfolio().open_positions == 0
def test_pipeline_worker_builds_dex_adapter_from_settings() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "venues": AppSettings.load().venues.model_copy(
                update={
                    "dex_adapter": "solana_stub",
                    "paper_execution_enabled": False,
                }
            )
        }
    )

    worker = PipelineWorker(settings, cast(Redis, FakeRedis()))

    assert isinstance(worker.dex_executor, SolanaDexAdapter)


def test_pipeline_worker_dispatches_dex_execution_by_intent_venue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeEvmExecutor(ExecutionAdapter):
        adapter_name = "fake_evm"

        def __init__(self) -> None:
            self.prepared_intents: list[str] = []

        def quote(self, intent: ExecutionIntent, risk) -> PreparedExecution.quote.__class__:  # type: ignore[attr-defined]
            from core.schemas import ExecutionQuote

            self.prepared_intents.append(intent.intent_id)
            return ExecutionQuote(
                quote_id=f"quote:{intent.intent_id}",
                venue_type=intent.venue_type,
                venue=intent.venue,
                estimated_notional_usd=risk.adjusted_notional_usd,
                estimated_slippage_bps=intent.max_slippage_bps,
                timestamp=datetime.now(UTC),
            )

        def execute(self, prepared: PreparedExecution) -> ExecutionReport:
            return ExecutionReport(
                intent_id=prepared.intent.intent_id,
                venue_type=prepared.intent.venue_type,
                venue=prepared.intent.venue,
                adapter_name=self.adapter_name,
                external_order_id="evm:1",
                quote_id=prepared.quote.quote_id,
                status="FILLED",
                executed_notional_usd=prepared.requested_notional_usd,
                message="evm_execution",
                simulation=True,
                timestamp=datetime.now(UTC),
            )

    settings = AppSettings.load()
    worker = PipelineWorker(settings, cast(Redis, FakeRedis()))
    fake_executor = FakeEvmExecutor()

    def fake_build_dex_adapter(overridden_settings: AppSettings) -> ExecutionAdapter:
        if overridden_settings.venues.dex_adapter == "evm_primary":
            return fake_executor
        return DexPaperExecutor()

    monkeypatch.setattr("core.pipeline.build_dex_adapter", fake_build_dex_adapter)

    intent = ExecutionIntent(
        intent_id="intent-base-1",
        token="AERO",
        chain="base",
        venue_type=VenueType.DEX,
        venue="evm_primary",
        action=ActionType.BUY,
        confidence=0.8,
        target_notional_usd=100.0,
        max_slippage_bps=100,
        state=TokenState.NARRATIVE_EXPLOSION,
        strategy="dex_momentum_v1",
    )
    risk = type(
        "RiskDecisionLike",
        (),
        {"adjusted_notional_usd": 100.0},
    )()

    execution = worker._dispatch_execution(intent, risk)  # type: ignore[arg-type]

    assert fake_executor.prepared_intents == ["intent-base-1"]
    assert execution.venue == "evm_primary"
    assert execution.adapter_name == "fake_evm"


def test_pipeline_worker_processes_events_and_publishes_outputs() -> None:
    settings = AppSettings.load()
    client = FakeRedis()
    worker = PipelineWorker(settings, cast(Redis, client))
    worker.ensure_streams("signal-workers")

    observed_at = datetime.now(UTC)
    client.xadd(
        settings.redis.raw_events_stream,
        {
            "kind": "onchain.liquidity_snapshot",
            "payload": build_onchain_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "liquidity_usd": 180_000,
                    "volume_5m_usd": 60_000,
                    "buy_pressure": 0.82,
                    "estimated_slippage_bps": 90,
                }
            ).model_dump_json(),
        },
    )
    client.xadd(
        settings.redis.raw_events_stream,
        {
            "kind": "wallet.cluster_snapshot",
            "payload": build_wallet_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "wallet_inflow_score": 0.70,
                }
            ).model_dump_json(),
        },
    )

    results = worker.poll_once("signal-workers", "worker-1")

    assert len(results) == 1
    assert isinstance(results[0].signal, TokenSignal)
    assert isinstance(results[0].transition, StateTransition)
    assert isinstance(results[0].execution, ExecutionReport)
    assert all(isinstance(entry, ExecutionLedgerEntry) for entry in results[0].execution_ledger)
    assert [entry.status for entry in results[0].execution_ledger] == [
        "SUBMITTED",
        "FILLED",
        "RECONCILED",
    ]
    assert settings.redis.signals_stream in client.streams
    assert settings.redis.decisions_stream in client.streams
    assert settings.redis.executions_stream in client.streams
    assert len(client.acked) == 2


def test_pipeline_worker_skips_discovery_mode_social_snapshots() -> None:
    settings = AppSettings.load()
    client = FakeRedis()
    worker = PipelineWorker(settings, cast(Redis, client))
    worker.ensure_streams("signal-workers")

    observed_at = datetime.now(UTC)
    discovery_event = EventEnvelope(
        event_id="social-discovery-1",
        event_type="social.signal_snapshot",
        source="social_reddit",
        chain="unknown",
        token="BONK",
        observed_at=observed_at,
        ingested_at=observed_at,
        payload={
            "social_sentiment": 0.8,
            "social_velocity": 0.7,
            "retrieval_mode": "discovery",
        },
    )
    client.xadd(
        settings.redis.raw_events_stream,
        {
            "kind": discovery_event.event_type,
            "payload": discovery_event.model_dump_json(),
        },
    )

    results = worker.poll_once("signal-workers", "worker-1")

    assert results == []
    assert len(client.acked) == 1
    assert settings.redis.signals_stream not in client.streams


def test_pipeline_worker_retries_once_and_persists_attempts() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "execution": AppSettings.load().execution.model_copy(update={"max_retries": 1})
        }
    )
    client = FakeRedis()
    engine = create_engine("sqlite:///:memory:")
    executor = RetryOnceExecutor()
    worker = PipelineWorker(
        settings,
        cast(Redis, client),
        db_engine=engine,
        dex_executor=executor,
    )
    worker.ensure_streams("signal-workers")
    observed_at = datetime.now(UTC)
    events = [
        build_onchain_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "liquidity_usd": 180_000,
                "volume_5m_usd": 60_000,
                "buy_pressure": 0.82,
                "estimated_slippage_bps": 90,
            }
        ),
        build_wallet_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "wallet_inflow_score": 0.70,
            }
        ),
    ]

    result = worker.process_events(events)
    repository = StorageRepository(engine)

    assert executor.calls == 2
    assert result.route.intent is not None
    assert repository.orders.get_execution_attempts(result.route.intent.intent_id) == 2
    assert count_rows(engine, "execution_reports") == 1


def test_pipeline_worker_skips_duplicate_intents_when_replayed() -> None:
    settings = AppSettings.load()
    client = FakeRedis()
    engine = create_engine("sqlite:///:memory:")
    worker = PipelineWorker(settings, cast(Redis, client), db_engine=engine)
    worker.ensure_streams("signal-workers")
    observed_at = datetime.now(UTC)
    events = [
        build_onchain_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "liquidity_usd": 180_000,
                "volume_5m_usd": 60_000,
                "buy_pressure": 0.82,
                "estimated_slippage_bps": 90,
            }
        ),
        build_wallet_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "wallet_inflow_score": 0.70,
            }
        ),
    ]

    first = worker.process_events(events)
    second = worker.process_events(events)

    assert first.route.intent is not None
    assert second.reconciliation is not None
    assert second.reconciliation.reasons == ["duplicate_intent_skipped"]
    assert count_rows(engine, "execution_reports") == 1


def test_pipeline_worker_persists_fsm_state_across_runs() -> None:
    settings = AppSettings.load()
    client = FakeRedis()
    engine = create_engine("sqlite:///:memory:")
    worker = PipelineWorker(settings, cast(Redis, client), db_engine=engine)
    worker.ensure_streams("signal-workers")
    repository = StorageRepository(engine)
    observed_at = datetime.now(UTC)

    strong_signal = [
        build_onchain_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "liquidity_usd": 180_000,
                "volume_5m_usd": 60_000,
                "buy_pressure": 0.82,
                "estimated_slippage_bps": 90,
            }
        ),
        build_wallet_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "wallet_inflow_score": 0.70,
            }
        ),
    ]
    weak_signal = [
        build_onchain_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "liquidity_usd": 20_000,
                "volume_5m_usd": 8_000,
                "buy_pressure": 0.40,
                "estimated_slippage_bps": 110,
            }
        )
    ]

    first = worker.process_events(strong_signal)
    second = worker.process_events(weak_signal)
    checkpoint = repository.checkpoints.load("fsm_state:solana:BONK")

    assert first.transition.new_state == TokenState.NARRATIVE_EXPLOSION
    assert second.transition.previous_state == TokenState.NARRATIVE_EXPLOSION
    assert second.transition.new_state == TokenState.NARRATIVE_EXPLOSION
    assert checkpoint is not None
    assert checkpoint.cursor == TokenState.NARRATIVE_EXPLOSION.value
    assert checkpoint.metadata["last_transition_timestamp"] == first.transition.timestamp


def test_pipeline_worker_attaches_fsm_context_to_decision_and_audit_outputs() -> None:
    settings = AppSettings.load()
    client = FakeRedis()
    engine = create_engine("sqlite:///:memory:")
    worker = PipelineWorker(settings, cast(Redis, client), db_engine=engine)
    worker.ensure_streams("signal-workers")
    observed_at = datetime.now(UTC)

    result = worker.process_events(
        [
            build_onchain_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "liquidity_usd": 180_000,
                    "volume_5m_usd": 60_000,
                    "buy_pressure": 0.82,
                    "estimated_slippage_bps": 90,
                }
            ),
            build_wallet_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "wallet_inflow_score": 0.70,
                }
            ),
        ]
    )

    assert result.route.fsm_context is not None
    assert result.route.fsm_context.current_state == TokenState.NARRATIVE_EXPLOSION
    assert result.route.fsm_context.previous_state == TokenState.UNKNOWN
    assert result.risk.fsm_context is not None
    assert result.risk.fsm_context.last_transition_timestamp == result.transition.timestamp
    assert result.execution is not None
    assert result.execution.fsm_context is not None
    assert result.execution.fsm_context.current_state == result.transition.new_state
    assert result.reconciliation is not None
    assert result.reconciliation.fsm_context is not None
    assert all(entry.fsm_context is not None for entry in result.execution_ledger)
    assert all(
        entry.fsm_context is not None
        and entry.fsm_context.current_state == result.transition.new_state
        for entry in result.execution_ledger
    )


def test_pipeline_worker_emits_social_confirmation_requests_on_transition() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "social_sources": {
                    "x": {
                        "enabled": True,
                        "platform": "x",
                        "provider": "x_snapshot_json",
                        "source_name": "social_x",
                        "query_template": "${cashtag} OR ${token}",
                        "source_url": "https://social-bridge.example/x/search.json",
                    },
                    "reddit": {
                        "enabled": True,
                        "platform": "reddit",
                        "provider": "reddit_search_json",
                        "source_name": "social_reddit",
                        "query_template": "{token}",
                    },
                }
            }
        }
    )
    client = FakeRedis()
    engine = create_engine("sqlite:///:memory:")
    worker = PipelineWorker(settings, cast(Redis, client), db_engine=engine)
    worker.ensure_streams("signal-workers")

    worker.process_events(
        [
            build_onchain_event(
                {
                    "token": "BONK",
                    "observed_at": datetime.now(UTC),
                    "liquidity_usd": 180_000,
                    "volume_5m_usd": 60_000,
                    "buy_pressure": 0.82,
                    "estimated_slippage_bps": 90,
                }
            ),
            build_wallet_event(
                {
                    "token": "BONK",
                    "observed_at": datetime.now(UTC),
                    "wallet_inflow_score": 0.70,
                }
            ),
        ]
    )

    request_events = [
        EventEnvelope.model_validate_json(message[1]["payload"])
        for message in client.streams[settings.redis.raw_events_stream]
        if message[1]["kind"] == "social.query_requested"
    ]

    assert len(request_events) == 2
    assert {event.source for event in request_events} == {"social_x", "social_reddit"}
    assert all(event.payload["metadata"]["trigger"] == "fsm_transition" for event in request_events)
    assert all(event.payload["query"] is None for event in request_events)


def test_pipeline_worker_recovers_submitted_orders_on_startup() -> None:
    settings = AppSettings.load()
    client = FakeRedis()
    engine = create_engine("sqlite:///:memory:")
    worker = PipelineWorker(settings, cast(Redis, client), db_engine=engine)
    worker.ensure_streams("signal-workers")
    repository = StorageRepository(engine)
    intent = ExecutionIntent(
        intent_id="recoverable-intent",
        token="BONK",
        chain="solana",
        venue_type=VenueType.DEX,
        venue="solana_primary",
        action=ActionType.BUY,
        confidence=0.8,
        target_notional_usd=250.0,
        max_slippage_bps=100,
        state=TokenState.NARRATIVE_EXPLOSION,
        strategy="paper_test",
    )
    repository.orders.upsert_order(intent, 250.0, "SUBMITTED")
    repository.audit.append_execution_ledger(
        [
            ExecutionLedgerEntry(
                intent_id=intent.intent_id,
                token=intent.token,
                venue_type=intent.venue_type,
                venue=intent.venue,
                stage="SUBMISSION",
                status="SUBMITTED",
                notional_usd=250.0,
                message="intent_created",
                timestamp=datetime.now(UTC),
            )
        ]
    )

    recovered_worker = PipelineWorker(settings, cast(Redis, client), db_engine=engine)
    recovered = recovered_worker.recover_pending_executions()

    assert recovered == [intent.intent_id]
    assert repository.orders.get_order_status(intent.intent_id) == "RECONCILED"


def test_pipeline_worker_sends_failed_batches_to_dead_letter_stream() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "execution": AppSettings.load().execution.model_copy(update={"max_retries": 0})
        }
    )
    client = FakeRedis()
    engine = create_engine("sqlite:///:memory:")
    worker = PipelineWorker(
        settings,
        cast(Redis, client),
        db_engine=engine,
        dex_executor=AlwaysFailExecutor(),
    )
    worker.ensure_streams("signal-workers")
    observed_at = datetime.now(UTC)
    client.xadd(
        settings.redis.raw_events_stream,
        {
            "kind": "onchain.liquidity_snapshot",
            "payload": build_onchain_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "liquidity_usd": 180_000,
                    "volume_5m_usd": 60_000,
                    "buy_pressure": 0.82,
                    "estimated_slippage_bps": 90,
                }
            ).model_dump_json(),
        },
    )
    client.xadd(
        settings.redis.raw_events_stream,
        {
            "kind": "wallet.cluster_snapshot",
            "payload": build_wallet_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "wallet_inflow_score": 0.70,
                }
            ).model_dump_json(),
        },
    )

    worker.poll_once("signal-workers", "worker-1")

    assert settings.redis.dead_letter_stream in client.streams
    assert len(client.streams[settings.redis.dead_letter_stream]) == 2


def test_pipeline_worker_respects_global_kill_switch() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "live": AppSettings.load().live.model_copy(
                update={
                    "rollout": AppSettings.load().live.rollout.model_copy(
                        update={"global_kill_switch_enabled": True}
                    )
                }
            )
        }
    )
    worker = PipelineWorker(settings, cast(Redis, FakeRedis()))
    observed_at = datetime.now(UTC)

    result = worker.process_events(
        [
            build_onchain_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "liquidity_usd": 180_000,
                    "volume_5m_usd": 60_000,
                    "buy_pressure": 0.82,
                    "estimated_slippage_bps": 90,
                }
            ),
            build_wallet_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "wallet_inflow_score": 0.70,
                }
            ),
        ]
    )

    assert "global_kill_switch_enabled" in result.risk.violations
    assert result.execution is None


def test_pipeline_worker_requires_live_dex_credentials() -> None:
    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "risk": base_settings.risk.model_copy(update={"live_trading_enabled": True}),
            "venues": base_settings.venues.model_copy(update={"paper_execution_enabled": False}),
            "live": base_settings.live.model_copy(
                update={
                    "credentials": base_settings.live.credentials.model_copy(
                        update={
                            "chain_wallets": {},
                            "dex_providers": {},
                            "cex_providers": {},
                        }
                    )
                }
            ),
        }
    )
    worker = PipelineWorker(settings, cast(Redis, FakeRedis()))
    observed_at = datetime.now(UTC)

    result = worker.process_events(
        [
            build_onchain_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "liquidity_usd": 180_000,
                    "volume_5m_usd": 60_000,
                    "buy_pressure": 0.82,
                    "estimated_slippage_bps": 90,
                }
            ),
            build_wallet_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "wallet_inflow_score": 0.70,
                }
            ),
        ]
    )

    assert "live_dex_credentials_missing" in result.risk.violations
    assert result.execution is None


def test_pipeline_worker_caps_live_rollout_notional_and_applies_balance_buffer() -> None:
    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "risk": base_settings.risk.model_copy(
                update={
                    "live_trading_enabled": True,
                    "max_token_exposure": 1.0,
                    "max_chain_exposure": 1.0,
                }
            ),
            "live": base_settings.live.model_copy(
                update={
                    "rollout": base_settings.live.rollout.model_copy(
                        update={
                            "capped_notional_usd": 100.0,
                            "min_available_balance_usd": 500.0,
                        }
                    ),
                    "credentials": base_settings.live.credentials.model_copy(
                        update={
                            "chain_wallets": {
                                "solana": {
                                    "private_key": "secret",
                                    "wallet_address": "wallet",
                                }
                            },
                        }
                    ),
                }
            ),
        }
    )
    worker = PipelineWorker(
        settings,
        cast(Redis, FakeRedis()),
        balance_provider=FakeBalanceProvider(550.0),
    )
    observed_at = datetime.now(UTC)

    result = worker.process_events(
        [
            build_onchain_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "liquidity_usd": 180_000,
                    "volume_5m_usd": 60_000,
                    "buy_pressure": 0.82,
                    "estimated_slippage_bps": 90,
                }
            ),
            build_wallet_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "wallet_inflow_score": 0.70,
                }
            ),
        ]
    )

    assert result.risk.allowed is True
    assert result.risk.adjusted_notional_usd == 50.0
    assert "notional_capped_by_live_rollout" in result.risk.warnings
    assert "notional_reduced_by_balance_buffer" in result.risk.warnings


def test_pipeline_worker_rejects_live_trade_when_balance_provider_unavailable() -> None:
    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "risk": base_settings.risk.model_copy(update={"live_trading_enabled": True}),
            "live": base_settings.live.model_copy(
                update={
                    "credentials": base_settings.live.credentials.model_copy(
                        update={
                            "chain_wallets": {
                                "solana": {
                                    "private_key": "secret",
                                    "wallet_address": "wallet",
                                }
                            },
                        }
                    )
                }
            ),
        }
    )
    worker = PipelineWorker(
        settings,
        cast(Redis, FakeRedis()),
        balance_provider=FakeBalanceProvider(None),
    )
    observed_at = datetime.now(UTC)

    result = worker.process_events(
        [
            build_onchain_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "liquidity_usd": 180_000,
                    "volume_5m_usd": 60_000,
                    "buy_pressure": 0.82,
                    "estimated_slippage_bps": 90,
                }
            ),
            build_wallet_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "wallet_inflow_score": 0.70,
                }
            ),
        ]
    )

    assert "balance_provider_unavailable" in result.risk.violations
    assert result.execution is None


def test_solana_rpc_balance_provider_uses_real_price_provider() -> None:
    from execution.solana_rpc import SolanaBalanceState
    from portfolio.balance_provider import SolanaRpcBalanceProvider

    class FakeSolanaRpcClient:
        def wallet_balance(self, wallet_address: str) -> SolanaBalanceState:
            return SolanaBalanceState(lamports=2_000_000_000, wallet_address=wallet_address)

    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "live": base_settings.live.model_copy(
                update={
                    "credentials": base_settings.live.credentials.model_copy(
                        update={
                            "chain_wallets": {
                                "solana": ChainWalletCredentialsConfig(
                                    wallet_address="wallet"
                                )
                            }
                        }
                    )
                }
            ),
        }
    )
    provider = SolanaRpcBalanceProvider(
        settings,
        rpc_client=FakeSolanaRpcClient(),
        price_provider=FakePriceProvider(125.0),
    )

    snapshot = provider.get_available_balance(
        ExecutionIntent(
            intent_id="intent-balance",
            token="BONK",
            chain="solana",
            venue_type=VenueType.DEX,
            venue="solana_primary",
            action=ActionType.BUY,
            confidence=0.8,
            target_notional_usd=100.0,
            max_slippage_bps=100,
            state=TokenState.NARRATIVE_EXPLOSION,
            strategy="balance_test",
        )
    )

    assert snapshot is not None
    assert snapshot.available_balance_usd == 250.0
    assert snapshot.source == "solana_rpc+test_price"


def test_evm_rpc_balance_provider_uses_real_price_provider() -> None:
    from execution.evm_rpc import EvmBalanceState
    from portfolio.balance_provider import EvmRpcBalanceProvider

    class FakeEvmRpcClient:
        def wallet_balance(self, wallet_address: str) -> EvmBalanceState:
            return EvmBalanceState(
                wei_balance=2_000_000_000_000_000_000,
                wallet_address=wallet_address,
            )

    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "live": base_settings.live.model_copy(
                update={
                    "balance": base_settings.live.balance.model_copy(
                        update={
                            "native_asset_sources": {
                                "base": {"provider": "evm_rpc"}
                            }
                        }
                    ),
                    "credentials": base_settings.live.credentials.model_copy(
                        update={
                            "chain_wallets": {
                                "base": ChainWalletCredentialsConfig(
                                    wallet_address="0xwallet"
                                )
                            }
                        }
                    )
                }
            ),
        }
    )
    provider = EvmRpcBalanceProvider(
        settings,
        chain="base",
        rpc_client=FakeEvmRpcClient(),
        price_provider=FakePriceProvider(2500.0),
    )

    snapshot = provider.get_available_balance(
        ExecutionIntent(
            intent_id="intent-eth-balance",
            token="PEPE",
            chain="base",
            venue_type=VenueType.DEX,
            venue="evm_primary",
            action=ActionType.BUY,
            confidence=0.8,
            target_notional_usd=100.0,
            max_slippage_bps=100,
            state=TokenState.NARRATIVE_EXPLOSION,
            strategy="balance_test",
        )
    )

    assert snapshot is not None
    assert snapshot.available_balance_usd == 5000.0
    assert snapshot.source == "evm_rpc+test_price"


def test_routed_balance_provider_dispatches_by_chain_and_provider() -> None:
    from portfolio.balance_provider import RoutedBalanceProvider

    class FakeEthereumBalanceProvider(BalanceProvider):
        def get_available_balance(self, intent: ExecutionIntent) -> BalanceSnapshot | None:
            return BalanceSnapshot(
                available_balance_usd=640.0,
                account_address=f"wallet:{intent.chain}",
                source="evm_rpc",
                native_balance=0.25,
            )

    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "live": base_settings.live.model_copy(
                update={
                    "balance": base_settings.live.balance.model_copy(
                        update={
                            "native_asset_sources": {
                                "base": {"provider": "evm_rpc"}
                            }
                        }
                    )
                }
            ),
        }
    )
    provider = RoutedBalanceProvider(
        settings,
        providers={("base", "evm_rpc"): FakeEthereumBalanceProvider()},
    )

    snapshot = provider.get_available_balance(
        ExecutionIntent(
            intent_id="intent-eth-balance",
            token="PEPE",
            chain="base",
            venue_type=VenueType.DEX,
            venue="evm_primary",
            action=ActionType.BUY,
            confidence=0.8,
            target_notional_usd=100.0,
            max_slippage_bps=100,
            state=TokenState.NARRATIVE_EXPLOSION,
            strategy="balance_test",
        )
    )

    assert snapshot is not None
    assert snapshot.available_balance_usd == 640.0
    assert snapshot.source == "evm_rpc"


def test_routed_balance_provider_rejects_unsupported_provider() -> None:
    from portfolio.balance_provider import RoutedBalanceProvider

    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "live": base_settings.live.model_copy(
                update={
                    "balance": base_settings.live.balance.model_copy(
                        update={
                            "native_asset_sources": {
                                "base": {"provider": "missing_rpc"}
                            }
                        }
                    )
                }
            ),
        }
    )
    provider = RoutedBalanceProvider(settings, providers={})

    with pytest.raises(
        ValueError,
        match="unsupported_live_balance_provider:base:missing_rpc",
    ):
        provider.get_available_balance(
            ExecutionIntent(
                intent_id="intent-eth-unsupported",
                token="PEPE",
                chain="base",
                venue_type=VenueType.DEX,
                venue="evm_primary",
                action=ActionType.BUY,
                confidence=0.8,
                target_notional_usd=100.0,
                max_slippage_bps=100,
                state=TokenState.NARRATIVE_EXPLOSION,
                strategy="balance_test",
            )
        )


def test_balance_factory_builds_routed_provider_for_live_preflight() -> None:
    from portfolio.balance_provider import RoutedBalanceProvider

    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
        }
    )

    provider = build_balance_provider(settings)

    assert isinstance(provider, RoutedBalanceProvider)


def test_balance_factory_builds_generic_evm_route_for_configured_chain() -> None:
    from portfolio.balance_provider import RoutedBalanceProvider

    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "live": base_settings.live.model_copy(
                update={
                    "balance": base_settings.live.balance.model_copy(
                        update={
                            "native_asset_sources": {
                                "base": {"provider": "evm_rpc"}
                            }
                        }
                    )
                }
            ),
            "venues": base_settings.venues.model_copy(
                update={
                    "native_asset_rpc": {
                        "base": {
                            "url": "https://rpc.base.example",
                            "timeout_seconds": 4.0,
                            "max_retries": 1,
                        }
                    }
                }
            ),
        }
    )

    provider = build_balance_provider(settings)

    assert isinstance(provider, RoutedBalanceProvider)
    assert ("base", "evm_rpc") in provider.providers


def test_pipeline_worker_enforces_position_preflight() -> None:
    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "risk": base_settings.risk.model_copy(update={"live_trading_enabled": True}),
            "live": base_settings.live.model_copy(
                update={
                    "credentials": base_settings.live.credentials.model_copy(
                        update={
                            "chain_wallets": {
                                "solana": {
                                    "private_key": "secret",
                                    "wallet_address": "wallet",
                                }
                            },
                        }
                    )
                }
            ),
        }
    )
    worker = PipelineWorker(settings, cast(Redis, FakeRedis()))
    worker.position_state.is_open = True
    observed_at = datetime.now(UTC)

    result = worker.process_events(
        [
            build_onchain_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "liquidity_usd": 180_000,
                    "volume_5m_usd": 60_000,
                    "buy_pressure": 0.82,
                    "estimated_slippage_bps": 90,
                }
            ),
            build_wallet_event(
                {
                    "token": "BONK",
                    "observed_at": observed_at,
                    "wallet_inflow_score": 0.70,
                }
            ),
        ]
    )

    assert "position_preflight_open_position" in result.risk.violations