from __future__ import annotations

import json
from datetime import UTC, datetime

from sqlalchemy import create_engine

from core.pipeline import PipelineResult
from core.router import RouteDecision
from core.schemas import (
    ActionType,
    ExecutionIntent,
    ExecutionLedgerEntry,
    ExecutionReport,
    FsmContext,
    PortfolioSnapshot,
    PositionState,
    ReconciliationResult,
    RiskDecision,
    StateTransition,
    TokenSignal,
    TokenState,
    VenueType,
)
from infra.postgres import count_rows, init_storage, persist_pipeline_result


def test_persist_pipeline_result_writes_rows() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)

    timestamp = datetime.now(UTC)
    intent = ExecutionIntent(
        intent_id="intent-1",
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
    result = PipelineResult(
        signal=TokenSignal(
            token="BONK",
            chain="solana",
            state_candidate=TokenState.NARRATIVE_EXPLOSION,
            sub_scores={"market_structure": 0.8},
            alpha_score=0.8,
            timestamp=int(timestamp.timestamp()),
        ),
        transition=StateTransition(
            previous_state=TokenState.UNKNOWN,
            new_state=TokenState.NARRATIVE_EXPLOSION,
            changed=True,
            reasons=["market_structure_strong"],
            timestamp=int(timestamp.timestamp()),
        ),
        route=RouteDecision(
            route="DEX_ENTRY",
            reasons=["dex_entry_conditions_met"],
            intent=intent,
        ),
        risk=RiskDecision(
            intent_id="intent-1",
            allowed=True,
            adjusted_notional_usd=250.0,
            timestamp=timestamp,
        ),
        execution=ExecutionReport(
            intent_id="intent-1",
            venue_type=VenueType.DEX,
            venue="solana_primary",
            status="FILLED",
            executed_notional_usd=250.0,
            message="paper_dex_execution",
            timestamp=timestamp,
        ),
        reconciliation=ReconciliationResult(
            intent_id="intent-1",
            position=PositionState(is_open=True, venue_type=VenueType.DEX, token_exposure=0.025),
            portfolio=PortfolioSnapshot(total_portfolio_usd=10_000.0, token_exposure=0.025),
            applied=True,
            reasons=["execution_reconciled"],
            timestamp=timestamp,
        ),
        execution_ledger=[
            ExecutionLedgerEntry(
                intent_id="intent-1",
                token="BONK",
                venue_type=VenueType.DEX,
                venue="solana_primary",
                stage="SUBMISSION",
                status="SUBMITTED",
                notional_usd=250.0,
                message="intent_created",
                timestamp=timestamp,
            ),
            ExecutionLedgerEntry(
                intent_id="intent-1",
                token="BONK",
                venue_type=VenueType.DEX,
                venue="solana_primary",
                stage="EXECUTION",
                status="FILLED",
                notional_usd=250.0,
                message="paper_dex_execution",
                timestamp=timestamp,
            ),
            ExecutionLedgerEntry(
                intent_id="intent-1",
                token="BONK",
                venue_type=VenueType.DEX,
                venue="solana_primary",
                stage="RECONCILIATION",
                status="RECONCILED",
                notional_usd=250.0,
                message="execution_reconciled",
                timestamp=timestamp,
            ),
        ],
    )

    persist_pipeline_result(engine, result)

    assert count_rows(engine, "token_signals") == 1
    assert count_rows(engine, "state_transitions") == 1
    assert count_rows(engine, "route_decisions") == 1
    assert count_rows(engine, "risk_decisions") == 1
    assert count_rows(engine, "execution_reports") == 1
    assert count_rows(engine, "reconciliation_results") == 1
    assert count_rows(engine, "orders") == 1
    assert count_rows(engine, "positions") == 1
    assert count_rows(engine, "portfolio_state") == 1
    assert count_rows(engine, "execution_ledger") == 3


def test_persist_pipeline_result_keeps_fsm_context_in_audit_payloads() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)

    timestamp = datetime.now(UTC)
    fsm_context = FsmContext(
        chain="solana",
        token="BONK",
        previous_state=TokenState.UNKNOWN,
        current_state=TokenState.NARRATIVE_EXPLOSION,
        changed=True,
        reasons=["market_structure_strong"],
        last_transition_timestamp=int(timestamp.timestamp()),
    )
    intent = ExecutionIntent(
        intent_id="intent-ctx-1",
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
    result = PipelineResult(
        signal=TokenSignal(
            token="BONK",
            chain="solana",
            state_candidate=TokenState.NARRATIVE_EXPLOSION,
            sub_scores={"market_structure": 0.8},
            alpha_score=0.8,
            timestamp=int(timestamp.timestamp()),
        ),
        transition=StateTransition(
            previous_state=TokenState.UNKNOWN,
            new_state=TokenState.NARRATIVE_EXPLOSION,
            changed=True,
            reasons=["market_structure_strong"],
            timestamp=int(timestamp.timestamp()),
        ),
        route=RouteDecision(
            route="DEX_ENTRY",
            reasons=["dex_entry_conditions_met"],
            intent=intent,
            fsm_context=fsm_context,
        ),
        risk=RiskDecision(
            intent_id="intent-ctx-1",
            allowed=True,
            adjusted_notional_usd=250.0,
            timestamp=timestamp,
            fsm_context=fsm_context,
        ),
        execution=ExecutionReport(
            intent_id="intent-ctx-1",
            venue_type=VenueType.DEX,
            venue="solana_primary",
            status="FILLED",
            executed_notional_usd=250.0,
            message="paper_dex_execution",
            timestamp=timestamp,
            fsm_context=fsm_context,
        ),
        reconciliation=ReconciliationResult(
            intent_id="intent-ctx-1",
            position=PositionState(is_open=True, venue_type=VenueType.DEX, token_exposure=0.025),
            portfolio=PortfolioSnapshot(total_portfolio_usd=10_000.0, token_exposure=0.025),
            applied=True,
            reasons=["execution_reconciled"],
            timestamp=timestamp,
            fsm_context=fsm_context,
        ),
        execution_ledger=[
            ExecutionLedgerEntry(
                intent_id="intent-ctx-1",
                token="BONK",
                venue_type=VenueType.DEX,
                venue="solana_primary",
                stage="SUBMISSION",
                status="SUBMITTED",
                notional_usd=250.0,
                message="intent_created",
                timestamp=timestamp,
                fsm_context=fsm_context,
            )
        ],
    )

    persist_pipeline_result(engine, result)

    with engine.connect() as connection:
        route_payload = json.loads(
            str(connection.exec_driver_sql("SELECT payload FROM route_decisions LIMIT 1").scalar())
        )
        risk_payload = json.loads(
            str(connection.exec_driver_sql("SELECT payload FROM risk_decisions LIMIT 1").scalar())
        )
        execution_payload = json.loads(
            str(connection.exec_driver_sql("SELECT payload FROM execution_reports LIMIT 1").scalar())
        )

    assert route_payload["fsm_context"]["current_state"] == "NARRATIVE_EXPLOSION"
    assert risk_payload["fsm_context"]["previous_state"] == "UNKNOWN"
    assert execution_payload["fsm_context"]["last_transition_timestamp"] == int(timestamp.timestamp())