from __future__ import annotations

from datetime import UTC, datetime

from core.schemas import (
    ActionType,
    ExecutionIntent,
    ExecutionReport,
    PortfolioSnapshot,
    PositionState,
    RiskDecision,
    TokenState,
    VenueType,
)
from execution.reconciliation import reconcile_execution


def test_reconcile_execution_updates_position_and_portfolio() -> None:
    intent = ExecutionIntent(
        intent_id="intent-1",
        token="BONK",
        chain="solana",
        venue_type=VenueType.DEX,
        venue="solana_primary",
        action=ActionType.BUY,
        confidence=0.8,
        target_notional_usd=500.0,
        max_slippage_bps=100,
        state=TokenState.NARRATIVE_EXPLOSION,
        strategy="paper_test",
    )
    risk = RiskDecision(
        intent_id="intent-1",
        allowed=True,
        adjusted_notional_usd=400.0,
        timestamp=datetime.now(UTC),
    )
    execution = ExecutionReport(
        intent_id="intent-1",
        venue_type=VenueType.DEX,
        venue="solana_primary",
        status="FILLED",
        executed_notional_usd=400.0,
        message="paper_dex_execution",
        timestamp=datetime.now(UTC),
    )

    result = reconcile_execution(
        PositionState(),
        PortfolioSnapshot(total_portfolio_usd=10_000.0),
        intent,
        risk,
        execution,
    )

    assert result.applied is True
    assert result.position.is_open is True
    assert result.portfolio.open_positions == 1
    assert result.portfolio.token_exposure > 0