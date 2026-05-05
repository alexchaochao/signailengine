from __future__ import annotations

from datetime import UTC, datetime

from core.schemas import (
	ActionType,
	ExecutionIntent,
	ExecutionReport,
	PortfolioSnapshot,
	PositionState,
	ReconciliationResult,
	RiskDecision,
	VenueType,
)


def reconcile_execution(
	position: PositionState,
	portfolio: PortfolioSnapshot,
	intent: ExecutionIntent | None,
	risk: RiskDecision,
	execution: ExecutionReport | None,
) -> ReconciliationResult:
	if intent is None or execution is None or not risk.allowed:
		return ReconciliationResult(
			intent_id=intent.intent_id if intent is not None else risk.intent_id,
			position=position,
			portfolio=portfolio,
			applied=False,
			reasons=["no_fill_to_reconcile"],
			timestamp=datetime.now(UTC),
		)

	updated_position = PositionState(**position.model_dump())
	updated_portfolio = PortfolioSnapshot(**portfolio.model_dump())

	portfolio_fraction = (
		execution.executed_notional_usd / updated_portfolio.total_portfolio_usd
		if updated_portfolio.total_portfolio_usd > 0
		else 0.0
	)

	if intent.action == ActionType.BUY:
		updated_position.is_open = True
		updated_position.venue_type = intent.venue_type
		updated_position.token_exposure += portfolio_fraction
		updated_portfolio.token_exposure += portfolio_fraction
		updated_portfolio.chain_exposure += portfolio_fraction
		updated_portfolio.open_positions += 1
	elif intent.action == ActionType.EXIT:
		updated_position.is_open = False
		updated_position.last_exit_timestamp = int(execution.timestamp.timestamp())
		updated_position.venue_type = VenueType.NO_TRADE
		updated_position.token_exposure = 0.0
		updated_portfolio.token_exposure = max(
			updated_portfolio.token_exposure - portfolio_fraction,
			0.0,
		)
		updated_portfolio.chain_exposure = max(
			updated_portfolio.chain_exposure - portfolio_fraction,
			0.0,
		)
		updated_portfolio.open_positions = max(updated_portfolio.open_positions - 1, 0)

	return ReconciliationResult(
		intent_id=intent.intent_id,
		position=updated_position,
		portfolio=updated_portfolio,
		applied=True,
		reasons=["execution_reconciled"],
		timestamp=datetime.now(UTC),
	)