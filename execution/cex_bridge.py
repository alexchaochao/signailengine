from __future__ import annotations

import random
from datetime import UTC, datetime

from core.schemas import (
	ExecutionIntent,
	ExecutionQuote,
	ExecutionReport,
	PreparedExecution,
	RiskDecision,
	VenueType,
)
from execution.base import ExecutionAdapter

# CEX paper sim params — CEX typically has lower slippage / failure rates
_CEX_FAILURE_RATE = 0.02
_CEX_PARTIAL_FILL_RATE = 0.08
_CEX_PARTIAL_FILL_PCT = 0.85
_CEX_SLIPPAGE_VOLATILITY = 0.15


class CexPaperExecutor(ExecutionAdapter):
	adapter_name = "binance_cex_paper"

	def quote(self, intent: ExecutionIntent, risk: RiskDecision) -> ExecutionQuote:
		return ExecutionQuote(
			quote_id=f"cex-quote:{intent.intent_id}",
			venue_type=VenueType.CEX,
			venue=intent.venue,
			estimated_notional_usd=risk.adjusted_notional_usd,
			estimated_slippage_bps=int(intent.max_slippage_bps),
			timestamp=datetime.now(UTC),
		)

	def execute(self, prepared: PreparedExecution) -> ExecutionReport:
		rng = random.Random(42 ^ hash(prepared.intent.intent_id))
		requested = prepared.requested_notional_usd
		base_slippage = prepared.intent.max_slippage_bps
		actual_slippage = int(base_slippage * (1.0 + rng.uniform(-_CEX_SLIPPAGE_VOLATILITY, _CEX_SLIPPAGE_VOLATILITY)))
		actual_slippage = max(actual_slippage, 0)

		roll = rng.random()
		if roll < _CEX_FAILURE_RATE:
			return ExecutionReport(
				intent_id=prepared.intent.intent_id,
				venue_type=VenueType.CEX,
				venue=prepared.intent.venue,
				adapter_name=self.adapter_name,
				external_order_id=f"cex-paper:{prepared.intent.intent_id}",
				quote_id=prepared.quote.quote_id,
				status="FAILED",
				executed_notional_usd=0.0,
				message=f"cex_paper_slippage_exceeded_{actual_slippage}bps",
				simulation=prepared.simulation,
				timestamp=datetime.now(UTC),
			)

		if roll < _CEX_FAILURE_RATE + _CEX_PARTIAL_FILL_RATE:
			executed = round(requested * _CEX_PARTIAL_FILL_PCT * (1.0 - actual_slippage / 10_000.0), 2)
			return ExecutionReport(
				intent_id=prepared.intent.intent_id,
				venue_type=VenueType.CEX,
				venue=prepared.intent.venue,
				adapter_name=self.adapter_name,
				external_order_id=f"cex-paper:{prepared.intent.intent_id}",
				quote_id=prepared.quote.quote_id,
				status="PARTIAL_FILL",
				executed_notional_usd=executed,
				message=f"cex_paper_partial_fill_slippage_{actual_slippage}bps",
				simulation=prepared.simulation,
				timestamp=datetime.now(UTC),
			)

		executed = round(requested * (1.0 - actual_slippage / 10_000.0), 2)
		return ExecutionReport(
			intent_id=prepared.intent.intent_id,
			venue_type=VenueType.CEX,
			venue=prepared.intent.venue,
			adapter_name=self.adapter_name,
			external_order_id=f"cex-paper:{prepared.intent.intent_id}",
			quote_id=prepared.quote.quote_id,
			status="FILLED",
			executed_notional_usd=executed,
			message=f"cex_paper_execution_slippage_{actual_slippage}bps",
			simulation=prepared.simulation,
			timestamp=datetime.now(UTC),
		)