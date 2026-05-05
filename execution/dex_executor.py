from __future__ import annotations

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


class DexPaperExecutor(ExecutionAdapter):
	adapter_name = "solana_dex_paper"

	def quote(self, intent: ExecutionIntent, risk: RiskDecision) -> ExecutionQuote:
		return ExecutionQuote(
			quote_id=f"dex-quote:{intent.intent_id}",
			venue_type=VenueType.DEX,
			venue=intent.venue,
			estimated_notional_usd=risk.adjusted_notional_usd,
			estimated_slippage_bps=int(intent.max_slippage_bps),
			timestamp=datetime.now(UTC),
		)

	def execute(self, prepared: PreparedExecution) -> ExecutionReport:
		return ExecutionReport(
			intent_id=prepared.intent.intent_id,
			venue_type=VenueType.DEX,
			venue=prepared.intent.venue,
			adapter_name=self.adapter_name,
			external_order_id=f"dex-paper:{prepared.intent.intent_id}",
			quote_id=prepared.quote.quote_id,
			status="FILLED",
			executed_notional_usd=prepared.requested_notional_usd,
			message="paper_dex_execution",
			simulation=prepared.simulation,
			timestamp=datetime.now(UTC),
		)