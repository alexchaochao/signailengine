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

# Paper‑mode parameters — deterministic seed for reproducibility,
# configurable slippage/pfill/failure rates.
_PAPER_SEED = 42
_FAILURE_RATE = 0.05       # 5 % of orders fail (e.g. network / slippage)
_PARTIAL_FILL_RATE = 0.15  # 15 % of orders fill partially
_PARTIAL_FILL_PCT = 0.6    # partial fills get 60 % of requested notional
_SLIPPAGE_VOLATILITY = 0.3 # random ±30 % around estimated slippage


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
		rng = random.Random(_PAPER_SEED ^ hash(prepared.intent.intent_id))
		requested = prepared.requested_notional_usd
		base_slippage = prepared.intent.max_slippage_bps
		# Apply random slippage volatility
		actual_slippage = int(base_slippage * (1.0 + rng.uniform(-_SLIPPAGE_VOLATILITY, _SLIPPAGE_VOLATILITY)))
		actual_slippage = max(actual_slippage, 0)

		# Determine fill status
		roll = rng.random()
		if roll < _FAILURE_RATE:
			return ExecutionReport(
				intent_id=prepared.intent.intent_id,
				venue_type=VenueType.DEX,
				venue=prepared.intent.venue,
				adapter_name=self.adapter_name,
				external_order_id=f"dex-paper:{prepared.intent.intent_id}",
				quote_id=prepared.quote.quote_id,
				status="FAILED",
				executed_notional_usd=0.0,
				message=f"paper_slippage_exceeded_{actual_slippage}bps",
				simulation=prepared.simulation,
				timestamp=datetime.now(UTC),
			)

		if roll < _FAILURE_RATE + _PARTIAL_FILL_RATE:
			executed = round(requested * _PARTIAL_FILL_PCT * (1.0 - actual_slippage / 10_000.0), 2)
			return ExecutionReport(
				intent_id=prepared.intent.intent_id,
				venue_type=VenueType.DEX,
				venue=prepared.intent.venue,
				adapter_name=self.adapter_name,
				external_order_id=f"dex-paper:{prepared.intent.intent_id}",
				quote_id=prepared.quote.quote_id,
				status="PARTIAL_FILL",
				executed_notional_usd=executed,
				message=f"paper_partial_fill_slippage_{actual_slippage}bps",
				simulation=prepared.simulation,
				timestamp=datetime.now(UTC),
			)

		executed = round(requested * (1.0 - actual_slippage / 10_000.0), 2)
		return ExecutionReport(
			intent_id=prepared.intent.intent_id,
			venue_type=VenueType.DEX,
			venue=prepared.intent.venue,
			adapter_name=self.adapter_name,
			external_order_id=f"dex-paper:{prepared.intent.intent_id}",
			quote_id=prepared.quote.quote_id,
			status="FILLED",
			executed_notional_usd=executed,
			message=f"paper_dex_execution_slippage_{actual_slippage}bps",
			simulation=prepared.simulation,
			timestamp=datetime.now(UTC),
		)