from __future__ import annotations

from core.schemas import StateTransition, TokenSignal, TokenState


class StateEngine:
	def __init__(self, min_transition_interval_seconds: int = 60) -> None:
		self.min_transition_interval_seconds = min_transition_interval_seconds

	def transition(
		self,
		previous_state: TokenState | None,
		signal: TokenSignal,
		*,
		seconds_since_last_transition: int | None = None,
	) -> StateTransition:
		prior_state = previous_state or TokenState.UNKNOWN

		if (
			seconds_since_last_transition is not None
			and seconds_since_last_transition < self.min_transition_interval_seconds
		):
			return StateTransition(
				previous_state=prior_state,
				new_state=prior_state,
				changed=False,
				reasons=["transition_rate_limited"],
				timestamp=signal.timestamp,
			)

		next_state, reasons = _derive_state(prior_state, signal)
		return StateTransition(
			previous_state=prior_state,
			new_state=next_state,
			changed=next_state != prior_state,
			reasons=reasons,
			timestamp=signal.timestamp,
		)


def _derive_state(previous_state: TokenState, signal: TokenSignal) -> tuple[TokenState, list[str]]:
	liquidity_usd = float(signal.features.get("liquidity_usd", 0.0))
	volume_5m_usd = float(signal.features.get("volume_5m_usd", 0.0))
	buy_pressure = float(signal.features.get("buy_pressure", 0.0))
	wallet_outflow_score = float(signal.features.get("wallet_outflow_score", 0.0))
	estimated_slippage_bps = float(signal.features.get("estimated_slippage_bps", 0.0))
	onchain_feature_quality = float(signal.features.get("onchain_feature_quality", 0.5))
	cex_listing_confirmed = bool(signal.features.get("cex_listing_confirmed", False))
	market_structure = signal.sub_scores.get("market_structure", 0.0)
	wallet_behavior = signal.sub_scores.get("wallet_behavior", 0.0)

	if liquidity_usd < 5_000:
		return TokenState.PRE_LAUNCH, ["liquidity_below_threshold"]

	if cex_listing_confirmed:
		return TokenState.CEX_LISTING, ["cex_listing_confirmed"]

	if market_structure < 0.45 and (
		wallet_outflow_score > 0.6
		or buy_pressure < 0.35
		or estimated_slippage_bps >= 220
		or onchain_feature_quality < 0.4
	):
		return TokenState.DISTRIBUTION, [
			"market_structure_weakening",
			"execution_conditions_deteriorating",
		]

	if signal.state_candidate in {
		TokenState.PRE_LAUNCH,
		TokenState.EARLY_LIQUIDITY,
		TokenState.NARRATIVE_EXPLOSION,
	}:
		return signal.state_candidate, ["signal_state_candidate_respected"]

	if market_structure >= 0.74 and wallet_behavior > 0.55:
		return TokenState.NARRATIVE_EXPLOSION, [
			"market_structure_strong",
			"wallet_behavior_positive",
		]

	if market_structure > 0.45 and volume_5m_usd >= 25_000:
		return TokenState.EARLY_LIQUIDITY, ["volume_and_liquidity_established"]

	return previous_state, ["no_transition"]