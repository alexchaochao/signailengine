from __future__ import annotations

from math import log10

from core.schemas import EventEnvelope, TokenSignal, TokenState


class SignalEngine:
	def build_signal(self, *events: EventEnvelope) -> TokenSignal:
		payloads = self._merge_payloads(events)

		liquidity_usd = _as_float(payloads.get("liquidity_usd", 0.0))
		volume_5m_usd = _as_float(payloads.get("volume_5m_usd", 0.0))
		buy_pressure = _bounded(_as_float(payloads.get("buy_pressure", 0.0)))
		holder_growth_15m = _bounded(_as_float(payloads.get("holder_growth_15m", 0.0)))
		wallet_inflow_score = _bounded(_as_float(payloads.get("wallet_inflow_score", 0.0)))
		wallet_outflow_score = _bounded(_as_float(payloads.get("wallet_outflow_score", 0.0)))
		social_sentiment = _bounded(_as_float(payloads.get("social_sentiment", 0.0)))
		social_velocity = _bounded(_as_float(payloads.get("social_velocity", 0.0)))
		cex_rumor_score = _bounded(_as_float(payloads.get("cex_rumor_score", 0.0)))
		cex_listing_confirmed = bool(payloads.get("cex_listing_confirmed", False))
		estimated_slippage_bps = _as_float(payloads.get("estimated_slippage_bps", 0.0))
		launch_alpha_score = _bounded(_as_float(payloads.get("launch_alpha_score", 0.0)))
		launch_candidate_status = str(payloads.get("launch_candidate_status", "")).upper()
		catalyst_alpha_score = _bounded(_as_float(payloads.get("catalyst_alpha_score", 0.0)))
		catalyst_credibility_score = _bounded(
			_as_float(payloads.get("catalyst_credibility_score", 0.0))
		)
		flow_alpha_score = _bounded(_as_float(payloads.get("flow_alpha_score", 0.0)))
		flow_candidate_status = str(payloads.get("flow_candidate_status", "")).upper()
		feature_quality = _as_feature_quality(payloads.get("feature_quality"))
		quality_score = _feature_quality_score(feature_quality)
		wallet_behavior = _bounded(
			max(wallet_inflow_score, launch_alpha_score * 0.85, flow_alpha_score * 0.8)
		)
		launch_support = _bounded(launch_alpha_score * 0.7 + wallet_behavior * 0.3)
		catalyst_support = _bounded(
			catalyst_alpha_score * 0.75 + catalyst_credibility_score * 0.25
		)
		flow_support = _bounded(
			flow_alpha_score * 0.65 + wallet_inflow_score * 0.25 + (1 - wallet_outflow_score) * 0.1
		)

		market_structure = _bounded(
			_normalize_notional(liquidity_usd) * 0.4
			+ _normalize_notional(volume_5m_usd) * 0.3
			+ buy_pressure * 0.2
			+ holder_growth_15m * 0.1
		)
		social_momentum = _bounded(social_sentiment * 0.6 + social_velocity * 0.4)
		execution_readiness = _bounded(
			_normalize_notional(liquidity_usd) * 0.6
			+ buy_pressure * 0.2
			+ (1 - min(estimated_slippage_bps / 300, 1)) * 0.15
			+ quality_score * 0.05
		)

		alpha_score = _bounded(
			market_structure * 0.33
			+ wallet_behavior * 0.25
			+ social_momentum * 0.08
			+ execution_readiness * 0.2
			+ launch_support * 0.06
			+ catalyst_support * 0.04
			+ flow_support * 0.04
			+ quality_score * 0.05
		)

		state_candidate, reasons = _classify_state(
			liquidity_usd=liquidity_usd,
			market_structure=market_structure,
			wallet_behavior=wallet_behavior,
			cex_listed=cex_listing_confirmed,
			volume_5m_usd=volume_5m_usd,
			buy_pressure=buy_pressure,
			estimated_slippage_bps=estimated_slippage_bps,
			feature_quality_score=quality_score,
			launch_alpha_score=launch_alpha_score,
			launch_candidate_status=launch_candidate_status,
			catalyst_alpha_score=catalyst_alpha_score,
			catalyst_credibility_score=catalyst_credibility_score,
			flow_alpha_score=flow_alpha_score,
			flow_candidate_status=flow_candidate_status,
		)

		reasons.extend(
			_score_reasons(
				market_structure,
				wallet_behavior,
				social_momentum,
				execution_readiness,
				feature_quality,
			)
		)

		return TokenSignal(
			token=str(payloads["token"]),
			chain=str(payloads.get("chain", "solana")),
			state_candidate=state_candidate,
			features={
				"liquidity_usd": liquidity_usd,
				"volume_5m_usd": volume_5m_usd,
				"buy_pressure": buy_pressure,
				"holder_growth_15m": holder_growth_15m,
				"wallet_inflow_score": wallet_inflow_score,
				"wallet_outflow_score": wallet_outflow_score,
				"social_sentiment": social_sentiment,
				"social_velocity": social_velocity,
				"cex_rumor_score": cex_rumor_score,
				"cex_listing_confirmed": cex_listing_confirmed,
				"estimated_slippage_bps": estimated_slippage_bps,
				"onchain_feature_quality": quality_score,
			},
			sub_scores={
				"market_structure": market_structure,
				"wallet_behavior": wallet_behavior,
				"social_momentum": social_momentum,
				"execution_readiness": execution_readiness,
			},
			alpha_score=alpha_score,
			reasons=_dedupe(reasons),
			timestamp=int(max(event.observed_at for event in events).timestamp()),
		)

	def _merge_payloads(self, events: tuple[EventEnvelope, ...]) -> dict[str, object]:
		if not events:
			raise ValueError("At least one event is required to build a signal")

		merged: dict[str, object] = {}

		for event in events:
			merged.setdefault("token", event.token)
			merged.setdefault("chain", event.chain)
			merged.update(event.payload)

		if "token" not in merged:
			raise ValueError("Merged payload must include token")

		return merged


def _classify_state(
	*,
	liquidity_usd: float,
	market_structure: float,
	wallet_behavior: float,
	cex_listed: bool,
	volume_5m_usd: float,
	buy_pressure: float,
	estimated_slippage_bps: float,
	feature_quality_score: float,
	launch_alpha_score: float,
	launch_candidate_status: str,
	catalyst_alpha_score: float,
	catalyst_credibility_score: float,
	flow_alpha_score: float,
	flow_candidate_status: str,
) -> tuple[TokenState, list[str]]:
	if liquidity_usd < 5_000:
		return TokenState.PRE_LAUNCH, ["liquidity_below_threshold"]

	if cex_listed:
		return TokenState.CEX_LISTING, ["cex_listing_confirmed"]

	if catalyst_alpha_score >= 0.72 and catalyst_credibility_score >= 0.6:
		return TokenState.NARRATIVE_EXPLOSION, [
			"catalyst_alpha_confirmed",
			"catalyst_credibility_sufficient",
		]

	if flow_candidate_status == "QUALIFIED" and flow_alpha_score >= 0.72:
		if volume_5m_usd >= 30_000:
			return TokenState.NARRATIVE_EXPLOSION, [
				"flow_alpha_candidate_qualified",
				"flow_rotation_accelerating",
			]
		return TokenState.EARLY_LIQUIDITY, [
			"flow_alpha_candidate_qualified",
			"flow_accumulation_building",
		]

	if launch_candidate_status == "QUALIFIED" and launch_alpha_score >= 0.7:
		if liquidity_usd >= 25_000 or volume_5m_usd >= 5_000:
			return TokenState.EARLY_LIQUIDITY, [
				"launch_alpha_candidate_qualified",
				"launch_flow_confirmed",
			]
		return TokenState.PRE_LAUNCH, [
			"launch_alpha_candidate_qualified",
			"launch_liquidity_forming",
		]

	if _is_distribution_phase(
		market_structure=market_structure,
		buy_pressure=buy_pressure,
		estimated_slippage_bps=estimated_slippage_bps,
		feature_quality_score=feature_quality_score,
	):
		return TokenState.DISTRIBUTION, [
			"market_structure_weakening",
			"execution_conditions_deteriorating",
		]

	if market_structure >= 0.74 and wallet_behavior > 0.55:
		return TokenState.NARRATIVE_EXPLOSION, [
			"market_structure_strong",
			"wallet_behavior_positive",
		]

	if market_structure > 0.45 and volume_5m_usd >= 25_000:
		return TokenState.EARLY_LIQUIDITY, ["volume_and_liquidity_established"]

	return TokenState.UNKNOWN, ["insufficient_signal_strength"]


def _score_reasons(
	market_structure: float,
	wallet_behavior: float,
	social_momentum: float,
	execution_readiness: float,
	feature_quality: dict[str, str],
) -> list[str]:
	reasons: list[str] = []

	if market_structure > 0.7:
		reasons.append("market_structure_high")
	if wallet_behavior > 0.5:
		reasons.append("wallet_inflow_positive")
	if social_momentum > 0.5:
		reasons.append("social_momentum_positive")
	if execution_readiness > 0.7:
		reasons.append("execution_ready")
	if feature_quality and all(value == "ok" for value in feature_quality.values()):
		reasons.append("feature_quality_confirmed")
	elif any(value not in {"", "ok"} for value in feature_quality.values()):
		reasons.append("feature_quality_degraded")

	return reasons


def _normalize_notional(value: float) -> float:
	return _bounded(log10(value + 1) / 6)


def _as_float(value: object) -> float:
	if isinstance(value, bool):
		return float(int(value))
	if isinstance(value, (int, float)):
		return float(value)
	return 0.0


def _bounded(value: float) -> float:
	return max(0.0, min(value, 1.0))


def _dedupe(values: list[str]) -> list[str]:
	return list(dict.fromkeys(values))


def _as_feature_quality(value: object) -> dict[str, str]:
	if not isinstance(value, dict):
		return {}
	return {
		str(key): str(item)
		for key, item in value.items()
		if isinstance(key, str) and item is not None
	}


def _feature_quality_score(feature_quality: dict[str, str]) -> float:
	if not feature_quality:
		return 0.5
	weights = {
		"ok": 1.0,
		"low_sample": 0.6,
		"degraded": 0.5,
		"stale": 0.2,
		"missing": 0.0,
	}
	scores = [weights.get(status, 0.4) for status in feature_quality.values()]
	return round(sum(scores) / len(scores), 6)


def _is_distribution_phase(
	*,
	market_structure: float,
	buy_pressure: float,
	estimated_slippage_bps: float,
	feature_quality_score: float,
) -> bool:
	if market_structure < 0.45 and (
		buy_pressure < 0.35
		or estimated_slippage_bps >= 220
		or feature_quality_score < 0.4
	):
		return True

	return estimated_slippage_bps >= 220 and feature_quality_score < 0.65