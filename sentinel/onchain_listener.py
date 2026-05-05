from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from core.schemas import EventEnvelope


def build_onchain_event(payload: dict[str, Any], source: str = "solana_listener") -> EventEnvelope:
	token = str(payload["token"])
	chain = str(payload.get("chain", "solana"))
	observed_at = _coerce_datetime(payload.get("observed_at"))

	normalized_payload = {
		"liquidity_usd": float(payload.get("liquidity_usd", 0.0)),
		"volume_5m_usd": float(payload.get("volume_5m_usd", 0.0)),
		"buy_pressure": float(payload.get("buy_pressure", 0.0)),
		"holder_growth_15m": float(payload.get("holder_growth_15m", 0.0)),
		"wallet_inflow_score": float(payload.get("wallet_inflow_score", 0.0)),
		"estimated_slippage_bps": float(payload.get("estimated_slippage_bps", 0.0)),
		"buy_pressure_window": str(payload.get("buy_pressure_window", "")),
		"feature_quality": dict(payload.get("feature_quality", {})),
		"formula_versions": dict(payload.get("formula_versions", {})),
	}

	return EventEnvelope(
		event_id=str(payload.get("event_id", uuid4())),
		event_type="onchain.liquidity_snapshot",
		source=source,
		chain=chain,
		token=token,
		observed_at=observed_at,
		ingested_at=datetime.now(UTC),
		payload=normalized_payload,
	)


def build_onchain_trade_event(
	payload: dict[str, Any],
	source: str = "solana_trade_collector",
) -> EventEnvelope:
	token = str(payload["token"])
	chain = str(payload.get("chain", "solana"))
	observed_at = _coerce_datetime(payload.get("observed_at"))

	normalized_payload = {
		"wallet_address": str(payload["wallet_address"]),
		"direction": str(payload["direction"]),
		"notional_usd": float(payload.get("notional_usd", 0.0)),
		"trade_count": int(payload.get("trade_count", 1)),
	}

	return EventEnvelope(
		event_id=str(payload.get("event_id", uuid4())),
		event_type="onchain.trade_fact",
		source=source,
		chain=chain,
		token=token,
		observed_at=observed_at,
		ingested_at=datetime.now(UTC),
		payload=normalized_payload,
	)


def _coerce_datetime(value: object) -> datetime:
	if isinstance(value, datetime):
		return value.astimezone(UTC)
	return datetime.now(UTC)