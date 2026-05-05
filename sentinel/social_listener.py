from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from core.schemas import EventEnvelope


def build_social_event(payload: dict[str, Any], source: str = "social_listener") -> EventEnvelope:
	token = str(payload["token"])
	chain = str(payload.get("chain", "solana"))
	observed_at = _coerce_datetime(payload.get("observed_at"))

	normalized_payload = {
		"social_sentiment": float(payload.get("social_sentiment", 0.0)),
		"social_velocity": float(payload.get("social_velocity", 0.0)),
	}

	return EventEnvelope(
		event_id=str(payload.get("event_id", uuid4())),
		event_type="social.signal_snapshot",
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