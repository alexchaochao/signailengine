from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from core.schemas import EventEnvelope
from sentinel.okx_wallet_registry_importer import TrackedWalletRegistryEntry
from sentinel.wallet_score_aggregator import (
	WalletScoreAggregator,
	WalletScoreSnapshot,
	WalletTokenFlow,
)


def build_wallet_event(payload: dict[str, Any], source: str = "wallet_tracker") -> EventEnvelope:
	token = str(payload["token"])
	chain = str(payload.get("chain", "solana"))
	observed_at = _coerce_datetime(payload.get("observed_at"))

	normalized_payload = {
		"wallet_inflow_score": float(payload.get("wallet_inflow_score", 0.0)),
		"wallet_outflow_score": float(payload.get("wallet_outflow_score", 0.0)),
		"tracked_wallet_count": int(payload.get("tracked_wallet_count", 0)),
	}

	return EventEnvelope(
		event_id=str(payload.get("event_id", uuid4())),
		event_type="wallet.cluster_snapshot",
		source=source,
		chain=chain,
		token=token,
		observed_at=observed_at,
		ingested_at=datetime.now(UTC),
		payload=normalized_payload,
	)


def build_wallet_event_from_snapshot(
	snapshot: WalletScoreSnapshot,
	source: str = "wallet_tracker",
) -> EventEnvelope:
	return build_wallet_event(
		{
			"token": snapshot.token,
			"chain": snapshot.chain,
			"observed_at": snapshot.window_end,
			"wallet_inflow_score": snapshot.wallet_inflow_score,
			"wallet_outflow_score": snapshot.wallet_outflow_score,
			"tracked_wallet_count": snapshot.tracked_wallet_count,
		},
		source=source,
	)


def build_wallet_event_from_registry_flows(
	chain: str,
	token: str,
	registry_entries: list[TrackedWalletRegistryEntry],
	flows: list[WalletTokenFlow],
	aggregator: WalletScoreAggregator | None = None,
	source: str = "wallet_tracker",
) -> EventEnvelope:
	wallet_aggregator = aggregator or WalletScoreAggregator()
	window_end = max((flow.observed_at for flow in flows), default=None)
	snapshot = wallet_aggregator.build_snapshot(
		chain,
		token,
		registry_entries,
		flows,
		window_end=window_end,
	)
	return build_wallet_event_from_snapshot(snapshot, source=source)


def _coerce_datetime(value: object) -> datetime:
	if isinstance(value, datetime):
		return value.astimezone(UTC)
	return datetime.now(UTC)