from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field
from redis import Redis

from core.config import AppSettings
from core.event_flow import publish_raw_events
from core.schemas import CollectorCheckpoint, EventEnvelope, RawEventRecord
from infra.metrics import Metrics
from infra.repository import StorageRepository


class OnchainTradeSourceEvent(BaseModel):
    chain: str = "solana"
    tx_hash: str
    log_index: int
    slot: int
    pool_address: str
    wallet_address: str
    token: str
    quote_asset: str
    token_amount: float
    quote_amount: float
    quote_amount_usd: float
    side: str | None = None
    route_hint: str | None = None
    observed_at: datetime


class OnchainCollectorResult(BaseModel):
    source_event_id: str
    raw_event_id: str
    checkpoint_cursor: str
    inserted: bool
    stream_message_id: str | None = None


class OnchainTradeCollector:
    def __init__(
        self,
        settings: AppSettings,
        repository: StorageRepository,
        redis_client: Redis | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.redis_client = redis_client
        self.metrics = metrics

    def collect_trade(
        self,
        payload: dict[str, Any],
        *,
        source_name: str = "solana_ws",
        checkpoint_key: str | None = None,
        publish: bool = True,
    ) -> OnchainCollectorResult:
        trade = OnchainTradeSourceEvent.model_validate(payload)
        source_event_id = build_trade_source_event_id(trade)
        existing = self.repository.raw_events.load(source_name, source_event_id)
        self._record_collector_metrics("received", trade, source_type="onchain_trade")

        raw_event = existing
        inserted = False
        if raw_event is None:
            raw_event = self.repository.raw_events.save(
                RawEventRecord(
                    source_type="onchain_trade",
                    source_name=source_name,
                    source_event_id=source_event_id,
                    chain=trade.chain,
                    token=trade.token,
                    observed_at=trade.observed_at.astimezone(UTC),
                    ingested_at=datetime.now(UTC),
                    cursor=str(trade.slot),
                    payload=_trade_payload(trade),
                )
            )
            inserted = True
        else:
            self._record_collector_metrics("duplicate", trade, source_type="onchain_trade")

        checkpoint_cursor = str(trade.slot)
        if checkpoint_key is not None:
            checkpoint_cursor = self._save_checkpoint(
                checkpoint_key,
                str(trade.slot),
                trade.observed_at.astimezone(UTC),
                source_name=source_name,
                source_event_id=source_event_id,
            )

        stream_message_id: str | None = None
        if inserted and publish and self.redis_client is not None:
            envelope = build_onchain_trade_raw_event(trade, source=source_name, event_id=source_event_id)
            stream_ids = publish_raw_events(self.redis_client, self.settings, envelope)
            stream_message_id = stream_ids[0] if stream_ids else None

        if inserted and trade.route_hint == "backfill":
            self._record_backfill_metric(trade, source_type="onchain_trade")

        return OnchainCollectorResult(
            source_event_id=source_event_id,
            raw_event_id=str(raw_event.id),
            checkpoint_cursor=checkpoint_cursor,
            inserted=inserted,
            stream_message_id=stream_message_id,
        )

    def _record_collector_metrics(
        self,
        outcome: str,
        trade: OnchainTradeSourceEvent,
        *,
        source_type: str,
    ) -> None:
        if self.metrics is None:
            return
        self.metrics.collector_events.labels(
            collector="onchain_trade_collector",
            source_type=source_type,
            outcome=outcome,
        ).inc()
        lag_seconds = max(0.0, (datetime.now(UTC) - trade.observed_at.astimezone(UTC)).total_seconds())
        self.metrics.collector_source_lag.labels(
            collector="onchain_trade_collector",
            source_type=source_type,
            chain=trade.chain,
            token=trade.token,
        ).set(lag_seconds)
        self.metrics.collector_last_watermark.labels(
            collector="onchain_trade_collector",
            source_type=source_type,
            chain=trade.chain,
            token=trade.token,
        ).set(trade.observed_at.astimezone(UTC).timestamp())

    def _record_backfill_metric(self, trade: OnchainTradeSourceEvent, *, source_type: str) -> None:
        if self.metrics is None:
            return
        self.metrics.collector_backfills.labels(
            collector="onchain_trade_collector",
            source_type=source_type,
        ).inc()

    def _save_checkpoint(
        self,
        checkpoint_key: str,
        candidate_cursor: str,
        observed_at: datetime,
        *,
        source_name: str,
        source_event_id: str,
    ) -> str:
        current = self.repository.checkpoints.load(checkpoint_key)
        next_cursor = _max_cursor(current.cursor if current is not None else None, candidate_cursor)
        self.repository.checkpoints.save(
            CollectorCheckpoint(
                checkpoint_key=checkpoint_key,
                cursor=next_cursor,
                observed_at=observed_at,
                metadata={
                    "source_name": source_name,
                    "last_source_event_id": source_event_id,
                },
            )
        )
        return next_cursor


def build_trade_source_event_id(trade: OnchainTradeSourceEvent) -> str:
    return f"{trade.chain}:{trade.tx_hash}:{trade.log_index}:{trade.token}"


def build_onchain_trade_raw_event(
    trade: OnchainTradeSourceEvent,
    *,
    source: str,
    event_id: str,
) -> EventEnvelope:
    return EventEnvelope(
        event_id=event_id,
        event_type="onchain.trade_raw",
        source=source,
        chain=trade.chain,
        token=trade.token,
        observed_at=trade.observed_at.astimezone(UTC),
        ingested_at=datetime.now(UTC),
        payload=_trade_payload(trade),
    )


def _trade_payload(trade: OnchainTradeSourceEvent) -> dict[str, object]:
    payload: dict[str, object] = {
        "tx_hash": trade.tx_hash,
        "log_index": trade.log_index,
        "slot": trade.slot,
        "pool_address": trade.pool_address,
        "wallet_address": trade.wallet_address,
        "token": trade.token,
        "quote_asset": trade.quote_asset,
        "token_amount": trade.token_amount,
        "quote_amount": trade.quote_amount,
        "quote_amount_usd": trade.quote_amount_usd,
        "observed_at": trade.observed_at.astimezone(UTC).isoformat(),
    }
    if trade.side:
        payload["side"] = trade.side
    if trade.route_hint:
        payload["route_hint"] = trade.route_hint
    return payload


def _max_cursor(current: str | None, candidate: str) -> str:
    if current is None:
        return candidate
    try:
        return candidate if int(candidate) > int(current) else current
    except ValueError:
        return candidate if candidate > current else current