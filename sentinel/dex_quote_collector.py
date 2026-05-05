from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel
from redis import Redis

from core.config import AppSettings
from core.event_flow import publish_raw_events
from core.schemas import CollectorCheckpoint, EventEnvelope, RawEventRecord
from infra.metrics import Metrics
from infra.repository import StorageRepository


class DexQuoteSourceEvent(BaseModel):
    chain: str = "solana"
    token: str
    quote_request_id: str
    quote_notional_usd: float
    expected_out_usd: float
    reference_mid_usd: float
    route_summary: dict[str, Any]
    quoted_at: datetime


class DexQuoteCollectorResult(BaseModel):
    source_event_id: str
    raw_event_id: str
    checkpoint_cursor: str
    inserted: bool
    stream_message_id: str | None = None


class DexQuoteCollector:
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

    def collect_quote(
        self,
        payload: dict[str, Any],
        *,
        source_name: str = "jupiter_quote",
        checkpoint_key: str | None = None,
        publish: bool = True,
    ) -> DexQuoteCollectorResult:
        quote = DexQuoteSourceEvent.model_validate(payload)
        source_event_id = build_quote_source_event_id(quote)
        existing = self.repository.raw_events.load(source_name, source_event_id)
        self._record_collector_metrics("received", quote, source_type="dex_quote")

        raw_event = existing
        inserted = False
        if raw_event is None:
            raw_event = self.repository.raw_events.save(
                RawEventRecord(
                    source_type="dex_quote",
                    source_name=source_name,
                    source_event_id=source_event_id,
                    chain=quote.chain,
                    token=quote.token,
                    observed_at=quote.quoted_at.astimezone(UTC),
                    ingested_at=datetime.now(UTC),
                    cursor=quote.quote_request_id,
                    payload=_quote_payload(quote),
                )
            )
            inserted = True
        else:
            self._record_collector_metrics("duplicate", quote, source_type="dex_quote")

        checkpoint_cursor = quote.quote_request_id
        if checkpoint_key is not None:
            self.repository.checkpoints.save(
                CollectorCheckpoint(
                    checkpoint_key=checkpoint_key,
                    cursor=quote.quote_request_id,
                    observed_at=quote.quoted_at.astimezone(UTC),
                    metadata={
                        "source_name": source_name,
                        "last_source_event_id": source_event_id,
                    },
                )
            )

        stream_message_id: str | None = None
        if inserted and publish and self.redis_client is not None:
            envelope = build_dex_quote_raw_event(quote, source=source_name, event_id=source_event_id)
            stream_ids = publish_raw_events(self.redis_client, self.settings, envelope)
            stream_message_id = stream_ids[0] if stream_ids else None

        if inserted and bool(quote.route_summary.get("backfill")):
            self._record_backfill_metric(source_type="dex_quote")

        return DexQuoteCollectorResult(
            source_event_id=source_event_id,
            raw_event_id=str(raw_event.id),
            checkpoint_cursor=checkpoint_cursor,
            inserted=inserted,
            stream_message_id=stream_message_id,
        )

    def _record_collector_metrics(
        self,
        outcome: str,
        quote: DexQuoteSourceEvent,
        *,
        source_type: str,
    ) -> None:
        if self.metrics is None:
            return
        self.metrics.collector_events.labels(
            collector="dex_quote_collector",
            source_type=source_type,
            outcome=outcome,
        ).inc()
        lag_seconds = max(0.0, (datetime.now(UTC) - quote.quoted_at.astimezone(UTC)).total_seconds())
        self.metrics.collector_source_lag.labels(
            collector="dex_quote_collector",
            source_type=source_type,
            chain=quote.chain,
            token=quote.token,
        ).set(lag_seconds)
        self.metrics.collector_last_watermark.labels(
            collector="dex_quote_collector",
            source_type=source_type,
            chain=quote.chain,
            token=quote.token,
        ).set(quote.quoted_at.astimezone(UTC).timestamp())

    def _record_backfill_metric(self, *, source_type: str) -> None:
        if self.metrics is None:
            return
        self.metrics.collector_backfills.labels(
            collector="dex_quote_collector",
            source_type=source_type,
        ).inc()


def build_quote_source_event_id(quote: DexQuoteSourceEvent) -> str:
    provider = str(quote.route_summary.get("provider", "unknown"))
    return (
        f"{quote.chain}:{quote.token}:{quote.quote_notional_usd}:"
        f"{quote.quoted_at.astimezone(UTC).isoformat()}:{provider}"
    )


def build_dex_quote_raw_event(
    quote: DexQuoteSourceEvent,
    *,
    source: str,
    event_id: str,
) -> EventEnvelope:
    return EventEnvelope(
        event_id=event_id,
        event_type="dex.quote_raw",
        source=source,
        chain=quote.chain,
        token=quote.token,
        observed_at=quote.quoted_at.astimezone(UTC),
        ingested_at=datetime.now(UTC),
        payload=_quote_payload(quote),
    )


def _quote_payload(quote: DexQuoteSourceEvent) -> dict[str, object]:
    return {
        "quote_request_id": quote.quote_request_id,
        "chain": quote.chain,
        "token": quote.token,
        "quote_notional_usd": quote.quote_notional_usd,
        "expected_out_usd": quote.expected_out_usd,
        "reference_mid_usd": quote.reference_mid_usd,
        "route_summary": quote.route_summary,
        "quoted_at": quote.quoted_at.astimezone(UTC).isoformat(),
    }