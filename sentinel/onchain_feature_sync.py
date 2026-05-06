from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from redis import Redis

from core.config import AppSettings
from core.event_flow import publish_raw_events
from infra.logging import get_logger
from infra.metrics import Metrics
from infra.repository import StorageRepository
from sentinel.dex_quote_collector import DexQuoteCollector, DexQuoteCollectorResult
from sentinel.feature_aggregator import OnchainFeatureAggregator, SlippageFeatureAggregator
from sentinel.onchain_collector import OnchainCollectorResult, OnchainTradeCollector
from sentinel.onchain_feature_publisher import OnchainFeaturePublisher
from sentinel.onchain_listener import build_onchain_trade_event


class OnchainFeatureSyncResult(BaseModel):
    source_type: str
    source_event_id: str
    inserted: bool
    published_message_id: str | None = None
    snapshot_feature_names: list[str] = Field(default_factory=list)


class OnchainFeatureSyncService:
    def __init__(
        self,
        settings: AppSettings,
        redis_client: Redis,
        repository: StorageRepository,
        metrics: Metrics | None = None,
    ) -> None:
        self.settings = settings
        self.redis_client = redis_client
        self.repository = repository
        self.metrics = metrics
        self.logger = get_logger("signalengine.onchain_feature_sync")
        self.trade_collector = OnchainTradeCollector(
            settings,
            repository,
            redis_client,
            metrics=metrics,
        )
        self.quote_collector = DexQuoteCollector(
            settings,
            repository,
            redis_client,
            metrics=metrics,
        )
        self.onchain_aggregator = OnchainFeatureAggregator(settings, repository, metrics=metrics)
        self.slippage_aggregator = SlippageFeatureAggregator(settings, repository, metrics=metrics)
        self.publisher = OnchainFeaturePublisher(settings, redis_client, repository)

    def ingest_trade(
        self,
        payload: dict[str, Any],
        *,
        source_name: str = "solana_ws",
        checkpoint_key: str | None = None,
    ) -> OnchainFeatureSyncResult:
        collector_result = self.trade_collector.collect_trade(
            payload,
            source_name=source_name,
            checkpoint_key=checkpoint_key,
            publish=False,
        )
        result = self._build_trade_result(payload, collector_result, source_name=source_name)
        self._log_result(payload, result)
        return result

    def ingest_quote(
        self,
        payload: dict[str, Any],
        *,
        source_name: str = "jupiter_quote",
        checkpoint_key: str | None = None,
    ) -> OnchainFeatureSyncResult:
        collector_result = self.quote_collector.collect_quote(
            payload,
            source_name=source_name,
            checkpoint_key=checkpoint_key,
            publish=False,
        )
        result = self._build_quote_result(payload, collector_result, source_name=source_name)
        self._log_result(payload, result)
        return result

    def ingest_record(self, record: dict[str, Any]) -> OnchainFeatureSyncResult:
        source_type = str(record.get("source_type", "")).strip().lower()
        payload = dict(record.get("payload", {}))
        source_name = str(record.get("source_name", "")).strip()
        checkpoint_key = record.get("checkpoint_key")
        if source_type == "onchain_trade":
            return self.ingest_trade(
                payload,
                source_name=source_name or "solana_ws",
                checkpoint_key=str(checkpoint_key) if checkpoint_key else None,
            )
        if source_type == "dex_quote":
            return self.ingest_quote(
                payload,
                source_name=source_name or "jupiter_quote",
                checkpoint_key=str(checkpoint_key) if checkpoint_key else None,
            )
        raise ValueError("unsupported_source_type")

    def ingest_jsonl(self, input_path: str | Path) -> list[OnchainFeatureSyncResult]:
        path = Path(input_path)
        results: list[OnchainFeatureSyncResult] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                results.append(self.ingest_record(__import__("json").loads(stripped)))
        self.logger.info(
            "onchain_feature_sync_batch_completed",
            extra={
                "service": "onchain_feature_sync",
                "outcome": f"processed_{len(results)}",
            },
        )
        return results

    def _build_trade_result(
        self,
        payload: dict[str, Any],
        collector_result: OnchainCollectorResult,
        *,
        source_name: str,
    ) -> OnchainFeatureSyncResult:
        snapshots = []
        published_message_id: str | None = None
        if collector_result.inserted:
            raw_event = self.repository.raw_events.load(source_name, collector_result.source_event_id)
            if raw_event is None:
                raise RuntimeError("missing_trade_raw_event")

            # Only publish onchain.trade_fact for trades above the minimum
            # notional threshold.  Micro-trades still update aggregator features
            # (buy pressure etc.) but are skipped from the stream to reduce
            # noise in downstream consumers such as wallet intelligence.
            notional_usd = float(payload.get("quote_amount_usd", 0.0))
            min_trade_notional = self.settings.features.onchain.min_trade_notional_usd
            if notional_usd >= min_trade_notional:
                publish_raw_events(
                    self.redis_client,
                    self.settings,
                    build_onchain_trade_event(
                        {
                            "chain": str(payload.get("chain", "solana")),
                            "token": str(payload["token"]),
                            "wallet_address": str(payload["wallet_address"]),
                            "direction": _trade_direction(payload.get("side")),
                            "notional_usd": notional_usd,
                            "trade_count": 1,
                            "observed_at": payload.get("observed_at"),
                            "event_id": collector_result.source_event_id,
                        },
                        source=source_name,
                    ),
                )

            snapshots = self.onchain_aggregator.ingest_raw_trade(raw_event)
            _, published_message_id = self.publisher.publish_latest(
                str(payload.get("chain", "solana")),
                str(payload["token"]),
            )
        return OnchainFeatureSyncResult(
            source_type="onchain_trade",
            source_event_id=collector_result.source_event_id,
            inserted=collector_result.inserted,
            published_message_id=published_message_id,
            snapshot_feature_names=[snapshot.feature_name for snapshot in snapshots],
        )

    def _build_quote_result(
        self,
        payload: dict[str, Any],
        collector_result: DexQuoteCollectorResult,
        *,
        source_name: str,
    ) -> OnchainFeatureSyncResult:
        snapshot_feature_names: list[str] = []
        published_message_id: str | None = None
        if collector_result.inserted:
            raw_event = self.repository.raw_events.load(source_name, collector_result.source_event_id)
            if raw_event is None:
                raise RuntimeError("missing_quote_raw_event")
            snapshot = self.slippage_aggregator.ingest_raw_quote(raw_event)
            snapshot_feature_names = [snapshot.feature_name]
            _, published_message_id = self.publisher.publish_latest(
                str(payload.get("chain", "solana")),
                str(payload["token"]),
            )
        return OnchainFeatureSyncResult(
            source_type="dex_quote",
            source_event_id=collector_result.source_event_id,
            inserted=collector_result.inserted,
            published_message_id=published_message_id,
            snapshot_feature_names=snapshot_feature_names,
        )

    def _log_result(self, payload: dict[str, Any], result: OnchainFeatureSyncResult) -> None:
        self.logger.info(
            "onchain_feature_sync_result",
            extra={
                "event_id": result.source_event_id,
                "token": str(payload.get("token", "")),
                "chain": str(payload.get("chain", "solana")),
                "service": "onchain_feature_sync",
                "outcome": (
                    f"published_{result.published_message_id}"
                    if result.published_message_id is not None
                    else ("inserted" if result.inserted else "duplicate")
                ),
            },
        )


def _trade_direction(side: object) -> str:
    side_value = str(side or "").strip().lower()
    if side_value == "buy":
        return "inflow"
    if side_value == "sell":
        return "outflow"
    return side_value or "inflow"