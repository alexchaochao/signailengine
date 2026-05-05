from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from redis import Redis

from core.config import AppSettings
from core.event_flow import publish_raw_events
from core.schemas import EventEnvelope, RawEventRecord
from discovery.catalyst_scanner import CatalystAlphaScanner
from discovery.flow_scanner import FlowAlphaScanner
from discovery.pool_scanner import LaunchAlphaScanner
from discovery.schemas import (
    AlphaCandidate,
    AlphaCandidateEvent,
    AlphaCandidateStatus,
    AlphaSnapshot,
    AlphaType,
    CatalystEventSnapshot,
    FlowActivitySnapshot,
    LaunchPoolSnapshot,
)

if TYPE_CHECKING:
	from infra.repository import StorageRepository


@dataclass(frozen=True)
class LaunchAlphaSyncResult:
    source_type: str
    candidate_id: str
    status: AlphaCandidateStatus
    score: float
    published_message_id: str | None = None


@dataclass(frozen=True)
class CatalystAlphaSyncResult:
    source_type: str
    candidate_id: str
    status: AlphaCandidateStatus
    score: float
    published_message_id: str | None = None


@dataclass(frozen=True)
class FlowAlphaSyncResult:
    source_type: str
    candidate_id: str
    status: AlphaCandidateStatus
    score: float
    published_message_id: str | None = None


class LaunchAlphaSyncService:
    def __init__(
        self,
        settings: AppSettings,
        redis_client: Redis,
        repository: StorageRepository,
        *,
        scanner: LaunchAlphaScanner | None = None,
    ) -> None:
        self.settings = settings
        self.redis_client = redis_client
        self.repository = repository
        self.scanner = scanner or LaunchAlphaScanner()
        self.logger = logging.getLogger("signalengine.launch_alpha_sync")

    def ingest_snapshot(
        self,
        payload: dict[str, object],
        *,
        source_name: str = "launch_alpha_backfill",
    ) -> LaunchAlphaSyncResult:
        snapshot = LaunchPoolSnapshot.model_validate(payload)
        candidate = self.scanner.evaluate(snapshot)
        previous_candidate = self.repository.discovery.load_candidate(candidate.candidate_id)
        candidate = self.repository.discovery.upsert_candidate(candidate)
        self.repository.discovery.save_snapshot(
            AlphaSnapshot(
                snapshot_id=snapshot.source_event_id,
                candidate_id=candidate.candidate_id,
                alpha_type=AlphaType.LAUNCH,
                chain=candidate.chain,
                token=candidate.token,
                observed_at=snapshot.observed_at,
                status=candidate.status,
                score=candidate.score,
                payload=snapshot.model_dump(mode="json"),
            )
        )
        self.repository.discovery.append_event(
            AlphaCandidateEvent(
                event_id=snapshot.source_event_id,
                candidate_id=candidate.candidate_id,
                event_type=f"launch_candidate_{candidate.status.value.lower()}",
                observed_at=snapshot.observed_at,
                payload={
                    "candidate": candidate.model_dump(mode="json"),
                    "snapshot": snapshot.model_dump(mode="json"),
                },
            )
        )
        self.repository.raw_events.save(
            RawEventRecord(
                source_type="launch_pool_snapshot",
                source_name=source_name,
                source_event_id=snapshot.source_event_id,
                chain=snapshot.chain,
                token=snapshot.token,
                observed_at=snapshot.observed_at,
                ingested_at=datetime.now(UTC),
                payload=snapshot.model_dump(mode="json"),
            )
        )
        event = EventEnvelope(
            event_id=snapshot.source_event_id,
            event_type="alpha.launch_candidate",
            source=source_name,
            chain=candidate.chain,
            token=candidate.token,
            observed_at=snapshot.observed_at,
            ingested_at=datetime.now(UTC),
            payload={
                "alpha_type": AlphaType.LAUNCH.value,
                "candidate_id": candidate.candidate_id,
                "pool_address": candidate.pool_address,
                "dex": candidate.dex,
                "status": candidate.status.value,
                "score": candidate.score,
                "launch_candidate_status": candidate.status.value,
                "launch_alpha_score": candidate.score,
                "launch_age_seconds": 0.0,
                "reasons": candidate.reasons,
                "buy_notional_5m_usd": candidate.buy_notional_5m_usd,
                "trade_count_5m": candidate.trade_count_5m,
                "unique_wallets_5m": candidate.unique_wallets_5m,
                "smart_money_wallets_5m": candidate.smart_money_wallets_5m,
                "initial_liquidity_usd": candidate.initial_liquidity_usd,
                "liquidity_usd": candidate.initial_liquidity_usd,
                "volume_5m_usd": candidate.buy_notional_5m_usd,
                "buy_pressure": _launch_buy_pressure(candidate),
                "wallet_inflow_score": _launch_wallet_inflow_score(candidate),
                "holder_growth_15m": _launch_holder_growth(candidate),
                "estimated_slippage_bps": _launch_slippage_bps(candidate),
                "feature_quality": {"launch_alpha": "ok"},
            },
        )
        message_id = publish_raw_events(self.redis_client, self.settings, event)[0]
        _publish_qualified_candidate_event(
            self.redis_client,
            self.settings,
            self.repository,
            source_name=source_name,
            snapshot=snapshot,
            previous_candidate=previous_candidate,
            candidate=candidate,
        )
        self.logger.info(
            "launch_alpha_sync_result",
            extra={
                "service": "launch_alpha_sync",
                "outcome": candidate.status.value.lower(),
                "candidate_id": candidate.candidate_id,
                "score": candidate.score,
            },
        )
        return LaunchAlphaSyncResult(
            source_type="launch_pool_snapshot",
            candidate_id=candidate.candidate_id,
            status=candidate.status,
            score=candidate.score,
            published_message_id=message_id,
        )

    def ingest_jsonl(self, input_path: str | Path) -> list[LaunchAlphaSyncResult]:
        results: list[LaunchAlphaSyncResult] = []
        path = Path(input_path)
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                record = json.loads(stripped)
                source_type = str(record.get("source_type", ""))
                if source_type != "launch_pool_snapshot":
                    raise ValueError(f"unsupported_launch_alpha_source_type:{source_type}")
                payload = record.get("payload")
                if not isinstance(payload, dict):
                    raise ValueError("launch_alpha_payload_must_be_object")
                results.append(self.ingest_snapshot(payload))
        return results


class CatalystAlphaSyncService:
    def __init__(
        self,
        settings: AppSettings,
        redis_client: Redis,
        repository: StorageRepository,
        *,
        scanner: CatalystAlphaScanner | None = None,
    ) -> None:
        self.settings = settings
        self.redis_client = redis_client
        self.repository = repository
        self.scanner = scanner or CatalystAlphaScanner()
        self.logger = logging.getLogger("signalengine.catalyst_alpha_sync")

    def ingest_snapshot(
        self,
        payload: dict[str, object],
        *,
        source_name: str = "catalyst_alpha_backfill",
    ) -> CatalystAlphaSyncResult:
        snapshot = CatalystEventSnapshot.model_validate(payload)
        candidate = self.scanner.evaluate(snapshot)
        previous_candidate = self.repository.discovery.load_candidate(candidate.candidate_id)
        candidate = self.repository.discovery.upsert_candidate(candidate)
        self.repository.discovery.save_snapshot(
            AlphaSnapshot(
                snapshot_id=snapshot.source_event_id,
                candidate_id=candidate.candidate_id,
                alpha_type=AlphaType.CATALYST,
                chain=candidate.chain,
                token=candidate.token,
                observed_at=snapshot.observed_at,
                status=candidate.status,
                score=candidate.score,
                payload=snapshot.model_dump(mode="json"),
            )
        )
        self.repository.discovery.append_event(
            AlphaCandidateEvent(
                event_id=snapshot.source_event_id,
                candidate_id=candidate.candidate_id,
                event_type=f"catalyst_candidate_{candidate.status.value.lower()}",
                observed_at=snapshot.observed_at,
                payload={
                    "candidate": candidate.model_dump(mode="json"),
                    "snapshot": snapshot.model_dump(mode="json"),
                },
            )
        )
        self.repository.raw_events.save(
            RawEventRecord(
                source_type="catalyst_event_snapshot",
                source_name=source_name,
                source_event_id=snapshot.source_event_id,
                chain=snapshot.chain,
                token=snapshot.token,
                observed_at=snapshot.observed_at,
                ingested_at=datetime.now(UTC),
                payload=snapshot.model_dump(mode="json"),
            )
        )
        event = EventEnvelope(
            event_id=snapshot.source_event_id,
            event_type="alpha.catalyst_candidate",
            source=source_name,
            chain=candidate.chain,
            token=candidate.token,
            observed_at=snapshot.observed_at,
            ingested_at=datetime.now(UTC),
            payload={
                "alpha_type": AlphaType.CATALYST.value,
                "candidate_id": candidate.candidate_id,
                "status": candidate.status.value,
                "score": candidate.score,
                "catalyst_alpha_score": candidate.score,
                "catalyst_credibility_score": snapshot.credibility_score,
                "catalyst_type": snapshot.catalyst_type,
                "headline": snapshot.headline,
                "reasons": candidate.reasons,
            },
        )
        message_id = publish_raw_events(self.redis_client, self.settings, event)[0]
        _publish_qualified_candidate_event(
            self.redis_client,
            self.settings,
            self.repository,
            source_name=source_name,
            snapshot=snapshot,
            previous_candidate=previous_candidate,
            candidate=candidate,
        )
        self.logger.info(
            "catalyst_alpha_sync_result",
            extra={
                "service": "catalyst_alpha_sync",
                "outcome": candidate.status.value.lower(),
                "candidate_id": candidate.candidate_id,
                "score": candidate.score,
            },
        )
        return CatalystAlphaSyncResult(
            source_type="catalyst_event_snapshot",
            candidate_id=candidate.candidate_id,
            status=candidate.status,
            score=candidate.score,
            published_message_id=message_id,
        )

    def ingest_jsonl(self, input_path: str | Path) -> list[CatalystAlphaSyncResult]:
        results: list[CatalystAlphaSyncResult] = []
        path = Path(input_path)
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                record = json.loads(stripped)
                source_type = str(record.get("source_type", "")).strip()
                if source_type != "catalyst_event_snapshot":
                    raise ValueError(f"unsupported_catalyst_alpha_source_type:{source_type}")
                payload = record.get("payload")
                if not isinstance(payload, dict):
                    raise ValueError("catalyst_alpha_payload_must_be_object")
                results.append(self.ingest_snapshot(payload))
        return results


class FlowAlphaSyncService:
    def __init__(
        self,
        settings: AppSettings,
        redis_client: Redis,
        repository: StorageRepository,
        *,
        scanner: FlowAlphaScanner | None = None,
    ) -> None:
        self.settings = settings
        self.redis_client = redis_client
        self.repository = repository
        self.scanner = scanner or FlowAlphaScanner()
        self.logger = logging.getLogger("signalengine.flow_alpha_sync")

    def ingest_snapshot(
        self,
        payload: dict[str, object],
        *,
        source_name: str = "flow_alpha_backfill",
        publish_event: bool = True,
    ) -> FlowAlphaSyncResult:
        snapshot = FlowActivitySnapshot.model_validate(payload)
        candidate = self.scanner.evaluate(snapshot)
        previous_candidate = self.repository.discovery.load_candidate(candidate.candidate_id)
        candidate = self.repository.discovery.upsert_candidate(candidate)
        self.repository.discovery.save_snapshot(
            AlphaSnapshot(
                snapshot_id=snapshot.source_event_id,
                candidate_id=candidate.candidate_id,
                alpha_type=AlphaType.FLOW,
                chain=candidate.chain,
                token=candidate.token,
                observed_at=snapshot.observed_at,
                status=candidate.status,
                score=candidate.score,
                payload=snapshot.model_dump(mode="json"),
            )
        )
        self.repository.discovery.append_event(
            AlphaCandidateEvent(
                event_id=snapshot.source_event_id,
                candidate_id=candidate.candidate_id,
                event_type=f"flow_candidate_{candidate.status.value.lower()}",
                observed_at=snapshot.observed_at,
                payload={
                    "candidate": candidate.model_dump(mode="json"),
                    "snapshot": snapshot.model_dump(mode="json"),
                },
            )
        )
        self.repository.raw_events.save(
            RawEventRecord(
                source_type="flow_activity_snapshot",
                source_name=source_name,
                source_event_id=snapshot.source_event_id,
                chain=snapshot.chain,
                token=snapshot.token,
                observed_at=snapshot.observed_at,
                ingested_at=datetime.now(UTC),
                payload=snapshot.model_dump(mode="json"),
            )
        )
        message_id: str | None = None
        if publish_event:
            event = EventEnvelope(
                event_id=snapshot.source_event_id,
                event_type="alpha.flow_candidate",
                source=source_name,
                chain=candidate.chain,
                token=candidate.token,
                observed_at=snapshot.observed_at,
                ingested_at=datetime.now(UTC),
                payload={
                    "alpha_type": AlphaType.FLOW.value,
                    "candidate_id": candidate.candidate_id,
                    "status": candidate.status.value,
                    "score": candidate.score,
                    "flow_alpha_score": candidate.score,
                    "flow_candidate_status": candidate.status.value,
                    "flow_type": snapshot.flow_type,
                    "wallet_inflow_score": _flow_wallet_inflow_score(snapshot),
                    "wallet_outflow_score": _flow_wallet_outflow_score(snapshot),
                    "volume_5m_usd": snapshot.smart_money_inflow_usd,
                    "buy_pressure": _flow_buy_pressure(snapshot),
                    "holder_growth_15m": _flow_holder_growth(snapshot),
                    "reasons": candidate.reasons,
                    "feature_quality": {"flow_alpha": "ok"},
                },
            )
            message_id = publish_raw_events(self.redis_client, self.settings, event)[0]
            _publish_qualified_candidate_event(
                self.redis_client,
                self.settings,
                self.repository,
                source_name=source_name,
                snapshot=snapshot,
                previous_candidate=previous_candidate,
                candidate=candidate,
            )
        self.logger.info(
            "flow_alpha_sync_result",
            extra={
                "service": "flow_alpha_sync",
                "outcome": (
                    f"{candidate.status.value.lower()}_observe_only"
                    if not publish_event
                    else candidate.status.value.lower()
                ),
                "candidate_id": candidate.candidate_id,
                "score": candidate.score,
            },
        )
        return FlowAlphaSyncResult(
            source_type="flow_activity_snapshot",
            candidate_id=candidate.candidate_id,
            status=candidate.status,
            score=candidate.score,
            published_message_id=message_id,
        )

    def ingest_jsonl(self, input_path: str | Path) -> list[FlowAlphaSyncResult]:
        results: list[FlowAlphaSyncResult] = []
        path = Path(input_path)
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                record = json.loads(stripped)
                source_type = str(record.get("source_type", "")).strip()
                if source_type != "flow_activity_snapshot":
                    raise ValueError(f"unsupported_flow_alpha_source_type:{source_type}")
                payload = record.get("payload")
                if not isinstance(payload, dict):
                    raise ValueError("flow_alpha_payload_must_be_object")
                results.append(self.ingest_snapshot(payload))
        return results


def _launch_buy_pressure(candidate: AlphaCandidate) -> float:
    if candidate.initial_liquidity_usd <= 0:
        return 0.0
    return round(min(0.35 + (candidate.buy_notional_5m_usd / candidate.initial_liquidity_usd), 1.0), 4)


def _launch_wallet_inflow_score(candidate: AlphaCandidate) -> float:
    wallet_mix = candidate.unique_wallets_5m / 12.0 if candidate.unique_wallets_5m > 0 else 0.0
    smart_money_bonus = candidate.smart_money_wallets_5m / 4.0 if candidate.smart_money_wallets_5m > 0 else 0.0
    return round(min(wallet_mix * 0.65 + smart_money_bonus * 0.35, 1.0), 4)


def _launch_holder_growth(candidate: AlphaCandidate) -> float:
    return round(min(candidate.unique_wallets_5m / 15.0, 1.0), 4)


def _launch_slippage_bps(candidate: AlphaCandidate) -> float:
    if candidate.initial_liquidity_usd >= 250_000:
        return 60.0
    if candidate.initial_liquidity_usd >= 100_000:
        return 90.0
    if candidate.initial_liquidity_usd >= 50_000:
        return 120.0
    return 180.0


def _publish_qualified_candidate_event(
    redis_client: Redis,
    settings: AppSettings,
    repository: StorageRepository,
    *,
    source_name: str,
    snapshot: LaunchPoolSnapshot | CatalystEventSnapshot | FlowActivitySnapshot,
    previous_candidate: AlphaCandidate | None,
    candidate: AlphaCandidate,
) -> str | None:
    if candidate.status != AlphaCandidateStatus.QUALIFIED:
        return None
    if previous_candidate is not None and previous_candidate.status == AlphaCandidateStatus.QUALIFIED:
        return None

    repository.discovery.append_event(
        AlphaCandidateEvent(
            event_id=f"{snapshot.source_event_id}:qualified",
            candidate_id=candidate.candidate_id,
            event_type="alpha_candidate_qualified",
            observed_at=snapshot.observed_at,
            payload={
                "alpha_type": candidate.alpha_type.value,
                "candidate": candidate.model_dump(mode="json"),
                "snapshot": snapshot.model_dump(mode="json"),
            },
        )
    )
    event = EventEnvelope(
        event_id=f"{snapshot.source_event_id}:qualified",
        event_type="alpha.candidate_qualified",
        source=source_name,
        chain=candidate.chain,
        token=candidate.token,
        observed_at=snapshot.observed_at,
        ingested_at=datetime.now(UTC),
        payload={
            "alpha_type": candidate.alpha_type.value,
            "candidate_id": candidate.candidate_id,
            "status": candidate.status.value,
            "score": candidate.score,
            "reasons": candidate.reasons,
            "candidate": candidate.model_dump(mode="json"),
            "snapshot": snapshot.model_dump(mode="json"),
        },
    )
    return publish_raw_events(redis_client, settings, event)[0]


def _flow_wallet_inflow_score(snapshot: FlowActivitySnapshot) -> float:
    inflow_scale = min(snapshot.smart_money_inflow_usd / 100_000.0, 1.0)
    buyer_scale = min(snapshot.unique_buyer_wallets_15m / 16.0, 1.0)
    return round(min(inflow_scale * 0.7 + buyer_scale * 0.3, 1.0), 4)


def _flow_wallet_outflow_score(snapshot: FlowActivitySnapshot) -> float:
    total_flow = snapshot.smart_money_inflow_usd + snapshot.smart_money_outflow_usd
    if total_flow <= 0:
        return 0.0
    return round(min(snapshot.smart_money_outflow_usd / total_flow, 1.0), 4)


def _flow_buy_pressure(snapshot: FlowActivitySnapshot) -> float:
    total_flow = snapshot.smart_money_inflow_usd + snapshot.smart_money_outflow_usd
    if total_flow <= 0:
        return 0.0
    return round(min(snapshot.smart_money_inflow_usd / total_flow, 1.0), 4)


def _flow_holder_growth(snapshot: FlowActivitySnapshot) -> float:
    return round(min(snapshot.unique_buyer_wallets_15m / 18.0, 1.0), 4)