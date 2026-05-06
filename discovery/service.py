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
from discovery.pool_scanner import LaunchAlphaScanner, LaunchAlphaThresholds
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
class FlowMeasurementSyncResult:
    source_type: str
    candidate_id: str
    status: AlphaCandidateStatus
    score: float
    published_message_id: str | None = None


@dataclass(frozen=True)
class SocialConfirmationSyncResult:
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
        if scanner is not None:
            self.scanner = scanner
        else:
            # Read thresholds from the first configured live source, falling
            # back to hardcoded defaults if no source is configured in env.
            acquisition = settings.acquisition
            source_configs = list(acquisition.launch_alpha_sources.values())
            if source_configs:
                thresholds = LaunchAlphaThresholds.from_source_config(source_configs[0])
            else:
                thresholds = LaunchAlphaThresholds()
            self.scanner = LaunchAlphaScanner(thresholds=thresholds)
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


class FlowMeasurementSyncService:
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
        self.logger = logging.getLogger("signalengine.flow_measurement_sync")

    def ingest_snapshot(
        self,
        payload: dict[str, object],
        *,
        source_name: str = "flow_measurement_backfill",
        publish_event: bool = True,
    ) -> FlowMeasurementSyncResult:
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
                event_type=f"flow_measurement_{candidate.status.value.lower()}",
                observed_at=snapshot.observed_at,
                payload={
                    "candidate": candidate.model_dump(mode="json"),
                    "snapshot": snapshot.model_dump(mode="json"),
                },
            )
        )
        self.repository.raw_events.save(
            RawEventRecord(
                source_type="flow_measurement_snapshot",
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
            "flow_measurement_sync_result",
            extra={
                "service": "flow_measurement_sync",
                "outcome": (
                    f"{candidate.status.value.lower()}_observe_only"
                    if not publish_event
                    else candidate.status.value.lower()
                ),
                "candidate_id": candidate.candidate_id,
                "score": candidate.score,
            },
        )
        return FlowMeasurementSyncResult(
            source_type="flow_measurement_snapshot",
            candidate_id=candidate.candidate_id,
            status=candidate.status,
            score=candidate.score,
            published_message_id=message_id,
        )

    def ingest_jsonl(self, input_path: str | Path) -> list[FlowMeasurementSyncResult]:
        results: list[FlowMeasurementSyncResult] = []
        path = Path(input_path)
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                record = json.loads(stripped)
                source_type = str(record.get("source_type", "")).strip()
                if source_type not in {"flow_activity_snapshot", "flow_measurement_snapshot"}:
                    raise ValueError(f"unsupported_flow_measurement_source_type:{source_type}")
                payload = record.get("payload")
                if not isinstance(payload, dict):
                    raise ValueError("flow_measurement_payload_must_be_object")
                results.append(self.ingest_snapshot(payload))
        return results


class SocialConfirmationSyncService:
    def __init__(
        self,
        settings: AppSettings,
        redis_client: Redis,
        repository: StorageRepository,
    ) -> None:
        self.settings = settings
        self.redis_client = redis_client
        self.repository = repository
        self.logger = logging.getLogger("signalengine.social_confirmation_sync")

    def ingest_analysis_event(
        self,
        event: EventEnvelope,
        *,
        source_name: str = "social_confirmation_sync",
    ) -> SocialConfirmationSyncResult:
        if event.event_type != "social.analysis_completed":
            raise ValueError(f"unsupported_social_confirmation_event:{event.event_type}")

        snapshot = _build_social_catalyst_snapshot(event)
        previous_candidate = self.repository.discovery.load_candidate(
            _social_candidate_id(snapshot.chain, snapshot.token, event.payload)
        )
        candidate = _build_social_candidate(snapshot, event.payload, previous_candidate)
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
                payload={
                    "snapshot": snapshot.model_dump(mode="json"),
                    "analysis_event": event.model_dump(mode="json"),
                },
            )
        )
        self.repository.discovery.append_event(
            AlphaCandidateEvent(
                event_id=snapshot.source_event_id,
                candidate_id=candidate.candidate_id,
                event_type=f"social_confirmation_{candidate.status.value.lower()}",
                observed_at=snapshot.observed_at,
                payload={
                    "candidate": candidate.model_dump(mode="json"),
                    "snapshot": snapshot.model_dump(mode="json"),
                    "analysis_event": event.model_dump(mode="json"),
                },
            )
        )
        event_envelope = EventEnvelope(
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
                "social_confirmation": event.payload.get("confirmation_score", 0.0),
                "social_spike_score": max(
                    float(event.payload.get("social_velocity", 0.0) or 0.0),
                    float(event.payload.get("engagement_score", 0.0) or 0.0),
                ),
                "llm_summary": event.payload.get("llm_summary"),
                "llm_risk_flags": event.payload.get("llm_risk_flags", []),
                "reasons": candidate.reasons,
                "feature_quality": {
                    "social_confirmation": (
                        "ok" if event.payload.get("analysis_status") == "matched" else "stale"
                    )
                },
            },
        )
        message_id = publish_raw_events(self.redis_client, self.settings, event_envelope)[0]
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
            "social_confirmation_sync_result",
            extra={
                "service": "social_confirmation_sync",
                "outcome": candidate.status.value.lower(),
                "candidate_id": candidate.candidate_id,
                "score": candidate.score,
            },
        )
        return SocialConfirmationSyncResult(
            source_type="social_analysis_completed",
            candidate_id=candidate.candidate_id,
            status=candidate.status,
            score=candidate.score,
            published_message_id=message_id,
        )


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


def _social_candidate_id(chain: str, token: str, payload: dict[str, object]) -> str:
    candidate_id = payload.get("candidate_id")
    if isinstance(candidate_id, str) and candidate_id:
        return candidate_id
    return f"social:{chain}:{token}"


def _build_social_catalyst_snapshot(event: EventEnvelope) -> CatalystEventSnapshot:
    payload = event.payload
    platform = str(payload.get("platform", payload.get("source_platform", "social")) or "social")
    query = str(payload.get("query", event.token) or event.token)
    llm_summary = payload.get("llm_summary")
    return CatalystEventSnapshot(
        source_event_id=event.event_id,
        chain=event.chain,
        token=event.token,
        catalyst_type=str(payload.get("llm_catalyst_type", "social_confirmation") or "social_confirmation"),
        headline=(
            str(llm_summary).strip()
            if isinstance(llm_summary, str) and llm_summary.strip()
            else f"{platform} social confirmation for {event.token}: {query}"
        ),
        observed_at=event.observed_at,
        impact_score=float(payload.get("confirmation_score", 0.0) or 0.0),
        credibility_score=float(payload.get("llm_credibility_score", payload.get("credibility_score", 0.0)) or 0.0),
        lead_time_minutes=0,
        venue=platform,
        metadata={
            "request_id": payload.get("request_id"),
            "analysis_status": payload.get("analysis_status"),
            "message_count": payload.get("message_count"),
            "unique_authors": payload.get("unique_authors"),
            "social_sentiment": payload.get("social_sentiment"),
            "social_velocity": payload.get("social_velocity"),
            "engagement_score": payload.get("engagement_score"),
            "snapshot_event_id": payload.get("snapshot_event_id"),
            "fsm_context": payload.get("fsm_context"),
            "llm_provider": payload.get("llm_provider"),
            "llm_model": payload.get("llm_model"),
            "llm_relevance_score": payload.get("llm_relevance_score"),
            "llm_entity_confidence": payload.get("llm_entity_confidence"),
            "llm_narrative_strength": payload.get("llm_narrative_strength"),
            "llm_noise_score": payload.get("llm_noise_score"),
            "llm_risk_flags": payload.get("llm_risk_flags"),
            "llm_summary": payload.get("llm_summary"),
        },
    )


def _build_social_candidate(
    snapshot: CatalystEventSnapshot,
    payload: dict[str, object],
    previous_candidate: AlphaCandidate | None,
) -> AlphaCandidate:
    confirmation_score = min(max(float(payload.get("confirmation_score", 0.0) or 0.0), 0.0), 1.0)
    message_count = max(int(payload.get("message_count", 0) or 0), 0)
    unique_authors = max(int(payload.get("unique_authors", 0) or 0), 0)
    matched = str(payload.get("analysis_status", "")) == "matched"
    reasons = _social_candidate_reasons(snapshot, payload, matched)
    qualified = (
        matched
        and confirmation_score >= 0.6
        and snapshot.credibility_score >= 0.25
        and message_count >= 2
        and unique_authors >= 2
    )
    status = AlphaCandidateStatus.QUALIFIED if qualified else AlphaCandidateStatus.OBSERVED
    if previous_candidate is not None and previous_candidate.status == AlphaCandidateStatus.QUALIFIED:
        status = AlphaCandidateStatus.QUALIFIED
        confirmation_score = max(confirmation_score, previous_candidate.score)

    first_seen_at = previous_candidate.first_seen_at if previous_candidate is not None else snapshot.observed_at
    created_at = previous_candidate.created_at if previous_candidate is not None else None
    metadata = dict(previous_candidate.metadata) if previous_candidate is not None else {}
    metadata.update(
        {
            "source_family": "social_confirmation",
            "request_id": payload.get("request_id"),
            "platform": snapshot.venue,
            "analysis_status": payload.get("analysis_status"),
            "mode": payload.get("mode"),
            "message_count": message_count,
            "unique_authors": unique_authors,
            "engagement_score": payload.get("engagement_score", 0.0),
            "credibility_score": snapshot.credibility_score,
            "social_velocity": payload.get("social_velocity", 0.0),
            "social_sentiment": payload.get("social_sentiment", 0.0),
            "fsm_context": payload.get("fsm_context"),
            "llm_provider": payload.get("llm_provider"),
            "llm_model": payload.get("llm_model"),
            "llm_relevance_score": payload.get("llm_relevance_score", 0.0),
            "llm_entity_confidence": payload.get("llm_entity_confidence", 0.0),
            "llm_narrative_strength": payload.get("llm_narrative_strength", 0.0),
            "llm_noise_score": payload.get("llm_noise_score", 0.0),
            "llm_risk_flags": payload.get("llm_risk_flags", []),
            "llm_summary": payload.get("llm_summary"),
        }
    )
    return AlphaCandidate(
        candidate_id=_social_candidate_id(snapshot.chain, snapshot.token, payload),
        alpha_type=AlphaType.CATALYST,
        chain=snapshot.chain,
        token=snapshot.token,
        pool_address=(previous_candidate.pool_address if previous_candidate is not None else "social-confirmation"),
        dex=(snapshot.venue or (previous_candidate.dex if previous_candidate is not None else "social")),
        quote_asset=(previous_candidate.quote_asset if previous_candidate is not None else "SOCIAL"),
        status=status,
        score=round(confirmation_score, 4),
        first_seen_at=first_seen_at,
        last_seen_at=snapshot.observed_at,
        initial_liquidity_usd=float(previous_candidate.initial_liquidity_usd if previous_candidate is not None else 0.0),
        liquidity_lock_ratio=previous_candidate.liquidity_lock_ratio if previous_candidate is not None else None,
        buy_notional_5m_usd=float(previous_candidate.buy_notional_5m_usd if previous_candidate is not None else 0.0),
        trade_count_5m=max(message_count, previous_candidate.trade_count_5m if previous_candidate is not None else 0),
        unique_wallets_5m=max(unique_authors, previous_candidate.unique_wallets_5m if previous_candidate is not None else 0),
        smart_money_wallets_5m=previous_candidate.smart_money_wallets_5m if previous_candidate is not None else 0,
        creator_hold_pct=previous_candidate.creator_hold_pct if previous_candidate is not None else None,
        reasons=reasons,
        metadata=metadata,
        created_at=created_at,
    )


def _social_candidate_reasons(
    snapshot: CatalystEventSnapshot,
    payload: dict[str, object],
    matched: bool,
) -> list[str]:
    reasons: list[str] = []
    if matched:
        reasons.append("social_confirmation_matched")
    else:
        reasons.append("social_confirmation_no_match")
    if snapshot.impact_score >= 0.6:
        reasons.append("social_confirmation_score_strong")
    if snapshot.credibility_score >= 0.25:
        reasons.append("social_credibility_sufficient")
    if int(payload.get("message_count", 0) or 0) >= 2:
        reasons.append("social_message_threshold_met")
    if int(payload.get("unique_authors", 0) or 0) >= 2:
        reasons.append("social_author_diversity_met")
    return reasons