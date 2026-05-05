from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from redis import Redis

from core.config import AppSettings
from core.event_flow import publish_raw_events
from core.schemas import EventEnvelope
from infra.redis_stream import read_models
from infra.repository import StorageRepository
from sentinel.okx_wallet_registry_importer import OkxLeaderboardRequest, OkxWalletRegistryImporter
from sentinel.wallet_refresh_job import OkxTrackedWalletRefreshJob, TrackedWalletRefreshRequest
from sentinel.wallet_score_aggregator import WalletScoreAggregator, WalletTokenFlow
from sentinel.wallet_tracker import build_wallet_event_from_snapshot


@dataclass(frozen=True)
class WalletIntelligenceSyncRequest:
    chain: str
    chain_index: str
    token: str
    time_frame: str = "3"
    sort_by: str = "1"
    wallet_type: str = "3"
    registry_version: str = "okx_registry_v1"
    raw_event_last_id: str = "0-0"
    raw_event_count: int = 1000
    refresh_limit: int = 20
    sync_key: str = "wallet_intelligence:default"


@dataclass(frozen=True)
class WalletIntelligenceSyncResult:
    imported_wallets: int
    refreshed_wallets: int
    projected_flows: int
    published_message_ids: list[str]
    last_raw_event_id: str


class WalletIntelligenceSyncService:
    def __init__(
        self,
        settings: AppSettings,
        redis_client: Redis,
        repository: StorageRepository,
        importer: OkxWalletRegistryImporter | None = None,
        refresh_job: OkxTrackedWalletRefreshJob | None = None,
        aggregator: WalletScoreAggregator | None = None,
    ) -> None:
        self.settings = settings
        self.redis_client = redis_client
        self.repository = repository
        self.importer = importer or OkxWalletRegistryImporter(settings)
        self.refresh_job = refresh_job or OkxTrackedWalletRefreshJob(settings)
        self.aggregator = aggregator or WalletScoreAggregator()

    def run(self, request: WalletIntelligenceSyncRequest) -> WalletIntelligenceSyncResult:
        raw_event_last_id = self._resolve_raw_event_last_id(request)
        sync_state = self.repository.wallet_intelligence.load_sync_state(request.sync_key)
        imported_entries = self.importer.import_wallets(
            OkxLeaderboardRequest(
                chain=request.chain,
                chain_index=request.chain_index,
                time_frame=request.time_frame,
                sort_by=request.sort_by,
                wallet_type=request.wallet_type,
            ),
            registry_version=request.registry_version,
        )
        self.repository.wallet_intelligence.upsert_registry_entries(imported_entries)
        active_registry = self.repository.wallet_intelligence.list_active_registry_entries(request.chain)
        refresh_targets = _prioritize_refresh_entries(imported_entries, active_registry)
        refresh_requests = [
            TrackedWalletRefreshRequest(
                chain=request.chain,
                chain_index=request.chain_index,
                wallet_address=entry.wallet_address,
                time_frame=request.time_frame,
            )
            for entry in refresh_targets[: request.refresh_limit]
        ]
        refreshed_states = self.refresh_job.refresh_wallets(refresh_requests)
        self.repository.wallet_intelligence.save_refresh_states(refreshed_states)
        return self._project_and_publish(
            request,
            raw_event_last_id=raw_event_last_id,
            active_registry=active_registry,
            imported_wallets=len(imported_entries),
            refreshed_wallets=len(refreshed_states),
            event_source="wallet_intelligence_sync",
        )

    def project_existing_registry(
        self,
        request: WalletIntelligenceSyncRequest,
    ) -> WalletIntelligenceSyncResult:
        raw_event_last_id = self._resolve_raw_event_last_id(request)
        active_registry = self.repository.wallet_intelligence.list_active_registry_entries(request.chain)
        return self._project_and_publish(
            request,
            raw_event_last_id=raw_event_last_id,
            active_registry=active_registry,
            imported_wallets=0,
            refreshed_wallets=0,
            event_source="wallet_flow_projection",
        )

    def _resolve_raw_event_last_id(self, request: WalletIntelligenceSyncRequest) -> str:
        sync_state = self.repository.wallet_intelligence.load_sync_state(request.sync_key)
        raw_event_last_id = request.raw_event_last_id
        if sync_state is not None and raw_event_last_id == "0-0":
            raw_event_last_id = str(sync_state["last_raw_event_id"])
        return raw_event_last_id

    def _project_and_publish(
        self,
        request: WalletIntelligenceSyncRequest,
        *,
        raw_event_last_id: str,
        active_registry,
        imported_wallets: int,
        refreshed_wallets: int,
        event_source: str,
    ) -> WalletIntelligenceSyncResult:
        projected_flows, last_seen_message_id = self._project_wallet_flows(
            request.chain,
            raw_event_last_id,
            request.raw_event_count,
        )
        self.repository.wallet_intelligence.append_wallet_flows(projected_flows)
        window_end = datetime.now(UTC)
        flows = self.repository.wallet_intelligence.load_wallet_flows(
            request.chain,
            request.token,
            since=window_end - timedelta(seconds=self.aggregator.window_seconds),
        )
        snapshot = self.aggregator.build_snapshot(
            request.chain,
            request.token,
            active_registry,
            flows,
            window_end=window_end,
        )
        event = build_wallet_event_from_snapshot(snapshot, source=event_source)
        message_ids = publish_raw_events(self.redis_client, self.settings, event)
        last_projected_event_id = last_seen_message_id or raw_event_last_id
        self.repository.wallet_intelligence.save_sync_state(
            request.sync_key,
            last_raw_event_id=last_projected_event_id,
            last_synced_at=window_end,
            last_published_at=event.observed_at,
        )
        return WalletIntelligenceSyncResult(
            imported_wallets=imported_wallets,
            refreshed_wallets=refreshed_wallets,
            projected_flows=len(projected_flows),
            published_message_ids=message_ids,
            last_raw_event_id=last_projected_event_id,
        )

    def _project_wallet_flows(
        self,
        chain: str,
        last_id: str,
        count: int,
    ) -> tuple[list[WalletTokenFlow], str | None]:
        registry_entries = self.repository.wallet_intelligence.list_active_registry_entries(chain)
        tracked_wallets = {entry.wallet_address for entry in registry_entries}
        projected: list[WalletTokenFlow] = []
        last_seen_message_id: str | None = None
        for message_id, event in read_models(
            self.redis_client,
            self.settings.redis.raw_events_stream,
            EventEnvelope,
            last_id=last_id,
            count=count,
        ):
            if last_id != "0-0" and not _stream_message_id_gt(message_id, last_id):
                continue
            last_seen_message_id = message_id
            if event.event_type != "onchain.trade_fact":
                continue
            wallet_address = str(event.payload.get("wallet_address", "")).strip()
            if wallet_address not in tracked_wallets:
                continue
            direction = str(event.payload.get("direction", "")).lower()
            if direction not in {"inflow", "outflow"}:
                continue
            projected.append(
                WalletTokenFlow(
                    chain=event.chain,
                    token=event.token,
                    wallet_address=wallet_address,
                    direction=direction,
                    notional_usd=float(event.payload.get("notional_usd", 0.0)),
                    observed_at=event.observed_at.astimezone(UTC),
                    trade_count=int(event.payload.get("trade_count", 1)),
                    flow_id=event.event_id,
                )
            )
        return projected, last_seen_message_id


def _stream_message_id_gt(message_id: str, other_message_id: str) -> bool:
    current_ms, current_seq = _parse_stream_message_id(message_id)
    other_ms, other_seq = _parse_stream_message_id(other_message_id)
    return (current_ms, current_seq) > (other_ms, other_seq)


def _parse_stream_message_id(message_id: str) -> tuple[int, int]:
    left, _, right = message_id.partition("-")
    try:
        return int(left), int(right or "0")
    except ValueError:
        return 0, 0


def _prioritize_refresh_entries(imported_entries, active_registry):
    prioritized = []
    seen_wallets: set[str] = set()
    for entry in list(imported_entries) + list(active_registry):
        if entry.wallet_address in seen_wallets:
            continue
        seen_wallets.add(entry.wallet_address)
        prioritized.append(entry)
    return prioritized