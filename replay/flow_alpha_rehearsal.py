from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, Field
from redis import Redis
from sqlalchemy.engine import Engine

from core.config import AppSettings
from core.event_flow import publish_raw_events
from core.pipeline import PipelineWorker
from core.schemas import PortfolioSnapshot, PositionState
from discovery.flow_live_sources import build_flow_live_sources
from discovery.service import FlowMeasurementSyncService
from infra.postgres import get_engine, init_storage
from infra.redis_stream import ensure_consumer_group, get_redis_client
from infra.repository import StorageRepository
from sentinel.okx_wallet_registry_importer import TrackedWalletRegistryEntry
from sentinel.onchain_listener import build_onchain_event
from sentinel.wallet_score_aggregator import WalletTokenFlow


class FlowMeasurementRehearsalStep(BaseModel):
    token: str
    route: str
    state: str
    risk_allowed: bool
    execution_status: str | None = None


class FlowMeasurementRehearsalReport(BaseModel):
    dataset: str
    seeded_wallets: int
    seeded_flows: int
    ingested_candidates: int
    supplemental_events: int
    processed_results: int
    steps: list[FlowMeasurementRehearsalStep] = Field(default_factory=list)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a flow alpha end-to-end local rehearsal")
    parser.add_argument(
        "--dataset",
        default="replay/datasets/flow_alpha_entry_rehearsal.jsonl",
    )
    parser.add_argument("--group")
    parser.add_argument("--consumer", default="flow-rehearsal")
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--json", action="store_true")
    return parser


def run_flow_measurement_rehearsal(
    settings: AppSettings,
    *,
    dataset_path: str | Path,
    group: str | None = None,
    consumer: str = "flow-rehearsal",
    count: int = 50,
    redis_client: Redis | None = None,
    engine: Engine | None = None,
) -> FlowMeasurementRehearsalReport:
    rehearsal_settings = _build_rehearsal_settings(settings)
    client = redis_client or get_redis_client(rehearsal_settings)
    db_engine = engine or get_engine(rehearsal_settings)
    init_storage(db_engine)
    repository = StorageRepository(db_engine)
    rehearsal_group = group or f"flow-rehearsal-{uuid4().hex[:8]}"
    dataset = _load_rehearsal_dataset(dataset_path)

    _reset_rehearsal_state(repository, dataset.tokens)
    repository.wallet_intelligence.upsert_registry_entries(dataset.registry_entries)
    repository.wallet_intelligence.append_wallet_flows(dataset.wallet_flows)

    ensure_consumer_group(
        client,
        rehearsal_settings.redis.raw_events_stream,
        rehearsal_group,
        create_from_id="$",
    )

    worker = PipelineWorker(rehearsal_settings, client, db_engine=db_engine)
    worker.position_state = PositionState()
    worker.portfolio_snapshot = PortfolioSnapshot()

    service = FlowMeasurementSyncService(rehearsal_settings, client, repository)
    ingested_candidates = 0
    for source in build_flow_live_sources(rehearsal_settings, repository):
        for snapshot in source.fetch_snapshots():
            service.ingest_snapshot(
                snapshot.model_dump(mode="json"),
                source_name=source.config.source_name or "flow_measurement_live",
            )
            ingested_candidates += 1

    supplemental_events = len(dataset.onchain_events)
    if dataset.onchain_events:
        publish_raw_events(
            client,
            rehearsal_settings,
            *(build_onchain_event(payload, source="flow_alpha_rehearsal") for payload in dataset.onchain_events),
        )

    pipeline_results = worker.poll_once(rehearsal_group, consumer, count=count)
    return FlowMeasurementRehearsalReport(
        dataset=str(dataset_path),
        seeded_wallets=len(dataset.registry_entries),
        seeded_flows=len(dataset.wallet_flows),
        ingested_candidates=ingested_candidates,
        supplemental_events=supplemental_events,
        processed_results=len(pipeline_results),
        steps=[
            FlowMeasurementRehearsalStep(
                token=result.signal.token,
                route=result.route.route,
                state=result.transition.new_state.value,
                risk_allowed=result.risk.allowed,
                execution_status=(result.execution.status if result.execution is not None else None),
            )
            for result in pipeline_results
        ],
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = AppSettings.load()
    report = run_flow_measurement_rehearsal(
        settings,
        dataset_path=args.dataset,
        group=args.group,
        consumer=args.consumer,
        count=args.count,
    )
    if args.json:
        print(report.model_dump_json(indent=2))
    else:
        print(json.dumps(report.model_dump(mode="json"), indent=2))
    return 0


class _FlowAlphaRehearsalDataset(BaseModel):
    registry_entries: list[TrackedWalletRegistryEntry]
    wallet_flows: list[WalletTokenFlow]
    onchain_events: list[dict[str, object]]
    tokens: set[str]


def _build_rehearsal_settings(settings: AppSettings) -> AppSettings:
    acquisition = settings.acquisition
    flow_sources = dict(acquisition.flow_alpha_sources)
    source_key = "base_aero_wallet_flow"
    current = flow_sources.get(source_key)
    if current is None:
        raise ValueError("missing_flow_alpha_rehearsal_source")
    flow_sources[source_key] = current.model_copy(
        update={
            "enabled": True,
            "chain": "base",
            "token": "AERO",
            "venue": "aerodrome",
            "source_name": current.source_name or "flow_alpha_base_aero",
        }
    )
    return settings.model_copy(
        update={
            "acquisition": acquisition.model_copy(
                update={
                    "flow_alpha_sources": flow_sources,
                }
            )
        }
    )


def _reset_rehearsal_state(repository: StorageRepository, tokens: set[str]) -> None:
    for token in tokens:
        repository.state.save_position(token, PositionState())
    repository.state.save_portfolio(PortfolioSnapshot())


def _load_rehearsal_dataset(dataset_path: str | Path) -> _FlowAlphaRehearsalDataset:
    registry_entries: list[TrackedWalletRegistryEntry] = []
    wallet_flows: list[WalletTokenFlow] = []
    onchain_events: list[dict[str, object]] = []
    tokens: set[str] = set()
    path = Path(dataset_path)
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            record = json.loads(stripped)
            record_type = str(record.get("record_type", "")).strip()
            payload = record.get("payload")
            if not isinstance(payload, dict):
                raise ValueError("flow_alpha_rehearsal_payload_must_be_object")
            if record_type == "tracked_wallet_registry_entry":
                registry_entries.append(_build_registry_entry(payload))
                continue
            if record_type == "wallet_token_flow":
                flow = _build_wallet_flow(payload)
                wallet_flows.append(flow)
                tokens.add(flow.token)
                continue
            if record_type == "onchain_liquidity_snapshot":
                normalized_payload = dict(payload)
                normalized_payload["observed_at"] = _coerce_datetime(payload.get("observed_at"))
                onchain_events.append(normalized_payload)
                token = payload.get("token")
                if isinstance(token, str) and token:
                    tokens.add(token)
                continue
            raise ValueError(f"unsupported_flow_alpha_rehearsal_record_type:{record_type}")
    dataset = _FlowAlphaRehearsalDataset(
        registry_entries=registry_entries,
        wallet_flows=wallet_flows,
        onchain_events=onchain_events,
        tokens=tokens,
    )
    return _rebase_dataset_timestamps(dataset)


def _build_registry_entry(payload: dict[str, object]) -> TrackedWalletRegistryEntry:
    return TrackedWalletRegistryEntry(
        wallet_address=str(payload["wallet_address"]),
        chain=str(payload["chain"]),
        wallet_class=str(payload.get("wallet_class", "smart_money")),
        weight=float(payload.get("weight", 1.0)),
        status=str(payload.get("status", "active")),
        source=str(payload.get("source", "flow_alpha_rehearsal")),
        source_metadata=dict(payload.get("source_metadata", {})),
        version=str(payload.get("version", "okx_registry_v1")),
        discovered_at=_coerce_datetime(payload.get("discovered_at")),
        last_seen_at=_coerce_datetime(payload.get("last_seen_at")),
        updated_at=_coerce_datetime(payload.get("updated_at")),
    )


def _build_wallet_flow(payload: dict[str, object]) -> WalletTokenFlow:
    return WalletTokenFlow(
        chain=str(payload["chain"]),
        token=str(payload["token"]),
        wallet_address=str(payload["wallet_address"]),
        direction=str(payload["direction"]),
        notional_usd=float(payload.get("notional_usd", 0.0)),
        observed_at=_coerce_datetime(payload.get("observed_at")),
        trade_count=int(payload.get("trade_count", 1)),
        flow_id=(str(payload["flow_id"]) if payload.get("flow_id") is not None else None),
    )


def _coerce_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str) and value:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    return datetime.now(UTC)


def _rebase_dataset_timestamps(dataset: _FlowAlphaRehearsalDataset) -> _FlowAlphaRehearsalDataset:
    timestamps: list[datetime] = []
    timestamps.extend(entry.updated_at.astimezone(UTC) for entry in dataset.registry_entries)
    timestamps.extend(flow.observed_at.astimezone(UTC) for flow in dataset.wallet_flows)
    for payload in dataset.onchain_events:
        observed_at = payload.get("observed_at")
        if isinstance(observed_at, datetime):
            timestamps.append(observed_at.astimezone(UTC))
    if not timestamps:
        return dataset

    latest_timestamp = max(timestamps)
    target_latest = datetime.now(UTC).replace(microsecond=0)
    delta = target_latest - latest_timestamp

    rebased_registry_entries = [
        TrackedWalletRegistryEntry(
            wallet_address=entry.wallet_address,
            chain=entry.chain,
            wallet_class=entry.wallet_class,
            weight=entry.weight,
            status=entry.status,
            source=entry.source,
            source_metadata=entry.source_metadata,
            version=entry.version,
            discovered_at=entry.discovered_at + delta,
            last_seen_at=entry.last_seen_at + delta,
            updated_at=entry.updated_at + delta,
        )
        for entry in dataset.registry_entries
    ]
    rebased_wallet_flows = [
        WalletTokenFlow(
            chain=flow.chain,
            token=flow.token,
            wallet_address=flow.wallet_address,
            direction=flow.direction,
            notional_usd=flow.notional_usd,
            observed_at=flow.observed_at + delta,
            trade_count=flow.trade_count,
            flow_id=flow.flow_id,
        )
        for flow in dataset.wallet_flows
    ]
    rebased_onchain_events: list[dict[str, object]] = []
    for payload in dataset.onchain_events:
        rebased_payload = dict(payload)
        observed_at = payload.get("observed_at")
        if isinstance(observed_at, datetime):
            rebased_payload["observed_at"] = observed_at + delta
        rebased_onchain_events.append(rebased_payload)

    return _FlowAlphaRehearsalDataset(
        registry_entries=rebased_registry_entries,
        wallet_flows=rebased_wallet_flows,
        onchain_events=rebased_onchain_events,
        tokens=dataset.tokens,
    )


if __name__ == "__main__":
    raise SystemExit(main())