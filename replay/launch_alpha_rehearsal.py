from __future__ import annotations

import argparse
import json
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, Field
from redis import Redis
from sqlalchemy.engine import Engine

from core.config import AppSettings
from core.pipeline import PipelineWorker
from core.schemas import PortfolioSnapshot, PositionState
from infra.redis_stream import ensure_consumer_group
from infra.postgres import get_engine, init_storage
from infra.redis_stream import get_redis_client
from infra.repository import StorageRepository
from discovery.service import LaunchAlphaSyncService


class LaunchAlphaRehearsalStep(BaseModel):
    token: str
    route: str
    state: str
    risk_allowed: bool
    execution_status: str | None = None


class LaunchAlphaRehearsalReport(BaseModel):
    dataset: str
    ingested_candidates: int
    processed_results: int
    steps: list[LaunchAlphaRehearsalStep] = Field(default_factory=list)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a launch alpha end-to-end local rehearsal")
    parser.add_argument(
        "--dataset",
        default="replay/datasets/launch_alpha_entry_rehearsal.jsonl",
    )
    parser.add_argument("--group")
    parser.add_argument("--consumer", default="launch-rehearsal")
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--json", action="store_true")
    return parser


def run_launch_alpha_rehearsal(
    settings: AppSettings,
    *,
    dataset_path: str | Path,
    group: str | None = None,
    consumer: str = "launch-rehearsal",
    count: int = 50,
    redis_client: Redis | None = None,
    engine: Engine | None = None,
) -> LaunchAlphaRehearsalReport:
    client = redis_client or get_redis_client(settings)
    db_engine = engine or get_engine(settings)
    init_storage(db_engine)
    repository = StorageRepository(db_engine)
    rehearsal_group = group or f"launch-rehearsal-{uuid4().hex[:8]}"
    _reset_rehearsal_state(repository, dataset_path)
    ensure_consumer_group(
        client,
        settings.redis.raw_events_stream,
        rehearsal_group,
        create_from_id="$",
    )
    worker = PipelineWorker(settings, client, db_engine=db_engine)
    worker.position_state = PositionState()
    worker.portfolio_snapshot = PortfolioSnapshot()
    service = LaunchAlphaSyncService(settings, client, repository)
    sync_results = service.ingest_jsonl(dataset_path)
    pipeline_results = worker.poll_once(rehearsal_group, consumer, count=count)
    return LaunchAlphaRehearsalReport(
        dataset=str(dataset_path),
        ingested_candidates=len(sync_results),
        processed_results=len(pipeline_results),
        steps=[
            LaunchAlphaRehearsalStep(
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
    report = run_launch_alpha_rehearsal(
        settings,
        dataset_path=args.dataset,
        group=args.group,
        consumer=args.consumer,
        count=args.count,
    )
    if args.json:
        print(report.model_dump_json(indent=2))
    else:
        print(
            json.dumps(
                report.model_dump(mode="json"),
                indent=2,
            )
        )
    return 0


def _reset_rehearsal_state(repository: StorageRepository, dataset_path: str | Path) -> None:
    tokens = _load_rehearsal_tokens(dataset_path)
    for token in tokens:
        repository.state.save_position(token, PositionState())
    repository.state.save_portfolio(PortfolioSnapshot())


def _load_rehearsal_tokens(dataset_path: str | Path) -> set[str]:
    tokens: set[str] = set()
    path = Path(dataset_path)
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            record = json.loads(stripped)
            payload = record.get("payload")
            if isinstance(payload, dict):
                token = payload.get("token")
                if isinstance(token, str) and token:
                    tokens.add(token)
    return tokens


if __name__ == "__main__":
    raise SystemExit(main())