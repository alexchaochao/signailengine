from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta

from pydantic import BaseModel, Field

from core.config import AppSettings
from core.event_flow import publish_raw_events
from core.pipeline import PipelineResult, PipelineWorker
from core.schemas import PortfolioSnapshot, PositionState, VenueType
from infra.postgres import get_engine, init_storage
from infra.redis_stream import get_redis_client
from sentinel.onchain_listener import build_onchain_event
from sentinel.wallet_tracker import build_wallet_event


class PaperScenarioStep(BaseModel):
    name: str
    token: str
    route: str
    risk_allowed: bool
    execution_status: str | None = None
    executed_notional_usd: float | None = None
    position_open: bool
    open_positions: int


class PaperScenarioReport(BaseModel):
    scenario: str
    token: str
    steps: list[PaperScenarioStep] = Field(default_factory=list)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run deterministic local paper strategy scenarios")
    parser.add_argument("scenario", choices=["entry", "exit", "roundtrip", "launch-entry"])
    parser.add_argument("--token", default="PAPERBONK")
    parser.add_argument("--chain", default="solana")
    parser.add_argument("--json", action="store_true")
    return parser


def run_entry_scenario(
    settings: AppSettings,
    worker: PipelineWorker,
    *,
    token: str,
    chain: str,
    observed_at: datetime | None = None,
) -> PaperScenarioStep:
    _reset_scenario_state(worker, token)
    base_time = (observed_at or datetime.now(UTC)).astimezone(UTC)
    onchain_event = build_onchain_event(
        {
            "token": token,
            "chain": chain,
            "observed_at": base_time,
            "liquidity_usd": 150000.0,
            "volume_5m_usd": 150000.0,
            "buy_pressure": 1.0,
            "estimated_slippage_bps": 0.0,
            "feature_quality": {
                "buy_pressure": "ok",
                "estimated_slippage_bps": "ok",
            },
        }
    )
    wallet_event = build_wallet_event(
        {
            "token": token,
            "chain": chain,
            "observed_at": base_time + timedelta(seconds=1),
            "wallet_inflow_score": 0.70,
            "wallet_outflow_score": 0.0,
            "tracked_wallet_count": 8,
        }
    )
    result = _publish_and_process(settings, worker, [onchain_event, wallet_event])
    _require_route(result, expected_route="DEX_ENTRY")
    return _build_step("entry", result, worker)


def run_exit_scenario(
    settings: AppSettings,
    worker: PipelineWorker,
    *,
    token: str,
    chain: str,
    observed_at: datetime | None = None,
    seed_open_position: bool = True,
) -> PaperScenarioStep:
    repository = _require_repository(worker)
    if seed_open_position:
        _seed_open_position(worker, token)

    base_time = (observed_at or datetime.now(UTC)).astimezone(UTC)
    onchain_event = build_onchain_event(
        {
            "token": token,
            "chain": chain,
            "observed_at": base_time,
            "liquidity_usd": 5000.0,
            "volume_5m_usd": 1000.0,
            "buy_pressure": 0.12,
            "estimated_slippage_bps": 260.0,
            "feature_quality": {
                "buy_pressure": "missing",
                "estimated_slippage_bps": "stale",
            },
        }
    )
    result = _publish_and_process(settings, worker, [onchain_event])
    _require_route(result, expected_route="DEX_EXIT")
    return _build_step("exit", result, worker)


def run_roundtrip_scenario(
    settings: AppSettings,
    worker: PipelineWorker,
    *,
    token: str,
    chain: str,
    observed_at: datetime | None = None,
) -> PaperScenarioReport:
    base_time = (observed_at or datetime.now(UTC)).astimezone(UTC)
    _reset_scenario_state(worker, token)
    entry = run_entry_scenario(
        settings,
        worker,
        token=token,
        chain=chain,
        observed_at=base_time,
    )
    exit_step = run_exit_scenario(
        settings,
        worker,
        token=token,
        chain=chain,
        observed_at=base_time + timedelta(minutes=5),
        seed_open_position=False,
    )
    return PaperScenarioReport(scenario="roundtrip", token=token, steps=[entry, exit_step])


def run_launch_entry_scenario(
    settings: AppSettings,
    worker: PipelineWorker,
    *,
    token: str,
    chain: str,
    observed_at: datetime | None = None,
) -> PaperScenarioStep:
    _reset_scenario_state(worker, token)
    base_time = (observed_at or datetime.now(UTC)).astimezone(UTC)
    from core.schemas import EventEnvelope

    launch_event = EventEnvelope(
        event_id=f"launch:{chain}:{token}:{int(base_time.timestamp())}",
        event_type="alpha.launch_candidate",
        source="launch_alpha_backfill",
        chain=chain,
        token=token,
        observed_at=base_time,
        ingested_at=base_time,
        payload={
            "launch_candidate_status": "QUALIFIED",
            "launch_alpha_score": 0.93,
            "liquidity_usd": 180000.0,
            "volume_5m_usd": 55000.0,
            "buy_pressure": 0.86,
            "wallet_inflow_score": 0.74,
            "holder_growth_15m": 0.72,
            "estimated_slippage_bps": 70.0,
            "feature_quality": {"launch_alpha": "ok"},
        },
    )
    result = _publish_and_process(settings, worker, [launch_event])
    _require_route(result, expected_route="DEX_ENTRY")
    return _build_step("launch_entry", result, worker)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    settings = AppSettings.load()
    redis_client = get_redis_client(settings)
    engine = get_engine(settings)
    init_storage(engine)
    worker = PipelineWorker(settings, redis_client, db_engine=engine)

    scenario = args.scenario
    token = str(args.token)
    chain = str(args.chain)

    if scenario == "entry":
        report = PaperScenarioReport(
            scenario="entry",
            token=token,
            steps=[run_entry_scenario(settings, worker, token=token, chain=chain)],
        )
    elif scenario == "launch-entry":
        report = PaperScenarioReport(
            scenario="launch-entry",
            token=token,
            steps=[run_launch_entry_scenario(settings, worker, token=token, chain=chain)],
        )
    elif scenario == "exit":
        report = PaperScenarioReport(
            scenario="exit",
            token=token,
            steps=[run_exit_scenario(settings, worker, token=token, chain=chain)],
        )
    else:
        report = run_roundtrip_scenario(settings, worker, token=token, chain=chain)

    if args.json:
        print(report.model_dump_json(indent=2))
    else:
        print(_render_report(report))

    return 0


def _publish_and_process(
    settings: AppSettings,
    worker: PipelineWorker,
    events: list,
) -> PipelineResult:
    publish_raw_events(worker.redis_client, settings, *events)
    return worker.process_events(events)


def _build_step(name: str, result: PipelineResult, worker: PipelineWorker) -> PaperScenarioStep:
    repository = worker.repository
    execution = result.execution
    if execution is None and repository is not None and result.route.intent is not None:
        execution = repository.audit.load_latest_execution_report(result.route.intent.intent_id)
    if repository is not None:
        position = repository.state.load_position(result.signal.token)
        portfolio = repository.state.load_portfolio()
    else:
        position = worker.position_state
        portfolio = worker.portfolio_snapshot
    return PaperScenarioStep(
        name=name,
        token=result.signal.token,
        route=result.route.route,
        risk_allowed=result.risk.allowed,
        execution_status=execution.status if execution is not None else None,
        executed_notional_usd=(
            execution.executed_notional_usd if execution is not None else None
        ),
        position_open=position.is_open,
        open_positions=portfolio.open_positions,
    )


def _require_repository(worker: PipelineWorker):
    if worker.repository is None:
        raise RuntimeError("paper_scenario_requires_database")
    return worker.repository


def _reset_scenario_state(worker: PipelineWorker, token: str) -> None:
    repository = _require_repository(worker)
    repository.state.save_position(token, PositionState())
    repository.state.save_portfolio(PortfolioSnapshot())
    worker.position_state = PositionState()
    worker.portfolio_snapshot = PortfolioSnapshot()


def _seed_open_position(worker: PipelineWorker, token: str) -> None:
    repository = _require_repository(worker)
    position = PositionState(is_open=True, venue_type=VenueType.DEX, token_exposure=0.03)
    portfolio = PortfolioSnapshot(
        total_portfolio_usd=10000.0,
        token_exposure=0.03,
        chain_exposure=0.03,
        open_positions=1,
        daily_pnl_fraction=0.0,
    )
    repository.state.save_position(token, position)
    repository.state.save_portfolio(portfolio)
    worker.position_state = position
    worker.portfolio_snapshot = portfolio


def _require_route(result: PipelineResult, *, expected_route: str) -> None:
    if result.route.route != expected_route:
        raise RuntimeError(
            json.dumps(
                {
                    "expected_route": expected_route,
                    "actual_route": result.route.route,
                    "signal": result.signal.model_dump(mode="json"),
                    "risk": result.risk.model_dump(mode="json"),
                }
            )
        )
    if result.execution is None and (
        result.reconciliation is None
        or "duplicate_intent_skipped" not in result.reconciliation.reasons
    ):
        raise RuntimeError("paper_scenario_missing_execution")


def _render_report(report: PaperScenarioReport) -> str:
    lines = [f"scenario={report.scenario} token={report.token}"]
    for step in report.steps:
        lines.append(
            " ".join(
                [
                    f"step={step.name}",
                    f"route={step.route}",
                    f"risk_allowed={str(step.risk_allowed).lower()}",
                    f"execution_status={step.execution_status or 'none'}",
                    f"executed_notional_usd={step.executed_notional_usd or 0.0}",
                    f"position_open={str(step.position_open).lower()}",
                    f"open_positions={step.open_positions}",
                ]
            )
        )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())