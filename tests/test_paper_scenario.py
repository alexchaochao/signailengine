from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from core.pipeline import PipelineWorker
from infra.postgres import init_storage
from replay.paper_scenario import (
    build_parser,
    run_entry_scenario,
    run_launch_entry_scenario,
    run_roundtrip_scenario,
)
from tests.test_pipeline import FakeRedis


def test_paper_scenario_parser_supports_roundtrip() -> None:
    parser = build_parser()

    args = parser.parse_args(["roundtrip", "--token", "PAPERBONK", "--chain", "solana", "--json"])

    assert args.scenario == "roundtrip"
    assert args.token == "PAPERBONK"
    assert args.chain == "solana"
    assert args.json is True


def test_paper_scenario_parser_supports_launch_entry() -> None:
    parser = build_parser()

    args = parser.parse_args(["launch-entry", "--token", "PAPERLAUNCH"])

    assert args.scenario == "launch-entry"
    assert args.token == "PAPERLAUNCH"


def test_run_entry_scenario_executes_paper_trade() -> None:
    settings = AppSettings.load()
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    worker = PipelineWorker(settings, cast(Redis, FakeRedis()), db_engine=engine)

    step = run_entry_scenario(
        settings,
        worker,
        token="PAPERBONK",
        chain="solana",
        observed_at=datetime(2026, 5, 3, 14, 30, tzinfo=UTC),
    )

    assert step.route == "DEX_ENTRY"
    assert step.risk_allowed is True
    assert step.execution_status == "FILLED"
    assert step.position_open is True
    assert step.open_positions == 1


def test_run_roundtrip_scenario_opens_and_closes_position() -> None:
    settings = AppSettings.load()
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    worker = PipelineWorker(settings, cast(Redis, FakeRedis()), db_engine=engine)

    report = run_roundtrip_scenario(
        settings,
        worker,
        token="PAPERROUND",
        chain="solana",
        observed_at=datetime(2026, 5, 3, 14, 30, tzinfo=UTC),
    )

    assert [step.route for step in report.steps] == ["DEX_ENTRY", "DEX_EXIT"]
    assert report.steps[0].position_open is True
    assert report.steps[1].position_open is False
    assert report.steps[1].open_positions == 0


def test_run_launch_entry_scenario_executes_paper_trade() -> None:
    settings = AppSettings.load()
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    worker = PipelineWorker(settings, cast(Redis, FakeRedis()), db_engine=engine)

    step = run_launch_entry_scenario(
        settings,
        worker,
        token="PAPERLAUNCH",
        chain="solana",
        observed_at=datetime(2026, 5, 3, 14, 35, tzinfo=UTC),
    )

    assert step.route == "DEX_ENTRY"
    assert step.risk_allowed is True
    assert step.execution_status == "FILLED"
    assert step.position_open is True
    assert step.open_positions == 1