from __future__ import annotations

import os

from sqlalchemy import create_engine

from core.config import AppSettings
from replay.flow_alpha_rehearsal import build_parser, run_flow_measurement_rehearsal
from tests.test_pipeline import FakeRedis


def _load_default_settings(monkeypatch) -> AppSettings:
    for key in list(os.environ):
        if key.startswith("SIGNALENGINE_"):
            monkeypatch.delenv(key, raising=False)
    return AppSettings.load()


def test_flow_alpha_rehearsal_parser_supports_json() -> None:
    parser = build_parser()

    args = parser.parse_args(["--json"])

    assert args.json is True
    assert args.dataset.endswith("flow_alpha_entry_rehearsal.jsonl")


def test_flow_alpha_rehearsal_runs_worker_end_to_end(monkeypatch) -> None:
    settings = _load_default_settings(monkeypatch)
    engine = create_engine("sqlite:///:memory:")
    report = run_flow_measurement_rehearsal(
        settings,
        dataset_path="/home/alex/Desktop/signalengine/replay/datasets/flow_alpha_entry_rehearsal.jsonl",
        redis_client=FakeRedis(),
        engine=engine,
    )

    assert report.seeded_wallets == 4
    assert report.seeded_flows == 5
    assert report.ingested_candidates == 1
    assert report.supplemental_events == 1
    assert report.processed_results == 1
    assert report.steps[0].route == "DEX_ENTRY"
    assert report.steps[0].risk_allowed is True
    assert report.steps[0].execution_status == "FILLED"