from __future__ import annotations

import os

from sqlalchemy import create_engine

from core.config import AppSettings
from replay.launch_alpha_rehearsal import build_parser, run_launch_alpha_rehearsal
from tests.test_pipeline import FakeRedis


def _load_default_settings(monkeypatch) -> AppSettings:
    for key in list(os.environ):
        if key.startswith("SIGNALENGINE_"):
            monkeypatch.delenv(key, raising=False)
    return AppSettings.load()


def test_launch_alpha_rehearsal_parser_supports_json() -> None:
    parser = build_parser()

    args = parser.parse_args(["--json"])

    assert args.json is True
    assert args.dataset.endswith("launch_alpha_entry_rehearsal.jsonl")


def test_launch_alpha_rehearsal_runs_worker_end_to_end(monkeypatch) -> None:
    settings = _load_default_settings(monkeypatch)
    engine = create_engine("sqlite:///:memory:")
    report = run_launch_alpha_rehearsal(
        settings,
        dataset_path="/home/alex/Desktop/signalengine/replay/datasets/launch_alpha_entry_rehearsal.jsonl",
        redis_client=FakeRedis(),
        engine=engine,
    )

    assert report.ingested_candidates == 1
    assert report.processed_results == 1
    assert report.steps[0].route == "DEX_ENTRY"
    assert report.steps[0].execution_status == "FILLED"