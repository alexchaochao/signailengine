from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from redis import Redis
from sqlalchemy import create_engine

from core.config import AppSettings
from core.pipeline import PipelineWorker
from core.schemas import (
    EventEnvelope,
    ExecutionReport,
    FeatureSnapshot,
    PreparedExecution,
    RawEventRecord,
    TokenState,
)
from execution.dex_executor import DexPaperExecutor
from infra.postgres import init_storage
from infra.repository import StorageRepository
from replay.runner import (
    ReplayRedis,
    build_parser,
    build_replay_batches,
    filter_replay_events,
    load_replay_events,
    main,
    render_replay_comparison,
    render_feature_replay_summary,
    render_replay_regression,
    replay_events,
    run_replay,
    run_replay_comparison,
    run_feature_replay_from_db,
    run_replay_regression,
    run_replay_report,
    write_replay_report,
)
from sentinel.onchain_listener import build_onchain_event
from sentinel.wallet_tracker import build_wallet_event


class RetryOnceExecutor(DexPaperExecutor):
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, prepared: PreparedExecution):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("transient_dex_error")
        return ExecutionReport(
            intent_id=prepared.intent.intent_id,
            venue_type=prepared.intent.venue_type,
            venue=prepared.intent.venue,
            adapter_name=self.adapter_name,
            external_order_id=f"dex-paper:{prepared.intent.intent_id}",
            quote_id=prepared.quote.quote_id,
            status="FILLED",
            executed_notional_usd=prepared.requested_notional_usd,
            message="paper_dex_execution",
            simulation=True,
            timestamp=datetime.now(UTC),
        )


def _event(event_id: str, token: str, observed_at: datetime) -> EventEnvelope:
    return EventEnvelope(
        event_id=event_id,
        event_type="onchain.liquidity_snapshot",
        source="test",
        chain="solana",
        token=token,
        observed_at=observed_at,
        ingested_at=observed_at,
        payload={"liquidity_usd": 100_000},
    )


def test_load_replay_events_orders_by_observed_at_then_event_id(tmp_path) -> None:
    source_path = tmp_path / "dataset.jsonl"
    later = _event("b", "BONK", datetime(2026, 5, 2, 0, 0, 2, tzinfo=UTC))
    earlier_second = _event("b", "WIF", datetime(2026, 5, 2, 0, 0, 1, tzinfo=UTC))
    earlier_first = _event("a", "BONK", datetime(2026, 5, 2, 0, 0, 1, tzinfo=UTC))
    source_path.write_text(
        "\n".join(
            [
                later.model_dump_json(),
                earlier_second.model_dump_json(),
                earlier_first.model_dump_json(),
            ]
        ),
        encoding="utf-8",
    )

    events = load_replay_events(source_path)

    assert [event.event_id for event in events] == ["a", "b", "b"]
    assert [event.token for event in events] == ["BONK", "WIF", "BONK"]


def test_run_replay_returns_summary(tmp_path) -> None:
    source_path = tmp_path / "dataset.jsonl"
    first = _event("event-1", "BONK", datetime(2026, 5, 2, 0, 0, 1, tzinfo=UTC))
    second = _event("event-2", "WIF", datetime(2026, 5, 2, 0, 0, 3, tzinfo=UTC))
    source_path.write_text(
        "\n".join([first.model_dump_json(), second.model_dump_json()]),
        encoding="utf-8",
    )

    summary = run_replay(source_path)

    assert summary.source_path == source_path
    assert summary.event_count == 2
    assert summary.batch_count == 2
    assert summary.pipeline_run_count == 2
    assert summary.token_count == 2
    assert summary.intent_count == 0
    assert summary.execution_count == 0
    assert summary.rejected_count == 0
    assert summary.first_observed_at == "2026-05-02T00:00:01+00:00"
    assert summary.last_observed_at == "2026-05-02T00:00:03+00:00"


def test_run_replay_report_includes_batch_details(tmp_path) -> None:
    observed_at = datetime(2026, 5, 2, 0, 0, 1, tzinfo=UTC)
    source_path = tmp_path / "dataset.jsonl"
    source_path.write_text(
        "\n".join(
            [
                build_onchain_event(
                    {
                        "event_id": "onchain-1",
                        "token": "BONK",
                        "observed_at": observed_at,
                        "liquidity_usd": 180_000,
                        "volume_5m_usd": 60_000,
                        "buy_pressure": 0.82,
                        "estimated_slippage_bps": 90,
                    },
                    source="dataset",
                ).model_dump_json(),
                build_wallet_event(
                    {
                        "event_id": "wallet-1",
                        "token": "BONK",
                        "observed_at": observed_at,
                        "wallet_inflow_score": 0.70,
                    },
                    source="dataset",
                ).model_dump_json(),
            ]
        ),
        encoding="utf-8",
    )

    report = run_replay_report(source_path)

    assert report.summary.pipeline_run_count == 1
    assert len(report.batches) == 1
    assert report.batches[0].route == "DEX_ENTRY"
    assert report.batches[0].signal.state_candidate == "NARRATIVE_EXPLOSION"
    assert report.batches[0].signal.alpha_score > 0.55
    assert report.batches[0].intent is not None
    assert report.batches[0].intent.strategy == "dex_momentum_v1"
    assert report.batches[0].risk.allowed is True
    assert report.batches[0].execution is not None
    assert report.batches[0].execution.status == "FILLED"
    assert report.batches[0].reconciliation is not None
    assert report.batches[0].reconciliation.applied is True
    assert report.batches[0].diagnostics.event_count == 2
    assert report.batches[0].diagnostics.average_ingest_latency_seconds >= 0.0
    assert (
        report.batches[0].diagnostics.max_ingest_latency_seconds
        >= report.batches[0].diagnostics.average_ingest_latency_seconds
    )
    assert report.batches[0].diagnostics.estimated_slippage_bps == 90.0
    assert report.batches[0].diagnostics.slippage_budget_bps == 90
    assert report.batches[0].diagnostics.slippage_headroom_bps == 0.0
    assert report.batches[0].diagnostics.signal_to_risk_latency_seconds >= 0.0
    assert report.batches[0].ledger_statuses == ["SUBMITTED", "FILLED", "RECONCILED"]
    assert report.attribution.route_counts == {"DEX_ENTRY": 1}
    assert report.attribution.execution_status_counts == {"FILLED": 1}
    assert report.attribution.fill_rate == 1.0
    assert report.attribution.strategy_requested_notional_usd == {
        "dex_momentum_v1": report.batches[0].intent.target_notional_usd,
    }
    assert report.attribution.strategy_executed_notional_usd == {
        "dex_momentum_v1": report.batches[0].execution.executed_notional_usd,
    }


def test_run_replay_handles_empty_dataset(tmp_path) -> None:
    source_path = tmp_path / "empty.jsonl"
    source_path.write_text("\n", encoding="utf-8")

    summary = run_replay(source_path)

    assert summary.event_count == 0
    assert summary.batch_count == 0
    assert summary.pipeline_run_count == 0
    assert summary.token_count == 0
    assert summary.first_observed_at is None
    assert summary.last_observed_at is None


def test_build_replay_batches_groups_same_token_same_timestamp() -> None:
    observed_at = datetime(2026, 5, 2, 0, 0, 1, tzinfo=UTC)
    events = [
        _event("a", "BONK", observed_at),
        _event("b", "BONK", observed_at),
        _event("c", "WIF", observed_at),
    ]

    batches = build_replay_batches(events)

    assert len(batches) == 2
    assert [event.event_id for event in batches[0]] == ["a", "b"]
    assert [event.event_id for event in batches[1]] == ["c"]


def test_filter_replay_events_filters_by_token_and_time() -> None:
    events = [
        _event("a", "BONK", datetime(2026, 5, 2, 0, 0, 1, tzinfo=UTC)),
        _event("b", "WIF", datetime(2026, 5, 2, 0, 0, 2, tzinfo=UTC)),
        _event("c", "BONK", datetime(2026, 5, 2, 0, 0, 3, tzinfo=UTC)),
    ]

    filtered, filters = filter_replay_events(
        events,
        tokens=["BONK"],
        start_observed_at="2026-05-02T00:00:02Z",
        end_observed_at="2026-05-02T00:00:03Z",
    )

    assert [event.event_id for event in filtered] == ["c"]
    assert filters.tokens == ["BONK"]
    assert filters.source_event_count == 3
    assert filters.selected_event_count == 1


def test_run_replay_processes_pipeline_batches(tmp_path) -> None:
    observed_at = datetime(2026, 5, 2, 0, 0, 1, tzinfo=UTC)
    source_path = tmp_path / "dataset.jsonl"
    onchain_event = build_onchain_event(
        {
            "event_id": "onchain-1",
            "token": "BONK",
            "observed_at": observed_at,
            "liquidity_usd": 180_000,
            "volume_5m_usd": 60_000,
            "buy_pressure": 0.82,
            "estimated_slippage_bps": 90,
        },
        source="dataset",
    )
    wallet_event = build_wallet_event(
        {
            "event_id": "wallet-1",
            "token": "BONK",
            "observed_at": observed_at,
            "wallet_inflow_score": 0.70,
        },
        source="dataset",
    )
    source_path.write_text(
        "\n".join([onchain_event.model_dump_json(), wallet_event.model_dump_json()]),
        encoding="utf-8",
    )

    summary = run_replay(source_path, settings=AppSettings.load())

    assert summary.event_count == 2
    assert summary.batch_count == 1
    assert summary.pipeline_run_count == 1
    assert summary.intent_count == 1
    assert summary.execution_count == 1
    assert summary.rejected_count == 0


def test_replay_parser_supports_json_flag() -> None:
    parser = build_parser()

    args = parser.parse_args([
        "replay/datasets/phase5_smoke.jsonl",
        "replay/datasets/phase5_risk_reject.jsonl",
        "--json",
        "--output",
        "report.json",
        "--baseline",
        "replay/datasets/phase5_smoke.jsonl",
        "--candidate",
        "replay/datasets/phase5_risk_reject.jsonl",
        "--token",
        "BONK",
        "--start-observed-at",
        "2026-05-02T00:00:00Z",
        "--end-observed-at",
        "2026-05-02T00:01:00Z",
    ])

    assert args.dataset_paths == [
        "replay/datasets/phase5_smoke.jsonl",
        "replay/datasets/phase5_risk_reject.jsonl",
    ]
    assert args.json is True
    assert args.output == "report.json"
    assert args.baseline == "replay/datasets/phase5_smoke.jsonl"
    assert args.candidate == "replay/datasets/phase5_risk_reject.jsonl"
    assert args.token == ["BONK"]
    assert args.start_observed_at == "2026-05-02T00:00:00Z"
    assert args.end_observed_at == "2026-05-02T00:01:00Z"


def test_replay_parser_supports_feature_replay_db_mode() -> None:
    parser = build_parser()

    args = parser.parse_args(
        [
            "--feature-replay-db-url",
            "sqlite:////tmp/feature.db",
            "--feature-chain",
            "solana",
            "--feature-token",
            "BONK",
            "--json",
        ]
    )

    assert args.feature_replay_db_url == "sqlite:////tmp/feature.db"
    assert args.feature_chain == "solana"
    assert args.feature_token == "BONK"
    assert args.json is True


def test_replay_main_prints_json_summary(tmp_path, capsys) -> None:
    source_path = tmp_path / "dataset.jsonl"
    source_path.write_text(
        _event("event-1", "BONK", datetime(2026, 5, 2, 0, 0, 1, tzinfo=UTC)).model_dump_json(),
        encoding="utf-8",
    )

    exit_code = main([str(source_path), "--json"])

    captured = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert captured["source_path"] == str(source_path)
    assert captured["event_count"] == 1


def test_run_feature_replay_from_db_rebuilds_summary(tmp_path) -> None:
    db_path = tmp_path / "feature-replay.db"
    engine = create_engine(f"sqlite:///{db_path}")
    init_storage(engine)
    repository = StorageRepository(engine)
    repository.raw_events.save(
        RawEventRecord(
            source_type="onchain_trade",
            source_name="solana_ws",
            source_event_id="solana:tx-1:1:BONK",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 2, 0, 0, 1, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 2, 0, 0, 1, tzinfo=UTC),
            cursor="1",
            payload={
                "pool_address": "pool-1",
                "wallet_address": "wallet-1",
                "token": "BONK",
                "token_amount": 10.0,
                "quote_amount": 2.0,
                "quote_amount_usd": 100.0,
                "side": "buy",
            },
        )
    )
    repository.raw_events.save(
        RawEventRecord(
            source_type="dex_quote",
            source_name="jupiter_quote",
            source_event_id="q1",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 2, 0, 0, 3, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 2, 0, 0, 4, tzinfo=UTC),
            cursor="req-1",
            payload={
                "quote_request_id": "req-1",
                "token": "BONK",
                "quote_notional_usd": 5000.0,
                "expected_out_usd": 4860.0,
                "reference_mid_usd": 5000.0,
                "route_summary": {"provider": "jupiter", "hops": 2},
            },
        )
    )
    repository.features.save_snapshot(
        FeatureSnapshot(
            chain="solana",
            token="BONK",
            feature_name="estimated_slippage_bps",
            feature_value=280.0,
            window_name="latest",
            as_of=datetime(2026, 5, 2, 0, 0, 3, tzinfo=UTC),
            sample_count=1,
            freshness_seconds=0.0,
            quality_flag="ok",
            formula_version="slip_v1",
            inputs={"quote_notional_usd": 5000.0, "route_provider": "jupiter"},
        )
    )

    summary = run_feature_replay_from_db(f"sqlite:///{db_path}", chain="solana", token="BONK")

    assert summary.raw_event_count == 2
    assert summary.replayed_trade_count == 1
    assert summary.replayed_quote_count == 1
    assert summary.ignored_event_count == 0
    assert len(summary.snapshot_diffs) == 2
    assert summary.snapshot_diffs[1].feature_name == "estimated_slippage_bps"
    assert summary.snapshot_diffs[1].source_inputs["route_provider"] == "jupiter"


def test_replay_main_prints_feature_replay_json(tmp_path, capsys) -> None:
    db_path = tmp_path / "feature-replay-main.db"
    engine = create_engine(f"sqlite:///{db_path}")
    init_storage(engine)
    repository = StorageRepository(engine)
    repository.raw_events.save(
        RawEventRecord(
            source_type="dex_quote",
            source_name="jupiter_quote",
            source_event_id="q1",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 2, 0, 0, 3, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 2, 0, 0, 4, tzinfo=UTC),
            cursor="req-1",
            payload={
                "quote_request_id": "req-1",
                "token": "BONK",
                "quote_notional_usd": 5000.0,
                "expected_out_usd": 4860.0,
                "reference_mid_usd": 5000.0,
                "route_summary": {"provider": "jupiter", "hops": 2},
            },
        )
    )

    exit_code = main([
        "--feature-replay-db-url",
        f"sqlite:///{db_path}",
        "--feature-chain",
        "solana",
        "--feature-token",
        "BONK",
        "--json",
    ])

    captured = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert captured["raw_event_count"] == 1
    assert captured["replayed_quote_count"] == 1
    assert "snapshot_diffs" in captured
    assert "source_inputs" in captured["snapshot_diffs"][1]



def test_write_replay_report_writes_json_file(tmp_path) -> None:
    source_path = Path("/home/alex/Desktop/signalengine/replay/datasets/phase5_smoke.jsonl")
    output_path = tmp_path / "report.json"

    report = run_replay_report(source_path)
    written_path = write_replay_report(report, output_path)
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert written_path == output_path
    assert written["summary"]["event_count"] == 2
    assert written["filters"]["selected_event_count"] == 2
    assert written["attribution"]["route_counts"] == {"DEX_ENTRY": 1}
    assert len(written["batches"]) == 1
    assert written["batches"][0]["signal"]["state_candidate"] == "NARRATIVE_EXPLOSION"
    assert written["batches"][0]["diagnostics"]["estimated_slippage_bps"] == 90.0


def test_replay_main_writes_output_report(tmp_path, capsys) -> None:
    source_path = Path("/home/alex/Desktop/signalengine/replay/datasets/phase5_smoke.jsonl")
    output_path = tmp_path / "report.json"

    exit_code = main([str(source_path), "--output", str(output_path)])

    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert "source_path=" in capsys.readouterr().out
    assert written["summary"]["pipeline_run_count"] == 1


def test_run_replay_report_applies_token_filter(tmp_path) -> None:
    observed_at = datetime(2026, 5, 2, 0, 0, 1, tzinfo=UTC)
    source_path = tmp_path / "dataset.jsonl"
    source_path.write_text(
        "\n".join(
            [
                build_onchain_event(
                    {
                        "event_id": "bonk-onchain",
                        "token": "BONK",
                        "observed_at": observed_at,
                        "liquidity_usd": 180_000,
                        "volume_5m_usd": 60_000,
                        "buy_pressure": 0.82,
                        "estimated_slippage_bps": 90,
                    }
                ).model_dump_json(),
                build_wallet_event(
                    {
                        "event_id": "bonk-wallet",
                        "token": "BONK",
                        "observed_at": observed_at,
                        "wallet_inflow_score": 0.70,
                    }
                ).model_dump_json(),
                build_onchain_event(
                    {
                        "event_id": "wif-onchain",
                        "token": "WIF",
                        "observed_at": observed_at,
                        "liquidity_usd": 180_000,
                        "volume_5m_usd": 60_000,
                        "buy_pressure": 0.82,
                        "estimated_slippage_bps": 90,
                    }
                ).model_dump_json(),
                build_wallet_event(
                    {
                        "event_id": "wif-wallet",
                        "token": "WIF",
                        "observed_at": observed_at,
                        "wallet_inflow_score": 0.70,
                    }
                ).model_dump_json(),
            ]
        ),
        encoding="utf-8",
    )

    report = run_replay_report(source_path, tokens=["BONK"])

    assert report.summary.event_count == 2
    assert report.summary.token_count == 1
    assert report.filters.selected_event_count == 2
    assert len(report.batches) == 1
    assert report.batches[0].token == "BONK"


def test_run_replay_report_applies_time_filter(tmp_path) -> None:
    first_time = datetime(2026, 5, 2, 0, 0, 1, tzinfo=UTC)
    second_time = datetime(2026, 5, 2, 0, 0, 5, tzinfo=UTC)
    source_path = tmp_path / "dataset.jsonl"
    source_path.write_text(
        "\n".join(
            [
                _event("event-1", "BONK", first_time).model_dump_json(),
                _event("event-2", "BONK", second_time).model_dump_json(),
            ]
        ),
        encoding="utf-8",
    )

    report = run_replay_report(
        source_path,
        start_observed_at="2026-05-02T00:00:04Z",
        end_observed_at="2026-05-02T00:00:05Z",
    )

    assert report.summary.event_count == 1
    assert report.filters.selected_event_count == 1
    assert report.summary.first_observed_at == "2026-05-02T00:00:05+00:00"


def test_phase5_smoke_dataset_exists_and_replays() -> None:
    source_path = Path("/home/alex/Desktop/signalengine/replay/datasets/phase5_smoke.jsonl")

    summary = run_replay(source_path)

    assert source_path.exists()
    assert summary.event_count == 2
    assert summary.batch_count == 1
    assert summary.pipeline_run_count == 1


def test_phase5_risk_reject_dataset_exists_and_replays() -> None:
    source_path = Path("/home/alex/Desktop/signalengine/replay/datasets/phase5_risk_reject.jsonl")

    report = run_replay_report(source_path)

    assert source_path.exists()
    assert report.summary.rejected_count == 1
    assert report.batches[0].risk.allowed is False
    assert "liquidity_below_minimum" in report.batches[0].risk.violations
    assert report.attribution.risk_rejection_reason_counts["liquidity_below_minimum"] == 1


def test_phase5_retry_once_dataset_replays_with_retrying_executor() -> None:
    source_path = Path("/home/alex/Desktop/signalengine/replay/datasets/phase5_retry_once.jsonl")
    settings = AppSettings.load().model_copy(
        update={
            "execution": AppSettings.load().execution.model_copy(update={"max_retries": 1})
        }
    )
    worker = PipelineWorker(
        settings,
        cast(Redis, ReplayRedis()),
        db_engine=create_engine("sqlite:///:memory:"),
        dex_executor=RetryOnceExecutor(),
    )
    events = load_replay_events(source_path)

    results = replay_events(events, settings=settings, worker=worker)
    report = run_replay_report(
        source_path,
        settings=settings,
        worker=PipelineWorker(
            settings,
            cast(Redis, ReplayRedis()),
            db_engine=create_engine("sqlite:///:memory:"),
            dex_executor=RetryOnceExecutor(),
        ),
    )

    assert source_path.exists()
    assert len(results) == 1
    assert report.summary.execution_count == 1
    assert report.batches[0].execution is not None
    assert report.batches[0].execution.status == "FILLED"


def test_phase5_duplicate_dataset_skips_second_replay() -> None:
    source_path = Path(
        "/home/alex/Desktop/signalengine/replay/datasets/phase5_duplicate_intent.jsonl"
    )
    worker = PipelineWorker(
        AppSettings.load(),
        cast(Redis, ReplayRedis()),
        db_engine=create_engine("sqlite:///:memory:"),
    )
    events = load_replay_events(source_path)

    replay_events(events, worker=worker)
    second_results = replay_events(events, worker=worker)

    assert source_path.exists()
    assert len(second_results) == 1
    assert second_results[0].reconciliation is not None
    assert second_results[0].reconciliation.reasons == ["duplicate_intent_skipped"]


def test_replay_events_default_worker_uses_persisted_fsm_state() -> None:
    observed_at = datetime(2026, 5, 5, 12, 0, tzinfo=UTC)
    events = [
        build_onchain_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "liquidity_usd": 180_000,
                "volume_5m_usd": 60_000,
                "buy_pressure": 0.82,
                "estimated_slippage_bps": 90,
            }
        ),
        build_wallet_event(
            {
                "token": "BONK",
                "observed_at": observed_at,
                "wallet_inflow_score": 0.70,
            }
        ),
        build_onchain_event(
            {
                "token": "BONK",
                "observed_at": datetime(2026, 5, 5, 12, 1, tzinfo=UTC),
                "liquidity_usd": 20_000,
                "volume_5m_usd": 8_000,
                "buy_pressure": 0.40,
                "estimated_slippage_bps": 110,
            }
        ),
    ]

    results = replay_events(events)

    assert len(results) == 2
    assert results[0].transition.new_state == TokenState.NARRATIVE_EXPLOSION
    assert results[1].transition.previous_state == TokenState.NARRATIVE_EXPLOSION
    assert results[1].transition.new_state == TokenState.NARRATIVE_EXPLOSION


def test_run_replay_comparison_returns_rows_for_multiple_datasets() -> None:
    report = run_replay_comparison(
        [
            "/home/alex/Desktop/signalengine/replay/datasets/phase5_smoke.jsonl",
            "/home/alex/Desktop/signalengine/replay/datasets/phase5_risk_reject.jsonl",
        ]
    )

    assert len(report.rows) == 2
    assert report.rows[0].dataset_path.endswith("phase5_smoke.jsonl")
    assert report.rows[1].dataset_path.endswith("phase5_risk_reject.jsonl")
    assert report.runs[0].batches[0].diagnostics.estimated_slippage_bps == 90.0
    assert report.rows[0].average_estimated_slippage_bps == 90.0
    assert report.rows[0].average_signal_to_risk_latency_seconds >= 0.0


def test_run_replay_regression_reports_baseline_candidate_deltas() -> None:
    report = run_replay_regression(
        "/home/alex/Desktop/signalengine/replay/datasets/phase5_smoke.jsonl",
        "/home/alex/Desktop/signalengine/replay/datasets/phase5_risk_reject.jsonl",
    )

    assert report.baseline.summary.source_path.name == "phase5_smoke.jsonl"
    assert report.candidate.summary.source_path.name == "phase5_risk_reject.jsonl"
    assert report.summary_deltas["execution_count"] == -1
    assert report.attribution_deltas["fill_rate"] == -1.0
    assert report.attribution_deltas["rejection_rate"] == 1.0
    assert "fill_rate:global" in report.regressions
    assert "rejection_rate:global" in report.regressions
    assert any(
        judgment.metric == "strategy_executed_notional_usd"
        and judgment.scope == "dex_momentum_v1"
        and judgment.status == "regression"
        for judgment in report.judgments
    )
    assert any(
        judgment.metric == "risk_rejection_reason_count"
        and judgment.scope == "liquidity_below_minimum"
        and judgment.status == "regression"
        for judgment in report.judgments
    )


def test_run_replay_regression_uses_configured_thresholds() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "replay": AppSettings.load().replay.model_copy(
                update={
                    "regression_thresholds": (
                        AppSettings.load().replay.regression_thresholds.model_copy(
                            update={
                                "fill_rate": 2.0,
                                "rejection_rate": 2.0,
                                "average_executed_notional_usd": 10_000.0,
                                "risk_rejection_reason_count": 2.0,
                                "strategy_executed_notional_usd": 10_000.0,
                            }
                        )
                    )
                }
            )
        }
    )

    report = run_replay_regression(
        "/home/alex/Desktop/signalengine/replay/datasets/phase5_smoke.jsonl",
        "/home/alex/Desktop/signalengine/replay/datasets/phase5_risk_reject.jsonl",
        settings=settings,
    )

    assert any(
        judgment.metric == "fill_rate"
        and judgment.scope == "global"
        and judgment.threshold == 2.0
        and judgment.status == "neutral"
        for judgment in report.judgments
    )
    assert any(
        judgment.metric == "strategy_executed_notional_usd"
        and judgment.scope == "dex_momentum_v1"
        and judgment.threshold == 10000.0
        and judgment.status == "neutral"
        for judgment in report.judgments
    )
    assert "fill_rate:global" not in report.regressions
    assert "strategy_executed_notional_usd:dex_momentum_v1" not in report.regressions


def test_render_replay_comparison_text_includes_diagnostics_columns() -> None:
    report = run_replay_comparison(
        [
            "/home/alex/Desktop/signalengine/replay/datasets/phase5_smoke.jsonl",
            "/home/alex/Desktop/signalengine/replay/datasets/phase5_risk_reject.jsonl",
        ]
    )

    rendered = render_replay_comparison(report, as_json=False)

    assert "dataset | events | batches | intents | execs" in rendered
    assert "risk_latency_s" in rendered
    assert "slippage_bps" in rendered
    assert "phase5_smoke.jsonl" in rendered


def test_render_replay_regression_text_includes_judgments() -> None:
    report = run_replay_regression(
        "/home/alex/Desktop/signalengine/replay/datasets/phase5_smoke.jsonl",
        "/home/alex/Desktop/signalengine/replay/datasets/phase5_risk_reject.jsonl",
    )

    rendered = render_replay_regression(report, as_json=False)

    assert "summary_deltas:" in rendered
    assert "judgments:" in rendered
    assert "fill_rate[global] status=regression" in rendered
    assert "strategy_executed_notional_usd[dex_momentum_v1] status=regression" in rendered


def test_replay_main_prints_comparison_json(tmp_path, capsys) -> None:
    output_path = tmp_path / "comparison.json"

    exit_code = main(
        [
            "/home/alex/Desktop/signalengine/replay/datasets/phase5_smoke.jsonl",
            "/home/alex/Desktop/signalengine/replay/datasets/phase5_risk_reject.jsonl",
            "--json",
            "--output",
            str(output_path),
        ]
    )

    captured = json.loads(capsys.readouterr().out)
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert len(captured["rows"]) == 2
    assert len(written["runs"]) == 2


def test_replay_main_prints_regression_json(tmp_path, capsys) -> None:
    output_path = tmp_path / "regression.json"

    exit_code = main(
        [
            "--baseline",
            "/home/alex/Desktop/signalengine/replay/datasets/phase5_smoke.jsonl",
            "--candidate",
            "/home/alex/Desktop/signalengine/replay/datasets/phase5_risk_reject.jsonl",
            "--json",
            "--output",
            str(output_path),
        ]
    )

    captured = json.loads(capsys.readouterr().out)
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert captured["summary_deltas"]["execution_count"] == -1
    assert "fill_rate:global" in written["regressions"]
    assert "rejection_rate:global" in written["regressions"]
    assert "execution_count:global" in written["regressions"]
    assert any(
        judgment["metric"] == "strategy_executed_notional_usd"
        and judgment["scope"] == "dex_momentum_v1"
        and judgment["status"] == "regression"
        for judgment in written["judgments"]
    )
    assert any(
        judgment["metric"] == "risk_rejection_reason_count"
        and judgment["scope"] == "liquidity_below_minimum"
        and judgment["status"] == "regression"
        for judgment in written["judgments"]
    )