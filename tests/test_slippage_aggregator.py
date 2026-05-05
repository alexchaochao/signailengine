from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import create_engine

from core.config import AppSettings
from core.schemas import RawEventRecord
from infra.postgres import count_rows, init_storage
from infra.repository import StorageRepository
from sentinel.feature_aggregator import SlippageFeatureAggregator, classify_quote


def test_classify_quote_computes_slippage_bps() -> None:
    raw_event = RawEventRecord(
        source_type="dex_quote",
        source_name="jupiter_quote",
        source_event_id="solana:BONK:5000.0:2026-05-03T12:00:03+00:00:jupiter",
        chain="solana",
        token="BONK",
        observed_at=datetime(2026, 5, 3, 12, 0, 3, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 3, 12, 0, 4, tzinfo=UTC),
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

    sample = classify_quote(raw_event)

    assert sample.slippage_bps == 280.0


def test_slippage_aggregator_builds_curve_and_snapshot() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load().model_copy(
        update={
            "features": {
                "slippage": {
                    "quote_notional_usd": [1000.0, 5000.0, 10000.0],
                    "publication_notional_usd": 5000.0,
                    "max_quote_age_seconds": 10000.0,
                    "allow_curve_fallback": True,
                }
            }
        }
    )
    aggregator = SlippageFeatureAggregator(settings, repository)
    first = repository.raw_events.save(
        RawEventRecord(
            source_type="dex_quote",
            source_name="jupiter_quote",
            source_event_id="q1",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 3, 12, 0, 1, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 2, tzinfo=UTC),
            cursor="req-1",
            payload={
                "quote_request_id": "req-1",
                "token": "BONK",
                "quote_notional_usd": 1000.0,
                "expected_out_usd": 980.0,
                "reference_mid_usd": 1000.0,
                "route_summary": {"provider": "jupiter", "hops": 1},
            },
        )
    )
    second = repository.raw_events.save(
        RawEventRecord(
            source_type="dex_quote",
            source_name="jupiter_quote",
            source_event_id="q2",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 3, 12, 0, 3, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 4, tzinfo=UTC),
            cursor="req-2",
            payload={
                "quote_request_id": "req-2",
                "token": "BONK",
                "quote_notional_usd": 5000.0,
                "expected_out_usd": 4860.0,
                "reference_mid_usd": 5000.0,
                "route_summary": {"provider": "jupiter", "hops": 2},
            },
        )
    )

    aggregator.ingest_raw_quote(first)
    snapshot = aggregator.ingest_raw_quote(second)
    curve = repository.features.load_latest_slippage_curve("solana", "BONK")
    latest = repository.features.load_latest_snapshot(
        "solana",
        "BONK",
        "estimated_slippage_bps",
        "latest",
    )

    assert count_rows(engine, "dex_quote_samples") == 2
    assert count_rows(engine, "slippage_curves") == 2
    assert curve is not None
    assert len(curve.sample_points) == 2
    assert snapshot.feature_value == 280.0
    assert latest is not None
    assert latest.feature_value == 280.0
    assert latest.quality_flag == "ok"
    assert latest.inputs["route_provider"] == "jupiter"
    quality = repository.features.load_latest_quality(
        "solana",
        "BONK",
        "estimated_slippage_bps",
    )
    assert quality is not None
    assert quality.degraded_reason is None
    assert quality.missing_sources == []


def test_slippage_aggregator_uses_curve_fallback_when_exact_notional_missing() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    settings = AppSettings.load().model_copy(
        update={
            "features": {
                "slippage": {
                    "publication_notional_usd": 5000.0,
                    "max_quote_age_seconds": 10000.0,
                    "allow_curve_fallback": True,
                }
            }
        }
    )
    aggregator = SlippageFeatureAggregator(settings, repository)
    repository.raw_events.save(
        RawEventRecord(
            source_type="dex_quote",
            source_name="jupiter_quote",
            source_event_id="q1",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 3, 12, 0, 1, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 2, tzinfo=UTC),
            cursor="req-1",
            payload={
                "quote_request_id": "req-1",
                "token": "BONK",
                "quote_notional_usd": 1000.0,
                "expected_out_usd": 980.0,
                "reference_mid_usd": 1000.0,
                "route_summary": {"provider": "jupiter", "hops": 1},
            },
        )
    )
    second = repository.raw_events.save(
        RawEventRecord(
            source_type="dex_quote",
            source_name="jupiter_quote",
            source_event_id="q2",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 3, 12, 0, 3, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 4, tzinfo=UTC),
            cursor="req-2",
            payload={
                "quote_request_id": "req-2",
                "token": "BONK",
                "quote_notional_usd": 10000.0,
                "expected_out_usd": 9500.0,
                "reference_mid_usd": 10000.0,
                "route_summary": {"provider": "jupiter", "hops": 2},
            },
        )
    )

    aggregator.ingest_raw_quote(second)
    latest = repository.features.load_latest_snapshot(
        "solana",
        "BONK",
        "estimated_slippage_bps",
        "latest",
    )

    assert latest is not None
    assert latest.quality_flag == "degraded"
    assert latest.feature_value > 0.0
    assert latest.inputs["missing_sources"] == ["exact_quote_sample"]
    quality = repository.features.load_latest_quality(
        "solana",
        "BONK",
        "estimated_slippage_bps",
    )
    assert quality is not None
    assert quality.degraded_reason == "curve_fallback"
    assert quality.missing_sources == ["exact_quote_sample"]