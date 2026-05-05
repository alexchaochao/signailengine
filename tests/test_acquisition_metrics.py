from __future__ import annotations

from datetime import UTC, datetime

from prometheus_client import CollectorRegistry
from sqlalchemy import create_engine

from core.config import AppSettings
from core.schemas import RawEventRecord
from core.worker import run_onchain_feature_live_sync
from infra.metrics import Metrics
from infra.postgres import init_storage
from infra.repository import StorageRepository
from sentinel.dex_quote_collector import DexQuoteCollector
from sentinel.feature_aggregator import OnchainFeatureAggregator, SlippageFeatureAggregator
from sentinel.onchain_live_sources import JupiterQuoteSource, SolanaWalletTradeSource
from sentinel.onchain_collector import OnchainTradeCollector


def test_collectors_update_event_lag_watermark_and_backfill_metrics() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    metrics = Metrics("signalengine_test", registry=CollectorRegistry())
    settings = AppSettings.load()
    trade_collector = OnchainTradeCollector(settings, repository, metrics=metrics)
    quote_collector = DexQuoteCollector(settings, repository, metrics=metrics)

    trade_collector.collect_trade(
        {
            "chain": "solana",
            "tx_hash": "tx-1",
            "log_index": 1,
            "slot": 10,
            "pool_address": "pool-1",
            "wallet_address": "wallet-1",
            "token": "BONK",
            "quote_asset": "USDC",
            "token_amount": 10.0,
            "quote_amount": 2.0,
            "quote_amount_usd": 100.0,
            "side": "buy",
            "route_hint": "backfill",
            "observed_at": datetime(2026, 5, 3, 12, 0, tzinfo=UTC),
        },
        publish=False,
    )
    quote_collector.collect_quote(
        {
            "chain": "solana",
            "token": "BONK",
            "quote_request_id": "req-1",
            "quote_notional_usd": 5000.0,
            "expected_out_usd": 4860.0,
            "reference_mid_usd": 5000.0,
            "route_summary": {"provider": "jupiter", "backfill": True},
            "quoted_at": datetime(2026, 5, 3, 12, 0, 3, tzinfo=UTC),
        },
        publish=False,
    )

    assert metrics.collector_events.labels(
        collector="onchain_trade_collector",
        source_type="onchain_trade",
        outcome="received",
    )._value.get() == 1.0
    assert metrics.collector_backfills.labels(
        collector="onchain_trade_collector",
        source_type="onchain_trade",
    )._value.get() == 1.0
    assert metrics.collector_last_watermark.labels(
        collector="dex_quote_collector",
        source_type="dex_quote",
        chain="solana",
        token="BONK",
    )._value.get() == datetime(2026, 5, 3, 12, 0, 3, tzinfo=UTC).timestamp()
    assert metrics.collector_source_lag.labels(
        collector="dex_quote_collector",
        source_type="dex_quote",
        chain="solana",
        token="BONK",
    )._value.get() >= 0.0


def test_aggregators_update_run_lag_and_watermark_metrics() -> None:
    engine = create_engine("sqlite:///:memory:")
    init_storage(engine)
    repository = StorageRepository(engine)
    metrics = Metrics("signalengine_test", registry=CollectorRegistry())
    settings = AppSettings.load()
    onchain_aggregator = OnchainFeatureAggregator(settings, repository, metrics=metrics)
    slippage_aggregator = SlippageFeatureAggregator(settings, repository, metrics=metrics)

    raw_trade = repository.raw_events.save(
        RawEventRecord(
            source_type="onchain_trade",
            source_name="solana_ws",
            source_event_id="solana:tx-1:1:BONK",
            chain="solana",
            token="BONK",
            observed_at=datetime(2026, 5, 3, 12, 0, 10, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 3, 12, 0, 11, tzinfo=UTC),
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
    raw_quote = repository.raw_events.save(
        RawEventRecord(
            source_type="dex_quote",
            source_name="jupiter_quote",
            source_event_id="q1",
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
    )

    onchain_aggregator.ingest_raw_trade(raw_trade)
    slippage_aggregator.ingest_raw_quote(raw_quote)

    assert metrics.aggregator_runs.labels(feature="buy_pressure", outcome="processed")._value.get() == 1.0
    assert metrics.aggregator_runs.labels(
        feature="estimated_slippage_bps",
        outcome="processed",
    )._value.get() == 1.0
    assert metrics.aggregator_last_watermark.labels(
        feature="buy_pressure",
        chain="solana",
        token="BONK",
    )._value.get() == datetime(2026, 5, 3, 12, 0, 10, tzinfo=UTC).timestamp()
    assert metrics.aggregator_source_lag.labels(
        feature="estimated_slippage_bps",
        chain="solana",
        token="BONK",
    )._value.get() >= 0.0


def test_live_source_sync_updates_poll_and_error_metrics(monkeypatch) -> None:
    metrics = Metrics("signalengine_test", registry=CollectorRegistry())
    calls: list[object] = []

    class StubCheckpointStore:
        def load(self, checkpoint_key):
            _ = checkpoint_key
            return None

        def save(self, checkpoint):
            calls.append(("checkpoint", checkpoint.checkpoint_key, checkpoint.cursor))

    class StubRepository:
        def __init__(self) -> None:
            self.checkpoints = StubCheckpointStore()

    class StubTradeSource:
        config = type(
            "Config",
            (),
            {
                "checkpoint_key": "trade-source",
                "source_name": "solana_rpc_wallet",
                "provider": "solana_rpc_wallet_watch",
            },
        )()

        def fetch_trades(self, last_cursor=None):
            _ = last_cursor
            return [
                type(
                    "Record",
                    (),
                    {
                        "cursor": "sig-1",
                        "observed_at": datetime(2026, 5, 3, 12, 0, tzinfo=UTC),
                        "payload": {"token": "BONK", "chain": "solana"},
                    },
                )()
            ]

    class StubQuoteSource:
        config = type("Config", (), {"source_name": "jupiter_quote_api"})()

        def fetch_quotes(self):
            raise RuntimeError("quote_down")

    class StubService:
        def __init__(self, settings, redis_client, repository, metrics) -> None:
            _ = settings, redis_client, repository, metrics

        def ingest_trade(self, payload, source_name):
            calls.append(("trade", payload["token"], source_name))

        def ingest_quote(self, payload, source_name):
            calls.append(("quote", payload["token"], source_name))

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: StubRepository())
    monkeypatch.setattr("core.worker.Metrics", lambda namespace: metrics)
    monkeypatch.setattr("core.worker.SolanaWalletTradeSource", StubTradeSource)
    monkeypatch.setattr("core.worker.JupiterQuoteSource", StubQuoteSource)
    monkeypatch.setattr("core.worker.build_live_sources", lambda settings: [StubTradeSource(), StubQuoteSource()])
    monkeypatch.setattr("core.worker.OnchainFeatureSyncService", StubService)

    assert run_onchain_feature_live_sync(AppSettings.load()) == 0
    assert metrics.live_source_polls.labels(source="solana_rpc_wallet", outcome="success")._value.get() == 1.0
    assert metrics.live_source_polls.labels(source="jupiter_quote_api", outcome="error")._value.get() == 1.0
    assert metrics.live_source_records.labels(source="solana_rpc_wallet", record_type="onchain_trade")._value.get() == 1.0
    assert metrics.live_source_last_success.labels(source="solana_rpc_wallet")._value.get() > 0.0
    assert metrics.live_source_last_error.labels(source="jupiter_quote_api")._value.get() > 0.0


def test_live_source_sync_enters_cooldown_and_emits_failure_threshold_alert(monkeypatch) -> None:
    metrics = Metrics("signalengine_test", registry=CollectorRegistry())
    alerts: list[tuple[str, str | None, str | None]] = []
    checkpoint_state: dict[str, object] = {}
    fetch_calls: list[str] = []

    class StubCheckpointStore:
        def load(self, checkpoint_key):
            return checkpoint_state.get(checkpoint_key)

        def save(self, checkpoint):
            checkpoint_state[checkpoint.checkpoint_key] = checkpoint

    class StubRepository:
        def __init__(self) -> None:
            self.checkpoints = StubCheckpointStore()

    class StubFailingTradeSource:
        config = type(
            "Config",
            (),
            {
                "checkpoint_key": "trade-source",
                "source_name": "solana_rpc_wallet",
                "provider": "solana_rpc_wallet_watch",
                "chain": "solana",
                "token": "BONK",
            },
        )()

        def fetch_trades(self, last_cursor=None):
            _ = last_cursor
            fetch_calls.append("fetch")
            raise RuntimeError("rpc_down")

    class StubAlertManager:
        def __init__(self, metrics, logger) -> None:
            _ = metrics, logger

        def emit(self, kind, *, token=None, chain=None, severity="warning", details=None):
            _ = severity, details
            alerts.append((kind, token, chain))

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: StubRepository())
    monkeypatch.setattr("core.worker.Metrics", lambda namespace: metrics)
    monkeypatch.setattr("core.worker.SolanaWalletTradeSource", StubFailingTradeSource)
    monkeypatch.setattr("core.worker.build_live_sources", lambda settings: [StubFailingTradeSource()])
    monkeypatch.setattr("core.worker.AlertManager", StubAlertManager)
    monkeypatch.setattr(
        "core.worker.OnchainFeatureSyncService",
        lambda settings, redis_client, repository, metrics: object(),
    )

    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "observability": base_settings.observability.model_copy(
                update={"max_consecutive_live_source_failures": 1}
            ),
            "acquisition": {
                "failure_backoff_seconds": 5.0,
                "source_cooldown_seconds": 30.0,
            },
        }
    )

    assert run_onchain_feature_live_sync(settings) == 0
    assert run_onchain_feature_live_sync(settings) == 0
    assert fetch_calls == ["fetch"]
    assert alerts == [("live_source_failure_threshold_exceeded", "BONK", "solana")]
    assert metrics.live_source_polls.labels(source="solana_rpc_wallet", outcome="error")._value.get() == 1.0
    assert metrics.live_source_polls.labels(source="solana_rpc_wallet", outcome="cooldown")._value.get() == 1.0
    assert metrics.live_source_consecutive_failures.labels(source="solana_rpc_wallet")._value.get() == 1.0
    assert metrics.live_source_next_eligible.labels(source="solana_rpc_wallet")._value.get() > 0.0