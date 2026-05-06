from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from redis import Redis

from core.config import AppSettings
from core.pipeline import PipelineWorker
from infra.logging import configure_logging
from infra.metrics import Metrics
from sentinel.onchain_listener import build_onchain_event
from sentinel.wallet_tracker import build_wallet_event


class FakeRedis:
    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.groups: set[tuple[str, str]] = set()
        self.acked: list[tuple[str, str, str]] = []
        self.counter = 0

    def xadd(self, stream_name: str, mapping: dict[str, str]) -> str:
        self.counter += 1
        message_id = f"{self.counter}-0"
        self.streams.setdefault(stream_name, []).append((message_id, mapping))
        return message_id

    def xgroup_create(
        self,
        stream_name: str,
        group_name: str,
        id: str = "0-0",
        mkstream: bool = False,
    ) -> bool:
        _ = id
        if mkstream:
            self.streams.setdefault(stream_name, [])
        self.groups.add((stream_name, group_name))
        return True

    def xreadgroup(
        self,
        group_name: str,
        consumer_name: str,
        streams: dict[str, str],
        count: int = 100,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        _ = group_name, consumer_name, block
        stream_name = next(iter(streams))
        return [(stream_name, self.streams.get(stream_name, [])[:count])]

    def xack(self, stream_name: str, group_name: str, message_id: str) -> int:
        self.acked.append((stream_name, group_name, message_id))
        return 1

    def xlen(self, stream_name: str) -> int:
        return len(self.streams.get(stream_name, []))


def test_worker_updates_metrics_for_processed_batch() -> None:
    configure_logging("INFO")
    settings = AppSettings.load()
    metrics = Metrics(settings.observability.service_namespace)
    worker = PipelineWorker(settings, cast(Redis, FakeRedis()), metrics=metrics)
    observed_at = datetime.now(UTC)

    worker.process_events(
        [
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
        ]
    )

    assert metrics.pipeline_runs.labels(outcome="executed")._value.get() == 1.0
    assert metrics.execution_reports.labels(venue="DEX", status="FILLED")._value.get() == 1.0
    assert metrics.worker_heartbeat.labels(service="pipeline", mode="process_events")._value.get() > 0.0
    assert metrics.redis_stream_backlog.labels(stream=settings.redis.signals_stream)._value.get() == 1.0
    assert metrics.redis_stream_backlog.labels(stream=settings.redis.decisions_stream)._value.get() == 3.0
    assert metrics.redis_stream_backlog.labels(stream=settings.redis.executions_stream)._value.get() == 1.0


def test_worker_emits_event_lag_and_risk_rejection_alerts() -> None:
    configure_logging("INFO")
    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "observability": base_settings.observability.model_copy(
                update={
                    "max_event_lag_seconds": 0.0,
                    "max_risk_rejections": 1,
                }
            ),
            "live": base_settings.live.model_copy(
                update={
                    "rollout": base_settings.live.rollout.model_copy(
                        update={"global_kill_switch_enabled": True}
                    )
                }
            ),
        }
    )
    metrics = Metrics(settings.observability.service_namespace)
    worker = PipelineWorker(settings, cast(Redis, FakeRedis()), metrics=metrics)
    observed_at = datetime.now(UTC)

    worker.process_events(
        [
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
        ]
    )

    assert metrics.alerts.labels(
        kind="event_lag_threshold_exceeded",
        severity="warning",
    )._value.get() == 1.0
    assert metrics.alerts.labels(
        kind="risk_rejection_threshold_exceeded",
        severity="warning",
    )._value.get() == 1.0


def test_worker_emits_adapter_failure_alert() -> None:
    configure_logging("INFO")
    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "execution": base_settings.execution.model_copy(update={"max_retries": 0}),
            "observability": base_settings.observability.model_copy(
                update={"max_consecutive_adapter_failures": 1}
            ),
        }
    )
    metrics = Metrics(settings.observability.service_namespace)
    worker = PipelineWorker(
        settings,
        cast(Redis, FakeRedis()),
        metrics=metrics,
        dex_executor=type(
            "AlwaysFailExecutor",
            (object,),
            {
                "prepare": lambda self, intent, risk: __import__(
                    "execution.dex_executor", fromlist=["DexPaperExecutor"]
                ).DexPaperExecutor().prepare(intent, risk),
                "execute": lambda self, prepared: (_ for _ in ()).throw(RuntimeError("dex_down")),
            },
        )(),
    )
    observed_at = datetime.now(UTC)

    try:
        worker.process_events(
            [
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
            ]
        )
    except RuntimeError:
        pass

    assert metrics.alerts.labels(
        kind="adapter_failure_threshold_exceeded",
        severity="warning",
    )._value.get() == 1.0