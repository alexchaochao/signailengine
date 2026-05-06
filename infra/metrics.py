from __future__ import annotations

from datetime import UTC, datetime

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server


class Metrics:
    def __init__(
        self,
        namespace: str = "signalengine",
        registry: CollectorRegistry | None = None,
    ) -> None:
        self.registry = registry or CollectorRegistry()
        self.events_ingested = Counter(
            "events_ingested_total",
            "Count of ingested events",
            ["source"],
            namespace=namespace,
            registry=self.registry,
        )
        self.decision_latency = Histogram(
            "decision_latency_seconds",
            "Latency of decision pipeline steps",
            ["stage"],
            namespace=namespace,
            registry=self.registry,
        )
        self.pipeline_runs = Counter(
            "pipeline_runs_total",
            "Count of pipeline runs",
            ["outcome"],
            namespace=namespace,
            registry=self.registry,
        )
        self.execution_reports = Counter(
            "execution_reports_total",
            "Count of emitted execution reports",
            ["venue", "status"],
            namespace=namespace,
            registry=self.registry,
        )
        self.alerts = Counter(
            "alerts_total",
            "Count of triggered runtime alerts",
            ["kind", "severity"],
            namespace=namespace,
            registry=self.registry,
        )
        self.collector_events = Counter(
            "collector_events_total",
            "Count of collector events by outcome",
            ["collector", "source_type", "outcome"],
            namespace=namespace,
            registry=self.registry,
        )
        self.collector_dropped_events = Counter(
            "collector_dropped_events_total",
            "Count of collector events dropped by reason",
            ["collector", "reason"],
            namespace=namespace,
            registry=self.registry,
        )
        self.collector_backfills = Counter(
            "collector_backfills_total",
            "Count of collector backfill events",
            ["collector", "source_type"],
            namespace=namespace,
            registry=self.registry,
        )
        self.collector_source_lag = Gauge(
            "collector_source_lag_seconds",
            "Observed source lag for collectors",
            ["collector", "source_type", "chain", "token"],
            namespace=namespace,
            registry=self.registry,
        )
        self.collector_last_watermark = Gauge(
            "collector_last_watermark_timestamp_seconds",
            "Last collector watermark timestamp",
            ["collector", "source_type", "chain", "token"],
            namespace=namespace,
            registry=self.registry,
        )
        self.aggregator_runs = Counter(
            "aggregator_runs_total",
            "Count of aggregator runs by feature and outcome",
            ["feature", "outcome"],
            namespace=namespace,
            registry=self.registry,
        )
        self.aggregator_source_lag = Gauge(
            "aggregator_source_lag_seconds",
            "Observed source lag for aggregators",
            ["feature", "chain", "token"],
            namespace=namespace,
            registry=self.registry,
        )
        self.aggregator_last_watermark = Gauge(
            "aggregator_last_watermark_timestamp_seconds",
            "Last aggregator watermark timestamp",
            ["feature", "chain", "token"],
            namespace=namespace,
            registry=self.registry,
        )
        self.live_source_polls = Counter(
            "live_source_polls_total",
            "Count of live source polls by source and outcome",
            ["source", "outcome"],
            namespace=namespace,
            registry=self.registry,
        )
        self.live_source_records = Counter(
            "live_source_records_total",
            "Count of normalized records fetched from live sources",
            ["source", "record_type"],
            namespace=namespace,
            registry=self.registry,
        )
        self.live_source_last_success = Gauge(
            "live_source_last_success_timestamp_seconds",
            "Timestamp of the last successful live source poll",
            ["source"],
            namespace=namespace,
            registry=self.registry,
        )
        self.live_source_last_error = Gauge(
            "live_source_last_error_timestamp_seconds",
            "Timestamp of the last failed live source poll",
            ["source"],
            namespace=namespace,
            registry=self.registry,
        )
        self.live_source_consecutive_failures = Gauge(
            "live_source_consecutive_failures",
            "Current consecutive failure count for each live source",
            ["source"],
            namespace=namespace,
            registry=self.registry,
        )
        self.live_source_next_eligible = Gauge(
            "live_source_next_eligible_timestamp_seconds",
            "Next eligible poll timestamp for each live source",
            ["source"],
            namespace=namespace,
            registry=self.registry,
        )
        self.worker_heartbeat = Gauge(
            "worker_heartbeat_timestamp_seconds",
            "Timestamp of the latest worker heartbeat",
            ["service", "mode"],
            namespace=namespace,
            registry=self.registry,
        )
        self.redis_stream_backlog = Gauge(
            "redis_stream_backlog_messages",
            "Observed Redis stream message count",
            ["stream"],
            namespace=namespace,
            registry=self.registry,
        )
        self.notification_deliveries = Counter(
            "notification_deliveries_total",
            "Count of notification deliveries by channel and status",
            ["channel", "status"],
            namespace=namespace,
            registry=self.registry,
        )

    def mark_heartbeat(self, *, service: str, mode: str) -> None:
        self.worker_heartbeat.labels(service=service, mode=mode).set(datetime.now(UTC).timestamp())


def start_metrics_server(host: str, port: int) -> None:
    start_http_server(port=port, addr=host)