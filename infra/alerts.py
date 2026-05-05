from __future__ import annotations

from logging import Logger
from typing import Any

from infra.metrics import Metrics


class AlertManager:
    def __init__(self, metrics: Metrics, logger: Logger) -> None:
        self.metrics = metrics
        self.logger = logger

    def emit(
        self,
        kind: str,
        *,
        token: str | None = None,
        chain: str | None = None,
        severity: str = "warning",
        details: dict[str, Any] | None = None,
    ) -> None:
        self.metrics.alerts.labels(kind=kind, severity=severity).inc()
        self.logger.warning(
            "alert_triggered",
            extra={
                "token": token,
                "chain": chain,
                "service": "alerting",
                "outcome": kind,
                "correlation_id": None,
                "event_id": None,
                **(details or {}),
            },
        )