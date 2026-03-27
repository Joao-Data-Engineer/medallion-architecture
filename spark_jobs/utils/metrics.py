"""
metrics.py

Structured logging and Prometheus metrics for pipeline observability.
Uses structlog for JSON log lines with consistent fields that are
machine-parseable (e.g. by Grafana Loki or CloudWatch Logs Insights).

Log line format:
  {
    "event": "silver_transform_complete",
    "pipeline_layer": "silver",
    "year": 2024,
    "month": 1,
    "rows_in": 3000000,
    "rows_out": 2850000,
    "rows_dropped": 150000,
    "duration_seconds": 45.2,
    "timestamp": "2024-02-01T03:15:00Z",
    "level": "info"
  }
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

import structlog
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

# Configure structlog for JSON output
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
    cache_logger_on_first_use=True,
)

logging.basicConfig(stream=sys.stdout, level=logging.WARNING)


class PipelineMetrics:
    """
    Emits structured log lines and Prometheus push metrics for a pipeline step.

    Usage:
        metrics = PipelineMetrics(layer="silver", year=2024, month=1)
        metrics.log("transform_complete", rows_in=3_000_000, rows_out=2_850_000)
    """

    def __init__(self, layer: str, year: int | None = None, month: int | None = None) -> None:
        self.layer = layer
        self.year = year
        self.month = month
        self._logger = structlog.get_logger(__name__).bind(
            pipeline_layer=layer,
            year=year,
            month=month,
        )

    def log(self, event: str, level: str = "info", **kwargs) -> None:
        log_fn = getattr(self._logger, level, self._logger.info)
        log_fn(event, **kwargs)

    def push_prometheus(
        self,
        rows_in: int,
        rows_out: int,
        duration_seconds: float,
        pushgateway_url: str = "http://localhost:9091",
    ) -> None:
        """Push batch metrics to Prometheus Pushgateway (optional)."""
        registry = CollectorRegistry()

        Gauge(
            "pipeline_rows_processed_total",
            "Total rows processed in this pipeline run",
            ["layer", "direction"],
            registry=registry,
        ).labels(layer=self.layer, direction="in").set(rows_in)

        Gauge(
            "pipeline_rows_processed_total",
            "Total rows processed in this pipeline run",
            ["layer", "direction"],
            registry=registry,
        ).labels(layer=self.layer, direction="out").set(rows_out)

        Gauge(
            "pipeline_duration_seconds",
            "Duration of the pipeline step in seconds",
            ["layer"],
            registry=registry,
        ).labels(layer=self.layer).set(duration_seconds)

        try:
            push_to_gateway(
                pushgateway_url,
                job=f"medallion_{self.layer}",
                registry=registry,
                grouping_key={"year": str(self.year), "month": str(self.month)},
            )
        except Exception as e:
            self._logger.warning("prometheus_push_failed", error=str(e))
