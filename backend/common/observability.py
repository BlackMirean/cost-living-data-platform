"""Prometheus metrics and optional OpenTelemetry tracing."""

from __future__ import annotations

import contextlib
import logging
import time
from typing import Any

from fastapi import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from backend.common.config import settings


logger = logging.getLogger(__name__)

REQUEST_COUNT = Counter(
    "cost_living_api_requests_total",
    "Total API requests.",
    ("method", "path", "status"),
)
REQUEST_DURATION = Histogram(
    "cost_living_api_request_duration_seconds",
    "API request duration in seconds.",
    ("method", "path"),
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
IN_FLIGHT_REQUESTS = Gauge(
    "cost_living_api_in_flight_requests",
    "Current in-flight API requests.",
    ("method", "path"),
)
RATE_LIMIT_BLOCKS = Counter(
    "cost_living_api_rate_limit_blocks_total",
    "Total API requests blocked by rate limiting.",
    ("method", "path", "backend"),
)
CACHE_LOOKUPS = Counter(
    "cost_living_api_cache_lookups_total",
    "API cache lookup outcomes.",
    ("outcome", "backend"),
)
PIPELINE_QUEUE_DEPTH = Gauge(
    "cost_living_pipeline_queue_depth",
    "Pipeline queue depth reported by workers or diagnostics.",
    ("queue",),
)

_OTEL_READY = False
_OTEL_INITIALIZED = False
_TRACER: Any = None


def initialize_observability() -> None:
    """Initialize optional OpenTelemetry tracing once."""

    global _OTEL_READY, _OTEL_INITIALIZED, _TRACER
    if _OTEL_INITIALIZED:
        return
    _OTEL_INITIALIZED = True
    if not getattr(settings, "otel_enabled", False):
        return
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

        resource = Resource.create({SERVICE_NAME: settings.otel_service_name})
        provider = TracerProvider(resource=resource)
        if settings.otel_exporter_otlp_endpoint:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

            provider.add_span_processor(
                BatchSpanProcessor(
                    OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint)
                )
            )
        elif settings.otel_console_exporter_enabled:
            provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        trace.set_tracer_provider(provider)
        _TRACER = trace.get_tracer(settings.otel_service_name)
        _OTEL_READY = True
    except Exception as exc:
        logger.warning("otel_initialization_failed error=%s", exc)


@contextlib.contextmanager
def request_span(name: str, attributes: dict[str, Any]):
    initialize_observability()
    if not _OTEL_READY or _TRACER is None:
        yield None
        return
    with _TRACER.start_as_current_span(name) as span:
        for key, value in attributes.items():
            if value is not None:
                span.set_attribute(key, value)
        yield span


def record_request(method: str, path: str, status_code: int, duration_seconds: float) -> None:
    REQUEST_COUNT.labels(method=method, path=path, status=str(status_code)).inc()
    REQUEST_DURATION.labels(method=method, path=path).observe(duration_seconds)


def track_in_flight(method: str, path: str):
    labels = IN_FLIGHT_REQUESTS.labels(method=method, path=path)
    labels.inc()
    started = time.perf_counter()

    def finish() -> float:
        labels.dec()
        return time.perf_counter() - started

    return finish


def record_rate_limit_block(method: str, path: str, backend: str) -> None:
    RATE_LIMIT_BLOCKS.labels(method=method, path=path, backend=backend).inc()


def record_cache_lookup(outcome: str, backend: str) -> None:
    CACHE_LOOKUPS.labels(outcome=outcome, backend=backend).inc()


def set_queue_depth(queue_name: str, depth: int) -> None:
    PIPELINE_QUEUE_DEPTH.labels(queue=queue_name).set(max(0, int(depth)))


def metrics_response() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
