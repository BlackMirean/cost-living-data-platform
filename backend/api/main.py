"""FastAPI backend for the cost-of-living dashboard."""

from __future__ import annotations

import time
import logging
import json
from collections.abc import Callable
from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from backend.common import analytics_store, source_registry
from backend.common.api_cache import ApiResponseCache
from backend.common.config import settings
from backend.common.observability import (
    initialize_observability,
    metrics_response,
    record_rate_limit_block,
    record_request,
    request_span,
    track_in_flight,
)
from backend.common.rate_limiter import ApiRateLimiter
from backend.common.request_context import (
    new_request_id,
    reset_request_id,
    set_request_id,
)
from backend.common.runtime_queue import runtime_queue_events, runtime_queue_status
from backend.common.work_queue import work_queue_status


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)
app = FastAPI(title=settings.api_title)
_CACHE = ApiResponseCache()
_RATE_LIMITER = ApiRateLimiter()
PLATFORM_PREFIX = "/api/cost-living"
RATE_LIMIT_EXCLUDED_PATHS = {"/", "/metrics", "/api/metrics", "/api/health"}
initialize_observability()


def client_ip(request: Any) -> str:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",", 1)[0].strip()
    return getattr(request.client, "host", None) or "unknown"


def request_id_from_headers(request: Any) -> str:
    raw_value = request.headers.get("x-request-id", "").strip()
    if raw_value and len(raw_value) <= 128:
        return raw_value
    return new_request_id()


@app.middleware("http")
async def support_platform_prefix(request: Any, call_next: Callable[[Any], Any]) -> Any:
    """Support the public API prefix, tracing, rate limiting and request logs."""

    request_id = request_id_from_headers(request)
    request_token = set_request_id(request_id)
    path = request.scope.get("path", "")
    original_path = path
    if path == PLATFORM_PREFIX:
        request.scope["path"] = "/api/health"
    elif path.startswith(f"{PLATFORM_PREFIX}/"):
        request.scope["path"] = "/api" + path[len(PLATFORM_PREFIX) :]
    route_path = request.scope.get("path", original_path)
    method = request.method
    finish_in_flight = track_in_flight(method, route_path)
    span_attrs = {
        "http.request.method": method,
        "url.path": original_path,
        "http.route": route_path,
        "client.address": client_ip(request),
        "request.id": request_id,
    }
    status_code = 500
    try:
        with request_span("cost_living.api_request", span_attrs) as span:
            if route_path not in RATE_LIMIT_EXCLUDED_PATHS:
                decision = _RATE_LIMITER.check(client_id=client_ip(request), route_key=route_path)
                if not decision.allowed:
                    status_code = 429
                    record_rate_limit_block(method, route_path, decision.backend)
                    response = JSONResponse(
                        status_code=429,
                        content={
                            "detail": "rate limit exceeded",
                            "request_id": request_id,
                            "retry_after_seconds": decision.reset_seconds,
                        },
                    )
                    response.headers["Retry-After"] = str(decision.reset_seconds)
                else:
                    response = await call_next(request)
                response.headers["X-RateLimit-Limit"] = str(decision.limit)
                response.headers["X-RateLimit-Remaining"] = str(decision.remaining)
                response.headers["X-RateLimit-Reset"] = str(decision.reset_seconds)
            else:
                response = await call_next(request)
            status_code = getattr(response, "status_code", status_code)
            if span is not None:
                span.set_attribute("http.response.status_code", status_code)
            return response
    except Exception:
        logger.exception("api_request_failed request_id=%s path=%s", request_id, original_path)
        raise
    finally:
        duration_seconds = finish_in_flight()
        record_request(method, route_path, status_code, duration_seconds)
        logger.info(
            json.dumps(
                {
                    "event": "api_request",
                    "request_id": request_id,
                    "method": method,
                    "path": original_path,
                    "route": route_path,
                    "status": status_code,
                    "duration_ms": round(duration_seconds * 1000, 2),
                },
                sort_keys=True,
            )
        )
        try:
            response.headers["X-Request-ID"] = request_id
        except Exception:
            pass
        reset_request_id(request_token)


def cached(key: tuple[Any, ...], producer: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    """Return short-lived cached analytics responses to reduce repeated ES work."""

    return _CACHE.get_or_set(key, producer)


def filter_key(
    platform: str | None,
    topic: str | None,
    source_group: str,
    start: str | None,
    end: str | None,
    quality: str,
    exclude_quality_flags: str | None,
) -> tuple[Any, ...]:
    return platform, topic, source_group, start, end, quality, exclude_quality_flags


@app.get("/")
def root() -> dict[str, str]:
    return {
        "service": "cost-of-living-api",
        "health": "/api/health",
        "docs": "/docs",
    }


@app.get("/api/health")
def health() -> dict[str, Any]:
    return analytics_store.health()


@app.get("/api/pipeline/status")
def pipeline_status() -> dict[str, Any]:
    return cached(("pipeline_status",), analytics_store.pipeline_status)


@app.get("/api/pipeline/runtime")
def pipeline_runtime() -> dict[str, Any]:
    return runtime_queue_status()


@app.get("/api/pipeline/events")
def pipeline_events(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    return runtime_queue_events(limit=limit)


@app.get("/api/pipeline/queues")
def pipeline_queues() -> dict[str, Any]:
    return work_queue_status()


@app.get("/api/cache/status")
def cache_status() -> dict[str, Any]:
    return _CACHE.status()


@app.get("/api/rate-limit/status")
def rate_limit_status() -> dict[str, Any]:
    return _RATE_LIMITER.status()


@app.get("/metrics", include_in_schema=False)
@app.get("/api/metrics")
def metrics() -> Any:
    return metrics_response()


@app.get("/api/platforms/plugins")
def platform_plugins() -> dict[str, Any]:
    return source_registry.platform_plugin_metadata()


@app.get("/api/stats/overview")
def stats_overview(
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = Query(default="all", pattern="^(all|social|media)$"),
    start: str | None = None,
    end: str | None = None,
    quality: str = Query(default="all", pattern="^(all|clean)$"),
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    key = ("overview", *filter_key(platform, topic, source_group, start, end, quality, exclude_quality_flags))
    return cached(
        key,
        lambda: analytics_store.overview(
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    )


@app.get("/api/trends/documents")
def trends_documents(
    period: str = Query(default="month", pattern="^(day|month)$"),
    time_field: str = Query(default="created_at", pattern="^(created_at|harvested_at|processed_at)$"),
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = Query(default="all", pattern="^(all|social|media)$"),
    start: str | None = None,
    end: str | None = None,
    quality: str = Query(default="all", pattern="^(all|clean)$"),
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    key = (
        "trends_documents",
        period,
        time_field,
        *filter_key(platform, topic, source_group, start, end, quality, exclude_quality_flags),
    )
    return cached(
        key,
        lambda: analytics_store.trends_documents(
            period=period,
            time_field=time_field,
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    )


@app.get("/api/categories/counts")
def categories_counts(
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = Query(default="all", pattern="^(all|social|media)$"),
    start: str | None = None,
    end: str | None = None,
    quality: str = Query(default="all", pattern="^(all|clean)$"),
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    key = ("category_counts", *filter_key(platform, topic, source_group, start, end, quality, exclude_quality_flags))
    return cached(
        key,
        lambda: analytics_store.category_counts(
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    )


@app.get("/api/categories/sentiment")
def categories_sentiment(
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = Query(default="all", pattern="^(all|social|media)$"),
    start: str | None = None,
    end: str | None = None,
    quality: str = Query(default="all", pattern="^(all|clean)$"),
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    key = (
        "category_sentiment",
        *filter_key(platform, topic, source_group, start, end, quality, exclude_quality_flags),
    )
    return cached(
        key,
        lambda: analytics_store.category_sentiment(
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    )


@app.get("/api/categories/share")
def categories_share(
    period: str = Query(default="month", pattern="^(day|month)$"),
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = Query(default="all", pattern="^(all|social|media)$"),
    start: str | None = None,
    end: str | None = None,
    quality: str = Query(default="all", pattern="^(all|clean)$"),
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    key = ("category_share", period, *filter_key(platform, topic, source_group, start, end, quality, exclude_quality_flags))
    return cached(
        key,
        lambda: analytics_store.category_share(
            period=period,
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    )


@app.get("/api/data-quality/summary")
def data_quality_summary(
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = Query(default="all", pattern="^(all|social|media)$"),
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    key = ("data_quality_summary", platform, topic, source_group, start, end)
    return cached(
        key,
        lambda: analytics_store.data_quality_summary(
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
        ),
    )


@app.get("/api/data-quality/comparison")
def data_quality_comparison(
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = Query(default="all", pattern="^(all|social|media)$"),
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    key = ("data_quality_comparison", platform, topic, source_group, start, end)
    return cached(
        key,
        lambda: analytics_store.quality_comparison(
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
        ),
    )


@app.get("/api/media/coverage")
def media_coverage(
    period: str = Query(default="month", pattern="^(day|month)$"),
    platform: str | None = None,
    topic: str | None = None,
    start: str | None = None,
    end: str | None = None,
    quality: str = Query(default="clean", pattern="^(all|clean)$"),
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    key = ("media_coverage", period, platform, topic, start, end, quality, exclude_quality_flags)
    return cached(
        key,
        lambda: analytics_store.media_coverage(
            period=period,
            platform=platform,
            topic=topic,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    )


@app.get("/api/platforms/categories")
def platforms_categories(
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = Query(default="all", pattern="^(all|social|media)$"),
    start: str | None = None,
    end: str | None = None,
    quality: str = Query(default="all", pattern="^(all|clean)$"),
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    key = (
        "platform_categories",
        *filter_key(platform, topic, source_group, start, end, quality, exclude_quality_flags),
    )
    return cached(
        key,
        lambda: analytics_store.platform_categories(
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    )


@app.get("/api/trends/categories")
def trends_categories(
    period: str = Query(default="month", pattern="^(day|month)$"),
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = Query(default="all", pattern="^(all|social|media)$"),
    start: str | None = None,
    end: str | None = None,
    quality: str = Query(default="all", pattern="^(all|clean)$"),
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    key = ("trends_categories", period, *filter_key(platform, topic, source_group, start, end, quality, exclude_quality_flags))
    return cached(
        key,
        lambda: analytics_store.trends_categories(
            period=period,
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    )


@app.get("/api/trends/sentiment")
def trends_sentiment(
    period: str = Query(default="month", pattern="^(day|month)$"),
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = Query(default="all", pattern="^(all|social|media)$"),
    start: str | None = None,
    end: str | None = None,
    quality: str = Query(default="all", pattern="^(all|clean)$"),
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    key = ("trends_sentiment", period, *filter_key(platform, topic, source_group, start, end, quality, exclude_quality_flags))
    return cached(
        key,
        lambda: analytics_store.trends_sentiment(
            period=period,
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    )


@app.get("/api/official/comparison")
def official_comparison(
    measure: str = "Percentage change from previous year",
    index_measure: str = "Index numbers",
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = Query(default="all", pattern="^(all|social|media)$"),
    start: str | None = None,
    end: str | None = None,
    quality: str = Query(default="all", pattern="^(all|clean)$"),
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    key = (
        "official_comparison",
        measure,
        index_measure,
        *filter_key(platform, topic, source_group, start, end, quality, exclude_quality_flags),
    )
    return cached(
        key,
        lambda: analytics_store.official_comparison(
            measure=measure,
            index_measure=index_measure,
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    )


@app.get("/api/categories/yoy-change")
def categories_yoy_change(
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = Query(default="all", pattern="^(all|social|media)$"),
    start: str | None = None,
    end: str | None = None,
    quality: str = Query(default="all", pattern="^(all|clean)$"),
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    key = ("yoy_change", *filter_key(platform, topic, source_group, start, end, quality, exclude_quality_flags))
    return cached(
        key,
        lambda: analytics_store.yoy_change(
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    )


@app.get("/api/categories/volatility")
def categories_volatility(
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = Query(default="all", pattern="^(all|social|media)$"),
    start: str | None = None,
    end: str | None = None,
    quality: str = Query(default="all", pattern="^(all|clean)$"),
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    key = ("volatility", *filter_key(platform, topic, source_group, start, end, quality, exclude_quality_flags))
    return cached(
        key,
        lambda: analytics_store.volatility(
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    )


@app.get("/api/categories/keywords")
def categories_keywords(
    category: str | None = None,
    limit: int = Query(default=20, ge=1, le=100),
    sample_size: int = Query(default=1000, ge=100, le=5000),
    platform: str | None = None,
    source_group: str = Query(default="all", pattern="^(all|social|media)$"),
    start: str | None = None,
    end: str | None = None,
    quality: str = Query(default="all", pattern="^(all|clean)$"),
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    key = (
        "keywords",
        category,
        limit,
        sample_size,
        platform,
        source_group,
        start,
        end,
        quality,
        exclude_quality_flags,
    )
    return cached(
        key,
        lambda: analytics_store.category_keywords(
            category=category,
            limit=limit,
            sample_size=sample_size,
            platform=platform,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    )


@app.get("/api/logs/errors")
def logs_errors(size: int = Query(default=50, ge=1, le=200)) -> dict[str, Any]:
    return cached(("logs_errors", size), lambda: analytics_store.error_logs(size=size))
