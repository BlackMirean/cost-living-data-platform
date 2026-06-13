"""FastAPI backend for the cost-of-living dashboard."""

from __future__ import annotations

import time
import logging
from collections.abc import Callable
from typing import Any

from fastapi import FastAPI, Query

from backend.common import analytics_store, source_registry
from backend.common.config import settings
from backend.common.runtime_queue import runtime_queue_events, runtime_queue_status


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)
app = FastAPI(title=settings.api_title)
_CACHE: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
PLATFORM_PREFIX = "/api/cost-living"


@app.middleware("http")
async def support_platform_prefix(request: Any, call_next: Callable[[Any], Any]) -> Any:
    """Support the public API prefix and log request outcomes."""

    started_at = time.perf_counter()
    path = request.scope.get("path", "")
    original_path = path
    if path == PLATFORM_PREFIX:
        request.scope["path"] = "/api/health"
    elif path.startswith(f"{PLATFORM_PREFIX}/"):
        request.scope["path"] = "/api" + path[len(PLATFORM_PREFIX) :]
    try:
        response = await call_next(request)
    except Exception:
        logger.exception("api_request_failed path=%s", original_path)
        raise
    elapsed_ms = (time.perf_counter() - started_at) * 1000
    logger.info(
        "api_request method=%s path=%s status=%s duration_ms=%.2f",
        request.method,
        original_path,
        getattr(response, "status_code", "unknown"),
        elapsed_ms,
    )
    return response


def cached(key: tuple[Any, ...], producer: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    """Return short-lived cached analytics responses to reduce repeated ES work."""

    ttl = settings.api_cache_ttl_seconds
    if ttl <= 0:
        return producer()
    now = time.monotonic()
    cached_value = _CACHE.get(key)
    if cached_value and now - cached_value[0] < ttl:
        return cached_value[1]
    value = producer()
    _CACHE[key] = (now, value)
    return value


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
