"""Chart-ready analytics queries for the cost-of-living dashboard API."""

from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import pstdev
from typing import Any

from backend.analytics.topics import COST_OF_LIVING_TOPICS, TOPIC_LABELS, cpi_items_for_topic
from backend.common.config import settings
from backend.common.document_store import (
    health as store_health,
    load_local_indicators,
    load_local_posts,
    load_local_raw_posts,
    use_local_store,
)
from backend.common.es_client import get_es_client
from backend.common.source_registry import (
    source_group_for_platform,
    source_group_platforms,
)


SENTIMENT_FIELD = "sentiment_score"
SOURCE_GROUP_PLATFORMS = source_group_platforms()
VALID_QUALITY_MODES = {"all", "clean"}
DEFAULT_CLEAN_EXCLUDED_FLAGS = ("metadata_heavy", "weak_australia_context")
VALID_PERIODS = {"day", "month"}
VALID_TIME_FIELDS = {"created_at", "harvested_at", "processed_at"}
SENTIMENT_LABELS = ("positive", "neutral", "negative")
_INDEX_PROPERTIES_CACHE: dict[str, dict[str, Any]] = {}

STOPWORDS = {
    "about",
    "after",
    "again",
    "against",
    "also",
    "and",
    "are",
    "australia",
    "australian",
    "been",
    "but",
    "can",
    "could",
    "for",
    "from",
    "has",
    "have",
    "into",
    "its",
    "just",
    "more",
    "not",
    "now",
    "our",
    "out",
    "over",
    "price",
    "prices",
    "said",
    "than",
    "that",
    "the",
    "their",
    "there",
    "they",
    "this",
    "through",
    "with",
    "would",
    "year",
}


def _round(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return round(float(value), digits)


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def _lag_minutes(value: Any) -> float | None:
    parsed = _parse_timestamp(value)
    if not parsed:
        return None
    return _round((datetime.now(timezone.utc) - parsed).total_seconds() / 60, 2)


def _source_group_for_platform(platform: Any) -> str:
    return source_group_for_platform(platform)


def _canonical_key(post: dict[str, Any]) -> str:
    return str(post.get("canonical_id") or post.get("raw_id") or post.get("id") or "")


def _unique_count_for_posts(posts: list[dict[str, Any]]) -> int:
    return len({key for key in (_canonical_key(post) for post in posts) if key})


def _duplicate_ratio(document_count: int, unique_document_count: int) -> float:
    if document_count <= 0:
        return 0.0
    return _round(max(document_count - unique_document_count, 0) / document_count) or 0.0


def _quality_flag_counts(posts: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for post in posts:
        counts.update(post.get("quality_flags") or [])
    return dict(counts)


def _topic_label(topic: str | None) -> str:
    if not topic:
        return "Unknown"
    return TOPIC_LABELS.get(topic, topic.replace("_", " ").title())


def _period_value(timestamp: str, period: str) -> str:
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if period == "month":
        return f"{parsed.year:04d}-{parsed.month:02d}"
    return parsed.date().isoformat()


def _validate_period(period: str) -> str:
    if period not in VALID_PERIODS:
        raise ValueError("period must be one of: day, month")
    return period


def _validate_time_field(time_field: str) -> str:
    if time_field not in VALID_TIME_FIELDS:
        raise ValueError("time_field must be one of: created_at, harvested_at, processed_at")
    return time_field


def _index_properties(index_name: str) -> dict[str, Any]:
    if index_name not in _INDEX_PROPERTIES_CACHE:
        try:
            mapping = get_es_client().indices.get_mapping(index=index_name)
            _INDEX_PROPERTIES_CACHE[index_name] = mapping[index_name].get("mappings", {}).get(
                "properties", {}
            )
        except Exception:
            _INDEX_PROPERTIES_CACHE[index_name] = {}
    return _INDEX_PROPERTIES_CACHE[index_name]


def _index_has_field(field: str, index_name: str | None = None) -> bool:
    return field in _index_properties(index_name or settings.posts_index)


def _term_field(field: str, index_name: str | None = None) -> str:
    properties = _index_properties(index_name or settings.posts_index)
    spec = properties.get(field, {})
    if spec.get("type") in {"keyword", "boolean", "integer", "long"}:
        return field
    if "keyword" in spec.get("fields", {}):
        return f"{field}.keyword"
    return field


def _processed_filter() -> dict[str, Any]:
    return {
        "bool": {
            "filter": [
                {"exists": {"field": SENTIMENT_FIELD}},
                {"term": {_term_field("processing_status"): "processed"}},
            ],
        }
    }


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _excluded_quality_flags(
    quality: str = "all",
    exclude_quality_flags: str | None = None,
) -> list[str]:
    quality = quality or "all"
    if quality not in VALID_QUALITY_MODES:
        raise ValueError("quality must be one of: all, clean")
    flags = _split_csv(exclude_quality_flags)
    if quality == "clean":
        flags.extend(DEFAULT_CLEAN_EXCLUDED_FLAGS)
    return sorted(set(flags))


def _post_filters(
    *,
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = "all",
    start: str | None = None,
    end: str | None = None,
    time_field: str = "created_at",
    quality: str = "all",
    exclude_quality_flags: str | None = None,
) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = [_processed_filter()]
    platforms = _split_csv(platform)
    topics = _split_csv(topic)
    excluded_flags = _excluded_quality_flags(quality, exclude_quality_flags)
    if source_group != "all":
        group_platforms = SOURCE_GROUP_PLATFORMS.get(source_group)
        if group_platforms is None:
            raise ValueError("source_group must be one of: all, social, media")
        filters.append({"terms": {_term_field("platform"): group_platforms}})
    if platforms:
        filters.append({"terms": {_term_field("platform"): platforms}})
    if topics:
        filters.append({"terms": {_term_field("topic"): topics}})
    if start or end:
        range_body: dict[str, Any] = {}
        if start:
            range_body["gte"] = start
        if end:
            range_body["lte"] = end
        filters.append({"range": {time_field: range_body}})
    if excluded_flags:
        filters.append(
            {
                "bool": {
                    "must_not": [
                        {"terms": {_term_field("quality_flags"): excluded_flags}},
                    ],
                }
            }
        )
    return filters


def _query(filters: list[dict[str, Any]]) -> dict[str, Any]:
    return {"bool": {"filter": filters}} if filters else {"match_all": {}}


def _histogram(period: str, field: str) -> dict[str, Any]:
    _validate_period(period)
    return {
        "field": field,
        "calendar_interval": "month" if period == "month" else "day",
        "format": "yyyy-MM" if period == "month" else "yyyy-MM-dd",
        "min_doc_count": 0,
    }


def _base_meta(**params: Any) -> dict[str, Any]:
    return {
        "indices": {
            "processed": settings.posts_index,
            "raw": settings.raw_posts_index,
            "official": settings.indicators_index,
        },
        "params": params,
    }


def _local_processed_posts() -> list[dict[str, Any]]:
    posts = []
    for post in load_local_posts():
        if post.get("processing_status") != "processed":
            continue
        if post.get(SENTIMENT_FIELD) is None:
            continue
        posts.append(post)
    return posts


def _local_filter_posts(
    posts: list[dict[str, Any]],
    *,
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = "all",
    start: str | None = None,
    end: str | None = None,
    time_field: str = "created_at",
    quality: str = "all",
    exclude_quality_flags: str | None = None,
) -> list[dict[str, Any]]:
    platforms = set(_split_csv(platform))
    topics = set(_split_csv(topic))
    group_platforms = set(SOURCE_GROUP_PLATFORMS.get(source_group, [])) if source_group != "all" else set()
    excluded_flags = set(_excluded_quality_flags(quality, exclude_quality_flags))
    filtered = []
    for post in posts:
        value = post.get(time_field)
        post_flags = set(post.get("quality_flags") or [])
        if platforms and post.get("platform") not in platforms:
            continue
        if group_platforms and post.get("platform") not in group_platforms:
            continue
        if topics and post.get("topic") not in topics:
            continue
        if excluded_flags and post_flags.intersection(excluded_flags):
            continue
        if start and value and value < start:
            continue
        if end and value and value > end:
            continue
        filtered.append(post)
    return filtered


def _local_sentiment_row(topic: str, posts: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(posts)
    counts = Counter(post.get("sentiment_label") for post in posts)
    scores = [float(post[SENTIMENT_FIELD]) for post in posts if post.get(SENTIMENT_FIELD) is not None]
    return {
        "cost_category": topic,
        "category_label": _topic_label(topic),
        "document_count": total,
        "complaint_count": total,
        "avg_sentiment": _round(sum(scores) / len(scores) if scores else None),
        "positive_ratio": _round(counts.get("positive", 0) / total if total else 0),
        "neutral_ratio": _round(counts.get("neutral", 0) / total if total else 0),
        "negative_ratio": _round(counts.get("negative", 0) / total if total else 0),
        "positive_count": counts.get("positive", 0),
        "neutral_count": counts.get("neutral", 0),
        "negative_count": counts.get("negative", 0),
    }


def health() -> dict[str, Any]:
    return store_health()


def pipeline_status() -> dict[str, Any]:
    if use_local_store():
        posts = _local_processed_posts()
        raw_posts = load_local_raw_posts()
        indicators = load_local_indicators()
        raw_status = Counter(post.get("analysis_status", "pending") for post in raw_posts)
        raw_source_status: dict[str, Counter[str]] = defaultdict(Counter)
        stale_cutoff = datetime.now(timezone.utc) - timedelta(minutes=settings.nlp_processing_stale_minutes)
        stale_processing = 0
        for post in raw_posts:
            source = post.get("source_index") or post.get("platform") or "unknown"
            status = post.get("analysis_status", "pending")
            raw_source_status[source][status] += 1
            started_at = _parse_timestamp(post.get("analysis_started_at"))
            if status == "processing" and started_at and started_at < stale_cutoff:
                stale_processing += 1
        sources = [
            {
                "source_index": source,
                "platform": None,
                "document_count": sum(status_counts.values()),
                "pending_documents": status_counts.get("pending", 0),
                "processing_documents": status_counts.get("processing", 0),
                "last_harvest_at": max(
                    (
                        post.get("harvested_at", "")
                        for post in raw_posts
                        if (post.get("source_index") or post.get("platform") or "unknown") == source
                    ),
                    default=None,
                ),
            }
            for source, status_counts in sorted(raw_source_status.items())
        ]
        posts_by_platform: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for post in posts:
            posts_by_platform[post.get("platform") or "unknown"].append(post)
        latest_by_platform = []
        for platform, grouped_posts in sorted(posts_by_platform.items()):
            latest_by_platform.append(
                {
                    "platform": platform,
                    "last_created_at": max((post.get("created_at", "") for post in grouped_posts), default=None),
                    "last_harvest_at": max((post.get("harvested_at", "") for post in grouped_posts), default=None),
                    "last_processed_at": max((post.get("processed_at", "") for post in grouped_posts), default=None),
                }
            )
        last_harvest_at = max((post.get("harvested_at", "") for post in raw_posts), default=None)
        last_processed_at = max((post.get("processed_at", "") for post in posts), default=None)
        return {
            "raw_documents": len(raw_posts),
            "processed_documents": len(posts),
            "unprocessed_documents": raw_status.get("pending", 0),
            "processing_documents": raw_status.get("processing", 0),
            "stale_processing_documents": stale_processing,
            "discarded_documents": raw_status.get("discarded", 0),
            "failed_documents": raw_status.get("error", 0),
            "official_indicators": len(indicators),
            "last_harvest_at": last_harvest_at,
            "last_processed_at": last_processed_at,
            "harvest_lag_minutes": _lag_minutes(last_harvest_at),
            "processing_lag_minutes": _lag_minutes(last_processed_at),
            "latest_official_period": max((doc.get("period", "") for doc in indicators), default=None),
            "pending_by_source": {
                source: counts.get("pending", 0) for source, counts in raw_source_status.items()
            },
            "processing_by_source": {
                source: counts.get("processing", 0) for source, counts in raw_source_status.items()
            },
            "quality_flags": _quality_flag_counts(posts),
            "latest_by_platform": latest_by_platform,
            "sources": sources,
        }

    client = get_es_client()
    processed = client.search(
        index=settings.posts_index,
        body={
            "size": 0,
            "track_total_hits": True,
            "query": _query(_post_filters()),
            "aggs": {
                "last_processed": {"max": {"field": "processed_at", "format": "strict_date_optional_time"}},
                "last_harvested": {"max": {"field": "harvested_at", "format": "strict_date_optional_time"}},
                "quality_flags": {"terms": {"field": _term_field("quality_flags"), "size": 20}},
                "platform_latest": {
                    "terms": {"field": _term_field("platform"), "size": 20},
                    "aggs": {
                        "last_created": {
                            "max": {"field": "created_at", "format": "strict_date_optional_time"}
                        },
                        "last_harvested": {
                            "max": {"field": "harvested_at", "format": "strict_date_optional_time"}
                        },
                        "last_processed": {
                            "max": {"field": "processed_at", "format": "strict_date_optional_time"}
                        },
                    },
                },
            },
        },
    )
    raw = client.search(
        index=settings.raw_posts_index,
        body={
            "size": 0,
            "track_total_hits": True,
            "aggs": {
                "by_status": {
                    "terms": {
                        "field": _term_field("analysis_status", settings.raw_posts_index),
                        "size": 10,
                    }
                },
                "stale_processing": {
                    "filter": {
                        "bool": {
                            "filter": [
                                {
                                    "term": {
                                        _term_field("analysis_status", settings.raw_posts_index): "processing"
                                    }
                                },
                                {
                                    "range": {
                                        "analysis_started_at": {
                                            "lt": f"now-{settings.nlp_processing_stale_minutes}m"
                                        }
                                    }
                                },
                            ]
                        }
                    }
                },
                "sources": {
                    "terms": {"field": _term_field("source_index", settings.raw_posts_index), "size": 20},
                    "aggs": {
                        "statuses": {
                            "terms": {
                                "field": _term_field("analysis_status", settings.raw_posts_index),
                                "size": 10,
                            }
                        },
                        "last_harvested": {
                            "max": {"field": "harvested_at", "format": "strict_date_optional_time"}
                        },
                        "platforms": {
                            "terms": {
                                "field": _term_field("platform", settings.raw_posts_index),
                                "size": 5,
                            }
                        },
                    },
                },
            },
        },
    )
    indicators = client.search(
        index=settings.indicators_index,
        body={
            "size": 0,
            "track_total_hits": True,
            "aggs": {
                "latest_period": {"max": {"field": "period_start", "format": "strict_date_optional_time"}},
            },
        },
    )
    status_counts = {
        bucket["key"]: bucket["doc_count"]
        for bucket in raw.get("aggregations", {}).get("by_status", {}).get("buckets", [])
    }
    sources = []
    pending_by_source = {}
    processing_by_source = {}
    for bucket in raw.get("aggregations", {}).get("sources", {}).get("buckets", []):
        platform_bucket = next(iter(bucket.get("platforms", {}).get("buckets", [])), {})
        source_status = {
            status_bucket["key"]: status_bucket["doc_count"]
            for status_bucket in bucket.get("statuses", {}).get("buckets", [])
        }
        pending_by_source[bucket["key"]] = source_status.get("pending", 0)
        processing_by_source[bucket["key"]] = source_status.get("processing", 0)
        sources.append(
            {
                "source_index": bucket["key"],
                "platform": platform_bucket.get("key"),
                "document_count": bucket["doc_count"],
                "pending_documents": source_status.get("pending", 0),
                "processing_documents": source_status.get("processing", 0),
                "last_harvest_at": bucket.get("last_harvested", {}).get("value_as_string"),
            }
        )
    quality_flags = {
        bucket["key"]: bucket["doc_count"]
        for bucket in processed.get("aggregations", {}).get("quality_flags", {}).get("buckets", [])
    }
    latest_by_platform = [
        {
            "platform": bucket["key"],
            "last_created_at": bucket.get("last_created", {}).get("value_as_string"),
            "last_harvest_at": bucket.get("last_harvested", {}).get("value_as_string"),
            "last_processed_at": bucket.get("last_processed", {}).get("value_as_string"),
        }
        for bucket in processed.get("aggregations", {}).get("platform_latest", {}).get("buckets", [])
    ]
    last_harvest_at = processed.get("aggregations", {}).get("last_harvested", {}).get("value_as_string")
    last_processed_at = processed.get("aggregations", {}).get("last_processed", {}).get("value_as_string")
    return {
        "raw_documents": raw.get("hits", {}).get("total", {}).get("value", 0),
        "processed_documents": processed.get("hits", {}).get("total", {}).get("value", 0),
        "unprocessed_documents": status_counts.get("pending", 0),
        "processing_documents": status_counts.get("processing", 0),
        "stale_processing_documents": raw.get("aggregations", {})
        .get("stale_processing", {})
        .get("doc_count", 0),
        "discarded_documents": status_counts.get("discarded", 0),
        "failed_documents": status_counts.get("error", 0),
        "official_indicators": indicators.get("hits", {}).get("total", {}).get("value", 0),
        "last_harvest_at": last_harvest_at,
        "last_processed_at": last_processed_at,
        "harvest_lag_minutes": _lag_minutes(last_harvest_at),
        "processing_lag_minutes": _lag_minutes(last_processed_at),
        "latest_official_period": indicators.get("aggregations", {})
        .get("latest_period", {})
        .get("value_as_string"),
        "pending_by_source": pending_by_source,
        "processing_by_source": processing_by_source,
        "quality_flags": quality_flags,
        "latest_by_platform": latest_by_platform,
        "sources": sources,
    }


def overview(
    *,
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = "all",
    start: str | None = None,
    end: str | None = None,
    quality: str = "all",
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    if use_local_store():
        posts = _local_filter_posts(
            _local_processed_posts(),
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        )
        platforms = Counter(post.get("platform") for post in posts)
        topics = Counter(post.get("topic") for post in posts)
        sentiments = Counter(post.get("sentiment_label") for post in posts)
        dates = [post.get("created_at") for post in posts if post.get("created_at")]
        return {
            "total_documents": len(posts),
            "total_complaints": len(posts),
            "negative_documents": sentiments.get("negative", 0),
            "platforms": dict(platforms),
            "categories": [
                {
                    "cost_category": key,
                    "category_label": _topic_label(key),
                    "document_count": value,
                }
                for key, value in topics.items()
            ],
            "sentiment": dict(sentiments),
            "date_range": {"start": min(dates) if dates else None, "end": max(dates) if dates else None},
            "meta": _base_meta(
                platform=platform,
                topic=topic,
                source_group=source_group,
                start=start,
                end=end,
                quality=quality,
                exclude_quality_flags=exclude_quality_flags,
            ),
        }

    result = get_es_client().search(
        index=settings.posts_index,
        body={
            "size": 0,
            "track_total_hits": True,
            "query": _query(
                _post_filters(
                    platform=platform,
                    topic=topic,
                    source_group=source_group,
                    start=start,
                    end=end,
                    quality=quality,
                    exclude_quality_flags=exclude_quality_flags,
                )
            ),
            "aggs": {
                "platforms": {"terms": {"field": _term_field("platform"), "size": 20}},
                "topics": {"terms": {"field": _term_field("topic"), "size": 50}},
                "sentiment": {"terms": {"field": _term_field("sentiment_label"), "size": 10}},
                "date_min": {"min": {"field": "created_at", "format": "strict_date_optional_time"}},
                "date_max": {"max": {"field": "created_at", "format": "strict_date_optional_time"}},
            },
        },
    )
    aggs = result.get("aggregations", {})
    sentiments = {bucket["key"]: bucket["doc_count"] for bucket in aggs.get("sentiment", {}).get("buckets", [])}
    return {
        "total_documents": result.get("hits", {}).get("total", {}).get("value", 0),
        "total_complaints": result.get("hits", {}).get("total", {}).get("value", 0),
        "negative_documents": sentiments.get("negative", 0),
        "platforms": {bucket["key"]: bucket["doc_count"] for bucket in aggs.get("platforms", {}).get("buckets", [])},
        "categories": [
            {
                "cost_category": bucket["key"],
                "category_label": _topic_label(bucket["key"]),
                "document_count": bucket["doc_count"],
            }
            for bucket in aggs.get("topics", {}).get("buckets", [])
        ],
        "sentiment": sentiments,
        "date_range": {
            "start": aggs.get("date_min", {}).get("value_as_string"),
            "end": aggs.get("date_max", {}).get("value_as_string"),
        },
        "meta": _base_meta(
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    }


def category_counts(**filters: Any) -> dict[str, Any]:
    if use_local_store():
        posts = _local_filter_posts(_local_processed_posts(), **filters)
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for post in posts:
            grouped[post.get("topic", "unknown")].append(post)
        counts = Counter({topic: len(topic_posts) for topic, topic_posts in grouped.items()})
        total = sum(counts.values())
        rows = [
            {
                "cost_category": topic,
                "category_label": _topic_label(topic),
                "complaint_count": count,
                "document_count": count,
                "unique_document_count": _unique_count_for_posts(grouped[topic]),
                "duplicate_ratio": _duplicate_ratio(count, _unique_count_for_posts(grouped[topic])),
                "percentage": _round(count / total if total else 0),
            }
            for topic, count in counts.most_common()
        ]
        return {
            "total_complaints": total,
            "total_unique_documents": _unique_count_for_posts(posts),
            "rows": rows,
            "meta": _base_meta(**filters),
        }

    result = get_es_client().search(
        index=settings.posts_index,
        body={
            "size": 0,
            "track_total_hits": True,
            "query": _query(_post_filters(**filters)),
            "aggs": {
                "unique_documents": {"cardinality": {"field": _term_field("canonical_id")}},
                "topics": {
                    "terms": {"field": _term_field("topic"), "size": 50},
                    "aggs": {"unique_documents": {"cardinality": {"field": _term_field("canonical_id")}}},
                },
            },
        },
    )
    total = result.get("hits", {}).get("total", {}).get("value", 0)
    rows = [
        {
            "cost_category": bucket["key"],
            "category_label": _topic_label(bucket["key"]),
            "complaint_count": bucket["doc_count"],
            "document_count": bucket["doc_count"],
            "unique_document_count": bucket.get("unique_documents", {}).get("value") or bucket["doc_count"],
            "duplicate_ratio": _duplicate_ratio(
                bucket["doc_count"],
                bucket.get("unique_documents", {}).get("value") or bucket["doc_count"],
            ),
            "percentage": _round(bucket["doc_count"] / total if total else 0),
        }
        for bucket in result.get("aggregations", {}).get("topics", {}).get("buckets", [])
    ]
    unique_total = result.get("aggregations", {}).get("unique_documents", {}).get("value") or total
    return {
        "total_complaints": total,
        "total_unique_documents": unique_total,
        "rows": rows,
        "meta": _base_meta(**filters),
    }


def category_sentiment(**filters: Any) -> dict[str, Any]:
    if use_local_store():
        posts = _local_filter_posts(_local_processed_posts(), **filters)
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for post in posts:
            grouped[post.get("topic", "unknown")].append(post)
        rows = [_local_sentiment_row(topic, grouped[topic]) for topic in grouped]
        rows.sort(key=lambda row: row["avg_sentiment"] if row["avg_sentiment"] is not None else 0)
        return {"rows": rows, "meta": _base_meta(**filters)}

    aggs = {
        "topics": {
            "terms": {"field": _term_field("topic"), "size": 50},
            "aggs": {
                "avg_sentiment": {"avg": {"field": SENTIMENT_FIELD}},
                **{
                    label: {"filter": {"term": {_term_field("sentiment_label"): label}}}
                    for label in SENTIMENT_LABELS
                },
            },
        }
    }
    result = get_es_client().search(
        index=settings.posts_index,
        body={"size": 0, "query": _query(_post_filters(**filters)), "aggs": aggs},
    )
    rows = []
    for bucket in result.get("aggregations", {}).get("topics", {}).get("buckets", []):
        total = bucket["doc_count"]
        counts = {label: bucket.get(label, {}).get("doc_count", 0) for label in SENTIMENT_LABELS}
        rows.append(
            {
                "cost_category": bucket["key"],
                "category_label": _topic_label(bucket["key"]),
                "document_count": total,
                "complaint_count": total,
                "avg_sentiment": _round(bucket.get("avg_sentiment", {}).get("value")),
                "positive_ratio": _round(counts["positive"] / total if total else 0),
                "neutral_ratio": _round(counts["neutral"] / total if total else 0),
                "negative_ratio": _round(counts["negative"] / total if total else 0),
                "positive_count": counts["positive"],
                "neutral_count": counts["neutral"],
                "negative_count": counts["negative"],
            }
        )
    rows.sort(key=lambda row: row["avg_sentiment"] if row["avg_sentiment"] is not None else 0)
    return {"rows": rows, "meta": _base_meta(**filters)}


def trends_documents(
    *,
    period: str = "month",
    time_field: str = "created_at",
    **filters: Any,
) -> dict[str, Any]:
    period = _validate_period(period)
    time_field = _validate_time_field(time_field)
    if use_local_store():
        posts = _local_filter_posts(_local_processed_posts(), time_field=time_field, **filters)
        counts: Counter[tuple[str, str]] = Counter()
        for post in posts:
            if post.get(time_field):
                counts[(_period_value(post[time_field], period), post.get("platform", "unknown"))] += 1
        rows = [
            {"period": row_period, "platform": platform, "document_count": count}
            for (row_period, platform), count in sorted(counts.items())
        ]
        return {"period": period, "time_field": time_field, "rows": rows, "meta": _base_meta(**filters)}

    result = get_es_client().search(
        index=settings.posts_index,
        body={
            "size": 0,
            "query": _query(_post_filters(time_field=time_field, **filters)),
            "aggs": {
                "periods": {
                    "date_histogram": _histogram(period, time_field),
                    "aggs": {
                        "platforms": {"terms": {"field": _term_field("platform"), "size": 20}}
                    },
                }
            },
        },
    )
    rows = []
    for period_bucket in result.get("aggregations", {}).get("periods", {}).get("buckets", []):
        for platform_bucket in period_bucket.get("platforms", {}).get("buckets", []):
            rows.append(
                {
                    "period": period_bucket["key_as_string"],
                    "platform": platform_bucket["key"],
                    "document_count": platform_bucket["doc_count"],
                }
            )
    return {"period": period, "time_field": time_field, "rows": rows, "meta": _base_meta(**filters)}


def _category_period_rows(
    *,
    period: str,
    include_sentiment: bool,
    **filters: Any,
) -> list[dict[str, Any]]:
    period = _validate_period(period)
    if use_local_store():
        posts = _local_filter_posts(_local_processed_posts(), **filters)
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for post in posts:
            if post.get("created_at"):
                grouped[(_period_value(post["created_at"], period), post.get("topic", "unknown"))].append(post)
        rows = []
        for (row_period, topic), bucket_posts in sorted(grouped.items()):
            row = {
                "period": row_period,
                "cost_category": topic,
                "category_label": _topic_label(topic),
                "complaint_count": len(bucket_posts),
                "document_count": len(bucket_posts),
                "unique_document_count": _unique_count_for_posts(bucket_posts),
                "duplicate_ratio": _duplicate_ratio(len(bucket_posts), _unique_count_for_posts(bucket_posts)),
            }
            if include_sentiment:
                row.update(_local_sentiment_row(topic, bucket_posts))
                row["period"] = row_period
            rows.append(row)
        return rows

    aggs: dict[str, Any] = {
        "topics": {
            "terms": {"field": _term_field("topic"), "size": 50},
            "aggs": {"unique_documents": {"cardinality": {"field": _term_field("canonical_id")}}},
        }
    }
    if include_sentiment:
        aggs["topics"]["aggs"].update(
            {
                "avg_sentiment": {"avg": {"field": SENTIMENT_FIELD}},
                "negative": {"filter": {"term": {_term_field("sentiment_label"): "negative"}}},
            }
        )
    result = get_es_client().search(
        index=settings.posts_index,
        body={
            "size": 0,
            "query": _query(_post_filters(**filters)),
            "aggs": {"periods": {"date_histogram": _histogram(period, "created_at"), "aggs": aggs}},
        },
    )
    rows = []
    for period_bucket in result.get("aggregations", {}).get("periods", {}).get("buckets", []):
        for topic_bucket in period_bucket.get("topics", {}).get("buckets", []):
            total = topic_bucket["doc_count"]
            row = {
                "period": period_bucket["key_as_string"],
                "cost_category": topic_bucket["key"],
                "category_label": _topic_label(topic_bucket["key"]),
                "complaint_count": total,
                "document_count": total,
                "unique_document_count": topic_bucket.get("unique_documents", {}).get("value") or total,
                "duplicate_ratio": _duplicate_ratio(
                    total,
                    topic_bucket.get("unique_documents", {}).get("value") or total,
                ),
            }
            if include_sentiment:
                negative = topic_bucket.get("negative", {}).get("doc_count", 0)
                row.update(
                    {
                        "avg_sentiment": _round(topic_bucket.get("avg_sentiment", {}).get("value")),
                        "negative_count": negative,
                        "negative_ratio": _round(negative / total if total else 0),
                    }
                )
            rows.append(row)
    return rows


def trends_categories(period: str = "month", **filters: Any) -> dict[str, Any]:
    rows = _category_period_rows(period=period, include_sentiment=False, **filters)
    return {"period": period, "rows": rows, "meta": _base_meta(**filters)}


def trends_sentiment(period: str = "month", **filters: Any) -> dict[str, Any]:
    rows = _category_period_rows(period=period, include_sentiment=True, **filters)
    return {"period": period, "rows": rows, "meta": _base_meta(**filters)}


def category_share(period: str = "month", **filters: Any) -> dict[str, Any]:
    rows = trends_categories(period=period, **filters)["rows"]
    totals: Counter[str] = Counter()
    for row in rows:
        totals[row["period"]] += row["complaint_count"]
    for row in rows:
        total = totals[row["period"]]
        row["percentage"] = _round(row["complaint_count"] / total if total else 0)
    return {"period": period, "rows": rows, "meta": _base_meta(**filters)}


def _is_clean_post(post: dict[str, Any], excluded_flags: set[str] | None = None) -> bool:
    excluded_flags = excluded_flags or set(DEFAULT_CLEAN_EXCLUDED_FLAGS)
    return not set(post.get("quality_flags") or []).intersection(excluded_flags)


def data_quality_summary(**filters: Any) -> dict[str, Any]:
    """Explain document quality, source composition and duplicate pressure."""

    query_filters = {
        key: value
        for key, value in filters.items()
        if key not in {"quality", "exclude_quality_flags"} and value is not None
    }
    clean_flags = set(DEFAULT_CLEAN_EXCLUDED_FLAGS)

    if use_local_store():
        posts = _local_filter_posts(_local_processed_posts(), **query_filters)
        total = len(posts)
        clean_count = sum(1 for post in posts if _is_clean_post(post, clean_flags))
        by_source_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
        by_topic_source = Counter(post.get("topic_source", "unknown") for post in posts)
        for post in posts:
            source_group = post.get("source_group") or _source_group_for_platform(post.get("platform"))
            by_source_group[source_group].append(post)
        return {
            "total_documents": total,
            "clean_documents": clean_count,
            "flagged_documents": total - clean_count,
            "clean_ratio": _round(clean_count / total if total else 0),
            "quality_flags": [
                {"flag": flag, "document_count": count}
                for flag, count in sorted(_quality_flag_counts(posts).items())
            ],
            "by_source_group": [
                {
                    "source_group": source_group,
                    "document_count": len(group_posts),
                    "clean_documents": sum(1 for post in group_posts if _is_clean_post(post, clean_flags)),
                    "clean_ratio": _round(
                        sum(1 for post in group_posts if _is_clean_post(post, clean_flags))
                        / len(group_posts)
                        if group_posts
                        else 0
                    ),
                }
                for source_group, group_posts in sorted(by_source_group.items())
            ],
            "topic_sources": [
                {"topic_source": source, "document_count": count}
                for source, count in sorted(by_topic_source.items())
            ],
            "duplicates": {
                "document_count": total,
                "unique_document_count": _unique_count_for_posts(posts),
                "duplicate_ratio": _duplicate_ratio(total, _unique_count_for_posts(posts)),
            },
            "meta": _base_meta(**query_filters),
        }

    clean_filter = {
        "bool": {
            "must_not": [
                {"terms": {_term_field("quality_flags"): list(clean_flags)}},
            ],
        }
    }
    result = get_es_client().search(
        index=settings.posts_index,
        body={
            "size": 0,
            "track_total_hits": True,
            "query": _query(_post_filters(**query_filters)),
            "aggs": {
                "clean": {"filter": clean_filter},
                "unique_documents": {"cardinality": {"field": _term_field("canonical_id")}},
                "quality_flags": {"terms": {"field": _term_field("quality_flags"), "size": 30}},
                "topic_sources": {"terms": {"field": _term_field("topic_source"), "size": 10}},
                "source_groups": {
                    "terms": {"field": _term_field("source_group"), "size": 10},
                    "aggs": {"clean": {"filter": clean_filter}},
                },
                "platforms": {
                    "terms": {"field": _term_field("platform"), "size": 20},
                    "aggs": {"clean": {"filter": clean_filter}},
                },
            },
        },
    )
    total = result.get("hits", {}).get("total", {}).get("value", 0)
    aggs = result.get("aggregations", {})
    source_group_buckets = aggs.get("source_groups", {}).get("buckets", [])
    if source_group_buckets:
        by_source_group = [
            {
                "source_group": bucket["key"],
                "document_count": bucket["doc_count"],
                "clean_documents": bucket.get("clean", {}).get("doc_count", 0),
                "clean_ratio": _round(
                    bucket.get("clean", {}).get("doc_count", 0) / bucket["doc_count"]
                    if bucket["doc_count"]
                    else 0
                ),
            }
            for bucket in source_group_buckets
        ]
    else:
        grouped: dict[str, dict[str, int]] = defaultdict(lambda: {"document_count": 0, "clean_documents": 0})
        for bucket in aggs.get("platforms", {}).get("buckets", []):
            group = _source_group_for_platform(bucket["key"])
            grouped[group]["document_count"] += bucket["doc_count"]
            grouped[group]["clean_documents"] += bucket.get("clean", {}).get("doc_count", 0)
        by_source_group = [
            {
                "source_group": group,
                "document_count": values["document_count"],
                "clean_documents": values["clean_documents"],
                "clean_ratio": _round(
                    values["clean_documents"] / values["document_count"]
                    if values["document_count"]
                    else 0
                ),
            }
            for group, values in sorted(grouped.items())
        ]

    unique_documents = aggs.get("unique_documents", {}).get("value") or total
    clean_documents = aggs.get("clean", {}).get("doc_count", 0)
    return {
        "total_documents": total,
        "clean_documents": clean_documents,
        "flagged_documents": total - clean_documents,
        "clean_ratio": _round(clean_documents / total if total else 0),
        "quality_flags": [
            {"flag": bucket["key"], "document_count": bucket["doc_count"]}
            for bucket in aggs.get("quality_flags", {}).get("buckets", [])
        ],
        "by_source_group": by_source_group,
        "topic_sources": [
            {"topic_source": bucket["key"], "document_count": bucket["doc_count"]}
            for bucket in aggs.get("topic_sources", {}).get("buckets", [])
        ],
        "duplicates": {
            "document_count": total,
            "unique_document_count": unique_documents,
            "duplicate_ratio": _duplicate_ratio(total, unique_documents),
        },
        "meta": _base_meta(**query_filters),
    }


def quality_comparison(**filters: Any) -> dict[str, Any]:
    """Compare all documents with the default clean view by topic."""

    query_filters = {
        key: value
        for key, value in filters.items()
        if key not in {"quality", "exclude_quality_flags"} and value is not None
    }

    if use_local_store():
        all_rows = category_counts(**query_filters, quality="all")["rows"]
        clean_rows = category_counts(**query_filters, quality="clean")["rows"]
        clean_by_topic = {row["cost_category"]: row for row in clean_rows}
        rows = []
        for row in all_rows:
            clean_row = clean_by_topic.get(row["cost_category"], {})
            all_count = row["document_count"]
            clean_count = clean_row.get("document_count", 0)
            rows.append(
                {
                    "cost_category": row["cost_category"],
                    "category_label": row["category_label"],
                    "all_document_count": all_count,
                    "clean_document_count": clean_count,
                    "excluded_document_count": all_count - clean_count,
                    "clean_ratio": _round(clean_count / all_count if all_count else 0),
                    "all_unique_document_count": row.get("unique_document_count", all_count),
                    "clean_unique_document_count": clean_row.get("unique_document_count", clean_count),
                }
            )
        rows.sort(key=lambda row: row["excluded_document_count"], reverse=True)
        return {
            "rows": rows,
            "quality_modes": {"all": "all processed documents", "clean": "excludes default quality flags"},
            "default_clean_excluded_flags": list(DEFAULT_CLEAN_EXCLUDED_FLAGS),
            "meta": _base_meta(**query_filters),
        }

    clean_filter = {
        "bool": {
            "must_not": [
                {"terms": {_term_field("quality_flags"): list(DEFAULT_CLEAN_EXCLUDED_FLAGS)}},
            ],
        }
    }
    result = get_es_client().search(
        index=settings.posts_index,
        body={
            "size": 0,
            "query": _query(_post_filters(**query_filters, quality="all")),
            "aggs": {
                "topics": {
                    "terms": {"field": _term_field("topic"), "size": 50},
                    "aggs": {
                        "unique_documents": {"cardinality": {"field": _term_field("canonical_id")}},
                        "clean": {
                            "filter": clean_filter,
                            "aggs": {
                                "unique_documents": {
                                    "cardinality": {"field": _term_field("canonical_id")}
                                }
                            },
                        },
                    },
                }
            },
        },
    )
    rows = []
    for bucket in result.get("aggregations", {}).get("topics", {}).get("buckets", []):
        clean_bucket = bucket.get("clean", {})
        all_count = bucket["doc_count"]
        clean_count = clean_bucket.get("doc_count", 0)
        rows.append(
            {
                "cost_category": bucket["key"],
                "category_label": _topic_label(bucket["key"]),
                "all_document_count": all_count,
                "clean_document_count": clean_count,
                "excluded_document_count": all_count - clean_count,
                "clean_ratio": _round(clean_count / all_count if all_count else 0),
                "all_unique_document_count": bucket.get("unique_documents", {}).get("value")
                or all_count,
                "clean_unique_document_count": clean_bucket.get("unique_documents", {}).get("value")
                or clean_count,
            }
        )
    rows.sort(key=lambda row: row["excluded_document_count"], reverse=True)
    return {
        "rows": rows,
        "quality_modes": {"all": "all processed documents", "clean": "excludes default quality flags"},
        "default_clean_excluded_flags": list(DEFAULT_CLEAN_EXCLUDED_FLAGS),
        "meta": _base_meta(**query_filters),
    }


def media_coverage(period: str = "month", **filters: Any) -> dict[str, Any]:
    """Return GDELT/media-only monthly coverage using the same category contract."""

    period = _validate_period(period)
    media_filters = {
        key: value
        for key, value in filters.items()
        if key not in {"source_group"} and value is not None
    }
    rows = trends_sentiment(period=period, source_group="media", **media_filters)["rows"]
    for row in rows:
        row["coverage_count"] = row["document_count"]
    return {
        "period": period,
        "source_group": "media",
        "rows": rows,
        "meta": _base_meta(source_group="media", **media_filters),
    }


def platform_categories(**filters: Any) -> dict[str, Any]:
    if use_local_store():
        posts = _local_filter_posts(_local_processed_posts(), **filters)
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        platform_totals = Counter(post.get("platform", "unknown") for post in posts)
        for post in posts:
            grouped[(post.get("platform", "unknown"), post.get("topic", "unknown"))].append(post)
        rows = []
        for (platform, topic), bucket_posts in sorted(grouped.items()):
            scores = [float(post[SENTIMENT_FIELD]) for post in bucket_posts]
            count = len(bucket_posts)
            rows.append(
                {
                    "platform": platform,
                    "cost_category": topic,
                    "category_label": _topic_label(topic),
                    "count": count,
                    "unique_document_count": _unique_count_for_posts(bucket_posts),
                    "duplicate_ratio": _duplicate_ratio(count, _unique_count_for_posts(bucket_posts)),
                    "avg_sentiment": _round(sum(scores) / len(scores) if scores else None),
                    "percentage_within_platform": _round(count / platform_totals[platform]),
                }
            )
        return {"rows": rows, "meta": _base_meta(**filters)}

    result = get_es_client().search(
        index=settings.posts_index,
        body={
            "size": 0,
            "query": _query(_post_filters(**filters)),
            "aggs": {
                "platforms": {
                    "terms": {"field": _term_field("platform"), "size": 20},
                    "aggs": {
                        "topics": {
                            "terms": {"field": _term_field("topic"), "size": 50},
                            "aggs": {
                                "avg_sentiment": {"avg": {"field": SENTIMENT_FIELD}},
                                "unique_documents": {"cardinality": {"field": _term_field("canonical_id")}},
                            },
                        }
                    },
                }
            },
        },
    )
    rows = []
    for platform_bucket in result.get("aggregations", {}).get("platforms", {}).get("buckets", []):
        platform_total = platform_bucket["doc_count"]
        for topic_bucket in platform_bucket.get("topics", {}).get("buckets", []):
            count = topic_bucket["doc_count"]
            rows.append(
                {
                    "platform": platform_bucket["key"],
                    "cost_category": topic_bucket["key"],
                    "category_label": _topic_label(topic_bucket["key"]),
                    "count": count,
                    "unique_document_count": topic_bucket.get("unique_documents", {}).get("value")
                    or count,
                    "duplicate_ratio": _duplicate_ratio(
                        count,
                        topic_bucket.get("unique_documents", {}).get("value") or count,
                    ),
                    "avg_sentiment": _round(topic_bucket.get("avg_sentiment", {}).get("value")),
                    "percentage_within_platform": _round(count / platform_total if platform_total else 0),
                }
            )
    return {"rows": rows, "meta": _base_meta(**filters)}


def _indicator_series(item_name: str, measure: str) -> list[dict[str, Any]]:
    if use_local_store():
        docs = [
            doc
            for doc in load_local_indicators()
            if doc.get("indicator") == "monthly_cpi"
            and doc.get("item_name") == item_name
            and measure.casefold() in doc.get("measure", "").casefold()
        ]
        return sorted(docs, key=lambda doc: doc.get("period_start", ""))

    result = get_es_client().search(
        index=settings.indicators_index,
        body={
            "size": 1000,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"indicator": "monthly_cpi"}},
                        {"term": {"item_name": item_name}},
                    ]
                }
            },
            "sort": [{"period_start": {"order": "asc"}}],
        },
    )
    docs = [hit["_source"] for hit in result.get("hits", {}).get("hits", [])]
    return [doc for doc in docs if measure.casefold() in doc.get("measure", "").casefold()]


def _selected_indicator(topic: str, measure: str) -> tuple[str | None, list[dict[str, Any]]]:
    for item_name in cpi_items_for_topic(topic):
        series = _indicator_series(item_name, measure)
        if series:
            return item_name, series
    return None, []


def official_comparison(
    *,
    measure: str = "Percentage change from previous year",
    index_measure: str = "Index numbers",
    period: str = "month",
    platform: str | None = None,
    topic: str | None = None,
    source_group: str = "all",
    start: str | None = None,
    end: str | None = None,
    quality: str = "all",
    exclude_quality_flags: str | None = None,
) -> dict[str, Any]:
    period = _validate_period(period)
    if period != "month":
        raise ValueError("official comparison is only available at monthly granularity")

    social_rows = trends_sentiment(
        period="month",
        platform=platform,
        topic=topic,
        source_group=source_group,
        start=start,
        end=end,
        quality=quality,
        exclude_quality_flags=exclude_quality_flags,
    )["rows"]
    social_by_topic: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in social_rows:
        social_by_topic[row["cost_category"]][row["period"]] = row

    requested_topics = _split_csv(topic) or sorted(social_by_topic)
    rows = []
    official_periods: list[str] = []
    for row_topic in requested_topics:
        selected_item, official_series = _selected_indicator(row_topic, measure)
        _, index_series = _selected_indicator(row_topic, index_measure)
        official_by_period = {doc["period"]: doc for doc in official_series}
        index_by_period = {doc["period"]: doc for doc in index_series}
        official_periods.extend(official_by_period)
        for row_period, social in sorted(social_by_topic.get(row_topic, {}).items()):
            official = official_by_period.get(row_period)
            if not official:
                continue
            rows.append(
                {
                    "period": row_period,
                    "cost_category": row_topic,
                    "category_label": _topic_label(row_topic),
                    "official_indicator": selected_item,
                    "official_measure": official.get("measure"),
                    "official_value": official.get("value"),
                    "official_index_value": index_by_period.get(row_period, {}).get("value"),
                    "complaint_count": social["complaint_count"],
                    "document_count": social["document_count"],
                    "negative_ratio": social.get("negative_ratio"),
                    "avg_sentiment": social.get("avg_sentiment"),
                }
            )

    return {
        "measure": measure,
        "index_measure": index_measure,
        "period": period,
        "rows": rows,
        "coverage": {
            "official_period_min": min(official_periods) if official_periods else None,
            "official_period_max": max(official_periods) if official_periods else None,
            "overlap_period_min": min((row["period"] for row in rows), default=None),
            "overlap_period_max": max((row["period"] for row in rows), default=None),
        },
        "meta": _base_meta(
            platform=platform,
            topic=topic,
            source_group=source_group,
            start=start,
            end=end,
            quality=quality,
            exclude_quality_flags=exclude_quality_flags,
        ),
    }


def yoy_change(**filters: Any) -> dict[str, Any]:
    rows = trends_categories(period="month", **filters)["rows"]
    by_topic: dict[str, dict[str, int]] = defaultdict(dict)
    for row in rows:
        by_topic[row["cost_category"]][row["period"]] = row["complaint_count"]
    all_periods = sorted({row["period"] for row in rows})
    latest_24 = all_periods[-24:]
    previous = set(latest_24[:12])
    current = set(latest_24[12:])
    out = []
    for topic, counts in by_topic.items():
        previous_count = sum(value for period, value in counts.items() if period in previous)
        current_count = sum(value for period, value in counts.items() if period in current)
        growth = None if previous_count == 0 else (current_count - previous_count) / previous_count
        out.append(
            {
                "cost_category": topic,
                "category_label": _topic_label(topic),
                "previous_period_count": previous_count,
                "current_period_count": current_count,
                "yoy_growth": _round(growth),
            }
        )
    out.sort(key=lambda row: row["yoy_growth"] if row["yoy_growth"] is not None else -999, reverse=True)
    return {"rows": out, "meta": _base_meta(**filters)}


def volatility(**filters: Any) -> dict[str, Any]:
    rows = trends_categories(period="month", **filters)["rows"]
    grouped: dict[str, list[int]] = defaultdict(list)
    for row in rows:
        grouped[row["cost_category"]].append(row["complaint_count"])
    out = [
        {
            "cost_category": topic,
            "category_label": _topic_label(topic),
            "monthly_count_std": _round(pstdev(values) if len(values) > 1 else 0),
            "months": len(values),
        }
        for topic, values in grouped.items()
    ]
    out.sort(key=lambda row: row["monthly_count_std"], reverse=True)
    return {"rows": out, "meta": _base_meta(**filters)}


def category_keywords(
    *,
    category: str | None = None,
    limit: int = 20,
    sample_size: int = 1000,
    **filters: Any,
) -> dict[str, Any]:
    categories = _split_csv(category) or sorted(COST_OF_LIVING_TOPICS)
    sample_size = min(max(sample_size, 100), 5000)
    keywords_by_topic = {topic: COST_OF_LIVING_TOPICS.get(topic, []) for topic in categories}
    rows = []

    if use_local_store():
        posts = _local_filter_posts(_local_processed_posts(), topic=category, **filters)
        docs = [{"topic": post.get("topic"), "text": post.get("text") or post.get("raw_text", "")} for post in posts]
    else:
        result = get_es_client().search(
            index=settings.posts_index,
            body={
                "size": sample_size,
                "track_total_hits": False,
                "terminate_after": sample_size,
                "query": _query(_post_filters(topic=category, **filters)),
                "_source": ["topic", "text", "raw_text"],
            },
        )
        docs = [hit["_source"] for hit in result.get("hits", {}).get("hits", [])]

    grouped_text: dict[str, list[str]] = defaultdict(list)
    for doc in docs:
        row_topic = doc.get("topic")
        if row_topic in keywords_by_topic:
            grouped_text[row_topic].append((doc.get("text") or doc.get("raw_text") or "").casefold())

    for topic, keywords in keywords_by_topic.items():
        text_blob = "\n".join(grouped_text.get(topic, []))
        counts = Counter()
        for keyword in keywords:
            lowered = keyword.casefold()
            if " " in lowered:
                count = text_blob.count(lowered)
            else:
                count = len(re.findall(rf"\b{re.escape(lowered)}\b", text_blob))
            if count:
                counts[keyword] = count
        if not counts and text_blob:
            tokens = re.findall(r"\b[a-z][a-z]{2,}\b", text_blob)
            counts.update(token for token in tokens if token not in STOPWORDS)
        for keyword, frequency in counts.most_common(limit):
            rows.append(
                {
                    "cost_category": topic,
                    "category_label": _topic_label(topic),
                    "keyword": keyword,
                    "frequency": frequency,
                }
            )

    return {
        "rows": rows,
        "sample_size": sample_size,
        "method": "configured_topic_keyword_frequency",
        "meta": _base_meta(category=category, limit=limit, **filters),
    }


def error_logs(size: int = 50) -> dict[str, Any]:
    size = min(max(size, 1), 200)
    if use_local_store():
        errors = [
            post for post in load_local_raw_posts() if post.get("analysis_status") in {"error", "failed"}
        ][:size]
        return {"rows": errors, "summary": {"errors": len(errors)}}

    client = get_es_client()
    status_result = client.search(
        index=settings.raw_posts_index,
        body={
            "size": 0,
            "track_total_hits": False,
            "timeout": "10s",
            "aggs": {
                "by_status": {
                    "terms": {
                        "field": _term_field("analysis_status", settings.raw_posts_index),
                        "size": 10,
                    }
                }
            },
        },
    )
    status_counts = {
        bucket["key"]: bucket["doc_count"]
        for bucket in status_result.get("aggregations", {}).get("by_status", {}).get("buckets", [])
    }
    error_count = status_counts.get("error", 0) + status_counts.get("failed", 0)
    if error_count == 0:
        return {
            "rows": [],
            "summary": {"errors": 0},
            "meta": {"raw_index": settings.raw_posts_index, "status_counts": status_counts},
        }

    result = client.search(
        index=settings.raw_posts_index,
        body={
            "size": size,
            "track_total_hits": False,
            "timeout": "10s",
            "terminate_after": size,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "terms": {
                                _term_field("analysis_status", settings.raw_posts_index): [
                                    "error",
                                    "failed",
                                ]
                            }
                        },
                    ]
                }
            },
            "_source": [
                "id",
                "source_index",
                "source_es_id",
                "platform",
                "analysis_status",
                "analysis_processed_at",
                "harvested_at",
                "created_at",
                "url",
            ],
        },
    )
    rows = []
    for hit in result.get("hits", {}).get("hits", []):
        source = hit["_source"]
        rows.append(
            {
                "timestamp": source.get("analysis_processed_at") or source.get("harvested_at"),
                "function_name": "cost-living-platform-nlp-worker",
                "stage": "processing",
                "status": source.get("analysis_status"),
                "error_type": "processing_error",
                "raw_id": source.get("id"),
                "platform": source.get("platform"),
                "source_index": source.get("source_index"),
                "source_es_id": source.get("source_es_id"),
                "url": source.get("url"),
            }
        )
    return {
        "rows": rows,
        "summary": {"errors": len(rows), "estimated_errors": error_count},
        "meta": {"raw_index": settings.raw_posts_index, "status_counts": status_counts},
    }
