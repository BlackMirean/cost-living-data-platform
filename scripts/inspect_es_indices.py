"""Read-only Elasticsearch index inspection utility."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from elasticsearch import NotFoundError  # noqa: E402

from backend.common.config import settings  # noqa: E402
from backend.common.es_client import get_es_client  # noqa: E402


DEFAULT_INDICES = [
    settings.bluesky_stream_raw_index,
    settings.mastodon_stream_raw_index,
    settings.gdelt_gkg_raw_index,
    settings.raw_posts_index,
    settings.posts_index,
    settings.indicators_index,
]

DATE_FIELDS = [
    "created_at",
    "collected_at",
    "harvested_at",
    "processed_at",
    "period_start",
]

TERM_FIELDS = [
    "platform",
    "stage",
    "category",
    "analysis_status",
    "sentiment_label",
    "topic",
    "source",
    "source_index",
    "domain",
    "has_engagement_metrics",
]

SAMPLE_FIELDS = [
    "id",
    "platform",
    "stage",
    "category",
    "analysis_status",
    "source",
    "source_id",
    "source_index",
    "source_es_id",
    "author",
    "author_handle",
    "author_display_name",
    "created_at",
    "collected_at",
    "harvested_at",
    "processed_at",
    "topic",
    "sentiment_score",
    "sentiment_label",
    "processing_status",
    "model_name",
    "model_version",
    "relevance_score",
    "search_term",
    "api_query",
    "harvested_query",
    "like_count",
    "reply_count",
    "repost_count",
    "quote_count",
    "has_engagement_metrics",
    "text",
    "url",
    "domain",
    "tone",
]


def truncate(value: Any, max_length: int = 220) -> Any:
    if not isinstance(value, str):
        return value
    if len(value) <= max_length:
        return value
    return value[: max_length - 1] + "..."


def mapping_properties(client: Any, index: str) -> dict[str, Any]:
    mapping = client.indices.get_mapping(index=index)
    return mapping[index].get("mappings", {}).get("properties", {})


def field_type(properties: dict[str, Any], field: str) -> str | None:
    spec = properties.get(field)
    if not spec:
        return None
    return spec.get("type")


def terms_field(properties: dict[str, Any], field: str) -> str | None:
    spec = properties.get(field)
    if not spec:
        return None
    if spec.get("type") in {"keyword", "boolean", "integer", "long"}:
        return field
    if "keyword" in spec.get("fields", {}):
        return f"{field}.keyword"
    return None


def date_range(client: Any, index: str, properties: dict[str, Any]) -> dict[str, Any]:
    aggs: dict[str, Any] = {}
    for field in DATE_FIELDS:
        if field_type(properties, field) == "date":
            aggs[f"{field}_min"] = {"min": {"field": field}}
            aggs[f"{field}_max"] = {"max": {"field": field}}
    if not aggs:
        return {}
    result = client.search(index=index, body={"size": 0, "aggs": aggs})
    values = result.get("aggregations", {})
    ranges: dict[str, Any] = {}
    for field in DATE_FIELDS:
        min_value = values.get(f"{field}_min", {}).get("value_as_string")
        max_value = values.get(f"{field}_max", {}).get("value_as_string")
        if min_value or max_value:
            ranges[field] = {"min": min_value, "max": max_value}
    return ranges


def term_summaries(client: Any, index: str, properties: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    aggs: dict[str, Any] = {}
    output_fields: dict[str, str] = {}
    for field in TERM_FIELDS:
        es_field = terms_field(properties, field)
        if not es_field:
            continue
        output_fields[field] = es_field
        aggs[field] = {"terms": {"field": es_field, "size": 12}}
    if not aggs:
        return {}
    result = client.search(index=index, body={"size": 0, "aggs": aggs})
    aggregations = result.get("aggregations", {})
    return {
        field: aggregations.get(field, {}).get("buckets", [])
        for field in output_fields
    }


def samples(client: Any, index: str, size: int) -> list[dict[str, Any]]:
    if size <= 0:
        return []
    result = client.search(
        index=index,
        body={
            "size": size,
            "query": {"match_all": {}},
            "_source": SAMPLE_FIELDS,
            "sort": [{"_doc": {"order": "asc"}}],
        },
    )
    rows = []
    for hit in result.get("hits", {}).get("hits", []):
        source = hit.get("_source", {})
        row = {"_id": hit.get("_id")}
        for field in SAMPLE_FIELDS:
            if field in source:
                row[field] = truncate(source[field])
        rows.append(row)
    return rows


def inspect_index(client: Any, index: str, sample_size: int) -> dict[str, Any]:
    try:
        properties = mapping_properties(client, index)
        count = int(client.count(index=index).get("count", 0))
    except NotFoundError:
        return {"index": index, "exists": False, "error": "index_not_found"}

    return {
        "index": index,
        "exists": True,
        "count": count,
        "fields": sorted(properties.keys()),
        "date_ranges": date_range(client, index, properties),
        "terms": term_summaries(client, index, properties),
        "samples": samples(client, index, sample_size),
    }


def print_human(report: list[dict[str, Any]]) -> None:
    for item in report:
        print(f"\n=== {item['index']} ===")
        if not item.get("exists"):
            print(f"status: missing ({item.get('error')})")
            continue
        print(f"documents: {item['count']}")
        print("fields:")
        print("  " + ", ".join(item["fields"]))
        if item["date_ranges"]:
            print("date ranges:")
            for field, value in item["date_ranges"].items():
                print(f"  {field}: {value.get('min')} -> {value.get('max')}")
        if item["terms"]:
            print("term distributions:")
            for field, buckets in item["terms"].items():
                compact = ", ".join(f"{bucket['key']}={bucket['doc_count']}" for bucket in buckets)
                print(f"  {field}: {compact}")
        if item["samples"]:
            print("samples:")
            for sample in item["samples"]:
                print(json.dumps(sample, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect Elasticsearch indices without modifying them.")
    parser.add_argument(
        "--indices",
        nargs="+",
        default=DEFAULT_INDICES,
        help="Indices to inspect. Defaults to the configured platform raw, unified raw, processed and indicator indices.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=2,
        help="Number of sample documents per index. Use 0 to disable samples.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON for automation or screenshots.")
    args = parser.parse_args()

    client = get_es_client()
    report = [inspect_index(client, index, args.sample_size) for index in args.indices]
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print_human(report)


if __name__ == "__main__":
    main()
