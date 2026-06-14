"""Import source-specific raw streams into the unified raw-posts index.

The default mode is a dry run. Use --write to persist records and
--reset-target when intentionally rebuilding the target index.
This script only reads source indices; it does not modify them.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from elasticsearch import helpers  # noqa: E402

from backend.analytics.topics import (  # noqa: E402
    has_australia_context,
    infer_topic_from_query,
    is_relevant_cost_of_living_text,
)
from backend.common.config import settings  # noqa: E402
from backend.common.es_client import ensure_raw_posts_index, get_es_client  # noqa: E402
from backend.common.source_registry import (  # noqa: E402
    configured_source_indices,
    social_platforms,
    source_choices_text,
    source_labels,
    source_names,
)


SOURCE_INDICES = configured_source_indices()
SOCIAL_PLATFORMS = social_platforms()
SOURCE_LABELS = source_labels()


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def stable_id(source_index: str, source_es_id: str) -> str:
    digest = hashlib.sha1(f"{source_index}|{source_es_id}".encode("utf-8")).hexdigest()
    return f"unified-{digest}"


def compact_text(value: Any, max_length: int) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) <= max_length:
        return text
    return text[:max_length]


def clean_str(value: Any) -> str:
    return str(value or "").strip()


def int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def social_count(doc: dict[str, Any], field: str, platform: str) -> int | None:
    if platform not in SOCIAL_PLATFORMS:
        return None
    value = int_or_none(doc.get(field))
    return 0 if value is None else value


def harvested_query(doc: dict[str, Any]) -> str:
    parts = [
        clean_str(doc.get("category")),
        clean_str(doc.get("api_query")),
        clean_str(doc.get("search_term")),
    ]
    return " | ".join(part for part in parts if part)


def author_value(doc: dict[str, Any]) -> str:
    return (
        clean_str(doc.get("author_handle"))
        or clean_str(doc.get("author_display_name"))
        or clean_str(doc.get("domain"))
        or "unknown"
    )


def should_keep_social(doc: dict[str, Any], strict_filter: bool) -> bool:
    if not strict_filter:
        return True
    text = doc.get("text") or ""
    query = harvested_query(doc)
    topic_hint = infer_topic_from_query(query)
    return has_australia_context(text) and is_relevant_cost_of_living_text(
        text,
        topic_hint=topic_hint,
        trust_topic_hint=bool(topic_hint),
    )


def build_payload(source_index: str, hit: dict[str, Any], doc: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_index": source_index,
        "source_es_id": hit.get("_id"),
        "source_fields": sorted(doc.keys()),
    }


def normalise_doc(
    source_name: str,
    source_index: str,
    hit: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any] | None:
    doc = hit.get("_source", {})
    platform = clean_str(doc.get("platform")) or source_name
    if platform == "gdelt_gkg":
        platform = "gdelt"

    if platform in SOCIAL_PLATFORMS and not should_keep_social(doc, args.strict_filter):
        return None

    source_es_id = clean_str(hit.get("_id")) or clean_str(doc.get("source_id"))
    if not source_es_id:
        return None

    source_id = clean_str(doc.get("source_id")) or clean_str(doc.get("post_id")) or source_es_id
    imported_at = now_iso()
    like_count = social_count(doc, "like_count", platform)
    reply_count = social_count(doc, "reply_count", platform)
    repost_count = social_count(doc, "repost_count", platform)
    quote_count = social_count(doc, "quote_count", platform)

    return {
        "id": stable_id(source_index, source_es_id),
        "platform": platform,
        "stage": clean_str(doc.get("stage")) or "raw",
        "source": SOURCE_LABELS.get(source_name, "raw_stream"),
        "source_index": source_index,
        "source_es_id": source_es_id,
        "source_id": source_id,
        "category": clean_str(doc.get("category")),
        "search_term": clean_str(doc.get("search_term")),
        "api_query": clean_str(doc.get("api_query")),
        "harvested_query": harvested_query(doc),
        "instance": clean_str(doc.get("instance")),
        "author": author_value(doc),
        "author_handle": clean_str(doc.get("author_handle")),
        "author_display_name": clean_str(doc.get("author_display_name")),
        "location_hint": clean_str(doc.get("instance")) or clean_str(doc.get("domain")),
        "text": compact_text(doc.get("text"), max_length=args.max_text_length),
        "url": clean_str(doc.get("url")) or clean_str(doc.get("gkg_url")),
        "created_at": doc.get("created_at") or doc.get("collected_at") or imported_at,
        "collected_at": doc.get("collected_at"),
        "harvested_at": doc.get("collected_at") or imported_at,
        "like_count": like_count,
        "reply_count": reply_count,
        "repost_count": repost_count,
        "quote_count": quote_count,
        "has_engagement_metrics": platform in SOCIAL_PLATFORMS,
        "domain": clean_str(doc.get("domain")),
        "gkg_file_id": clean_str(doc.get("gkg_file_id")),
        "gkg_url": clean_str(doc.get("gkg_url")),
        "tone": clean_str(doc.get("tone")),
        "analysis_status": "pending",
        "engagement": {
            "like_count": like_count,
            "reply_count": reply_count,
            "repost_count": repost_count,
            "quote_count": quote_count,
        },
        "payload": build_payload(source_index, hit, doc),
    }


def source_query(args: argparse.Namespace) -> dict[str, Any]:
    filters: list[dict[str, Any]] = []
    if args.lookback_hours:
        start = datetime.now(tz=timezone.utc) - timedelta(hours=args.lookback_hours)
        filters.append({"range": {"collected_at": {"gte": start.isoformat()}}})
    if args.start_date:
        filters.append({"range": {"created_at": {"gte": args.start_date}}})
    if args.end_date:
        filters.append({"range": {"created_at": {"lte": args.end_date}}})
    if filters:
        return {"query": {"bool": {"filter": filters}}}
    return {"query": {"match_all": {}}}


def iter_hits(client: Any, index: str, args: argparse.Namespace) -> Iterable[dict[str, Any]]:
    query = source_query(args)
    limit = args.limit_per_index
    for seen, hit in enumerate(
        helpers.scan(
            client,
            index=index,
            query=query,
            size=args.scan_size,
            preserve_order=False,
        ),
        start=1,
    ):
        if limit > 0 and seen > limit:
            break
        yield hit


def bulk_write(client: Any, target_index: str, docs: list[dict[str, Any]]) -> int:
    if not docs:
        return 0
    created, _errors = helpers.bulk(
        client,
        [
            {
                "_op_type": "create",
                "_index": target_index,
                "_id": doc["id"],
                "_source": doc,
            }
            for doc in docs
        ],
        raise_on_error=False,
    )
    return int(created)


def selected_indices(args: argparse.Namespace) -> list[tuple[str, str]]:
    selected: list[tuple[str, str]] = []
    sources = list(args.sources)
    for source in sources:
        if source not in SOURCE_INDICES:
            raise ValueError("Unknown source: " + source + ". Choices: " + source_choices_text())
        selected.append((source, SOURCE_INDICES[source]))
    return selected


def import_index(client: Any, source_name: str, source_index: str, args: argparse.Namespace) -> dict[str, Any]:
    seen = 0
    normalised = 0
    skipped = 0
    written = 0
    examples: list[dict[str, Any]] = []
    batch: list[dict[str, Any]] = []

    for hit in iter_hits(client, source_index, args):
        seen += 1
        doc = normalise_doc(source_name, source_index, hit, args)

        if not doc:
            skipped += 1
            continue
        normalised += 1
        if len(examples) < args.sample_size:
            examples.append(
                {
                    "id": doc["id"],
                    "platform": doc["platform"],
                    "source_index": doc["source_index"],
                    "source_es_id": doc["source_es_id"],
                    "created_at": doc["created_at"],
                    "category": doc["category"],
                    "like_count": doc["like_count"],
                    "reply_count": doc["reply_count"],
                    "repost_count": doc["repost_count"],
                    "quote_count": doc["quote_count"],
                    "text": doc["text"][:220],
                }
            )
        if args.write:
            batch.append(doc)
            if len(batch) >= args.bulk_size:
                written += bulk_write(client, args.target_index, batch)
                batch = []

    if args.write and batch:
        written += bulk_write(client, args.target_index, batch)

    if args.write:
        client.indices.refresh(index=args.target_index)

    return {
        "source": source_name,
        "source_index": source_index,
        "seen": seen,
        "normalised": normalised,
        "skipped": skipped,
        "written": written,
        "examples": examples,
    }


def prepare_target_index(client: Any, args: argparse.Namespace) -> None:
    if not args.write:
        return
    if args.reset_target and client.indices.exists(index=args.target_index):
        client.indices.delete(index=args.target_index)
    ensure_raw_posts_index(client)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import Bluesky, Mastodon and GDELT raw streams into the unified raw-posts index."
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        default=source_names(),
        help="Sources to import. Choices: " + source_choices_text() + ". Defaults to all sources.",
    )
    parser.add_argument(
        "--target-index",
        default=settings.raw_posts_index,
        help="Target unified raw index. Defaults to RAW_POSTS_INDEX / settings.raw_posts_index.",
    )
    parser.add_argument(
        "--limit-per-index",
        type=int,
        default=1000,
        help="Maximum records per source. Use 0 for all records. Default 1000 is suitable for smoke tests.",
    )
    parser.add_argument("--start-date", default=None, help="Only import documents with created_at >= this value.")
    parser.add_argument("--end-date", default=None, help="Only import documents with created_at <= this value.")
    parser.add_argument(
        "--lookback-hours",
        type=float,
        default=0,
        help="Only scan source documents collected in the last N hours. Use 0 for no limit.",
    )
    parser.add_argument(
        "--strict-filter",
        action="store_true",
        help="Keep only social records that have Australian context and cost-of-living relevance.",
    )
    parser.add_argument("--scan-size", type=int, default=1000, help="Elasticsearch scan page size.")
    parser.add_argument("--bulk-size", type=int, default=1000, help="Elasticsearch bulk write batch size.")
    parser.add_argument("--sample-size", type=int, default=5, help="Normalised sample count per source.")
    parser.add_argument(
        "--max-text-length",
        type=int,
        default=10000,
        help="Maximum characters retained for each text field.",
    )
    parser.add_argument(
        "--reset-target",
        action="store_true",
        help="Delete and recreate the target index before writing. Source indices are not modified.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write records to the target index. Without this flag, the script runs in dry-run mode.",
    )
    return parser


def run_import(args: argparse.Namespace) -> dict[str, Any]:
    client = get_es_client()
    prepare_target_index(client, args)

    summaries = []
    for source_name, source_index in selected_indices(args):
        summaries.append(import_index(client, source_name, source_index, args))

    return {
        "mode": "write" if args.write else "dry-run",
        "target_index": args.target_index,
        "reset_target": args.reset_target,
        "strict_filter": args.strict_filter,
        "limit_per_index": args.limit_per_index,
        "sources": args.sources,
        "summaries": summaries,
        "note": (
            "Dry run only; add --write after validating the samples."
            if not args.write
            else "Records were written to the target index. Source indices were not modified."
        ),
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    output = run_import(args)
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
