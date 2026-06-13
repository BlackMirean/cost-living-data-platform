"""Build monthly topic rollups for faster dashboard reads.

The command is dry-run by default. Add --write only after the target Elasticsearch
index has been reviewed or created in the cloud environment.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from elasticsearch import helpers

from backend.common import analytics_store
from backend.common.config import settings
from backend.common.es_client import ensure_monthly_metrics_index, get_es_client


def _metric_id(period: str, source_group: str, quality: str, cost_category: str) -> str:
    digest = hashlib.sha1(f"{period}|{source_group}|{quality}|{cost_category}".encode("utf-8")).hexdigest()
    return f"monthly-topic-{digest}"


def _period_start(period: str) -> str:
    return f"{period}-01T00:00:00+00:00"


def build_rollup_documents(source_groups: list[str], quality: str) -> list[dict[str, Any]]:
    generated_at = datetime.now(timezone.utc).isoformat()
    docs: list[dict[str, Any]] = []
    for source_group in source_groups:
        result = analytics_store.trends_sentiment(
            period="month",
            source_group=source_group,
            quality=quality,
        )
        for row in result["rows"]:
            metric_id = _metric_id(row["period"], source_group, quality, row["cost_category"])
            docs.append(
                {
                    "metric_id": metric_id,
                    "period": row["period"],
                    "period_start": _period_start(row["period"]),
                    "source_group": source_group,
                    "quality": quality,
                    "cost_category": row["cost_category"],
                    "category_label": row["category_label"],
                    "document_count": row["document_count"],
                    "unique_document_count": row.get("unique_document_count", row["document_count"]),
                    "duplicate_ratio": row.get("duplicate_ratio", 0.0),
                    "negative_count": row.get("negative_count", 0),
                    "negative_ratio": row.get("negative_ratio"),
                    "avg_sentiment": row.get("avg_sentiment"),
                    "generated_at": generated_at,
                }
            )
    return docs


def write_rollups(docs: list[dict[str, Any]], reset_index: bool = False) -> int:
    client = get_es_client()
    ensure_monthly_metrics_index(client, reset=reset_index)
    actions = [
        {
            "_op_type": "index",
            "_index": settings.monthly_metrics_index,
            "_id": doc["metric_id"],
            "_source": doc,
        }
        for doc in docs
    ]
    if not actions:
        return 0
    helpers.bulk(client, actions, raise_on_error=False)
    client.indices.refresh(index=settings.monthly_metrics_index)
    return len(actions)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build monthly topic rollups.")
    parser.add_argument("--source-groups", default="all,social,media")
    parser.add_argument("--quality", choices=["all", "clean"], default="clean")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--reset-index", action="store_true")
    args = parser.parse_args()

    source_groups = [item.strip() for item in args.source_groups.split(",") if item.strip()]
    docs = build_rollup_documents(source_groups=source_groups, quality=args.quality)
    if args.write:
        written = write_rollups(docs, reset_index=args.reset_index)
        print(json.dumps({"written": written, "index": settings.monthly_metrics_index}, indent=2))
        return

    print(
        json.dumps(
            {
                "dry_run": True,
                "index": settings.monthly_metrics_index,
                "document_count": len(docs),
                "preview": docs[:5],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
