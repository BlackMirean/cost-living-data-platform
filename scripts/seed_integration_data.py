"""Seed a small deterministic dataset for Docker Compose integration tests."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from elasticsearch import helpers


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from backend.common.config import settings  # noqa: E402
from backend.common.es_client import (  # noqa: E402
    ensure_indicators_index,
    ensure_monthly_metrics_index,
    ensure_raw_posts_index,
    get_es_client,
)
from scripts.apply_elasticsearch_lifecycle import apply_lifecycle, BACKING_INDEX  # noqa: E402


RAW_DOCS = [
    {
        "_id": "integration-raw-1",
        "id": "integration-raw-1",
        "platform": "bluesky",
        "source": "integration",
        "source_id": "integration-blue-1",
        "source_index": "integration",
        "stage": "raw",
        "category": "groceries",
        "text": "Australian grocery prices and supermarket bills keep rising for families.",
        "created_at": "2026-06-01T00:00:00Z",
        "harvested_at": "2026-06-01T00:01:00Z",
        "analysis_status": "pending",
    },
    {
        "_id": "integration-raw-2",
        "id": "integration-raw-2",
        "platform": "mastodon",
        "source": "integration",
        "source_id": "integration-masto-1",
        "source_index": "integration",
        "stage": "raw",
        "category": "housing",
        "text": "Rent increases and mortgage stress are hurting households in Australia.",
        "created_at": "2026-06-02T00:00:00Z",
        "harvested_at": "2026-06-02T00:01:00Z",
        "analysis_status": "pending",
    },
]


INDICATORS = [
    {
        "_id": "integration-cpi-rents",
        "id": "integration-cpi-rents",
        "source": "abs",
        "indicator": "monthly_cpi",
        "measure": "Percentage change from previous year",
        "item_name": "Rents",
        "period": "2026-06",
        "period_start": "2026-06-01T00:00:00Z",
        "value": 6.1,
        "unit": "percent",
        "created_at": "2026-06-03T00:00:00Z",
        "harvested_at": "2026-06-03T00:00:00Z",
    },
    {
        "_id": "integration-cpi-food",
        "id": "integration-cpi-food",
        "source": "abs",
        "indicator": "monthly_cpi",
        "measure": "Percentage change from previous year",
        "item_name": "Food and non-alcoholic beverages",
        "period": "2026-06",
        "period_start": "2026-06-01T00:00:00Z",
        "value": 4.2,
        "unit": "percent",
        "created_at": "2026-06-03T00:00:00Z",
        "harvested_at": "2026-06-03T00:00:00Z",
    },
]


def delete_if_exists(client: Any, name: str) -> None:
    if client.indices.exists(index=name):
        client.indices.delete(index=name)


def reset_indices(client: Any) -> None:
    for alias in (settings.processed_posts_write_index, settings.posts_current_alias):
        try:
            for index in client.indices.get_alias(name=alias).keys():
                client.indices.delete_alias(index=index, name=alias, ignore_unavailable=True)
        except Exception:
            pass
    for index in (
        settings.raw_posts_index,
        settings.indicators_index,
        settings.monthly_metrics_index,
        BACKING_INDEX,
        "cost_living_processed_posts",
    ):
        delete_if_exists(client, index)


def bulk_index(client: Any, index: str, docs: list[dict[str, Any]]) -> None:
    actions = []
    for doc in docs:
        source = dict(doc)
        doc_id = source.pop("_id")
        actions.append({"_op_type": "index", "_index": index, "_id": doc_id, "_source": source})
    helpers.bulk(client, actions, refresh=True, request_timeout=120)


def seed(reset: bool) -> dict[str, Any]:
    client = get_es_client()
    if reset:
        reset_indices(client)
    apply_lifecycle()
    ensure_raw_posts_index(client)
    ensure_indicators_index(client)
    ensure_monthly_metrics_index(client)
    bulk_index(client, settings.raw_posts_index, RAW_DOCS)
    bulk_index(client, settings.indicators_index, INDICATORS)
    return {
        "raw_index": settings.raw_posts_index,
        "raw_documents": len(RAW_DOCS),
        "indicators_index": settings.indicators_index,
        "indicator_documents": len(INDICATORS),
        "processed_write_alias": settings.processed_posts_write_index,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed Docker Compose integration data.")
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()
    print(json.dumps(seed(reset=args.reset), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
