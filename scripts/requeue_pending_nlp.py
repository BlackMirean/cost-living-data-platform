"""Rebuild NLP work queue messages from Elasticsearch raw processing state."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from backend.common.config import settings  # noqa: E402
from backend.common.es_client import get_es_client  # noqa: E402
from backend.common.work_queue import RedisWorkQueue, enqueue_nlp_work  # noqa: E402
from backend.processing.nlp_worker import pending_query  # noqa: E402


def count_pending(client: Any) -> int:
    result = client.count(index=settings.raw_posts_index, body=pending_query(), request_timeout=60)
    return int(result.get("count", 0))


def reset_error_documents(client: Any, *, dry_run: bool) -> int:
    body = {
        "script": {
            "source": """
                ctx._source.analysis_status = 'pending';
                ctx._source.analysis_error = null;
                ctx._source.analysis_started_at = null;
            """,
            "lang": "painless",
        },
        "query": {
            "bool": {
                "should": [
                    {"term": {"analysis_status": "error"}},
                    {"term": {"analysis_status.keyword": "error"}},
                ],
                "minimum_should_match": 1,
            }
        },
    }
    count = client.count(index=settings.raw_posts_index, body={"query": body["query"]}, request_timeout=60)
    if dry_run:
        return int(count.get("count", 0))
    result = client.update_by_query(
        index=settings.raw_posts_index,
        body=body,
        conflicts="proceed",
        refresh=True,
        wait_for_completion=True,
        request_timeout=600,
    )
    return int(result.get("updated", 0))


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild Redis NLP queue from raw Elasticsearch status.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reset-errors", action="store_true", help="Move raw records in error state back to pending.")
    parser.add_argument("--reason", default="manual_requeue")
    args = parser.parse_args()

    client = get_es_client()
    reset_count = 0
    if args.reset_errors:
        reset_count = reset_error_documents(client, dry_run=args.dry_run)
    pending_count = count_pending(client)
    if args.dry_run:
        result = {
            "dry_run": True,
            "raw_index": settings.raw_posts_index,
            "reset_error_documents": reset_count,
            "pending_or_stale_documents": pending_count,
            "queue_key": RedisWorkQueue().queue_key(settings.nlp_queue_name),
        }
    else:
        result = enqueue_nlp_work(reason=args.reason, document_count=pending_count)
        result.update(
            {
                "dry_run": False,
                "raw_index": settings.raw_posts_index,
                "reset_error_documents": reset_count,
                "pending_or_stale_documents": pending_count,
            }
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
