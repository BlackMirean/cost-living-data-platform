"""Shared document-store helpers for ingestion and health checks."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from elasticsearch import NotFoundError, helpers

from backend.common.config import settings
from backend.common.es_client import (
    ensure_indicators_index,
    ensure_raw_posts_index,
    get_es_client,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
LOCAL_STORE_PATH = REPO_ROOT / "data" / "local_store" / "posts.json"
LOCAL_RAW_POSTS_PATH = REPO_ROOT / "data" / "local_store" / "raw_posts.json"
LOCAL_INDICATORS_PATH = REPO_ROOT / "data" / "local_store" / "indicators.json"


def use_local_store() -> bool:
    """Return true when the project is running without Elasticsearch."""

    return settings.elasticsearch_url.startswith("memory://")


def _relative_name(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT))


def load_local_posts() -> list[dict[str, Any]]:
    if not LOCAL_STORE_PATH.exists():
        return []
    with LOCAL_STORE_PATH.open("r", encoding="utf-8") as store_file:
        return json.load(store_file)


def save_local_posts(posts: list[dict[str, Any]]) -> None:
    LOCAL_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOCAL_STORE_PATH.open("w", encoding="utf-8") as store_file:
        json.dump(posts, store_file, indent=2)


def load_local_raw_posts() -> list[dict[str, Any]]:
    if not LOCAL_RAW_POSTS_PATH.exists():
        return []
    with LOCAL_RAW_POSTS_PATH.open("r", encoding="utf-8") as store_file:
        return json.load(store_file)


def save_local_raw_posts(posts: list[dict[str, Any]]) -> None:
    LOCAL_RAW_POSTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOCAL_RAW_POSTS_PATH.open("w", encoding="utf-8") as store_file:
        json.dump(posts, store_file, indent=2)


def load_local_indicators() -> list[dict[str, Any]]:
    if not LOCAL_INDICATORS_PATH.exists():
        return []
    with LOCAL_INDICATORS_PATH.open("r", encoding="utf-8") as store_file:
        return json.load(store_file)


def save_local_indicators(indicators: list[dict[str, Any]]) -> None:
    LOCAL_INDICATORS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOCAL_INDICATORS_PATH.open("w", encoding="utf-8") as store_file:
        json.dump(indicators, store_file, indent=2)


def health() -> dict[str, Any]:
    """Return compact health details for the active document store."""

    if use_local_store():
        return {
            "status": "ok",
            "service": "cost-of-living-api",
            "store": "memory",
            "posts_index": _relative_name(LOCAL_STORE_PATH),
            "raw_posts_index": _relative_name(LOCAL_RAW_POSTS_PATH),
            "indicators_index": _relative_name(LOCAL_INDICATORS_PATH),
            "processed_documents": len(load_local_posts()),
            "raw_documents": len(load_local_raw_posts()),
            "official_indicators": len(load_local_indicators()),
        }

    client = get_es_client()
    try:
        es_ok = client.ping()
        posts_exists = client.indices.exists(index=settings.posts_index) if es_ok else False
        raw_exists = client.indices.exists(index=settings.raw_posts_index) if es_ok else False
        indicators_exists = (
            client.indices.exists(index=settings.indicators_index) if es_ok else False
        )
    except Exception as exc:
        return {
            "status": "degraded",
            "service": "cost-of-living-api",
            "store": "elasticsearch",
            "elasticsearch": "down",
            "posts_index": settings.posts_index,
            "raw_posts_index": settings.raw_posts_index,
            "indicators_index": settings.indicators_index,
            "error": str(exc),
        }

    return {
        "status": "ok" if es_ok and posts_exists and indicators_exists else "degraded",
        "service": "cost-of-living-api",
        "store": "elasticsearch",
        "elasticsearch": "ok" if es_ok else "down",
        "posts_index": settings.posts_index,
        "posts_index_exists": bool(posts_exists),
        "raw_posts_index": settings.raw_posts_index,
        "raw_posts_index_exists": bool(raw_exists),
        "indicators_index": settings.indicators_index,
        "indicators_index_exists": bool(indicators_exists),
    }


def index_raw_posts(docs: list[dict[str, Any]], reset: bool = False) -> int:
    """Index unified raw records into the active raw-posts store."""

    if not docs:
        return 0

    if use_local_store():
        existing = [] if reset else load_local_raw_posts()
        by_id = {doc["id"]: doc for doc in existing}
        for doc in docs:
            by_id[doc["id"]] = doc
        save_local_raw_posts(list(by_id.values()))
        return len(docs)

    client = get_es_client()
    ensure_raw_posts_index(client, reset=reset)
    helpers.bulk(
        client,
        [
            {
                "_op_type": "index",
                "_index": settings.raw_posts_index,
                "_id": doc["id"],
                "_source": doc,
            }
            for doc in docs
        ],
    )
    client.indices.refresh(index=settings.raw_posts_index)
    return len(docs)


def reset_raw_posts_store() -> None:
    """Clear and recreate the raw-posts store for an explicit backfill reset."""

    if use_local_store():
        save_local_raw_posts([])
        return

    ensure_raw_posts_index(get_es_client(), reset=True)


def index_indicators(docs: list[dict[str, Any]], reset: bool = False) -> int:
    """Index official indicator documents into the active indicator store."""

    if not docs:
        return 0

    if use_local_store():
        existing = [] if reset else load_local_indicators()
        by_id = {doc["id"]: doc for doc in existing}
        for doc in docs:
            by_id[doc["id"]] = doc
        save_local_indicators(list(by_id.values()))
        return len(docs)

    client = get_es_client()
    ensure_indicators_index(client, reset=reset)
    helpers.bulk(
        client,
        [
            {
                "_op_type": "index",
                "_index": settings.indicators_index,
                "_id": doc["id"],
                "_source": doc,
            }
            for doc in docs
        ],
    )
    client.indices.refresh(index=settings.indicators_index)
    return len(docs)


def list_indicators(
    indicator: str | None = None,
    item_name: str | None = None,
    size: int = 100,
) -> list[dict[str, Any]]:
    """List official indicator documents with light filtering."""

    if use_local_store():
        docs = load_local_indicators()
        if indicator:
            docs = [doc for doc in docs if doc.get("indicator") == indicator]
        if item_name:
            needle = item_name.casefold()
            docs = [doc for doc in docs if needle in doc.get("item_name", "").casefold()]
        docs.sort(key=lambda doc: doc.get("period_start", ""), reverse=True)
        return docs[:size]

    filters: list[dict[str, Any]] = []
    if indicator:
        filters.append({"term": {"indicator": indicator}})
    if item_name:
        filters.append({"match": {"item_name": item_name}})

    try:
        result = get_es_client().search(
            index=settings.indicators_index,
            body={
                "size": size,
                "query": {"bool": {"filter": filters}} if filters else {"match_all": {}},
                "sort": [{"period_start": {"order": "desc"}}],
            },
        )
    except NotFoundError:
        return []
    return [hit["_source"] for hit in result.get("hits", {}).get("hits", [])]

