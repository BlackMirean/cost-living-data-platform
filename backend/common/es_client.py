"""Elasticsearch helpers for the cost-of-living data platform."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from elasticsearch import Elasticsearch

from backend.common.config import settings


REPO_ROOT = Path(__file__).resolve().parents[2]
POSTS_MAPPING_PATH = REPO_ROOT / "database" / "mappings" / "posts.json"
RAW_POSTS_MAPPING_PATH = REPO_ROOT / "database" / "mappings" / "raw_posts.json"
INDICATORS_MAPPING_PATH = REPO_ROOT / "database" / "mappings" / "indicators.json"
MONTHLY_METRICS_MAPPING_PATH = REPO_ROOT / "database" / "mappings" / "monthly_topic_metrics.json"


def get_es_client() -> Elasticsearch:
    """Create an Elasticsearch client using the configured URL."""

    kwargs: dict[str, Any] = {
        "request_timeout": 30,
        "max_retries": 3,
        "retry_on_timeout": True,
    }
    if settings.elasticsearch_username:
        kwargs["basic_auth"] = (
            settings.elasticsearch_username,
            settings.elasticsearch_password,
        )
    if settings.elasticsearch_url.startswith("https://"):
        kwargs["verify_certs"] = settings.elasticsearch_verify_certs
        kwargs["ssl_show_warn"] = False

    return Elasticsearch(
        settings.elasticsearch_url,
        **kwargs,
    )


def load_posts_mapping() -> dict[str, Any]:
    """Load the posts index mapping from the database folder."""

    with POSTS_MAPPING_PATH.open("r", encoding="utf-8") as mapping_file:
        return json.load(mapping_file)


def load_raw_posts_mapping() -> dict[str, Any]:
    """Load the raw posts index mapping from the database folder."""

    with RAW_POSTS_MAPPING_PATH.open("r", encoding="utf-8") as mapping_file:
        return json.load(mapping_file)


def load_indicators_mapping() -> dict[str, Any]:
    """Load the official indicators index mapping from the database folder."""

    with INDICATORS_MAPPING_PATH.open("r", encoding="utf-8") as mapping_file:
        return json.load(mapping_file)


def load_monthly_metrics_mapping() -> dict[str, Any]:
    """Load the monthly rollup index mapping from the database folder."""

    with MONTHLY_METRICS_MAPPING_PATH.open("r", encoding="utf-8") as mapping_file:
        return json.load(mapping_file)


def wait_for_elasticsearch(timeout_seconds: int = 90) -> bool:
    """Wait until Elasticsearch responds to ping."""

    client = get_es_client()
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            if client.ping():
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def ensure_posts_index(
    client: Elasticsearch | None = None,
    reset: bool = False,
    index_name: str | None = None,
) -> None:
    """Create the posts index if it does not exist."""

    client = client or get_es_client()
    index_name = index_name or settings.posts_index

    if reset and client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)

    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name, body=load_posts_mapping())


def ensure_raw_posts_index(client: Elasticsearch | None = None, reset: bool = False) -> None:
    """Create the raw harvested posts index if it does not exist."""

    client = client or get_es_client()
    index_name = settings.raw_posts_index

    if reset and client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)

    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name, body=load_raw_posts_mapping())


def ensure_indicators_index(client: Elasticsearch | None = None, reset: bool = False) -> None:
    """Create the official indicators index if it does not exist."""

    client = client or get_es_client()
    index_name = settings.indicators_index

    if reset and client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)

    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name, body=load_indicators_mapping())


def ensure_monthly_metrics_index(client: Elasticsearch | None = None, reset: bool = False) -> None:
    """Create the monthly dashboard rollup index if it does not exist."""

    client = client or get_es_client()
    index_name = settings.monthly_metrics_index

    if reset and client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)

    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name, body=load_monthly_metrics_mapping())
