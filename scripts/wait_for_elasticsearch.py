"""Wait for local Elasticsearch to become reachable."""

from __future__ import annotations

import sys

from backend.common.config import settings
from backend.common.es_client import wait_for_elasticsearch


if settings.elasticsearch_url.startswith("memory://"):
    print("Using memory store; Elasticsearch wait skipped.")
    sys.exit(0)

if wait_for_elasticsearch(timeout_seconds=120):
    print("Elasticsearch is ready.")
    sys.exit(0)

print("Elasticsearch did not become ready in time.")
sys.exit(1)
