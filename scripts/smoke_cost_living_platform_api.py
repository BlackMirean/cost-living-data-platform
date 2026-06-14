"""Smoke-test the deployed cost-living platform API routes."""

from __future__ import annotations

import argparse
import json
import sys
import time
from urllib.parse import urlencode

import requests


ENDPOINTS: list[tuple[str, dict[str, str]]] = [
    ("/health", {}),
    ("/pipeline/status", {}),
    ("/pipeline/runtime", {}),
    ("/pipeline/events", {"limit": "20"}),
    ("/cache/status", {}),
    ("/platforms/plugins", {}),
    ("/stats/overview", {"source_group": "social", "quality": "clean"}),
    ("/trends/documents", {"period": "month", "source_group": "social", "quality": "clean"}),
    ("/categories/counts", {"source_group": "social", "quality": "clean"}),
    ("/categories/sentiment", {"source_group": "social", "quality": "clean"}),
    ("/categories/share", {"period": "month", "source_group": "social", "quality": "clean"}),
    ("/data-quality/summary", {"source_group": "all"}),
    ("/data-quality/comparison", {"source_group": "all"}),
    ("/media/coverage", {"period": "month", "quality": "clean"}),
    ("/platforms/categories", {"source_group": "all", "quality": "clean"}),
    ("/trends/categories", {"period": "month", "source_group": "social", "quality": "clean"}),
    ("/trends/sentiment", {"period": "month", "source_group": "social", "quality": "clean"}),
    ("/official/comparison", {"topic": "groceries", "source_group": "social", "quality": "clean"}),
    ("/categories/yoy-change", {"source_group": "social", "quality": "clean"}),
    ("/categories/volatility", {"source_group": "social", "quality": "clean"}),
    ("/categories/keywords", {"category": "housing", "limit": "10", "sample_size": "1000", "quality": "clean"}),
    ("/logs/errors", {"size": "20"}),
]


def request_url(base_url: str, prefix: str, path: str, params: dict[str, str]) -> str:
    query = urlencode(params)
    url = f"{base_url.rstrip('/')}{prefix.rstrip('/')}{path}"
    return f"{url}?{query}" if query else url


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test all platform API routes.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8010")
    parser.add_argument("--prefix", default="/api/cost-living")
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args()

    rows = []
    failures = []
    for path, params in ENDPOINTS:
        url = request_url(args.base_url, args.prefix, path, params)
        started = time.monotonic()
        try:
            response = requests.get(url, timeout=args.timeout)
            duration = time.monotonic() - started
            payload = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
            ok = response.status_code == 200
            row = {
                "path": path,
                "status_code": response.status_code,
                "ok": ok,
                "duration_seconds": round(duration, 3),
                "rows": len(payload.get("rows", [])) if isinstance(payload, dict) else None,
            }
        except Exception as exc:
            row = {
                "path": path,
                "status_code": None,
                "ok": False,
                "duration_seconds": None,
                "error": f"{type(exc).__name__}: {exc}",
            }
        rows.append(row)
        if not row["ok"]:
            failures.append(row)
        print(json.dumps(row, ensure_ascii=False), flush=True)

    summary = {"total": len(rows), "passed": len(rows) - len(failures), "failed": len(failures)}
    print(json.dumps({"summary": summary}, ensure_ascii=False), flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
