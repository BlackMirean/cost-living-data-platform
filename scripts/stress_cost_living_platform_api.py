"""Small concurrent access test for the cost-living platform API."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode

import requests


REQUESTS: list[tuple[str, dict[str, str]]] = [
    ("/health", {}),
    ("/pipeline/status", {}),
    ("/stats/overview", {"source_group": "social", "quality": "clean"}),
    ("/categories/counts", {"source_group": "social", "quality": "clean"}),
    ("/categories/sentiment", {"source_group": "social", "quality": "clean"}),
    ("/trends/documents", {"period": "month", "source_group": "social", "quality": "clean"}),
]


def build_url(base_url: str, prefix: str, path: str, params: dict[str, str]) -> str:
    query = urlencode(params)
    url = f"{base_url.rstrip('/')}{prefix.rstrip('/')}{path}"
    return f"{url}?{query}" if query else url


def call(path: str, url: str, timeout: float) -> dict[str, object]:
    started = time.monotonic()
    try:
        response = requests.get(url, timeout=timeout)
        duration = time.monotonic() - started
        return {
            "path": path,
            "ok": response.status_code == 200,
            "status_code": response.status_code,
            "duration": duration,
        }
    except Exception as exc:
        duration = time.monotonic() - started
        return {
            "path": path,
            "ok": False,
            "status_code": None,
            "duration": duration,
            "error": f"{type(exc).__name__}: {exc}",
        }


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * fraction)))
    return ordered[index]


def duration_summary(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"average_duration": None, "p95_duration": None, "max_duration": None}
    return {
        "average_duration": round(statistics.mean(values), 3),
        "p95_duration": round(percentile(values, 0.95) or 0, 3),
        "max_duration": round(max(values), 3),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a bounded API burst test.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8010")
    parser.add_argument("--prefix", default="/api/cost-living")
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--output-json", default="", help="Optional path for the summary JSON.")
    args = parser.parse_args()

    requests_to_run = [
        (path, build_url(args.base_url, args.prefix, path, params))
        for _ in range(args.rounds)
        for path, params in REQUESTS
    ]
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(call, path, url, args.timeout)
            for path, url in requests_to_run
        ]
        results = [future.result() for future in as_completed(futures)]
    elapsed = time.monotonic() - started

    durations = [float(item["duration"]) for item in results if item.get("duration") is not None]
    failures = [item for item in results if not item.get("ok")]
    status_counts = Counter(str(item.get("status_code") or "error") for item in results)
    per_path_durations: dict[str, list[float]] = defaultdict(list)
    for item in results:
        if item.get("duration") is not None:
            per_path_durations[str(item["path"])].append(float(item["duration"]))
    summary = {
        "total_requests": len(results),
        "endpoint_count": len(REQUESTS),
        "success_count": len(results) - len(failures),
        "failure_count": len(failures),
        **duration_summary(durations),
        "status_counts": dict(sorted(status_counts.items())),
        "rate_limited_requests": int(status_counts.get("429", 0)),
        "per_path": {
            path: {"count": len(values), **duration_summary(values)}
            for path, values in sorted(per_path_durations.items())
        },
        "elapsed_seconds": round(elapsed, 3),
        "workers": args.workers,
        "rounds": args.rounds,
    }
    if args.output_json:
        with open(args.output_json, "w", encoding="utf-8") as output_file:
            json.dump({"summary": summary, "results": results}, output_file, ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    if failures:
        print(json.dumps({"sample_failures": failures[:5]}, ensure_ascii=False, indent=2), flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
