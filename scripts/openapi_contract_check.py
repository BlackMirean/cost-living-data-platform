"""Validate deployed API responses against the public OpenAPI surface."""

from __future__ import annotations

import argparse
import json
import sys
import time
from urllib.parse import urlencode

import requests
from jsonschema import Draft202012Validator, ValidationError


CHECKS: list[dict[str, object]] = [
    {"path": "/health", "params": {}, "keys": {"status", "service"}},
    {"path": "/pipeline/status", "params": {}, "keys": {"raw_documents", "processed_documents"}},
    {"path": "/pipeline/runtime", "params": {}, "keys": {"configured", "available"}},
    {"path": "/pipeline/queues", "params": {}, "keys": {"configured", "available", "queue_key", "depth"}},
    {"path": "/cache/status", "params": {}, "keys": {"enabled", "backend"}},
    {"path": "/rate-limit/status", "params": {}, "keys": {"enabled", "limit", "backend"}},
    {"path": "/platforms/plugins", "params": {}, "keys": {"plugins", "count", "groups"}},
    {"path": "/stats/overview", "params": {"source_group": "all"}, "keys": {"total_documents", "platforms"}},
    {"path": "/categories/counts", "params": {"source_group": "all"}, "keys": {"rows", "meta"}},
    {"path": "/categories/sentiment", "params": {"source_group": "all"}, "keys": {"rows", "meta"}},
    {"path": "/trends/documents", "params": {"period": "month", "source_group": "all"}, "keys": {"rows", "meta"}},
    {"path": "/official/comparison", "params": {"topic": "groceries"}, "keys": {"rows", "meta"}},
]


def build_url(base_url: str, prefix: str, path: str, params: dict[str, str] | object) -> str:
    params = dict(params) if isinstance(params, dict) else {}
    query = urlencode(params)
    url = f"{base_url.rstrip('/')}{prefix.rstrip('/')}{path}"
    return f"{url}?{query}" if query else url


def fetch_json(url: str, timeout: float) -> dict[str, object]:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()


def resolve_local_refs(value: object, document: dict[str, object]) -> object:
    if isinstance(value, dict):
        ref = value.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/"):
            target: object = document
            for part in ref[2:].split("/"):
                if not isinstance(target, dict):
                    raise ValueError(f"invalid local OpenAPI reference: {ref}")
                target = target[part.replace("~1", "/").replace("~0", "~")]
            return resolve_local_refs(target, document)
        return {key: resolve_local_refs(item, document) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_local_refs(item, document) for item in value]
    return value


def response_schema(openapi: dict[str, object], declared_path: str) -> dict[str, object] | None:
    paths = openapi.get("paths", {})
    if not isinstance(paths, dict):
        return None
    path_doc = paths.get(declared_path, {})
    if not isinstance(path_doc, dict):
        return None
    get_doc = path_doc.get("get", {})
    if not isinstance(get_doc, dict):
        return None
    responses = get_doc.get("responses", {})
    if not isinstance(responses, dict):
        return None
    response_doc = responses.get("200", {})
    if not isinstance(response_doc, dict):
        return None
    content = response_doc.get("content", {})
    if not isinstance(content, dict):
        return None
    media = content.get("application/json", {})
    if not isinstance(media, dict):
        return None
    schema = media.get("schema")
    if not isinstance(schema, dict) or not schema:
        return None
    return resolve_local_refs(schema, openapi)  # type: ignore[return-value]


def schema_path(prefix: str, path: str) -> str:
    if prefix.rstrip("/") == "/api/cost-living":
        return f"/api{path}"
    return f"{prefix.rstrip('/')}{path}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate OpenAPI-declared API response contracts.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--prefix", default="/api/cost-living")
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()

    openapi = fetch_json(f"{args.base_url.rstrip('/')}/openapi.json", args.timeout)
    paths = openapi.get("paths", {})
    failures: list[dict[str, object]] = []
    checks = []
    for check in CHECKS:
        path = str(check["path"])
        declared_path = schema_path(args.prefix, path)
        row = {
            "path": path,
            "declared_path": declared_path,
            "declared": declared_path in paths,
            "ok": False,
        }
        if not row["declared"]:
            failures.append({**row, "error": "missing_from_openapi"})
            checks.append(row)
            continue
        started = time.monotonic()
        try:
            payload = fetch_json(build_url(args.base_url, args.prefix, path, check.get("params", {})), args.timeout)
            schema = response_schema(openapi, declared_path)
            schema_error = None
            if schema is not None:
                try:
                    Draft202012Validator(schema).validate(payload)
                except ValidationError as exc:
                    schema_error = exc.message
            missing = sorted(set(check["keys"]) - set(payload.keys()))  # type: ignore[arg-type]
            row.update(
                {
                    "ok": not missing and not schema_error,
                    "duration_seconds": round(time.monotonic() - started, 3),
                    "missing_keys": missing,
                    "schema_checked": schema is not None,
                }
            )
            if schema_error:
                row["schema_error"] = schema_error
            if missing or schema_error:
                failures.append(row)
        except Exception as exc:
            row.update(
                {
                    "ok": False,
                    "duration_seconds": round(time.monotonic() - started, 3),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            failures.append(row)
        checks.append(row)

    result = {
        "total": len(checks),
        "passed": len(checks) - len(failures),
        "failed": len(failures),
        "checks": checks,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
