"""Incremental GDELT GKG archive harvester."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
import urllib3

from backend.common.config import settings
from backend.harvesters.gdelt_archive import (
    ensure_gdelt_raw_index,
    iter_gkg_archives,
    parse_datetime,
    process_gkg_archive,
    utc_now,
)


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

FUNCTION_NAME = "harvest-gdelt-gkg-stream"
STATE_INDEX = settings.social_harvest_state_index


def es_request(
    method: str,
    path: str,
    *,
    raise_for_status: bool = True,
    **kwargs: Any,
) -> requests.Response:
    response = requests.request(
        method,
        f"{settings.elasticsearch_url.rstrip('/')}{path}",
        auth=(settings.elasticsearch_username or "elastic", settings.elasticsearch_password),
        verify=settings.elasticsearch_verify_certs,
        timeout=kwargs.pop("timeout", 120),
        **kwargs,
    )
    if raise_for_status:
        response.raise_for_status()
    return response


def get_state() -> dict[str, Any]:
    response = requests.get(
        f"{settings.elasticsearch_url.rstrip('/')}/{STATE_INDEX}/_doc/{FUNCTION_NAME}",
        auth=(settings.elasticsearch_username or "elastic", settings.elasticsearch_password),
        verify=settings.elasticsearch_verify_certs,
        timeout=30,
    )
    if response.status_code == 404:
        return {}
    response.raise_for_status()
    return response.json().get("_source") or {}


def update_state(last_archive_timestamp: str, stats: dict[str, Any]) -> None:
    payload = {
        "function": FUNCTION_NAME,
        "platform": "gdelt",
        "instance": "gkg_archive_incremental",
        "last_file_timestamp": last_archive_timestamp,
        "last_archive_timestamp": last_archive_timestamp,
        "last_success_at": utc_now(),
        "stats": stats,
    }
    es_request(
        "PUT",
        f"/{STATE_INDEX}/_doc/{FUNCTION_NAME}?refresh=true",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload, ensure_ascii=False),
        timeout=60,
    )


def incremental_start_time() -> tuple[datetime, bool]:
    state = get_state()
    timestamp = state.get("last_archive_timestamp") or state.get("last_file_timestamp")
    if timestamp:
        return parse_datetime(timestamp), False
    return datetime.now(timezone.utc) - timedelta(hours=settings.gdelt_gkg_initial_lookback_hours), True


def log_incremental_event(event: str, **fields: Any) -> None:
    logger.info(json.dumps({"event": event, "component": "gdelt_incremental", **fields}, sort_keys=True, default=str))


def main() -> dict[str, Any]:
    ensure_gdelt_raw_index(es_request=es_request)
    started = time.monotonic()
    since, first_run = incremental_start_time()
    until = datetime.now(timezone.utc)

    processed = 0
    indexed_docs = 0
    rows_seen = 0
    matched_rows = 0
    failed = 0
    last_archive_timestamp = since.isoformat()
    details = []

    log_incremental_event(
        "gdelt_incremental_started",
        start_from=since.isoformat(),
        checked_until=until.isoformat(),
        batch_size=settings.gdelt_gkg_incremental_batch_size,
    )

    for archive in iter_gkg_archives(since, until):
        elapsed = time.monotonic() - started
        if (
            processed >= settings.gdelt_gkg_incremental_batch_size
            or elapsed >= settings.gdelt_gkg_incremental_max_runtime_seconds
        ):
            break
        try:
            result = process_gkg_archive(archive, es_request=es_request)
            rows_seen += result["rows_seen"]
            matched_rows += result["matched_rows"]
            indexed_docs += result["indexed_docs"]
            details.append(result)
        except Exception as exc:
            failed += 1
            details.append({"archive_id": archive.archive_id, "error": f"{type(exc).__name__}: {exc}"})
            log_incremental_event(
                "gdelt_incremental_archive_failed",
                archive_id=archive.archive_id,
                error=f"{type(exc).__name__}: {exc}",
            )

        processed += 1
        last_archive_timestamp = archive.timestamp_iso
        stats = {
            "first_run": first_run,
            "interval_start": since.isoformat(),
            "interval_end": until.isoformat(),
            "last_archive_timestamp": last_archive_timestamp,
            "processed_archives": processed,
            "failed_archives": failed,
            "rows_seen": rows_seen,
            "matched_rows": matched_rows,
            "indexed_docs": indexed_docs,
        }
        update_state(last_archive_timestamp, stats)

    if processed == 0:
        stats = {
            "first_run": first_run,
            "interval_start": since.isoformat(),
            "interval_end": until.isoformat(),
            "last_archive_timestamp": last_archive_timestamp,
            "processed_archives": 0,
            "failed_archives": 0,
            "rows_seen": 0,
            "matched_rows": 0,
            "indexed_docs": 0,
        }
        update_state(last_archive_timestamp, stats)

    summary = {
        "function": FUNCTION_NAME,
        "source": "gdelt_gkg_archive_incremental",
        "raw_index": settings.gdelt_gkg_raw_index,
        "state_index": STATE_INDEX,
        "start_from": since.isoformat(),
        "checked_until": until.isoformat(),
        "processed_archives": processed,
        "failed_archives": failed,
        "rows_seen": rows_seen,
        "matched_rows": matched_rows,
        "indexed_docs": indexed_docs,
        "last_archive_timestamp": last_archive_timestamp,
        "details": details[:10],
    }
    log_incremental_event("gdelt_incremental_finished", **summary)
    return summary
