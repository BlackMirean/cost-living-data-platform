"""Historical GDELT GKG archive backfill."""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backend.common.config import settings
from backend.harvesters.gdelt_archive import (
    ensure_gdelt_raw_index,
    iter_gkg_archives,
    parse_datetime,
    process_gkg_archive,
)
from backend.harvesters.streams.gdelt_gkg import es_request


logger = logging.getLogger(__name__)
UTC = timezone.utc
REPO_ROOT = Path(__file__).resolve().parents[2]
CHECKPOINT_VERSION = 1


def parse_utc_datetime(value: str, *, end_of_day: bool = False) -> datetime:
    cleaned = value.strip()
    if len(cleaned) == 10:
        suffix = "T23:59:59+00:00" if end_of_day else "T00:00:00+00:00"
        cleaned = f"{cleaned}{suffix}"
    return parse_datetime(cleaned).astimezone(UTC).replace(microsecond=0)


def default_start_datetime(months: int, now: datetime | None = None) -> datetime:
    current = (now or datetime.now(tz=UTC)).astimezone(UTC).replace(microsecond=0)
    days = max(1, round(months * 365 / 12))
    return current - timedelta(days=days)


def checkpoint_path(path: str | Path | None = None) -> Path:
    candidate = Path(path or settings.gdelt_gkg_backfill_checkpoint_path)
    return candidate if candidate.is_absolute() else REPO_ROOT / candidate


def load_checkpoint(path: str | Path | None = None) -> dict[str, Any]:
    resolved = checkpoint_path(path)
    if not resolved.exists():
        return {"version": CHECKPOINT_VERSION, "completed": {}}
    with resolved.open("r", encoding="utf-8") as state_file:
        state = json.load(state_file)
    state.setdefault("version", CHECKPOINT_VERSION)
    state.setdefault("completed", {})
    return state


def save_checkpoint(state: dict[str, Any], path: str | Path | None = None) -> None:
    resolved = checkpoint_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    with resolved.open("w", encoding="utf-8") as state_file:
        json.dump(state, state_file, indent=2, sort_keys=True)


def backfill_plan(
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    months: int | None = None,
    max_archives: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    end_at = parse_utc_datetime(end_date, end_of_day=True) if end_date else (
        now or datetime.now(tz=UTC)
    ).astimezone(UTC).replace(microsecond=0)
    start_at = (
        parse_utc_datetime(start_date)
        if start_date
        else default_start_datetime(months or settings.gdelt_gkg_backfill_months, now=end_at)
    )
    archive_cap = settings.gdelt_gkg_backfill_max_archives if max_archives is None else max_archives
    return {
        "source": "gdelt_gkg_archive_backfill",
        "start_at": start_at.isoformat(),
        "end_at": end_at.isoformat(),
        "masterfile_url": settings.gdelt_gkg_masterfilelist_url,
        "raw_index": settings.gdelt_gkg_raw_index,
        "checkpoint_path": str(checkpoint_path()),
        "request_delay_seconds": settings.gdelt_gkg_backfill_request_delay_seconds,
        "max_archives": archive_cap,
    }


def log_backfill_event(event: str, **fields: Any) -> None:
    logger.info(json.dumps({"event": event, "component": "gdelt_backfill", **fields}, sort_keys=True, default=str))


def run_gdelt_backfill(
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    months: int | None = None,
    resume: bool = True,
    reset_checkpoint: bool = False,
    state_path: str | Path | None = None,
    request_delay_seconds: float | None = None,
    max_archives: int | None = None,
) -> dict[str, Any]:
    plan = backfill_plan(
        start_date=start_date,
        end_date=end_date,
        months=months,
        max_archives=max_archives,
    )
    start_at = parse_utc_datetime(plan["start_at"])
    end_at = parse_utc_datetime(plan["end_at"])
    delay = (
        settings.gdelt_gkg_backfill_request_delay_seconds
        if request_delay_seconds is None
        else request_delay_seconds
    )
    archive_cap = int(plan["max_archives"] or 0)
    state = {"version": CHECKPOINT_VERSION, "completed": {}} if reset_checkpoint else load_checkpoint(state_path)
    if reset_checkpoint:
        save_checkpoint(state, state_path)

    ensure_gdelt_raw_index(es_request=es_request)
    summary: dict[str, Any] = {
        "source": "gdelt_gkg_archive_backfill",
        "start_at": start_at.isoformat(),
        "end_at": end_at.isoformat(),
        "archives_seen": 0,
        "processed_archives": 0,
        "skipped_archives": 0,
        "failed_archives": 0,
        "rows_seen": 0,
        "matched_rows": 0,
        "indexed_docs": 0,
    }
    completed = state.setdefault("completed", {})
    log_backfill_event("gdelt_backfill_started", **summary)

    for archive in iter_gkg_archives(start_at, end_at):
        if archive_cap and summary["processed_archives"] >= archive_cap:
            break
        summary["archives_seen"] += 1
        if resume and archive.archive_id in completed:
            summary["skipped_archives"] += 1
            continue

        try:
            result = process_gkg_archive(archive, es_request=es_request)
        except Exception as exc:
            summary["failed_archives"] += 1
            log_backfill_event(
                "gdelt_backfill_archive_failed",
                archive_id=archive.archive_id,
                error=f"{type(exc).__name__}: {exc}",
            )
            if delay > 0:
                time.sleep(delay)
            continue

        summary["processed_archives"] += 1
        summary["rows_seen"] += result["rows_seen"]
        summary["matched_rows"] += result["matched_rows"]
        summary["indexed_docs"] += result["indexed_docs"]
        completed[archive.archive_id] = {
            "archive_id": archive.archive_id,
            "archive_timestamp": archive.timestamp_iso,
            "indexed_docs": result["indexed_docs"],
            "finished_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        }
        save_checkpoint(state, state_path)
        log_backfill_event("gdelt_backfill_archive_finished", **completed[archive.archive_id])
        if delay > 0:
            time.sleep(delay)

    save_checkpoint(state, state_path)
    log_backfill_event("gdelt_backfill_finished", **summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill historical GDELT GKG archive documents.")
    parser.add_argument("--start-date", default=None, help="UTC start date/datetime, e.g. 2026-05-01.")
    parser.add_argument("--end-date", default=None, help="UTC end date/datetime, default now.")
    parser.add_argument("--months", type=int, default=settings.gdelt_gkg_backfill_months)
    parser.add_argument("--state-path", default=None, help="Checkpoint JSON path.")
    parser.add_argument("--delay", type=float, default=None, help="Delay between archive downloads.")
    parser.add_argument("--max-archives", type=int, default=settings.gdelt_gkg_backfill_max_archives)
    parser.add_argument("--reset-checkpoint", action="store_true", help="Clear the backfill checkpoint first.")
    parser.add_argument("--no-resume", action="store_true", help="Ignore completed checkpoint entries.")
    parser.add_argument("--dry-run", action="store_true", help="Print the backfill plan without harvesting.")
    args = parser.parse_args()

    if args.dry_run:
        print(
            json.dumps(
                backfill_plan(
                    start_date=args.start_date,
                    end_date=args.end_date,
                    months=args.months,
                    max_archives=args.max_archives,
                ),
                indent=2,
            )
        )
        return

    summary = run_gdelt_backfill(
        start_date=args.start_date,
        end_date=args.end_date,
        months=args.months,
        resume=not args.no_resume,
        reset_checkpoint=args.reset_checkpoint,
        state_path=args.state_path,
        request_delay_seconds=args.delay,
        max_archives=args.max_archives,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
