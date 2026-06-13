"""Historical GDELT backfill for the cost-of-living raw data pipeline."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

from backend.common.config import settings
from backend.common.document_store import index_raw_posts, reset_raw_posts_store
from backend.harvesters.gdelt_news_harvester import (
    is_gdelt_article_in_scope,
    raw_gdelt_article,
    search_gdelt_articles,
)


UTC = timezone.utc
REPO_ROOT = Path(__file__).resolve().parents[2]
CHECKPOINT_VERSION = 1


def parse_utc_datetime(value: str, *, end_of_day: bool = False) -> datetime:
    """Parse an ISO date/datetime into a timezone-aware UTC datetime."""

    cleaned = value.strip()
    if len(cleaned) == 10:
        suffix = "T23:59:59+00:00" if end_of_day else "T00:00:00+00:00"
        cleaned = f"{cleaned}{suffix}"
    parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0)


def gdelt_timestamp(value: datetime) -> str:
    """Format a datetime as the compact timestamp required by GDELT DOC API."""

    return value.astimezone(UTC).strftime("%Y%m%d%H%M%S")


def default_start_datetime(months: int, now: datetime | None = None) -> datetime:
    """Return an approximate UTC start date for a month-based backfill window."""

    now = (now or datetime.now(tz=UTC)).astimezone(UTC).replace(microsecond=0)
    days = max(1, round(months * 365 / 12))
    return now - timedelta(days=days)


def iter_date_windows(
    start_at: datetime,
    end_at: datetime,
    window_days: int,
) -> Iterable[tuple[datetime, datetime]]:
    """Yield half-open UTC windows [start, end) for historical harvesting."""

    if window_days < 1:
        raise ValueError("window_days must be at least 1")
    if end_at <= start_at:
        raise ValueError("end_at must be after start_at")

    cursor = start_at.astimezone(UTC).replace(microsecond=0)
    final_end = end_at.astimezone(UTC).replace(microsecond=0)
    step = timedelta(days=window_days)
    while cursor < final_end:
        next_end = min(cursor + step, final_end)
        yield cursor, next_end
        cursor = next_end


def checkpoint_path(path: str | Path | None = None) -> Path:
    """Resolve the checkpoint file path."""

    candidate = Path(path or settings.gdelt_backfill_state_path)
    if candidate.is_absolute():
        return candidate
    return REPO_ROOT / candidate


def load_checkpoint(path: str | Path | None = None) -> dict[str, Any]:
    """Load checkpoint state, returning an empty state if none exists."""

    resolved = checkpoint_path(path)
    if not resolved.exists():
        return {"version": CHECKPOINT_VERSION, "completed": {}}
    with resolved.open("r", encoding="utf-8") as state_file:
        state = json.load(state_file)
    state.setdefault("version", CHECKPOINT_VERSION)
    state.setdefault("completed", {})
    return state


def save_checkpoint(state: dict[str, Any], path: str | Path | None = None) -> None:
    """Persist checkpoint state after each successful query/window request."""

    resolved = checkpoint_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    with resolved.open("w", encoding="utf-8") as state_file:
        json.dump(state, state_file, indent=2, sort_keys=True)


def checkpoint_key(query: str, start_at: datetime, end_at: datetime) -> str:
    """Return a deterministic key for one query/window unit of work."""

    return f"gdelt|{query}|{gdelt_timestamp(start_at)}|{gdelt_timestamp(end_at)}"


def parse_query_override(value: str | None) -> list[str] | None:
    """Parse a comma-separated query override from CLI input."""

    if value is None:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def backfill_plan(
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    months: int | None = None,
    window_days: int | None = None,
    limit: int | None = None,
    queries: list[str] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a serialisable plan without calling the API."""

    query_list = queries or settings.gdelt_query_list or settings.cost_of_living_query_list
    end_at = parse_utc_datetime(end_date, end_of_day=True) if end_date else (
        now or datetime.now(tz=UTC)
    ).astimezone(UTC).replace(microsecond=0)
    start_at = (
        parse_utc_datetime(start_date)
        if start_date
        else default_start_datetime(months or settings.gdelt_backfill_months, now=end_at)
    )
    days_per_window = window_days or settings.gdelt_backfill_window_days
    windows = list(iter_date_windows(start_at, end_at, days_per_window))
    per_request_limit = limit or settings.gdelt_backfill_limit
    estimated_requests = len(windows) * len(query_list)

    return {
        "source": "gdelt_backfill",
        "start_at": start_at.isoformat(),
        "end_at": end_at.isoformat(),
        "window_days": days_per_window,
        "limit_per_request": per_request_limit,
        "request_delay_seconds": settings.gdelt_backfill_request_delay_seconds,
        "request_timeout_seconds": settings.gdelt_backfill_request_timeout_seconds,
        "retries": settings.gdelt_backfill_retries,
        "windows": len(windows),
        "queries": len(query_list),
        "estimated_requests": estimated_requests,
        "estimated_candidate_raw_articles": estimated_requests * per_request_limit,
        "query_terms": query_list,
    }


def run_gdelt_backfill(
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    months: int | None = None,
    window_days: int | None = None,
    limit: int | None = None,
    queries: list[str] | None = None,
    resume: bool = True,
    reset: bool = False,
    state_path: str | Path | None = None,
    request_delay_seconds: float | None = None,
    request_timeout_seconds: float | None = None,
    retries: int | None = None,
    max_windows: int | None = None,
    max_requests: int | None = None,
) -> dict[str, Any]:
    """Backfill GDELT articles into the raw index using small resumable windows."""

    plan = backfill_plan(
        start_date=start_date,
        end_date=end_date,
        months=months,
        window_days=window_days,
        limit=limit,
        queries=queries,
    )
    query_list = list(plan["query_terms"])
    start_at = parse_utc_datetime(plan["start_at"])
    end_at = parse_utc_datetime(plan["end_at"])
    days_per_window = int(plan["window_days"])
    windows = list(iter_date_windows(start_at, end_at, days_per_window))

    request_limit = limit or settings.gdelt_backfill_limit
    delay = (
        settings.gdelt_backfill_request_delay_seconds
        if request_delay_seconds is None
        else request_delay_seconds
    )
    timeout = (
        settings.gdelt_backfill_request_timeout_seconds
        if request_timeout_seconds is None
        else request_timeout_seconds
    )
    retry_count = settings.gdelt_backfill_retries if retries is None else retries
    window_cap = settings.gdelt_backfill_max_windows if max_windows is None else max_windows
    request_cap = settings.gdelt_backfill_max_requests if max_requests is None else max_requests
    state = load_checkpoint(state_path)

    if reset:
        reset_raw_posts_store()
        state = {"version": CHECKPOINT_VERSION, "completed": {}}
        save_checkpoint(state, state_path)

    summary: dict[str, Any] = {
        "source": "gdelt_backfill",
        "start_at": start_at.isoformat(),
        "end_at": end_at.isoformat(),
        "window_days": days_per_window,
        "queries": len(query_list),
        "request_delay_seconds": delay,
        "request_timeout_seconds": timeout,
        "retries": retry_count,
        "windows_seen": 0,
        "attempted_requests": 0,
        "requests": 0,
        "skipped": 0,
        "indexed": 0,
        "errors": 0,
    }

    completed = state.setdefault("completed", {})
    for window_index, (window_start, window_end) in enumerate(windows):
        if window_cap and window_index >= window_cap:
            break
        if request_cap and summary["attempted_requests"] >= request_cap:
            break
        summary["windows_seen"] += 1
        for query in query_list:
            if request_cap and summary["attempted_requests"] >= request_cap:
                save_checkpoint(state, state_path)
                return summary

            key = checkpoint_key(query, window_start, window_end)
            if resume and key in completed:
                summary["skipped"] += 1
                continue

            summary["attempted_requests"] += 1
            try:
                articles = search_gdelt_articles(
                    query=query,
                    limit=request_limit,
                    timespan=None,
                    retries=retry_count,
                    start_datetime=gdelt_timestamp(window_start),
                    end_datetime=gdelt_timestamp(window_end),
                    request_timeout_seconds=timeout,
                    retry_delay_seconds=delay,
                )
                scoped_articles = [article for article in articles if is_gdelt_article_in_scope(article)]
                skipped = len(articles) - len(scoped_articles)
                if skipped:
                    print(
                        f"Skipped {skipped} out-of-scope GDELT articles before raw indexing",
                        flush=True,
                    )
                raw_docs = [raw_gdelt_article(article, query=query) for article in scoped_articles]
                indexed = index_raw_posts(raw_docs)
            except (requests.RequestException, ValueError) as exc:
                summary["errors"] += 1
                print(
                    "Skipped GDELT backfill "
                    f"query='{query}' window={window_start.isoformat()}..{window_end.isoformat()}: {exc}",
                    flush=True,
                )
                if delay > 0:
                    time.sleep(delay)
                continue

            summary["requests"] += 1
            summary["indexed"] += indexed
            completed[key] = {
                "query": query,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "indexed": indexed,
                "finished_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
            }
            save_checkpoint(state, state_path)
            print(
                "GDELT backfill indexed "
                f"{indexed} raw docs for query='{query}' "
                f"window={window_start.date()}..{window_end.date()}",
                flush=True,
            )
            if delay > 0:
                time.sleep(delay)

    save_checkpoint(state, state_path)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill historical GDELT raw documents.")
    parser.add_argument("--start-date", default=None, help="UTC start date/datetime, e.g. 2025-05-01.")
    parser.add_argument("--end-date", default=None, help="UTC end date/datetime, default now.")
    parser.add_argument("--months", type=int, default=settings.gdelt_backfill_months)
    parser.add_argument("--window-days", type=int, default=settings.gdelt_backfill_window_days)
    parser.add_argument("--limit", type=int, default=settings.gdelt_backfill_limit)
    parser.add_argument("--queries", default=None, help="Comma-separated query override.")
    parser.add_argument("--state-path", default=None, help="Checkpoint JSON path.")
    parser.add_argument("--delay", type=float, default=None, help="Delay between GDELT requests.")
    parser.add_argument("--timeout", type=float, default=None, help="HTTP timeout per GDELT request.")
    parser.add_argument("--retries", type=int, default=None, help="Retries per GDELT request.")
    parser.add_argument("--max-windows", type=int, default=settings.gdelt_backfill_max_windows)
    parser.add_argument("--max-requests", type=int, default=settings.gdelt_backfill_max_requests)
    parser.add_argument("--reset", action="store_true", help="Clear raw store and checkpoint first.")
    parser.add_argument("--no-resume", action="store_true", help="Ignore completed checkpoint entries.")
    parser.add_argument("--dry-run", action="store_true", help="Print the request plan without harvesting.")
    args = parser.parse_args()

    queries = parse_query_override(args.queries)
    if args.dry_run:
        print(
            json.dumps(
                backfill_plan(
                    start_date=args.start_date,
                    end_date=args.end_date,
                    months=args.months,
                    window_days=args.window_days,
                    limit=args.limit,
                    queries=queries,
                ),
                indent=2,
            )
        )
        return

    summary = run_gdelt_backfill(
        start_date=args.start_date,
        end_date=args.end_date,
        months=args.months,
        window_days=args.window_days,
        limit=args.limit,
        queries=queries,
        resume=not args.no_resume,
        reset=args.reset,
        state_path=args.state_path,
        request_delay_seconds=args.delay,
        request_timeout_seconds=args.timeout,
        retries=args.retries,
        max_windows=args.max_windows,
        max_requests=args.max_requests,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
