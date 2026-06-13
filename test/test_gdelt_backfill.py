from datetime import datetime, timezone

from backend.harvesters import gdelt_backfill


UTC = timezone.utc


def test_iter_date_windows_splits_half_open_windows():
    windows = list(
        gdelt_backfill.iter_date_windows(
            datetime(2026, 5, 1, tzinfo=UTC),
            datetime(2026, 5, 15, tzinfo=UTC),
            window_days=7,
        )
    )

    assert len(windows) == 2
    assert windows[0][0].isoformat() == "2026-05-01T00:00:00+00:00"
    assert windows[0][1].isoformat() == "2026-05-08T00:00:00+00:00"
    assert windows[1][0].isoformat() == "2026-05-08T00:00:00+00:00"
    assert windows[1][1].isoformat() == "2026-05-15T00:00:00+00:00"


def test_backfill_plan_counts_requests():
    plan = gdelt_backfill.backfill_plan(
        start_date="2026-05-01",
        end_date="2026-05-15",
        window_days=7,
        queries=["rent increase", "groceries"],
    )

    assert plan["windows"] == 3
    assert plan["queries"] == 2
    assert plan["estimated_requests"] == 6
    assert plan["estimated_candidate_raw_articles"] == 6 * plan["limit_per_request"]


def test_run_gdelt_backfill_indexes_each_window_and_resumes(monkeypatch, tmp_path):
    indexed_batches = []

    def fake_search_gdelt_articles(**kwargs):
        return [
            {
                "url": f"https://example.com/{kwargs['query']}/{kwargs['start_datetime']}",
                "title": "Australian renters face rising costs",
                "seendate": kwargs["start_datetime"],
                "domain": "example.com",
                "language": "English",
                "sourcecountry": "AU",
            }
        ]

    def fake_index_raw_posts(docs):
        indexed_batches.append(docs)
        return len(docs)

    monkeypatch.setattr(gdelt_backfill, "search_gdelt_articles", fake_search_gdelt_articles)
    monkeypatch.setattr(gdelt_backfill, "index_raw_posts", fake_index_raw_posts)
    monkeypatch.setattr(gdelt_backfill.time, "sleep", lambda _: None)

    state_path = tmp_path / "gdelt_backfill.json"
    summary = gdelt_backfill.run_gdelt_backfill(
        start_date="2026-05-01",
        end_date="2026-05-08",
        window_days=7,
        limit=50,
        queries=["rent increase"],
        state_path=state_path,
        request_delay_seconds=0,
    )

    assert summary["requests"] == 2
    assert summary["indexed"] == 2
    assert len(indexed_batches) == 2

    resumed = gdelt_backfill.run_gdelt_backfill(
        start_date="2026-05-01",
        end_date="2026-05-08",
        window_days=7,
        limit=50,
        queries=["rent increase"],
        state_path=state_path,
        request_delay_seconds=0,
    )

    assert resumed["requests"] == 0
    assert resumed["skipped"] == 2
