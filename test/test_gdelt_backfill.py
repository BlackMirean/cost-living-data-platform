from datetime import datetime, timezone

from backend.harvesters import gdelt_backfill
from backend.harvesters.gdelt_archive import GdeltArchiveFile


UTC = timezone.utc


def test_backfill_plan_uses_archive_source_names(tmp_path, monkeypatch):
    monkeypatch.setattr(gdelt_backfill.settings, "gdelt_gkg_backfill_checkpoint_path", str(tmp_path / "state.json"))
    monkeypatch.setattr(gdelt_backfill.settings, "gdelt_gkg_raw_index", "cost_living_gdelt_raw_stream")

    plan = gdelt_backfill.backfill_plan(
        start_date="2026-06-01",
        end_date="2026-06-02",
        max_archives=25,
    )

    assert plan["source"] == "gdelt_gkg_archive_backfill"
    assert plan["max_archives"] == 25
    assert plan["raw_index"] == "cost_living_gdelt_raw_stream"
    assert plan["checkpoint_path"].endswith("state.json")


def test_run_gdelt_backfill_processes_archives_and_resumes(monkeypatch, tmp_path):
    archives = [
        GdeltArchiveFile(
            archive_id="20260601000000",
            timestamp=datetime(2026, 6, 1, tzinfo=UTC),
            file_size=1,
            md5="abc",
            url="https://example.com/a.zip",
        ),
        GdeltArchiveFile(
            archive_id="20260601001500",
            timestamp=datetime(2026, 6, 1, 0, 15, tzinfo=UTC),
            file_size=1,
            md5="def",
            url="https://example.com/b.zip",
        ),
    ]
    processed = []

    monkeypatch.setattr(gdelt_backfill, "ensure_gdelt_raw_index", lambda **kwargs: None)
    monkeypatch.setattr(gdelt_backfill, "iter_gkg_archives", lambda *args, **kwargs: iter(archives))
    monkeypatch.setattr(gdelt_backfill.time, "sleep", lambda _: None)

    def fake_process(archive, **kwargs):
        processed.append(archive.archive_id)
        return {"rows_seen": 10, "matched_rows": 2, "indexed_docs": 2}

    monkeypatch.setattr(gdelt_backfill, "process_gkg_archive", fake_process)

    state_path = tmp_path / "gdelt_gkg_backfill.json"
    summary = gdelt_backfill.run_gdelt_backfill(
        start_date="2026-06-01",
        end_date="2026-06-02",
        state_path=state_path,
        request_delay_seconds=0,
    )

    assert summary["processed_archives"] == 2
    assert summary["indexed_docs"] == 4
    assert processed == ["20260601000000", "20260601001500"]

    resumed = gdelt_backfill.run_gdelt_backfill(
        start_date="2026-06-01",
        end_date="2026-06-02",
        state_path=state_path,
        request_delay_seconds=0,
    )

    assert resumed["processed_archives"] == 0
    assert resumed["skipped_archives"] == 2
