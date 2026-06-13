import hashlib
import io
import json
import zipfile
from datetime import datetime, timezone

from backend.harvesters import gdelt_archive


UTC = timezone.utc


class FakeResponse:
    def __init__(self, *, lines=None, content=b"", status_code=200):
        self.lines = lines or []
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)

    def iter_lines(self, decode_unicode=False):
        yield from self.lines

    def json(self):
        return {"items": [{"index": {"status": 201}}]}


def gkg_zip(row: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("20260614000000.gkg.csv", row)
    return buffer.getvalue()


def matching_gkg_row() -> str:
    fields = [""] * 27
    fields[0] = "record-1"
    fields[3] = "example.com"
    fields[4] = "https://example.com/rent"
    fields[7] = "ECON_COST_OF_LIVING"
    fields[8] = "rental crisis australia rent increase"
    fields[9] = "Australia"
    fields[10] = "Melbourne"
    fields[23] = "Australian renters"
    return "\t".join(fields)


def test_iter_gkg_archives_reads_masterfile_entries():
    lines = [
        "100 abc http://data.gdeltproject.org/gdeltv2/20260613000000.gkg.csv.zip",
        "100 def http://data.gdeltproject.org/gdeltv2/20260614000000.gkg.csv.zip",
        "100 skip http://data.gdeltproject.org/gdeltv2/20260614000000.export.csv.zip",
    ]

    archives = list(
        gdelt_archive.iter_gkg_archives(
            datetime(2026, 6, 13, 1, tzinfo=UTC),
            datetime(2026, 6, 14, 1, tzinfo=UTC),
            http_get=lambda *args, **kwargs: FakeResponse(lines=lines),
        )
    )

    assert [archive.archive_id for archive in archives] == ["20260614000000"]
    assert archives[0].md5 == "def"


def test_process_gkg_archive_bytes_filters_and_bulk_indexes(monkeypatch):
    monkeypatch.setattr(gdelt_archive.settings, "gdelt_gkg_raw_index", "cost_living_gdelt_raw_stream")
    content = gkg_zip(matching_gkg_row())
    archive = gdelt_archive.GdeltArchiveFile(
        archive_id="20260614000000",
        timestamp=datetime(2026, 6, 14, tzinfo=UTC),
        file_size=len(content),
        md5=hashlib.md5(content).hexdigest(),
        url="https://example.com/20260614000000.gkg.csv.zip",
    )
    captured = {}

    def fake_es_request(method, path, **kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["body"] = kwargs["data"].decode("utf-8")
        return FakeResponse()

    result = gdelt_archive.process_gkg_archive_bytes(archive, content, es_request=fake_es_request)

    assert result["rows_seen"] == 1
    assert result["matched_rows"] == 1
    assert result["indexed_docs"] == 1
    bulk_lines = [json.loads(line) for line in captured["body"].strip().splitlines()]
    assert bulk_lines[0]["index"]["_index"] == "cost_living_gdelt_raw_stream"
    assert bulk_lines[1]["platform"] == "gdelt"
    assert bulk_lines[1]["category"] == "housing"
    assert "australia" in bulk_lines[1]["matched_terms"]


def test_process_gkg_archive_bytes_rejects_bad_md5():
    content = gkg_zip(matching_gkg_row())
    archive = gdelt_archive.GdeltArchiveFile(
        archive_id="20260614000000",
        timestamp=datetime(2026, 6, 14, tzinfo=UTC),
        file_size=len(content),
        md5="bad",
        url="https://example.com/20260614000000.gkg.csv.zip",
    )

    try:
        gdelt_archive.process_gkg_archive_bytes(archive, content)
    except ValueError as exc:
        assert "MD5 mismatch" in str(exc)
    else:
        raise AssertionError("expected MD5 mismatch")
