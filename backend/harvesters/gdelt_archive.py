"""Shared GDELT GKG archive ingestion helpers.

The production GDELT path reads the public master file list, downloads GKG
CSV ZIP archives, filters rows in-process and writes only relevant records to
Elasticsearch. Incremental harvesting and historical backfill both use this
module.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import re
import time
import zipfile
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from backend.common.config import settings


logger = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[2]
GDELT_RAW_MAPPING_PATH = REPO_ROOT / "database" / "mappings" / "gdelt_raw_stream.json"
GKG_URL_RE = re.compile(r"/(\d{14})\.gkg\.csv\.zip$")
SPACE_RE = re.compile(r"\s+")

GENERAL_TERMS = [
    "cost of living",
    "cost-of-living",
    "affordability",
    "afford",
    "unaffordable",
    "inflation",
    "expensive",
    "too expensive",
    "more expensive",
    "prices",
    "price",
    "price rise",
    "price rises",
    "price increase",
    "price increases",
    "price hike",
    "price hikes",
    "bill shock",
    "living costs",
    "rising costs",
    "rising prices",
    "cost pressure",
    "financial pressure",
    "budget pressure",
    "budget squeeze",
    "struggling",
    "struggle",
    "hard to pay",
    "harder to afford",
    "rental crisis",
    "rent increase",
    "rent hike",
    "mortgage stress",
]

AUSTRALIA_TERMS = [
    "australia",
    "australian",
    "melbourne",
    "sydney",
    "brisbane",
    "perth",
    "adelaide",
    "canberra",
    "tasmania",
    "hobart",
    "darwin",
    "nsw",
    "vic",
    "qld",
    "wa",
    "myki",
    "opal",
    "medicare",
    "coles",
    "woolworths",
    "woolies",
    "centrelink",
]

CATEGORY_KEYWORDS = {
    "housing": ["rent", "rental", "renting", "landlord", "housing", "tenant", "renters", "mortgage"],
    "groceries": ["groceries", "grocery", "supermarket", "coles", "woolworths", "woolies", "aldi", "food"],
    "fuel": ["petrol", "fuel", "diesel", "servo", "bowser"],
    "energy": ["electricity", "power bill", "energy bill", "gas bill", "utilities", "power"],
    "eating_out": ["coffee", "cafe", "takeaway", "ubereats", "doordash", "restaurant", "brunch"],
    "transport": ["myki", "opal", "public transport", "train fare", "bus fare", "tram fare", "toll", "parking"],
    "healthcare": ["gp", "doctor", "dentist", "medicare", "pharmacy", "medicine", "health insurance", "bulk billing"],
    "home_goods": ["furniture", "appliance", "fridge", "washing machine", "sofa", "mattress", "whitegoods"],
    "education": ["school fees", "tuition fees", "education costs", "uni fees", "childcare fees", "hecs", "student debt"],
}

CATEGORY_SEARCH_TERMS = {
    "housing": [
        "rent increase australia",
        "rent hike australia",
        "rental crisis australia",
        "housing affordability australia",
        "cant afford rent australia",
        "rent too expensive australia",
        "rent melbourne",
        "rent sydney",
        "landlord raised rent australia",
        "tenant rent increase australia",
    ],
    "groceries": [
        "grocery bill australia",
        "groceries too expensive australia",
        "supermarket prices australia",
        "coles prices australia",
        "woolworths prices australia",
        "food prices australia",
        "grocery prices melbourne",
        "grocery prices sydney",
    ],
    "fuel": [
        "petrol prices australia",
        "fuel prices australia",
        "petrol too expensive australia",
        "fuel cost australia",
        "petrol melbourne",
        "petrol sydney",
        "diesel prices australia",
        "servo prices australia",
    ],
    "energy": [
        "power bill australia",
        "electricity bill australia",
        "energy bill australia",
        "gas bill australia",
        "electricity prices australia",
        "power prices australia",
        "bill shock electricity australia",
    ],
    "eating_out": [
        "coffee price australia",
        "coffee too expensive melbourne",
        "takeaway expensive australia",
        "ubereats expensive australia",
        "doordash expensive australia",
        "restaurant prices australia",
        "brunch expensive melbourne",
    ],
    "transport": [
        "public transport cost australia",
        "myki fare melbourne",
        "opal fare sydney",
        "train fare australia",
        "bus fare australia",
        "tram fare melbourne",
        "toll road cost australia",
        "parking cost australia",
    ],
    "healthcare": [
        "gp cost australia",
        "doctor cost australia",
        "dentist expensive australia",
        "medicare gap australia",
        "medicine cost australia",
        "pharmacy cost australia",
        "health insurance expensive australia",
        "gp gap fee australia",
    ],
    "home_goods": [
        "furniture prices australia",
        "appliance prices australia",
        "fridge expensive australia",
        "washing machine expensive australia",
        "household items expensive australia",
        "home goods prices australia",
        "mattress expensive australia",
        "sofa expensive australia",
        "whitegoods prices australia",
    ],
    "education": [
        "school fees australia",
        "tuition fees australia",
        "education costs australia",
        "uni fees australia",
        "university fees australia",
        "childcare fees australia",
        "textbook costs australia",
        "school costs australia",
        "hecs debt australia",
        "student debt australia",
        "childcare too expensive australia",
        "uni too expensive australia",
    ],
}

BLOCKED_TERMS = {
    "housing": ["west end", "musical", "broadway", "show"],
    "fuel": ["war profits", "renewables", "solar", "geopolitics"],
    "home_goods": ["interior design", "home decor", "styling tips", "gift guide", "sale now on"],
    "education": ["education policy", "curriculum reform", "school ranking", "teaching strategy", "student visa"],
}


@dataclass(frozen=True)
class GdeltArchiveFile:
    archive_id: str
    timestamp: datetime
    file_size: int
    md5: str
    url: str

    @property
    def timestamp_iso(self) -> str:
        return self.timestamp.astimezone(timezone.utc).replace(microsecond=0).isoformat()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["_id"] = self.archive_id
        payload["timestamp"] = self.timestamp_iso
        return payload


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    if len(str(value)) == 14 and str(value).isdigit():
        return datetime.strptime(str(value), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def normalize(text: str) -> str:
    return SPACE_RE.sub(" ", text.lower()).strip()


def term_matches(text: str, term: str) -> bool:
    escaped = re.escape(term.lower()).replace(r"\ ", r"\s+")
    return re.search(rf"(?<!\w){escaped}(?!\w)", text) is not None


def contains_any(text: str, terms: list[str]) -> list[str]:
    return [term for term in terms if term_matches(text, term)]


def gkg_created_at(archive_id: str) -> str:
    return datetime.strptime(archive_id, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).isoformat()


def row_text(fields: list[str]) -> str:
    important_indexes = [3, 4, 7, 8, 9, 10, 11, 12, 13, 14, 23, 24, 26]
    parts = [fields[index] for index in important_indexes if index < len(fields) and fields[index]]
    return normalize(" ".join(parts))


def best_search_term(category: str, text: str) -> str:
    for search_term in CATEGORY_SEARCH_TERMS.get(category, []):
        tokens = [token for token in normalize(search_term).split() if token]
        if tokens and all(term_matches(text, token) for token in tokens):
            return search_term
    return CATEGORY_SEARCH_TERMS.get(category, [category])[0]


def match_categories(text: str) -> list[tuple[str, list[str]]]:
    australia_hits = contains_any(text, AUSTRALIA_TERMS)
    if not australia_hits:
        return []
    general_hits = contains_any(text, GENERAL_TERMS)
    if not general_hits:
        return []
    matches: list[tuple[str, list[str]]] = []
    for category, keywords in CATEGORY_KEYWORDS.items():
        if contains_any(text, BLOCKED_TERMS.get(category, [])):
            continue
        category_hits = contains_any(text, keywords)
        if category_hits:
            matches.append((category, sorted(set(category_hits + general_hits + australia_hits))))
    return matches


def iter_gkg_archives(
    start_at: datetime,
    end_at: datetime,
    *,
    masterfile_url: str | None = None,
    http_get: Callable[..., Any] = requests.get,
) -> Iterable[GdeltArchiveFile]:
    """Yield GKG archive metadata in the half-open range (start_at, end_at]."""

    url = masterfile_url or settings.gdelt_gkg_masterfilelist_url
    response = http_get(url, stream=True, timeout=settings.gdelt_gkg_masterfilelist_timeout_seconds)
    response.raise_for_status()
    for raw_line in response.iter_lines(decode_unicode=True):
        if not raw_line:
            continue
        parts = raw_line.strip().split()
        if len(parts) != 3:
            continue
        size_text, md5, archive_url = parts
        if not archive_url.endswith(".gkg.csv.zip"):
            continue
        match = GKG_URL_RE.search(archive_url)
        if not match:
            continue
        timestamp = parse_datetime(match.group(1))
        if timestamp <= start_at or timestamp > end_at:
            continue
        yield GdeltArchiveFile(
            archive_id=match.group(1),
            timestamp=timestamp,
            file_size=int(size_text),
            md5=md5.lower(),
            url=archive_url,
        )


def load_gdelt_raw_mapping() -> dict[str, Any]:
    with GDELT_RAW_MAPPING_PATH.open("r", encoding="utf-8") as mapping_file:
        return json.load(mapping_file)


def ensure_gdelt_raw_index(index_name: str | None = None, es_request: Callable[..., Any] | None = None) -> None:
    request = es_request or default_es_request
    target_index = index_name or settings.gdelt_gkg_raw_index
    response = request(
        "PUT",
        f"/{target_index}",
        headers={"Content-Type": "application/json"},
        data=json.dumps(load_gdelt_raw_mapping()),
        timeout=60,
        raise_for_status=False,
    )
    if response.status_code not in (200, 400):
        response.raise_for_status()


def build_gkg_documents(archive: GdeltArchiveFile | dict[str, Any], fields: list[str], sequence: int) -> list[dict[str, Any]]:
    archive_id = archive.archive_id if isinstance(archive, GdeltArchiveFile) else str(archive.get("_id") or archive["archive_id"])
    archive_url = archive.url if isinstance(archive, GdeltArchiveFile) else archive["url"]
    text = row_text(fields)
    categories = match_categories(text)
    if not categories:
        return []
    record_id = fields[0] if fields else f"{archive_id}:{sequence}"
    source_url = fields[4] if len(fields) > 4 else ""
    source_domain = fields[3] if len(fields) > 3 else ""
    docs = []
    for category, hits in categories:
        docs.append(
            {
                "_doc_id": f"{record_id}:{category}",
                "platform": "gdelt",
                "stage": "raw",
                "category": category,
                "search_term": best_search_term(category, text),
                "source_id": record_id,
                "url": source_url,
                "domain": source_domain,
                "author_handle": source_domain,
                "author_display_name": source_domain,
                "text": text,
                "created_at": gkg_created_at(archive_id),
                "like_count": 0,
                "reply_count": 0,
                "repost_count": 0,
                "quote_count": 0,
                "gkg_file_id": archive_id,
                "gkg_url": archive_url,
                "themes": fields[7] if len(fields) > 7 else "",
                "v2themes": fields[8] if len(fields) > 8 else "",
                "locations": fields[9] if len(fields) > 9 else "",
                "v2locations": fields[10] if len(fields) > 10 else "",
                "persons": fields[11] if len(fields) > 11 else "",
                "organizations": fields[13] if len(fields) > 13 else "",
                "tone": fields[15] if len(fields) > 15 else "",
                "all_names": fields[23] if len(fields) > 23 else "",
                "matched_terms": hits,
                "collected_at": utc_now(),
            }
        )
    return docs


def default_es_request(
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


def bulk_index_gkg_documents(
    docs: list[dict[str, Any]],
    *,
    index_name: str | None = None,
    es_request: Callable[..., Any] | None = None,
) -> dict[str, int]:
    if not docs:
        return {"attempted": 0, "indexed": 0, "failed": 0}

    request = es_request or default_es_request
    target_index = index_name or settings.gdelt_gkg_raw_index
    lines: list[str] = []
    for doc in docs:
        payload = dict(doc)
        doc_id = payload.pop("_doc_id")
        lines.append(json.dumps({"index": {"_index": target_index, "_id": doc_id}}, ensure_ascii=False))
        lines.append(json.dumps(payload, ensure_ascii=False))
    response = request(
        "POST",
        "/_bulk?refresh=false",
        headers={"Content-Type": "application/x-ndjson"},
        data=("\n".join(lines) + "\n").encode("utf-8"),
        timeout=180,
    )
    result = response.json()
    failed = [
        item
        for item in result.get("items", [])
        if int((item.get("index") or {}).get("status", 500)) >= 300
    ]
    return {"attempted": len(docs), "indexed": len(docs) - len(failed), "failed": len(failed)}


def process_gkg_archive_bytes(
    archive: GdeltArchiveFile,
    content: bytes,
    *,
    es_request: Callable[..., Any] | None = None,
    bulk_size: int | None = None,
) -> dict[str, Any]:
    expected_md5 = archive.md5.lower()
    actual_md5 = hashlib.md5(content).hexdigest()
    if expected_md5 and expected_md5 != actual_md5:
        raise ValueError(f"MD5 mismatch for {archive.archive_id}")

    rows_seen = 0
    docs_buffer: list[dict[str, Any]] = []
    indexed = 0
    failed = 0
    matched_rows = 0
    max_bulk = bulk_size or settings.gdelt_gkg_bulk_size
    with zipfile.ZipFile(io.BytesIO(content)) as zip_archive:
        member_name = zip_archive.namelist()[0]
        with zip_archive.open(member_name) as csv_file:
            for raw_line in io.TextIOWrapper(csv_file, encoding="utf-8", errors="replace"):
                rows_seen += 1
                fields = raw_line.rstrip("\n").split("\t")
                docs = build_gkg_documents(archive, fields, rows_seen)
                if docs:
                    matched_rows += 1
                    docs_buffer.extend(docs)
                if len(docs_buffer) >= max_bulk:
                    result = bulk_index_gkg_documents(docs_buffer, es_request=es_request)
                    indexed += result["indexed"]
                    failed += result["failed"]
                    docs_buffer = []
    if docs_buffer:
        result = bulk_index_gkg_documents(docs_buffer, es_request=es_request)
        indexed += result["indexed"]
        failed += result["failed"]

    return {
        "archive_id": archive.archive_id,
        "archive_timestamp": archive.timestamp_iso,
        "rows_seen": rows_seen,
        "matched_rows": matched_rows,
        "indexed_docs": indexed,
        "failed_docs": failed,
        "bytes_downloaded": len(content),
    }


def process_gkg_archive(
    archive: GdeltArchiveFile,
    *,
    http_get: Callable[..., Any] = requests.get,
    es_request: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    response = http_get(archive.url, timeout=settings.gdelt_gkg_download_timeout_seconds)
    response.raise_for_status()
    result = process_gkg_archive_bytes(archive, response.content, es_request=es_request)
    result["duration_ms"] = (time.perf_counter() - started) * 1000
    logger.info(
        json.dumps(
            {
                "event": "gdelt_archive_processed",
                "archive_id": archive.archive_id,
                "indexed_docs": result["indexed_docs"],
                "matched_rows": result["matched_rows"],
                "rows_seen": result["rows_seen"],
                "duration_ms": result["duration_ms"],
            },
            sort_keys=True,
        )
    )
    return result
