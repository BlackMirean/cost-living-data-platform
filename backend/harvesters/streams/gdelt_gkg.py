from __future__ import annotations

import hashlib
import io
import json
import os
import re
import time
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Iterable

import requests
import urllib3

from backend.common.config import settings

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

FUNCTION_NAME = "harvest-gdelt-gkg-stream"
MASTERFILELIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
QUEUE_INDEX = settings.gdelt_gkg_queue_index
RAW_INDEX = settings.gdelt_gkg_raw_index
STATE_INDEX = settings.social_harvest_state_index
BATCH_SIZE = int(os.getenv("GDELT_GKG_STREAM_BATCH_SIZE", "4"))
MAX_RUNTIME_SECONDS = int(os.getenv("GDELT_GKG_STREAM_MAX_RUNTIME_SECONDS", "240"))

ES_URL = settings.elasticsearch_url.rstrip("/")
ES_USERNAME = settings.elasticsearch_username or "elastic"
ES_PASSWORD = settings.elasticsearch_password
ES_VERIFY_TLS = settings.elasticsearch_verify_certs

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

SPACE_RE = re.compile(r"\s+")
GKG_URL_RE = re.compile(r"/(\d{14})\.gkg\.csv\.zip$")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def es_request(method: str, path: str, **kwargs) -> requests.Response:
    response = requests.request(
        method,
        f"{ES_URL.rstrip('/')}{path}",
        auth=(ES_USERNAME, ES_PASSWORD),
        verify=ES_VERIFY_TLS,
        timeout=kwargs.pop("timeout", 120),
        **kwargs,
    )
    response.raise_for_status()
    return response


def parse_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    if len(str(value)) == 14 and str(value).isdigit():
        return datetime.strptime(str(value), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def get_state() -> dict:
    response = requests.get(
        f"{ES_URL.rstrip('/')}/{STATE_INDEX}/_doc/{FUNCTION_NAME}",
        auth=(ES_USERNAME, ES_PASSWORD),
        verify=ES_VERIFY_TLS,
        timeout=30,
    )
    if response.status_code == 404:
        return {}
    response.raise_for_status()
    return response.json().get("_source") or {}


def update_state(last_file_timestamp: str, stats: dict) -> None:
    payload = {
        "function": FUNCTION_NAME,
        "platform": "gdelt",
        "instance": "gkg",
        "last_file_timestamp": last_file_timestamp,
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


def max_known_queue_timestamp() -> datetime:
    body = {"size": 0, "aggs": {"max_ts": {"max": {"field": "timestamp"}}}}
    try:
        response = es_request(
            "GET",
            f"/{QUEUE_INDEX}/_search",
            headers={"Content-Type": "application/json"},
            data=json.dumps(body),
            timeout=60,
        )
        value = response.json().get("aggregations", {}).get("max_ts", {}).get("value_as_string")
        if value:
            return parse_datetime(value)
    except Exception:
        pass
    return datetime.now(timezone.utc)


def stream_start_time() -> tuple[datetime, bool]:
    state = get_state()
    if state.get("last_file_timestamp"):
        return parse_datetime(state["last_file_timestamp"]), False
    return datetime.now(timezone.utc) - timedelta(hours=settings.gdelt_gkg_initial_lookback_hours), True


def iter_masterfile_entries_after(start: datetime, end: datetime) -> Iterable[dict]:
    response = requests.get(MASTERFILELIST_URL, stream=True, timeout=180)
    response.raise_for_status()
    for raw_line in response.iter_lines(decode_unicode=True):
        if not raw_line:
            continue
        parts = raw_line.strip().split()
        if len(parts) != 3:
            continue
        size_text, md5, url = parts
        if not url.endswith(".gkg.csv.zip"):
            continue
        match = GKG_URL_RE.search(url)
        if not match:
            continue
        timestamp = parse_datetime(match.group(1))
        if timestamp <= start or timestamp > end:
            continue
        yield {
            "_id": match.group(1),
            "timestamp": timestamp.isoformat(),
            "file_size": int(size_text),
            "md5": md5,
            "url": url,
        }


def ensure_raw_index() -> None:
    mapping = {
        "mappings": {
            "properties": {
                "created_at": {"type": "date"},
                "category": {"type": "keyword"},
                "platform": {"type": "keyword"},
                "source_id": {"type": "keyword"},
                "gkg_file_id": {"type": "keyword"},
            }
        }
    }
    response = requests.put(
        f"{ES_URL.rstrip('/')}/{RAW_INDEX}",
        auth=(ES_USERNAME, ES_PASSWORD),
        verify=ES_VERIFY_TLS,
        headers={"Content-Type": "application/json"},
        data=json.dumps(mapping),
        timeout=60,
    )
    if response.status_code not in (200, 400):
        response.raise_for_status()


def search_pending_files(size: int) -> list[dict]:
    body = {
        "size": size,
        "query": {"term": {"status": "pending"}},
        "sort": [{"timestamp": {"order": "asc"}}],
    }
    response = es_request(
        "GET",
        f"/{QUEUE_INDEX}/_search",
        headers={"Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=60,
    )
    return [
        {"_id": hit["_id"], **(hit.get("_source") or {})}
        for hit in response.json().get("hits", {}).get("hits", [])
    ]


def update_queue_doc(doc_id: str, fields: dict) -> dict:
    response = es_request(
        "POST",
        f"/{QUEUE_INDEX}/_update/{doc_id}?refresh=true",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"doc": fields}, ensure_ascii=False),
        timeout=60,
    )
    return response.json()


def claim_next_pending_file() -> dict | None:
    for file_doc in search_pending_files(20):
        body = {
            "script": {
                "lang": "painless",
                "source": """
                    if (ctx._source.status == params.pending) {
                        ctx._source.status = params.processing;
                        ctx._source.started_at = params.started_at;
                    } else {
                        ctx.op = 'none';
                    }
                """,
                "params": {
                    "pending": "pending",
                    "processing": "processing",
                    "started_at": utc_now(),
                },
            }
        }
        response = es_request(
            "POST",
            f"/{QUEUE_INDEX}/_update/{file_doc['_id']}?refresh=true",
            headers={"Content-Type": "application/json"},
            data=json.dumps(body),
            timeout=60,
        ).json()
        if response.get("result") == "updated":
            return file_doc
    return None


def normalize(text: str) -> str:
    return SPACE_RE.sub(" ", text.lower()).strip()


def term_matches(text: str, term: str) -> bool:
    escaped = re.escape(term.lower()).replace(r"\ ", r"\s+")
    return re.search(rf"(?<!\w){escaped}(?!\w)", text) is not None


def contains_any(text: str, terms: list[str]) -> list[str]:
    return [term for term in terms if term_matches(text, term)]


def gkg_created_at(file_id: str) -> str:
    parsed = datetime.strptime(file_id, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    return parsed.isoformat()


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
        blocked_hits = contains_any(text, BLOCKED_TERMS.get(category, []))
        if blocked_hits:
            continue
        category_hits = contains_any(text, keywords)
        if category_hits:
            matches.append((category, sorted(set(category_hits + general_hits + australia_hits))))
    return matches


def build_docs(file_doc: dict, fields: list[str], sequence: int) -> list[dict]:
    text = row_text(fields)
    categories = match_categories(text)
    if not categories:
        return []
    record_id = fields[0] if fields else f"{file_doc['_id']}:{sequence}"
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
                "created_at": gkg_created_at(file_doc["_id"]),
                "like_count": 0,
                "reply_count": 0,
                "repost_count": 0,
                "quote_count": 0,
                "gkg_file_id": file_doc["_id"],
                "gkg_url": file_doc["url"],
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


def bulk_index_docs(docs: list[dict]) -> dict:
    if not docs:
        return {"attempted": 0, "indexed": 0, "failed": 0}
    lines: list[str] = []
    for doc in docs:
        payload = dict(doc)
        doc_id = payload.pop("_doc_id")
        lines.append(json.dumps({"index": {"_index": RAW_INDEX, "_id": doc_id}}, ensure_ascii=False))
        lines.append(json.dumps(payload, ensure_ascii=False))
    response = es_request(
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


def process_zip_file(file_doc: dict) -> dict:
    response = requests.get(file_doc["url"], timeout=180)
    response.raise_for_status()
    expected_md5 = str(file_doc.get("md5") or "").lower()
    actual_md5 = hashlib.md5(response.content).hexdigest()
    if expected_md5 and expected_md5 != actual_md5:
        raise ValueError(f"MD5 mismatch for {file_doc['_id']}")

    rows_seen = 0
    docs_buffer: list[dict] = []
    indexed = 0
    matched_rows = 0
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        member_name = archive.namelist()[0]
        with archive.open(member_name) as csv_file:
            for raw_line in io.TextIOWrapper(csv_file, encoding="utf-8", errors="replace"):
                rows_seen += 1
                fields = raw_line.rstrip("\n").split("\t")
                docs = build_docs(file_doc, fields, rows_seen)
                if docs:
                    matched_rows += 1
                    docs_buffer.extend(docs)
                if len(docs_buffer) >= 500:
                    indexed += bulk_index_docs(docs_buffer)["indexed"]
                    docs_buffer = []
    if docs_buffer:
        indexed += bulk_index_docs(docs_buffer)["indexed"]
    return {
        "rows_seen": rows_seen,
        "matched_rows": matched_rows,
        "indexed_docs": indexed,
        "bytes_downloaded": len(response.content),
    }


def main() -> dict:
    ensure_raw_index()
    started = time.monotonic()
    since, first_run = stream_start_time()
    until = datetime.now(timezone.utc)

    processed = 0
    indexed_docs = 0
    rows_seen = 0
    matched_rows = 0
    failed = 0
    last_file_timestamp = since.isoformat()
    details = []

    for file_doc in iter_masterfile_entries_after(since, until):
        if processed >= BATCH_SIZE or (time.monotonic() - started) >= MAX_RUNTIME_SECONDS:
            break
        try:
            result = process_zip_file(file_doc)
            processed += 1
            rows_seen += result["rows_seen"]
            matched_rows += result["matched_rows"]
            indexed_docs += result["indexed_docs"]
            last_file_timestamp = file_doc["timestamp"]
            details.append({"file_id": file_doc["_id"], **result})
        except Exception as exc:
            failed += 1
            processed += 1
            last_file_timestamp = file_doc["timestamp"]
            details.append({"file_id": file_doc["_id"], "error": f"{type(exc).__name__}: {exc}"})

        stats = {
            "first_run": first_run,
            "interval_start": since.isoformat(),
            "interval_end": until.isoformat(),
            "last_file_timestamp": last_file_timestamp,
            "processed_files": processed,
            "failed_files": failed,
            "rows_seen": rows_seen,
            "matched_rows": matched_rows,
            "indexed_docs": indexed_docs,
        }
        update_state(last_file_timestamp, stats)

    if processed == 0:
        stats = {
            "first_run": first_run,
            "interval_start": since.isoformat(),
            "interval_end": until.isoformat(),
            "last_file_timestamp": last_file_timestamp,
            "processed_files": 0,
            "failed_files": 0,
            "rows_seen": 0,
            "matched_rows": 0,
            "indexed_docs": 0,
        }
        update_state(last_file_timestamp, stats)

    return {
        "function": FUNCTION_NAME,
        "raw_index": RAW_INDEX,
        "state_index": STATE_INDEX,
        "start_from": since.isoformat(),
        "checked_until": until.isoformat(),
        "processed_files": processed,
        "failed_files": failed,
        "rows_seen": rows_seen,
        "matched_rows": matched_rows,
        "indexed_docs": indexed_docs,
        "last_file_timestamp": last_file_timestamp,
        "details": details[:10],
    }


def harvest_gdelt_gkg_stream() -> dict:
    return main()
