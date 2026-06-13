from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
import urllib3

from backend.common.config import settings

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

FUNCTION_NAME = "harvest-bluesky-stream"
PLATFORM = "bluesky"
INSTANCE = "bsky.social"
RAW_INDEX = settings.bluesky_stream_raw_index
STATE_INDEX = settings.social_harvest_state_index
INITIAL_LOOKBACK_DAYS = settings.social_stream_initial_lookback_days
REQUEST_LIMIT = 100
REQUEST_INTERVAL_SECONDS = 0.5
FIRST_RUN_MAX_PAGES = settings.social_stream_first_run_max_pages or None
INCREMENTAL_MAX_PAGES = settings.social_stream_incremental_max_pages or None
MAX_RETRIES = 5
RETRY_WAIT_SECONDS = 15

BLUESKY_BASE_URL = settings.bluesky_stream_base_url.rstrip("/")
BLUESKY_HANDLE = settings.bluesky_stream_handle
BLUESKY_APP_PASSWORD = settings.bluesky_stream_app_password

AUSTRALIA_TERMS = (
    "australia",
    "australian",
    "aussie",
    "sydney",
    "melbourne",
    "brisbane",
    "perth",
    "adelaide",
    "canberra",
    "hobart",
    "darwin",
    "nsw",
    "vic",
    "qld",
    "wa",
    "tasmania",
    "coles",
    "woolworths",
    "woolies",
    "aldi",
    "myki",
    "opal",
    "medicare",
)

CATEGORY_QUERIES = {
    "housing": ["rent melbourne", "rent sydney"],
    "groceries": ["coles australia", "woolworths australia"],
    "energy": ["electricity australia", "energy bill australia"],
    "fuel": ["petrol melbourne", "petrol sydney"],
    "eating_out": ["coffee melbourne", "coffee sydney"],
    "transport": ["public transport australia", "myki melbourne"],
    "healthcare": ["medicare australia", "doctor australia"],
    "education": ["hecs australia", "school fees australia"],
    "home_goods": ["furniture australia", "appliances australia"],
}

CATEGORY_EXPRESSIONS = {
    "housing": "australia AND (rent OR rental OR housing OR landlord OR tenant)",
    "groceries": "australia AND (groceries OR grocery OR supermarket OR coles OR woolworths OR aldi)",
    "energy": "australia AND (electricity OR power bill OR energy bill OR gas bill OR utilities)",
    "fuel": "australia AND (petrol OR fuel OR diesel OR servo OR bowser)",
    "eating_out": "australia AND (coffee OR cafe OR takeaway OR restaurant OR ubereats OR doordash)",
    "transport": "australia AND (public transport OR myki OR opal OR train fare OR toll OR parking)",
    "healthcare": "australia AND (gp OR doctor OR dentist OR medicare OR pharmacy OR medicine)",
    "education": "australia AND (school fees OR tuition fees OR childcare fees OR hecs OR student debt)",
    "home_goods": "australia AND (furniture OR appliance OR fridge OR mattress OR whitegoods)",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_whitespace(value: Optional[str]) -> str:
    if not value:
        return ""
    return " ".join(str(value).split())


ES_URL = settings.elasticsearch_url.rstrip("/")
ES_USERNAME = settings.elasticsearch_username or "elastic"
ES_PASSWORD = settings.elasticsearch_password
ES_VERIFY_TLS = settings.elasticsearch_verify_certs


def es_request(method: str, path: str, **kwargs) -> requests.Response:
    response = requests.request(
        method,
        f"{ES_URL}{path}",
        auth=(ES_USERNAME, ES_PASSWORD),
        verify=ES_VERIFY_TLS,
        timeout=kwargs.pop("timeout", 60),
        **kwargs,
    )
    response.raise_for_status()
    return response


def ensure_index(index: str) -> None:
    mapping = {
        "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 1}},
        "mappings": {
            "properties": {
                "platform": {"type": "keyword"},
                "instance": {"type": "keyword"},
                "stage": {"type": "keyword"},
                "category": {"type": "keyword"},
                "search_term": {"type": "keyword"},
                "source_id": {"type": "keyword"},
                "author_handle": {"type": "keyword"},
                "created_at": {"type": "date"},
                "collected_at": {"type": "date"},
                "text": {"type": "text"},
            }
        },
    }
    response = requests.put(
        f"{ES_URL}/{index}",
        auth=(ES_USERNAME, ES_PASSWORD),
        verify=ES_VERIFY_TLS,
        headers={"Content-Type": "application/json"},
        data=json.dumps(mapping),
        timeout=60,
    )
    if response.status_code not in (200, 400):
        response.raise_for_status()


def get_state() -> dict:
    response = requests.get(
        f"{ES_URL}/{STATE_INDEX}/_doc/{FUNCTION_NAME}",
        auth=(ES_USERNAME, ES_PASSWORD),
        verify=ES_VERIFY_TLS,
        timeout=30,
    )
    if response.status_code == 404:
        return {}
    response.raise_for_status()
    return response.json().get("_source") or {}


def update_state(since: datetime, until: datetime, stats: dict) -> None:
    payload = {
        "function": FUNCTION_NAME,
        "platform": PLATFORM,
        "instance": INSTANCE,
        "last_interval_start": iso_utc(since),
        "last_interval_end": iso_utc(until),
        "last_success_at": iso_utc(utc_now()),
        "stats": stats,
    }
    es_request(
        "PUT",
        f"/{STATE_INDEX}/_doc/{FUNCTION_NAME}?refresh=true",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
    )


def harvest_interval() -> tuple[datetime, datetime, bool]:
    now = utc_now()
    state = get_state()
    previous_until = parse_datetime(state.get("last_interval_end"))
    if previous_until:
        return previous_until, now, False
    return now - timedelta(days=INITIAL_LOOKBACK_DAYS), now, True


def has_australia_context(*parts: str) -> bool:
    haystack = " ".join(part for part in parts if part).lower()
    return any(term_matches(haystack, term) for term in AUSTRALIA_TERMS)


def term_matches(text: str, term: str) -> bool:
    escaped = re.escape(term.lower()).replace(r"\ ", r"\s+")
    return re.search(rf"(?<!\w){escaped}(?!\w)", text) is not None


def create_bluesky_session() -> requests.Session:
    session = requests.Session()
    response = session.post(
        f"{BLUESKY_BASE_URL}/xrpc/com.atproto.server.createSession",
        json={"identifier": BLUESKY_HANDLE, "password": BLUESKY_APP_PASSWORD},
        timeout=30,
    )
    response.raise_for_status()
    session.headers.update({"Authorization": f"Bearer {response.json()['accessJwt']}"})
    return session


def search_posts(session: requests.Session, query: str, since: datetime, until: datetime, max_pages: Optional[int]) -> list[dict]:
    posts: list[dict] = []
    cursor: Optional[str] = None
    seen_cursors: set[str] = set()
    pages = 0
    while max_pages is None or pages < max_pages:
        params = {
            "q": query,
            "limit": REQUEST_LIMIT,
            "sort": "latest",
            "since": iso_utc(since),
            "until": iso_utc(until),
        }
        if cursor:
            params["cursor"] = cursor
        last_error: Optional[Exception] = None
        page_payload: Optional[dict] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = session.get(
                    f"{BLUESKY_BASE_URL}/xrpc/app.bsky.feed.searchPosts",
                    params=params,
                    timeout=45,
                )
                response.raise_for_status()
                page_payload = response.json() or {}
                break
            except requests.RequestException as exc:
                last_error = exc
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                wait_seconds = RETRY_WAIT_SECONDS * attempt
                print(f"[{FUNCTION_NAME}] retry query={query} status={status_code} after {wait_seconds}s")
                time.sleep(wait_seconds)
        if page_payload is None:
            print(f"[{FUNCTION_NAME}] skip query={query} error={last_error}")
            break
        page_posts = list(page_payload.get("posts") or [])
        if not page_posts:
            break
        posts.extend(page_posts)
        pages += 1
        cursor = page_payload.get("cursor")
        if not cursor or cursor in seen_cursors:
            break
        seen_cursors.add(cursor)
        time.sleep(REQUEST_INTERVAL_SECONDS)
    return posts


def build_row(category: str, query: str, post: dict, collected_at: str) -> Optional[dict]:
    record = post.get("record") or {}
    author = post.get("author") or {}
    text = normalize_whitespace(record.get("text"))
    created_at = record.get("createdAt")
    source_id = post.get("uri") or post.get("cid")
    if not source_id or not text or not has_australia_context(text, query, author.get("handle") or ""):
        return None
    return {
        "platform": PLATFORM,
        "instance": INSTANCE,
        "stage": "raw",
        "category": category,
        "search_term": CATEGORY_EXPRESSIONS.get(category, query),
        "api_query": query,
        "source_id": source_id,
        "cid": post.get("cid"),
        "author_handle": author.get("handle") or "",
        "author_display_name": author.get("displayName"),
        "text": text,
        "created_at": created_at,
        "like_count": post.get("likeCount", 0),
        "reply_count": post.get("replyCount", 0),
        "repost_count": post.get("repostCount", 0),
        "quote_count": post.get("quoteCount", 0),
        "url": "",
        "collected_at": collected_at,
    }


def bulk_index(rows: list[dict]) -> dict:
    if not rows:
        return {"attempted": 0, "indexed": 0, "failed": 0, "errors": False}
    lines: list[str] = []
    for row in rows:
        doc_id = f"{PLATFORM}:{row['source_id']}"
        lines.append(json.dumps({"index": {"_index": RAW_INDEX, "_id": doc_id}}, ensure_ascii=False))
        lines.append(json.dumps(row, ensure_ascii=False, default=str))
    response = es_request(
        "POST",
        "/_bulk?refresh=true",
        headers={"Content-Type": "application/x-ndjson"},
        data=("\n".join(lines) + "\n").encode("utf-8"),
        timeout=120,
    )
    result = response.json()
    items = result.get("items") or []
    failed = [item for item in items if int((item.get("index") or {}).get("status", 500)) >= 300]
    return {
        "attempted": len(rows),
        "indexed": len(items) - len(failed),
        "failed": len(failed),
        "errors": bool(result.get("errors")),
    }


def main() -> dict:
    ensure_index(RAW_INDEX)
    ensure_index(STATE_INDEX)
    if not BLUESKY_HANDLE or not BLUESKY_APP_PASSWORD:
        return {
            "function": FUNCTION_NAME,
            "index": RAW_INDEX,
            "skipped": True,
            "reason": "missing_bluesky_credentials",
        }
    since, until, first_run = harvest_interval()
    collected_at = iso_utc(utc_now())
    session = create_bluesky_session()
    rows_by_id: dict[str, dict] = {}
    raw_seen = 0

    max_pages = FIRST_RUN_MAX_PAGES if first_run else INCREMENTAL_MAX_PAGES
    for category, queries in CATEGORY_QUERIES.items():
        for query in queries[:2]:
            print(f"[{FUNCTION_NAME}] category={category} query={query} max_pages={max_pages}")
            posts = search_posts(session, query, since, until, max_pages)
            raw_seen += len(posts)
            for post in posts:
                row = build_row(category, query, post, collected_at)
                if row:
                    rows_by_id[row["source_id"]] = row
            time.sleep(REQUEST_INTERVAL_SECONDS)

    rows = sorted(rows_by_id.values(), key=lambda item: item.get("created_at") or "")
    bulk_result = bulk_index(rows)
    stats = {
        "first_run": first_run,
        "interval_start": iso_utc(since),
        "interval_end": iso_utc(until),
        "raw_seen": raw_seen,
        "matched_rows": len(rows),
        "elasticsearch": bulk_result,
    }
    update_state(since, until, stats)
    return {"function": FUNCTION_NAME, "index": RAW_INDEX, **stats}


def harvest_bluesky_stream() -> dict:
    return main()
