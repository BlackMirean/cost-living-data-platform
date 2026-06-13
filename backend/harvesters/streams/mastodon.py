from __future__ import annotations

import html
import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
import urllib3

from backend.common.config import settings

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PLATFORM = "mastodon"
RAW_INDEX = settings.mastodon_stream_raw_index
STATE_INDEX = settings.social_harvest_state_index

MASTODON_STREAM_CONFIGS = {
    "mastodon_au": {
        "function_name": "harvest-mastodon-au-stream",
        "instance": "mastodon_au",
        "base_url": settings.mastodon_au_base_url,
        "access_token": settings.mastodon_au_access_token,
    },
    "mastodon_social": {
        "function_name": "harvest-mastodon-social-stream",
        "instance": "mastodon_social",
        "base_url": settings.mastodon_social_base_url,
        "access_token": settings.mastodon_social_access_token,
    },
    "aus_social": {
        "function_name": "harvest-aus-social-stream",
        "instance": "aus_social",
        "base_url": settings.aus_social_base_url,
        "access_token": settings.aus_social_access_token,
    },
}

FUNCTION_NAME = MASTODON_STREAM_CONFIGS["mastodon_au"]["function_name"]
INSTANCE = MASTODON_STREAM_CONFIGS["mastodon_au"]["instance"]
MASTODON_BASE_URL = MASTODON_STREAM_CONFIGS["mastodon_au"]["base_url"].rstrip("/")
CURRENT_STREAM_ACCESS_TOKEN = MASTODON_STREAM_CONFIGS["mastodon_au"]["access_token"]


def configure_stream(stream_name: str) -> None:
    config = MASTODON_STREAM_CONFIGS[stream_name]
    global FUNCTION_NAME, INSTANCE, MASTODON_BASE_URL, CURRENT_STREAM_ACCESS_TOKEN
    FUNCTION_NAME = config["function_name"]
    INSTANCE = config["instance"]
    MASTODON_BASE_URL = str(config["base_url"]).rstrip("/")
    CURRENT_STREAM_ACCESS_TOKEN = str(config["access_token"] or "")


INITIAL_LOOKBACK_DAYS = settings.social_stream_initial_lookback_days
REQUEST_LIMIT = 40
REQUEST_INTERVAL_SECONDS = 1.0
FIRST_RUN_MAX_PAGES = settings.social_stream_first_run_max_pages or None
INCREMENTAL_MAX_PAGES = settings.social_stream_incremental_max_pages or None
MAX_RETRIES = 5
RETRY_WAIT_SECONDS = 15

HTML_TAG_RE = re.compile(r"<[^>]+>")

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
    "mastodon.au",
    "aus.social",
)

CATEGORY_QUERIES = {
    "housing": ["RentalCrisis", "HousingCrisis"],
    "groceries": ["Groceries", "FoodPrices"],
    "energy": ["EnergyBill", "ElectricityPrices"],
    "fuel": ["PetrolPrices", "FuelPrices"],
    "eating_out": ["CostOfLiving", "Takeaway"],
    "transport": ["PublicTransport", "TransportCosts"],
    "healthcare": ["Medicare", "HealthInsurance"],
    "education": ["HECS", "StudentDebt"],
    "home_goods": ["FurniturePrices", "AppliancePrices"],
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


def strip_html(raw: Optional[str]) -> str:
    if not raw:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", str(raw), flags=re.IGNORECASE)
    text = HTML_TAG_RE.sub(" ", text)
    return normalize_whitespace(html.unescape(text))


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


def search_statuses(query: str, max_pages: Optional[int], since: datetime) -> list[dict]:
    headers = {"Authorization": f"Bearer {CURRENT_STREAM_ACCESS_TOKEN}"}
    tag = query.lstrip("#")
    statuses: list[dict] = []
    max_id: Optional[str] = None
    pages = 0
    while max_pages is None or pages < max_pages:
        params = {"limit": REQUEST_LIMIT}
        if max_id:
            params["max_id"] = max_id
        last_error: Optional[Exception] = None
        page_statuses: Optional[list[dict]] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.get(
                    f"{MASTODON_BASE_URL}/api/v1/timelines/tag/{tag}",
                    headers=headers,
                    params=params,
                    timeout=15,
                )
                response.raise_for_status()
                page_statuses = list(response.json() or [])
                break
            except requests.RequestException as exc:
                last_error = exc
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                is_timeout = isinstance(exc, requests.Timeout)
                temporary_server_error = status_code is not None and status_code >= 500
                if status_code == 429:
                    wait_seconds = RETRY_WAIT_SECONDS * attempt
                elif is_timeout or temporary_server_error:
                    wait_seconds = RETRY_WAIT_SECONDS * attempt
                else:
                    print(f"[{FUNCTION_NAME}] skip query={query} status={status_code} error={last_error}")
                    return statuses
                print(f"[{FUNCTION_NAME}] retry query={query} status={status_code} after {wait_seconds}s")
                time.sleep(wait_seconds)
        if page_statuses is None:
            print(f"[{FUNCTION_NAME}] skip query={query} error={last_error}")
            break
        if not page_statuses:
            break
        statuses.extend(page_statuses)
        pages += 1
        oldest = parse_datetime(page_statuses[-1].get("created_at"))
        if oldest and oldest < since:
            break
        next_max_id = str(page_statuses[-1].get("id") or "")
        if not next_max_id or next_max_id == max_id:
            break
        max_id = next_max_id
        time.sleep(REQUEST_INTERVAL_SECONDS)
    return statuses


def build_row(category: str, query: str, status: dict, collected_at: str, since: datetime, until: datetime) -> Optional[dict]:
    created_at = parse_datetime(status.get("created_at"))
    if not created_at or created_at < since or created_at > until:
        return None
    text = strip_html(status.get("content"))
    account = status.get("account") or {}
    url = status.get("url") or status.get("uri") or ""
    source_id = status.get("uri") or status.get("id")
    if not source_id or not text or not has_australia_context(text, query, account.get("acct") or "", url, MASTODON_BASE_URL):
        return None
    return {
        "platform": PLATFORM,
        "instance": INSTANCE,
        "stage": "raw",
        "category": category,
        "search_term": CATEGORY_EXPRESSIONS.get(category, query),
        "api_query": f"#{query}",
        "source_id": str(source_id),
        "post_id": str(status.get("id") or ""),
        "author_handle": account.get("acct") or account.get("username") or "",
        "author_display_name": account.get("display_name"),
        "text": text,
        "created_at": iso_utc(created_at),
        "like_count": status.get("favourites_count", 0),
        "reply_count": status.get("replies_count", 0),
        "repost_count": status.get("reblogs_count", 0),
        "quote_count": 0,
        "url": url,
        "collected_at": collected_at,
    }


def bulk_index(rows: list[dict]) -> dict:
    if not rows:
        return {"attempted": 0, "indexed": 0, "failed": 0, "errors": False}
    lines: list[str] = []
    for row in rows:
        doc_id = f"{PLATFORM}:{INSTANCE}:{row['source_id']}"
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
    since, until, first_run = harvest_interval()
    collected_at = iso_utc(utc_now())
    rows_by_id: dict[str, dict] = {}
    raw_seen = 0

    max_pages = FIRST_RUN_MAX_PAGES if first_run else INCREMENTAL_MAX_PAGES
    for category, queries in CATEGORY_QUERIES.items():
        for query in queries[:2]:
            print(f"[{FUNCTION_NAME}] category={category} query={query} max_pages={max_pages}")
            statuses = search_statuses(query, max_pages, since)
            raw_seen += len(statuses)
            for status in statuses:
                row = build_row(category, query, status, collected_at, since, until)
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


def harvest_mastodon_au_stream() -> dict:
    configure_stream("mastodon_au")
    return main()


def harvest_mastodon_social_stream() -> dict:
    configure_stream("mastodon_social")
    return main()


def harvest_aus_social_stream() -> dict:
    configure_stream("aus_social")
    return main()
