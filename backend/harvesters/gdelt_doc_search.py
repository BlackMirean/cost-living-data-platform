"""Optional GDELT DOC API search helpers.

Production ingestion uses the GDELT GKG archive pipeline in
``backend.harvesters.gdelt_archive``. This module is kept for small diagnostic
searches and tests only.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests

from backend.analytics.topics import (
    GDELT_COVERAGE_TERMS,
    gdelt_terms_for_topic,
    infer_topic_from_query,
)
from backend.common.config import settings
from backend.common.document_store import index_raw_posts
from backend.harvesters.raw_records import raw_post_document


UTC = timezone.utc
MIN_GDELT_TOKEN_LENGTH = 4
MAX_GDELT_TERMS_PER_BLOCK = 8
MAX_GDELT_ARTLIST_RECORDS = 250
GDELT_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
GDELT_LOCATION_SEARCH_TERMS = [
    "Australia",
    "Australian",
    "Melbourne",
    "Sydney",
    "Brisbane",
    "Perth",
]


def quote_gdelt_term(value: str) -> str:
    """Quote multi-word GDELT search terms."""

    cleaned = value.strip().replace('"', "")
    if not cleaned:
        return cleaned
    if " " in cleaned or "-" in cleaned:
        return f'"{cleaned}"'
    return cleaned


def is_gdelt_searchable_term(value: str) -> bool:
    """Return whether a term is long enough for GDELT keyword search."""

    cleaned = value.strip()
    if not cleaned:
        return False
    tokens = GDELT_TOKEN_RE.findall(cleaned)
    return bool(tokens) and all(len(token) >= MIN_GDELT_TOKEN_LENGTH for token in tokens)


def gdelt_search_terms(terms: list[str], max_terms: int | None = None) -> list[str]:
    """Dedupe and drop terms that GDELT rejects as too short."""

    result: list[str] = []
    seen: set[str] = set()
    for term in terms:
        cleaned = term.strip()
        key = cleaned.casefold()
        if not is_gdelt_searchable_term(cleaned) or key in seen:
            continue
        result.append(cleaned)
        seen.add(key)
        if max_terms is not None and len(result) >= max_terms:
            break
    return result


def gdelt_or_block(terms: list[str]) -> str:
    """Build a stable OR block for GDELT query terms."""

    return " OR ".join(quote_gdelt_term(term) for term in gdelt_search_terms(terms))


def gdelt_group(terms: list[str]) -> str:
    """Parenthesize only OR groups, which is what GDELT accepts."""

    block = gdelt_or_block(terms)
    if " OR " in block:
        return f"({block})"
    return block


def gdelt_operator_value(value: str) -> str:
    """Normalise a GDELT operator value such as sourcecountry or sourcelang."""

    return re.sub(r"\s+", "", value.strip().lower())


def gdelt_scope_filters() -> list[str]:
    """Return GDELT query operators that keep the dataset Australia-focused."""

    filters: list[str] = []
    source_country = gdelt_operator_value(settings.gdelt_source_country)
    source_language = gdelt_operator_value(settings.gdelt_source_language)
    if source_country:
        filters.append(f"sourcecountry:{source_country}")
    if source_language:
        filters.append(f"sourcelang:{source_language}")
    return filters


def is_gdelt_article_in_scope(article: dict[str, Any]) -> bool:
    """Return whether a GDELT article matches the configured source scope."""

    expected_country = gdelt_operator_value(settings.gdelt_source_country)
    expected_language = gdelt_operator_value(settings.gdelt_source_language)
    actual_country = gdelt_operator_value(
        article.get("sourcecountry") or article.get("sourceCountry") or ""
    )
    actual_language = gdelt_operator_value(article.get("language") or "")

    country_aliases = {
        expected_country,
        "au" if expected_country == "australia" else expected_country,
    }
    language_aliases = {
        expected_language,
        "eng" if expected_language == "english" else expected_language,
    }

    if expected_country and actual_country not in country_aliases:
        return False
    if expected_language and actual_language not in language_aliases:
        return False
    return True


def build_gdelt_query(query: str) -> str:
    """Build an Australia-focused GDELT DOC query for one topic term."""

    topic = infer_topic_from_query(query)
    seed_terms = gdelt_search_terms([query])
    if not seed_terms:
        seed_terms = gdelt_search_terms(
            [*gdelt_terms_for_topic(topic), *GDELT_COVERAGE_TERMS],
            max_terms=MAX_GDELT_TERMS_PER_BLOCK,
        )
    scope_filters = gdelt_scope_filters()
    location_block = "" if scope_filters else gdelt_group(GDELT_LOCATION_SEARCH_TERMS)
    parts = [gdelt_group(seed_terms), location_block, *scope_filters]
    return " ".join(part for part in parts if part)


def parse_gdelt_date(value: str | None) -> str:
    """Parse GDELT article dates into ISO-8601 UTC strings."""

    if not value:
        return datetime.now(tz=UTC).replace(microsecond=0).isoformat()

    cleaned = value.strip()
    if cleaned.isdigit() and len(cleaned) >= 14:
        return datetime.strptime(cleaned[:14], "%Y%m%d%H%M%S").replace(tzinfo=UTC).isoformat()
    if cleaned.isdigit() and len(cleaned) == 8:
        return datetime.strptime(cleaned, "%Y%m%d").replace(tzinfo=UTC).isoformat()

    parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat()


def search_gdelt_articles(
    query: str,
    limit: int,
    timespan: str | None = None,
    retries: int | None = None,
    start_datetime: str | None = None,
    end_datetime: str | None = None,
    request_timeout_seconds: float | None = None,
    retry_delay_seconds: float | None = None,
) -> list[dict[str, Any]]:
    """Search the GDELT DOC 2.0 API for matching news articles."""

    if timespan and (start_datetime or end_datetime):
        raise ValueError("Use either timespan or start/end datetime for GDELT, not both.")

    retry_count = settings.gdelt_retries if retries is None else retries
    timeout_seconds = (
        settings.gdelt_request_timeout_seconds
        if request_timeout_seconds is None
        else request_timeout_seconds
    )
    retry_delay = (
        settings.gdelt_request_delay_seconds
        if retry_delay_seconds is None
        else retry_delay_seconds
    )
    params = {
        "query": build_gdelt_query(query),
        "mode": "artlist",
        "format": "json",
        "maxrecords": max(1, min(limit, MAX_GDELT_ARTLIST_RECORDS)),
        "sort": "datedesc",
    }
    if start_datetime or end_datetime:
        if start_datetime:
            params["startdatetime"] = start_datetime
        if end_datetime:
            params["enddatetime"] = end_datetime
    else:
        params["timespan"] = timespan or settings.gdelt_timespan

    for attempt in range(retry_count + 1):
        try:
            response = requests.get(
                settings.gdelt_doc_api_url,
                params=params,
                timeout=timeout_seconds,
            )
        except requests.RequestException:
            if attempt >= retry_count:
                raise
            time.sleep(settings.gdelt_request_delay_seconds)
            continue

        if response.status_code == 429 and attempt < retry_count:
            retry_after = response.headers.get("Retry-After", "")
            try:
                sleep_seconds = max(float(retry_after), 0)
            except ValueError:
                sleep_seconds = 0
            if sleep_seconds <= 0:
                sleep_seconds = max(retry_delay, 5) * (2**attempt)
            time.sleep(sleep_seconds)
            continue
        response.raise_for_status()
        try:
            payload = response.json()
        except (json.JSONDecodeError, ValueError):
            preview = response.text.replace("\n", " ")[:200]
            print(
                "Skipped GDELT query "
                f"'{query}': expected JSON but received {response.headers.get('content-type', 'unknown')} "
                f"response: {preview}",
                flush=True,
            )
            return []
        return payload.get("articles", [])
    return []


def article_text(article: dict[str, Any]) -> str:
    """Return the best available article text for raw storage."""

    parts = [
        article.get("title", ""),
        article.get("description", ""),
    ]
    return ". ".join(part.strip() for part in parts if part and part.strip())


def raw_gdelt_article(article: dict[str, Any], query: str) -> dict[str, Any]:
    """Store the original GDELT article before filtering and NLP."""

    text = article_text(article)
    created_at = parse_gdelt_date(article.get("seendate") or article.get("date"))
    url = article.get("url") or ""
    domain = article.get("domain") or article.get("sourceCommonName") or "unknown"
    source_country = article.get("sourcecountry") or article.get("sourceCountry") or ""
    doc_id = hashlib.sha1(f"gdelt|{url}|{created_at}|{text}".encode("utf-8")).hexdigest()
    return raw_post_document(
        platform="gdelt",
        source="gdelt_doc_api",
        source_id=url or doc_id,
        author=domain,
        location_hint=source_country or "Australia-related GDELT query",
        text=text,
        query=query,
        url=url,
        created_at=created_at,
        engagement={},
        payload=article,
        raw_id=f"gdelt-{doc_id}",
    )


def harvest_gdelt_news_raw(
    reset: bool = False,
    limit: int | None = None,
    timespan: str | None = None,
) -> int:
    """Harvest GDELT records into the raw store without scenario filtering or NLP."""

    docs: list[dict[str, Any]] = []
    per_query_limit = limit or settings.gdelt_limit
    search_timespan = timespan or settings.gdelt_timespan
    queries = settings.gdelt_query_list or settings.cost_of_living_query_list
    started = time.monotonic()

    print(
        "Starting raw GDELT harvest "
        f"queries={len(queries)} limit={per_query_limit} timespan={search_timespan} "
        f"request_timeout={settings.gdelt_request_timeout_seconds} "
        f"max_runtime={settings.gdelt_max_runtime_seconds}",
        flush=True,
    )
    for index, query in enumerate(queries):
        elapsed = time.monotonic() - started
        if elapsed >= settings.gdelt_max_runtime_seconds:
            print(
                f"Stopping raw GDELT harvest after {elapsed:.1f}s with {len(docs)} "
                "raw documents collected.",
                flush=True,
            )
            break

        print(f"Searching raw GDELT query {index + 1}/{len(queries)}: {query}", flush=True)
        try:
            articles = search_gdelt_articles(
                query=query,
                limit=per_query_limit,
                timespan=search_timespan,
            )
        except requests.RequestException as exc:
            print(f"Skipped GDELT query '{query}': {exc}", flush=True)
            articles = []
        print(f"GDELT query '{query}' returned {len(articles)} raw article candidates", flush=True)
        scoped_articles = [article for article in articles if is_gdelt_article_in_scope(article)]
        skipped = len(articles) - len(scoped_articles)
        if skipped:
            print(f"Skipped {skipped} out-of-scope GDELT articles before raw indexing", flush=True)
        docs.extend(raw_gdelt_article(article, query=query) for article in scoped_articles)
        if index < len(queries) - 1:
            time.sleep(settings.gdelt_request_delay_seconds)

    print(f"Indexing {len(docs)} raw GDELT documents", flush=True)
    indexed_count = index_raw_posts(docs, reset=reset)
    print(f"Indexed {indexed_count} raw GDELT documents", flush=True)
    return indexed_count
