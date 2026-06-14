"""NLP worker that converts unified raw posts into the processed contract."""

from __future__ import annotations

import argparse
import hashlib
import re
from datetime import datetime, timezone
from typing import Any

from elasticsearch import helpers
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from backend.analytics.topics import (
    classify_cost_of_living_topic,
    has_australia_context,
    matched_keywords,
)
from backend.common.config import settings
from backend.common.es_client import ensure_posts_index, get_es_client
from backend.common.source_registry import (
    social_platforms,
    source_group_for_platform,
)


MODEL_NAME = "vader"
MODEL_VERSION = "cost_living_topic_sentiment_2026_06"
SOCIAL_PLATFORMS = social_platforms()

COMPLAINT_KEYWORDS = [
    "expensive",
    "too expensive",
    "cant afford",
    "can't afford",
    "afford",
    "affordability",
    "costs too much",
    "cost too much",
    "costing more",
    "costing too much",
    "more expensive",
    "getting expensive",
    "getting pricier",
    "so expensive",
    "so pricey",
    "pricey",
    "overpriced",
    "price hike",
    "prices up",
    "price rise",
    "price rises",
    "price increase",
    "price increases",
    "gone up",
    "goes up",
    "going up",
    "keep going up",
    "keeps going up",
    "everything is going up",
    "increase",
    "increased",
    "raising",
    "rising",
    "outrageous",
    "ridiculous",
    "crazy prices",
    "out of control",
    "through the roof",
    "gone through the roof",
    "struggling",
    "struggle",
    "struggling with bills",
    "struggling to pay",
    "broke",
    "cost of living",
    "bill shock",
    "budget",
    "tight budget",
    "budget pressure",
    "budget squeeze",
    "rent hike",
    "rent increase",
    "price gouging",
    "stress",
    "pressure",
    "cost pressure",
    "financial pressure",
    "hurting",
    "hurts",
    "squeezed",
    "squeeze",
    "unaffordable",
    "hard to pay",
    "hard to keep up",
    "getting harder",
    "living is getting harder",
    "harder to afford",
    "breaking the bank",
    "stretching the budget",
    "too much",
]

AD_KEYWORDS = [
    "register",
    "join now",
    "follow link",
    "tickets",
    "sale",
    "promotion",
    "special offer",
    "buy now",
    "subscribe",
]

sia = SentimentIntensityAnalyzer()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(text: Any) -> str:
    """Return a lower-cased text field with common social markup removed."""

    if not text:
        return ""

    cleaned = str(text)
    cleaned = re.sub(r"http\S+", "", cleaned)
    cleaned = re.sub(r"#\w+", "", cleaned)
    cleaned = re.sub(r"@\w+", "", cleaned)
    cleaned = re.sub(r"<.*?>", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip().lower()


def normalize_date(date_str: Any) -> str | None:
    """Normalise ISO and compact GDELT timestamps to UTC ISO strings."""

    if not date_str:
        return None

    try:
        value = str(date_str)
        if re.match(r"\d{8}T\d{6}Z", value):
            parsed = datetime.strptime(value, "%Y%m%dT%H%M%SZ")
            return parsed.replace(tzinfo=timezone.utc).isoformat()

        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def calculate_relevance(raw_doc: dict[str, Any], text: str) -> int:
    """Return a lightweight relevance score used to discard ads and empty text."""

    score = 0
    if any(keyword in text for keyword in COMPLAINT_KEYWORDS):
        score += 2
    if any(keyword in text for keyword in AD_KEYWORDS):
        score -= 3
    if len(text) < 10:
        score -= 2

    if str(raw_doc.get("platform") or "").casefold() in SOCIAL_PLATFORMS:
        engagement = (
            (raw_doc.get("like_count") or 0)
            + (raw_doc.get("reply_count") or 0)
            + (raw_doc.get("repost_count") or 0)
            + (raw_doc.get("quote_count") or 0)
        )
        if engagement == 0:
            score -= 1

    return score


def canonical_id_for_raw(raw: dict[str, Any], raw_id: str) -> str:
    """Return a stable cross-topic id used to reason about duplicate records."""

    platform = str(raw.get("platform") or "unknown").casefold()
    if platform == "gdelt":
        value = raw.get("url") or raw.get("gkg_url") or raw.get("source_id") or raw.get("source_es_id")
    else:
        value = raw.get("source_id") or raw.get("source_es_id") or raw_id
    digest = hashlib.sha1(f"{platform}|{value or raw_id}".encode("utf-8")).hexdigest()
    return f"canonical-{digest}"


def infer_topic(cleaned_text: str, harvest_category: str | None) -> tuple[str, str]:
    """Classify topic from text and fall back to the harvesting category."""

    classified = classify_cost_of_living_topic(cleaned_text)
    if classified != "cost_of_living":
        return classified, "text_keywords"
    if harvest_category:
        return harvest_category, "harvest_category_fallback"
    return classified, "default"


def quality_flags(raw: dict[str, Any], cleaned_text: str, relevance_score: int | None) -> list[str]:
    """Return lightweight quality flags that help explain downstream results."""

    flags: list[str] = []
    platform = str(raw.get("platform") or "").casefold()
    raw_text = str(raw.get("text") or "")
    lowered = raw_text.casefold()

    if platform == "gdelt":
        if lowered.count(";") >= 6 or "tax_" in lowered or "wb_" in lowered:
            flags.append("metadata_heavy")
        if raw.get("url") and not has_australia_context(raw_text):
            flags.append("weak_australia_context")
        if not raw.get("url"):
            flags.append("missing_url")

    if platform in SOCIAL_PLATFORMS:
        engagement = (
            (raw.get("like_count") or 0)
            + (raw.get("reply_count") or 0)
            + (raw.get("repost_count") or 0)
            + (raw.get("quote_count") or 0)
        )
        if engagement == 0:
            flags.append("zero_engagement")

    if relevance_score is not None and relevance_score < 0:
        flags.append("low_relevance")
    if len(cleaned_text) < 30:
        flags.append("short_text")

    return sorted(set(flags))


def get_sentiment_label(score: float) -> str:
    """Convert a VADER compound score into the project sentiment labels."""

    if score <= -0.05:
        return "negative"
    if score >= 0.05:
        return "positive"
    return "neutral"


def build_contract_doc(
    raw: dict[str, Any],
    raw_id: str,
    cleaned_text: str | None,
    relevance_score: int | None,
    sentiment_score: float | None,
    label: str | None,
    processing_status: str,
) -> dict[str, Any]:
    """Build one processed-post document from a raw input document."""

    harvest_category = raw.get("category")
    topic, topic_source = infer_topic(cleaned_text or "", harvest_category)
    processed_doc = {
        "raw_id": raw_id,
        "canonical_id": canonical_id_for_raw(raw, raw_id),
        "source_index": raw.get("source_index"),
        "source_es_id": raw.get("source_es_id"),
        "platform": raw.get("platform"),
        "source_group": source_group_for_platform(raw.get("platform")),
        "created_at": normalize_date(raw.get("created_at")),
        "harvested_at": normalize_date(raw.get("harvested_at") or raw.get("collected_at")),
        "processed_at": utc_now_iso(),
        "text": cleaned_text,
        "raw_text": raw.get("text"),
        "category": harvest_category,
        "harvest_category": harvest_category,
        "topic": topic,
        "topic_source": topic_source,
        "matched_keywords": matched_keywords(cleaned_text or ""),
        "sentiment_label": label,
        "sentiment_score": sentiment_score,
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "processor_version": MODEL_VERSION,
        "processing_status": processing_status,
        "like_count": raw.get("like_count"),
        "reply_count": raw.get("reply_count"),
        "repost_count": raw.get("repost_count"),
        "quote_count": raw.get("quote_count"),
        "url": raw.get("url"),
        "relevance_score": relevance_score,
        "quality_flags": quality_flags(raw, cleaned_text or "", relevance_score),
    }
    return processed_doc


def pending_query(stale_minutes: int | None = None) -> dict[str, Any]:
    """Return the query for raw documents that still need NLP processing."""

    stale_minutes = stale_minutes or settings.nlp_processing_stale_minutes
    return {
        "query": {
            "bool": {
                "should": [
                    {"term": {"analysis_status": "pending"}},
                    {"term": {"analysis_status.keyword": "pending"}},
                    {
                        "bool": {
                            "must_not": [
                                {"exists": {"field": "analysis_status"}},
                            ],
                        },
                    },
                    {
                        "bool": {
                            "filter": [
                                {"term": {"analysis_status": "processing"}},
                                {
                                    "range": {
                                        "analysis_started_at": {
                                            "lt": f"now-{stale_minutes}m",
                                        }
                                    }
                                },
                            ],
                        },
                    },
                ],
                "minimum_should_match": 1,
            },
        },
    }


def fetch_pending_docs(
    client: Any,
    raw_index: str,
    batch_size: int,
    max_docs: int | None = None,
) -> list[dict[str, Any]]:
    """Yield pending raw documents from Elasticsearch."""

    max_docs = max_docs or batch_size
    result = client.search(
        index=raw_index,
        body={
            **pending_query(),
            "size": max_docs,
            "sort": ["_doc"],
        },
        request_timeout=60,
    )
    return list(result.get("hits", {}).get("hits", []))


def claim_pending_docs(
    client: Any,
    raw_index: str,
    *,
    batch_size: int,
    max_docs: int,
) -> list[dict[str, Any]]:
    """Atomically mark a bounded set of pending raw documents as processing and return them."""

    hits = fetch_pending_docs(client, raw_index, batch_size=batch_size, max_docs=max_docs)
    if not hits:
        return []

    started_at = utc_now_iso()
    hit_by_id = {hit["_id"]: hit for hit in hits}
    actions = []
    for hit in hits:
        source = hit.get("_source", {})
        actions.append(
            {
                "_op_type": "update",
                "_index": raw_index,
                "_id": hit["_id"],
                "script": {
                    "source": """
                        boolean claimable = false;
                        if (params.expected_status == null) {
                            claimable = !ctx._source.containsKey('analysis_status')
                                || ctx._source.analysis_status == null;
                        } else if (ctx._source.analysis_status == params.expected_status) {
                            claimable = params.expected_started_at == null
                                || ctx._source.analysis_started_at == params.expected_started_at;
                        }
                        if (claimable) {
                            ctx._source.analysis_status = 'processing';
                            ctx._source.analysis_started_at = params.started_at;
                        } else {
                            ctx.op = 'noop';
                        }
                    """,
                    "params": {
                        "expected_status": source.get("analysis_status"),
                        "expected_started_at": source.get("analysis_started_at"),
                        "started_at": started_at,
                    },
                },
            }
        )
    claimed_hits = []
    for ok, item in helpers.streaming_bulk(client, actions, raise_on_error=False):
        update_result = item.get("update", {})
        if ok and update_result.get("result") != "noop" and update_result.get("_id") in hit_by_id:
            claimed_hits.append(hit_by_id[update_result["_id"]])
    if claimed_hits:
        client.indices.refresh(index=raw_index)
    return claimed_hits


def flush_bulk_actions(client: Any, actions: list[dict[str, Any]], refresh_index: str | None = None) -> int:
    """Write and clear bulk actions, returning the number of attempted actions."""

    if not actions:
        return 0
    attempted = len(actions)
    helpers.bulk(client, actions, raise_on_error=False)
    actions.clear()
    if refresh_index:
        client.indices.refresh(index=refresh_index)
    return attempted


def process_hit(hit: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    """Process one raw ES hit and return raw ES id, status and processed doc."""

    raw = hit["_source"]
    raw_es_id = hit["_id"]
    raw_id = raw.get("id") or raw_es_id

    try:
        cleaned = clean_text(raw.get("text", ""))
        relevance = calculate_relevance(raw, cleaned)
        if relevance < -2:
            return (
                raw_es_id,
                "discarded",
                build_contract_doc(
                    raw=raw,
                    raw_id=raw_id,
                    cleaned_text=cleaned,
                    relevance_score=relevance,
                    sentiment_score=None,
                    label=None,
                    processing_status="discarded",
                ),
            )

        score = round(float(sia.polarity_scores(cleaned)["compound"]), 4)
        return (
            raw_es_id,
            "processed",
            build_contract_doc(
                raw=raw,
                raw_id=raw_id,
                cleaned_text=cleaned,
                relevance_score=relevance,
                sentiment_score=score,
                label=get_sentiment_label(score),
                processing_status="processed",
            ),
        )
    except Exception:
        return (
            raw_es_id,
            "error",
            build_contract_doc(
                raw=raw,
                raw_id=raw_id,
                cleaned_text=None,
                relevance_score=None,
                sentiment_score=None,
                label=None,
                processing_status="error",
            ),
        )


def process_batch(
    *,
    client: Any | None = None,
    raw_index: str | None = None,
    processed_index: str | None = None,
    batch_size: int | None = None,
    max_docs: int | None = None,
    bulk_size: int | None = None,
) -> dict[str, int]:
    """Process pending raw documents and write one contract document per raw doc."""

    client = client or get_es_client()
    raw_index = raw_index or settings.raw_posts_index
    processed_index = processed_index or settings.processed_posts_write_index or settings.posts_index
    batch_size = batch_size or settings.nlp_batch_size
    max_docs = max_docs or settings.nlp_max_docs_per_run
    bulk_size = bulk_size or settings.nlp_bulk_size
    ensure_posts_index(client, index_name=processed_index)

    processed_actions = []
    raw_update_actions = []
    processed_count = 0
    discarded_count = 0
    error_count = 0
    total_written = 0
    raw_status_updated = 0

    claimed_hits = claim_pending_docs(
        client,
        raw_index,
        batch_size=batch_size,
        max_docs=max_docs,
    )

    for hit in claimed_hits:
        raw_es_id, raw_status, processed_doc = process_hit(hit)
        raw_id = processed_doc["raw_id"]

        if raw_status == "processed":
            processed_count += 1
        elif raw_status == "discarded":
            discarded_count += 1
        else:
            error_count += 1

        processed_actions.append(
            {
                "_op_type": "index",
                "_index": processed_index,
                "_id": raw_id,
                "_source": processed_doc,
            }
        )
        raw_update_actions.append(
            {
                "_op_type": "update",
                "_index": raw_index,
                "_id": raw_es_id,
                "doc": {
                    "analysis_status": raw_status,
                    "analysis_processed_at": utc_now_iso(),
                },
            }
        )

        if len(processed_actions) >= bulk_size:
            total_written += flush_bulk_actions(client, processed_actions)
        if len(raw_update_actions) >= bulk_size:
            raw_status_updated += flush_bulk_actions(client, raw_update_actions)

    total_written += flush_bulk_actions(client, processed_actions)
    raw_status_updated += flush_bulk_actions(client, raw_update_actions)
    if total_written:
        client.indices.refresh(index=processed_index)
    if raw_status_updated:
        client.indices.refresh(index=raw_index)

    return {
        "claimed": len(claimed_hits),
        "processed": processed_count,
        "discarded": discarded_count,
        "error": error_count,
        "total_written": total_written,
        "raw_status_updated": raw_status_updated,
    }


def main() -> dict[str, int]:
    """Fission entry point."""

    return process_batch()


def cli() -> None:
    parser = argparse.ArgumentParser(description="Process pending raw posts into processed posts.")
    parser.add_argument("--batch-size", type=int, default=settings.nlp_batch_size)
    parser.add_argument("--max-docs", type=int, default=settings.nlp_max_docs_per_run)
    parser.add_argument("--bulk-size", type=int, default=settings.nlp_bulk_size)
    args = parser.parse_args()
    print(
        process_batch(batch_size=args.batch_size, max_docs=args.max_docs, bulk_size=args.bulk_size),
        flush=True,
    )


if __name__ == "__main__":
    cli()
