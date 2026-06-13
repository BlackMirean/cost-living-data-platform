"""Helpers for storing unprocessed harvested records."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


UTC = timezone.utc


def stable_raw_id(platform: str, *parts: object) -> str:
    raw = "|".join(str(part or "") for part in (platform, *parts))
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return f"{platform}-{digest}"


def raw_post_document(
    *,
    platform: str,
    source: str,
    source_id: str,
    text: str,
    query: str,
    created_at: str,
    author: str = "unknown",
    location_hint: str = "",
    url: str = "",
    engagement: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
    raw_id: str | None = None,
) -> dict[str, Any]:
    """Return a durable raw document before scenario filtering or NLP."""

    doc_id = raw_id or stable_raw_id(platform, source_id, url, created_at, text)
    engagement = engagement or {}
    like_count = engagement.get("like_count")
    reply_count = engagement.get("reply_count", engagement.get("replies_count"))
    repost_count = engagement.get("repost_count", engagement.get("reblogs_count"))
    quote_count = engagement.get("quote_count")
    return {
        "id": doc_id,
        "platform": platform,
        "stage": "raw",
        "source": source,
        "source_id": source_id,
        "author": author or "unknown",
        "location_hint": location_hint,
        "text": text or "",
        "harvested_query": query,
        "url": url or "",
        "created_at": created_at,
        "harvested_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "like_count": like_count,
        "reply_count": reply_count,
        "repost_count": repost_count,
        "quote_count": quote_count,
        "has_engagement_metrics": bool(engagement),
        "analysis_status": "pending",
        "engagement": engagement,
        "payload": payload or {},
    }
