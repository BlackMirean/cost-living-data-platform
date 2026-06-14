"""Redis-backed work queues for asynchronous pipeline workers."""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

from backend.common.config import settings
from backend.common.observability import set_queue_depth


logger = logging.getLogger(__name__)


class RedisWorkQueue:
    """Small Redis list wrapper used by KEDA-scaled workers."""

    def __init__(self, settings_obj: Any = settings, client: Any | None = None) -> None:
        self.settings = settings_obj
        self.client = client if client is not None else self._build_client()
        self._available = bool(self.enabled and self.client is not None)

    @property
    def enabled(self) -> bool:
        return bool(getattr(self.settings, "redis_enabled", False))

    @property
    def available(self) -> bool:
        return bool(self.enabled and self.client is not None and self._available)

    @property
    def prefix(self) -> str:
        return getattr(self.settings, "redis_queue_prefix", "cost_living_pipeline")

    def queue_key(self, queue_name: str | None = None) -> str:
        name = queue_name or getattr(self.settings, "nlp_queue_name", "nlp")
        return f"{self.prefix}:queue:{name}"

    def _build_client(self) -> Any | None:
        if not self.enabled:
            return None
        try:
            import redis

            return redis.Redis.from_url(getattr(self.settings, "redis_url"), decode_responses=True)
        except Exception as exc:
            logger.warning("redis_work_queue_unavailable error=%s", exc)
            return None

    def enqueue(self, queue_name: str, payload: dict[str, Any]) -> bool:
        if not self.available:
            return False
        payload = {
            "message_id": str(uuid.uuid4()),
            "queued_at": time.time(),
            **payload,
        }
        key = self.queue_key(queue_name)
        try:
            self.client.lpush(key, json.dumps(payload, sort_keys=True, default=str))
            set_queue_depth(key, self.depth(queue_name))
            return True
        except Exception as exc:
            self._available = False
            logger.warning("redis_work_enqueue_failed queue=%s error=%s", key, exc)
            return False

    def dequeue(self, queue_name: str, timeout_seconds: int = 15) -> dict[str, Any] | None:
        if not self.available:
            return None
        key = self.queue_key(queue_name)
        try:
            item = self.client.brpop(key, timeout=timeout_seconds)
            set_queue_depth(key, self.depth(queue_name))
            if not item:
                return None
            _queue, raw_payload = item
            if isinstance(raw_payload, bytes):
                raw_payload = raw_payload.decode("utf-8")
            try:
                payload = json.loads(raw_payload)
            except json.JSONDecodeError as exc:
                logger.warning("redis_work_poison_payload queue=%s error=%s", key, exc)
                return self.poison_message(raw_payload, f"JSONDecodeError: {exc}")
            if isinstance(payload, dict):
                return payload
            logger.warning("redis_work_poison_payload queue=%s error=payload_not_object", key)
            return self.poison_message(raw_payload, "payload_not_object")
        except Exception as exc:
            self._available = False
            logger.warning("redis_work_dequeue_failed queue=%s error=%s", key, exc)
            return None

    def depth(self, queue_name: str | None = None) -> int:
        if not self.available:
            return 0
        key = self.queue_key(queue_name)
        try:
            depth = int(self.client.llen(key))
            set_queue_depth(key, depth)
            return depth
        except Exception as exc:
            self._available = False
            logger.warning("redis_work_depth_failed queue=%s error=%s", key, exc)
            return 0

    def poison_message(self, raw_payload: Any, error: str) -> dict[str, Any]:
        return {
            "kind": "poison_message",
            "message_id": str(uuid.uuid4()),
            "attempts": 0,
            "error": error,
            "raw_payload": str(raw_payload)[:2000],
        }

    def dead_letter(
        self,
        message: dict[str, Any],
        *,
        reason: str,
        error: str | None = None,
        queue_name: str | None = None,
    ) -> bool:
        dead_letter_queue = queue_name or getattr(self.settings, "nlp_queue_dead_letter_name", "nlp:dead-letter")
        payload = {
            "kind": "dead_letter",
            "reason": reason,
            "error": error,
            "dead_lettered_at": time.time(),
            "original_message_id": message.get("message_id"),
            "original_kind": message.get("kind"),
            "attempts": int(message.get("attempts") or 0),
            "original_message": message,
        }
        return self.enqueue(dead_letter_queue, payload)

    def status(self) -> dict[str, Any]:
        queue_name = getattr(self.settings, "nlp_queue_name", "nlp")
        dead_letter_queue = getattr(self.settings, "nlp_queue_dead_letter_name", "nlp:dead-letter")
        return {
            "configured": self.enabled,
            "available": self.available,
            "queue": queue_name,
            "queue_key": self.queue_key(queue_name),
            "depth": self.depth(queue_name),
            "dead_letter_queue": dead_letter_queue,
            "dead_letter_queue_key": self.queue_key(dead_letter_queue),
            "dead_letter_depth": self.depth(dead_letter_queue),
        }


def enqueue_nlp_work(*, reason: str, document_count: int, queue: RedisWorkQueue | None = None) -> dict[str, Any]:
    """Queue enough NLP work items to cover newly pending raw documents."""

    queue = queue or RedisWorkQueue()
    batch_size = max(1, int(getattr(settings, "nlp_batch_size", 250)))
    max_messages = max(1, int(getattr(settings, "nlp_queue_max_messages_per_sync", 50)))
    requested = 0
    queued = 0
    if document_count > 0:
        requested = min(max_messages, (int(document_count) + batch_size - 1) // batch_size)
    for _ in range(requested):
        ok = queue.enqueue(
            getattr(settings, "nlp_queue_name", "nlp"),
            {
                "kind": "nlp_batch",
                "reason": reason,
                "document_count": int(document_count),
                "batch_size": batch_size,
            },
        )
        if ok:
            queued += 1
    return {
        "enabled": queue.enabled,
        "available": queue.available,
        "queue_key": queue.queue_key(getattr(settings, "nlp_queue_name", "nlp")),
        "document_count": int(document_count),
        "requested_messages": requested,
        "queued_messages": queued,
        "depth": queue.depth(getattr(settings, "nlp_queue_name", "nlp")),
    }


def work_queue_status() -> dict[str, Any]:
    return RedisWorkQueue().status()
