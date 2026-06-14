"""Short-lived API response cache with optional Redis sharing."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable
from typing import Any

from backend.common.config import settings
from backend.common.observability import record_cache_lookup


logger = logging.getLogger(__name__)


class ApiResponseCache:
    """Cache analytics responses in Redis when available, with memory fallback."""

    def __init__(self, settings_obj: Any = settings, client: Any | None = None) -> None:
        self.settings = settings_obj
        self.client = client if client is not None else self._build_client()
        self._redis_available = bool(self.redis_configured and self.client is not None)
        self._memory: dict[str, tuple[float, dict[str, Any]]] = {}
        self._hits = 0
        self._misses = 0
        self._sets = 0
        self._errors = 0

    @property
    def ttl_seconds(self) -> float:
        return float(getattr(self.settings, "api_cache_ttl_seconds", 0))

    @property
    def redis_configured(self) -> bool:
        return bool(getattr(self.settings, "redis_enabled", False))

    @property
    def redis_available(self) -> bool:
        return bool(self.redis_configured and self.client is not None and self._redis_available)

    @property
    def key_prefix(self) -> str:
        prefix = getattr(self.settings, "redis_queue_prefix", "cost_living_pipeline")
        return f"{prefix}:api_cache"

    def _build_client(self) -> Any | None:
        if not self.redis_configured:
            return None
        try:
            import redis

            return redis.Redis.from_url(getattr(self.settings, "redis_url"), decode_responses=True)
        except Exception as exc:
            logger.warning("api_cache_redis_unavailable error=%s", exc)
            return None

    def _cache_key(self, key: tuple[Any, ...]) -> str:
        encoded = json.dumps(key, sort_keys=True, default=str, separators=(",", ":"))
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        return f"{self.key_prefix}:{digest}"

    def _memory_get(self, cache_key: str, now: float) -> dict[str, Any] | None:
        cached = self._memory.get(cache_key)
        if not cached:
            return None
        created_at, value = cached
        if now - created_at >= self.ttl_seconds:
            self._memory.pop(cache_key, None)
            return None
        return value

    def _redis_get(self, cache_key: str) -> dict[str, Any] | None:
        if not self.redis_available:
            return None
        try:
            raw_value = self.client.get(cache_key)
            if raw_value is None:
                return None
            if isinstance(raw_value, bytes):
                raw_value = raw_value.decode("utf-8")
            value = json.loads(raw_value)
            return value if isinstance(value, dict) else None
        except Exception as exc:
            self._redis_available = False
            self._errors += 1
            logger.warning("api_cache_redis_get_failed key=%s error=%s", cache_key, exc)
            return None

    def _redis_set(self, cache_key: str, value: dict[str, Any]) -> None:
        if not self.redis_available:
            return
        try:
            payload = json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))
            self.client.setex(cache_key, max(1, int(self.ttl_seconds)), payload)
        except Exception as exc:
            self._redis_available = False
            self._errors += 1
            logger.warning("api_cache_redis_set_failed key=%s error=%s", cache_key, exc)

    def get_or_set(self, key: tuple[Any, ...], producer: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        if self.ttl_seconds <= 0:
            self._misses += 1
            return producer()

        cache_key = self._cache_key(key)
        now = time.monotonic()

        value = self._redis_get(cache_key)
        if value is not None:
            self._hits += 1
            record_cache_lookup("hit", "redis")
            return value

        value = self._memory_get(cache_key, now)
        if value is not None:
            self._hits += 1
            record_cache_lookup("hit", "memory")
            return value

        self._misses += 1
        value = producer()
        self._memory[cache_key] = (now, value)
        self._redis_set(cache_key, value)
        self._sets += 1
        record_cache_lookup("miss", "redis" if self.redis_available else "memory")
        return value

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.ttl_seconds > 0,
            "backend": "redis" if self.redis_available else "memory",
            "redis_configured": self.redis_configured,
            "redis_available": self.redis_available,
            "ttl_seconds": self.ttl_seconds,
            "memory_entries": len(self._memory),
            "hits": self._hits,
            "misses": self._misses,
            "sets": self._sets,
            "errors": self._errors,
        }
