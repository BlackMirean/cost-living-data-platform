"""Redis-backed API rate limiting with memory fallback."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from backend.common.config import settings


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    limit: int
    remaining: int
    reset_seconds: int
    backend: str


class ApiRateLimiter:
    """Fixed-window per-client rate limiter.

    Redis provides shared enforcement across API replicas. If Redis is not
    configured or fails, an in-process limiter keeps local development usable.
    """

    def __init__(self, settings_obj: Any = settings, client: Any | None = None) -> None:
        self.settings = settings_obj
        self.client = client if client is not None else self._build_client()
        self._redis_available = bool(self.redis_configured and self.client is not None)
        self._memory: dict[str, tuple[int, float]] = {}
        self._errors = 0

    @property
    def enabled(self) -> bool:
        return bool(getattr(self.settings, "api_rate_limit_enabled", False))

    @property
    def limit(self) -> int:
        return int(getattr(self.settings, "api_rate_limit_requests_per_minute", 600))

    @property
    def window_seconds(self) -> int:
        return int(getattr(self.settings, "api_rate_limit_window_seconds", 60))

    @property
    def redis_configured(self) -> bool:
        return bool(getattr(self.settings, "redis_enabled", False))

    @property
    def redis_available(self) -> bool:
        return bool(self.redis_configured and self.client is not None and self._redis_available)

    @property
    def key_prefix(self) -> str:
        prefix = getattr(self.settings, "redis_queue_prefix", "cost_living_pipeline")
        return f"{prefix}:rate_limit"

    def _build_client(self) -> Any | None:
        if not self.redis_configured:
            return None
        try:
            import redis

            return redis.Redis.from_url(getattr(self.settings, "redis_url"), decode_responses=True)
        except Exception as exc:
            logger.warning("api_rate_limiter_redis_unavailable error=%s", exc)
            return None

    def _key(self, client_id: str, route_key: str, now: float) -> str:
        window = int(now // self.window_seconds)
        safe_client = client_id.replace(":", "_").replace("/", "_")
        safe_route = route_key.replace(":", "_").replace("/", "_")
        return f"{self.key_prefix}:{safe_client}:{safe_route}:{window}"

    def _memory_check(self, key: str, now: float) -> RateLimitDecision:
        count, reset_at = self._memory.get(key, (0, now + self.window_seconds))
        if now >= reset_at:
            count = 0
            reset_at = now + self.window_seconds
        count += 1
        self._memory[key] = (count, reset_at)
        remaining = max(0, self.limit - count)
        return RateLimitDecision(
            allowed=count <= self.limit,
            limit=self.limit,
            remaining=remaining,
            reset_seconds=max(1, int(reset_at - now)),
            backend="memory",
        )

    def _redis_check(self, key: str) -> RateLimitDecision | None:
        if not self.redis_available:
            return None
        try:
            count = int(self.client.incr(key))
            if count == 1:
                self.client.expire(key, self.window_seconds)
            ttl = int(self.client.ttl(key))
            reset_seconds = ttl if ttl > 0 else self.window_seconds
            return RateLimitDecision(
                allowed=count <= self.limit,
                limit=self.limit,
                remaining=max(0, self.limit - count),
                reset_seconds=reset_seconds,
                backend="redis",
            )
        except Exception as exc:
            self._redis_available = False
            self._errors += 1
            logger.warning("api_rate_limiter_redis_failed key=%s error=%s", key, exc)
            return None

    def check(self, *, client_id: str, route_key: str) -> RateLimitDecision:
        if not self.enabled or self.limit <= 0:
            return RateLimitDecision(
                allowed=True,
                limit=max(0, self.limit),
                remaining=max(0, self.limit),
                reset_seconds=self.window_seconds,
                backend="disabled",
            )
        now = time.time()
        key = self._key(client_id, route_key, now)
        return self._redis_check(key) or self._memory_check(key, now)

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "limit": self.limit,
            "window_seconds": self.window_seconds,
            "backend": "redis" if self.redis_available else "memory",
            "redis_configured": self.redis_configured,
            "redis_available": self.redis_available,
            "memory_keys": len(self._memory),
            "errors": self._errors,
        }
