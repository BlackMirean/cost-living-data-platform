"""Optional Redis-backed runtime queue and job lock helpers."""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from backend.common.config import settings


logger = logging.getLogger(__name__)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def event_summary(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": event.get("event_id"),
        "run_id": event.get("run_id"),
        "job_name": event.get("job_name"),
        "status": event.get("status"),
        "emitted_at": event.get("emitted_at"),
        "duration_ms": event.get("duration_ms"),
        "error": event.get("error"),
    }


def log_job_event(event: dict[str, Any]) -> None:
    logger.info(
        json.dumps(
            {
                "event": "pipeline_job",
                "event_id": event.get("event_id"),
                "run_id": event.get("run_id"),
                "job_name": event.get("job_name"),
                "status": event.get("status"),
                "duration_ms": event.get("duration_ms"),
                "error": event.get("error"),
            },
            sort_keys=True,
            default=str,
        )
    )


@dataclass
class RuntimeLock:
    """A Redis lock acquired for one pipeline job invocation."""

    runtime: "RedisRuntime"
    key: str
    token: str
    acquired: bool
    enabled: bool

    def release(self) -> None:
        if not self.enabled or not self.acquired:
            return
        self.runtime.release_lock(self)


class RedisRuntime:
    """Small Redis adapter used by scheduled jobs and runtime diagnostics."""

    def __init__(self, client: Any | None = None, settings_obj: Any = settings) -> None:
        self.settings = settings_obj
        self.client = client if client is not None else self._build_client()
        self._available = bool(self.settings.redis_enabled and self.client is not None)

    @property
    def enabled(self) -> bool:
        return bool(self.settings.redis_enabled and self.client is not None and self._available)

    @property
    def queue_key(self) -> str:
        return f"{self.settings.redis_queue_prefix}:events"

    def _build_client(self) -> Any | None:
        if not self.settings.redis_enabled:
            return None
        try:
            import redis

            return redis.Redis.from_url(self.settings.redis_url, decode_responses=True)
        except Exception as exc:
            logger.warning("redis_runtime_unavailable error=%s", exc)
            return None

    def lock_key(self, job_name: str) -> str:
        return f"{self.settings.redis_queue_prefix}:lock:{job_name}"

    def acquire_lock(self, job_name: str, ttl_seconds: int | None = None) -> RuntimeLock:
        key = self.lock_key(job_name)
        token = str(uuid.uuid4())
        if not self.enabled:
            return RuntimeLock(self, key, token, acquired=True, enabled=False)
        try:
            acquired = bool(
                self.client.set(
                    key,
                    token,
                    nx=True,
                    ex=int(ttl_seconds or self.settings.redis_lock_ttl_seconds),
                )
            )
            return RuntimeLock(self, key, token, acquired=acquired, enabled=True)
        except Exception as exc:
            self._available = False
            logger.warning("redis_lock_fail_open job=%s error=%s", job_name, exc)
            return RuntimeLock(self, key, token, acquired=True, enabled=False)

    def release_lock(self, lock: RuntimeLock) -> None:
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        end
        return 0
        """
        try:
            self.client.eval(script, 1, lock.key, lock.token)
        except Exception as exc:
            self._available = False
            logger.warning("redis_lock_release_failed key=%s error=%s", lock.key, exc)

    def emit_event(
        self,
        job_name: str,
        status: str,
        payload: dict[str, Any] | None = None,
        *,
        run_id: str | None = None,
        error: str | None = None,
        duration_ms: float | None = None,
    ) -> dict[str, Any]:
        event = {
            "event_id": str(uuid.uuid4()),
            "run_id": run_id,
            "job_name": job_name,
            "status": status,
            "emitted_at": utc_now(),
            "duration_ms": duration_ms,
            "payload": payload or {},
            "error": error,
        }
        if not self.enabled:
            return event
        try:
            self.client.rpush(self.queue_key, json.dumps(event, sort_keys=True, default=str))
            self.client.ltrim(self.queue_key, -int(self.settings.redis_event_queue_max_length), -1)
            self.client.expire(self.queue_key, int(self.settings.redis_event_ttl_seconds))
        except Exception as exc:
            self._available = False
            logger.warning("redis_event_emit_failed job=%s status=%s error=%s", job_name, status, exc)
        return event

    def recent_events(self, limit: int = 50) -> dict[str, Any]:
        capped_limit = max(1, min(int(limit), 500))
        result: dict[str, Any] = {
            "configured": bool(self.settings.redis_enabled),
            "enabled": self.enabled,
            "queue_key": self.queue_key,
            "limit": capped_limit,
            "events": [],
        }
        if not self.enabled:
            return result
        try:
            raw_events = self.client.lrange(self.queue_key, -capped_limit, -1)
            result["events"] = [event_summary(json.loads(item)) for item in raw_events]
            result["count"] = len(result["events"])
            result["available"] = True
        except Exception as exc:
            self._available = False
            result["available"] = False
            result["recent_event_error"] = str(exc)
            result["enabled"] = self.enabled
        return result

    def status(self) -> dict[str, Any]:
        result = {
            "configured": bool(self.settings.redis_enabled),
            "enabled": self.enabled,
            "available": False,
            "url_configured": bool(self.settings.redis_url),
            "queue_key": self.queue_key,
            "lock_prefix": f"{self.settings.redis_queue_prefix}:lock:",
            "event_ttl_seconds": self.settings.redis_event_ttl_seconds,
            "event_queue_max_length": self.settings.redis_event_queue_max_length,
        }
        if self.enabled:
            try:
                result["recent_event_count"] = int(self.client.llen(self.queue_key))
                result["available"] = True
            except Exception as exc:
                self._available = False
                result["recent_event_error"] = str(exc)
        result["enabled"] = self.enabled
        return result


def runtime_queue_status() -> dict[str, Any]:
    return RedisRuntime().status()


def runtime_queue_events(limit: int = 50) -> dict[str, Any]:
    return RedisRuntime().recent_events(limit=limit)


def run_pipeline_job(
    job_name: str,
    job: Callable[[], dict[str, Any]],
    *,
    runtime: RedisRuntime | None = None,
    lock_ttl_seconds: int | None = None,
) -> dict[str, Any]:
    """Run a scheduled job with optional Redis event logging and overlap protection."""

    runtime = runtime or RedisRuntime()
    run_id = str(uuid.uuid4())
    lock = runtime.acquire_lock(job_name, ttl_seconds=lock_ttl_seconds)
    if not lock.acquired:
        event = runtime.emit_event(job_name, "skipped", {"reason": "lock_held"}, run_id=run_id)
        log_job_event(event)
        return {
            "source": job_name,
            "skipped": True,
            "reason": "lock_held",
            "runtime_queue": {"enabled": runtime.enabled, "event": event_summary(event)},
        }

    started = time.perf_counter()
    started_event = runtime.emit_event(job_name, "started", run_id=run_id)
    log_job_event(started_event)
    try:
        result = job()
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000
        failed_event = runtime.emit_event(
            job_name,
            "failed",
            run_id=run_id,
            error=f"{type(exc).__name__}: {exc}",
            duration_ms=elapsed_ms,
        )
        log_job_event(failed_event)
        raise
    finally:
        lock.release()

    elapsed_ms = (time.perf_counter() - started) * 1000
    event = runtime.emit_event(job_name, "succeeded", {"result": result}, run_id=run_id, duration_ms=elapsed_ms)
    log_job_event(event)
    if runtime.enabled and isinstance(result, dict):
        result.setdefault("runtime_queue", {"enabled": True, "event": event_summary(event)})
    return result
