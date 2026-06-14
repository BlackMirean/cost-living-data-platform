"""KEDA-scaled NLP worker that consumes Redis work items."""

from __future__ import annotations

import argparse
import json
import logging
import time
import uuid
from typing import Any

from backend.common.config import settings
from backend.common.runtime_queue import RedisRuntime, event_summary, log_job_event
from backend.common.work_queue import RedisWorkQueue
from backend.processing.nlp_worker import process_batch


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


JOB_NAME = "cost-living-platform-nlp-worker"


def _attempts(message: dict[str, Any]) -> int:
    try:
        return max(0, int(message.get("attempts") or 0))
    except (TypeError, ValueError):
        return 0


def _max_attempts() -> int:
    return max(1, int(getattr(settings, "nlp_queue_max_attempts", 3)))


def _message_error(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def _dead_letter(
    message: dict[str, Any],
    *,
    queue: RedisWorkQueue,
    runtime: RedisRuntime,
    run_id: str,
    reason: str,
    started: float,
    error: str | None = None,
) -> dict[str, Any]:
    ok = queue.dead_letter(message, reason=reason, error=error)
    elapsed_ms = (time.perf_counter() - started) * 1000
    event = runtime.emit_event(
        JOB_NAME,
        "dead_lettered",
        {
            "message_id": message.get("message_id"),
            "reason": reason,
            "dead_lettered": ok,
            "attempts": _attempts(message),
            "dead_letter_queue": getattr(settings, "nlp_queue_dead_letter_name", "nlp:dead-letter"),
        },
        run_id=run_id,
        error=error,
        duration_ms=elapsed_ms,
    )
    log_job_event(event)
    return {
        "source": JOB_NAME,
        "message_id": message.get("message_id"),
        "status": "dead_lettered",
        "reason": reason,
        "attempts": _attempts(message),
        "dead_lettered": ok,
        "runtime_queue": {"enabled": runtime.enabled, "event": event_summary(event)},
    }


def _retry_or_dead_letter(
    message: dict[str, Any],
    *,
    queue: RedisWorkQueue,
    runtime: RedisRuntime,
    run_id: str,
    error: str,
    started: float,
) -> dict[str, Any]:
    attempts = _attempts(message) + 1
    retry_message = {
        **message,
        "attempts": attempts,
        "last_error": error,
        "last_failed_at": time.time(),
    }
    if attempts >= _max_attempts():
        return _dead_letter(
            retry_message,
            queue=queue,
            runtime=runtime,
            run_id=run_id,
            reason="max_attempts_exceeded",
            error=error,
            started=started,
        )

    ok = queue.enqueue(settings.nlp_queue_name, retry_message)
    elapsed_ms = (time.perf_counter() - started) * 1000
    event = runtime.emit_event(
        JOB_NAME,
        "retry_scheduled",
        {
            "message_id": message.get("message_id"),
            "attempts": attempts,
            "max_attempts": _max_attempts(),
            "requeued": ok,
        },
        run_id=run_id,
        error=error,
        duration_ms=elapsed_ms,
    )
    log_job_event(event)
    return {
        "source": JOB_NAME,
        "message_id": message.get("message_id"),
        "status": "retry_scheduled",
        "attempts": attempts,
        "max_attempts": _max_attempts(),
        "requeued": ok,
        "runtime_queue": {"enabled": runtime.enabled, "event": event_summary(event)},
    }


def _validate_message(message: dict[str, Any]) -> str | None:
    kind = message.get("kind")
    if kind == "nlp_batch":
        return None
    if kind == "poison_message":
        return str(message.get("error") or "poison_message")
    return f"unsupported_message_kind:{kind!r}"


def process_message(
    message: dict[str, Any],
    *,
    runtime: RedisRuntime | None = None,
    queue: RedisWorkQueue | None = None,
) -> dict[str, Any]:
    runtime = runtime or RedisRuntime()
    queue = queue or RedisWorkQueue()
    run_id = str(uuid.uuid4())
    started = time.perf_counter()
    started_event = runtime.emit_event(JOB_NAME, "started", {"message": message}, run_id=run_id)
    log_job_event(started_event)
    validation_error = _validate_message(message)
    if validation_error:
        return _dead_letter(
            message,
            queue=queue,
            runtime=runtime,
            run_id=run_id,
            reason="poison_message",
            error=validation_error,
            started=started,
        )

    try:
        result = process_batch()
    except Exception as exc:
        return _retry_or_dead_letter(
            message,
            queue=queue,
            runtime=runtime,
            run_id=run_id,
            error=_message_error(exc),
            started=started,
        )

    if result.get("claimed", 0) >= settings.nlp_batch_size:
        queue.enqueue(
            settings.nlp_queue_name,
            {
                "kind": "nlp_batch",
                "reason": "continue_backlog",
                "batch_size": settings.nlp_batch_size,
            },
        )

    elapsed_ms = (time.perf_counter() - started) * 1000
    succeeded_event = runtime.emit_event(
        JOB_NAME,
        "succeeded",
        {"message_id": message.get("message_id"), "result": result},
        run_id=run_id,
        duration_ms=elapsed_ms,
    )
    log_job_event(succeeded_event)
    return {
        "source": JOB_NAME,
        "message_id": message.get("message_id"),
        "status": "succeeded",
        "attempts": _attempts(message),
        **result,
        "runtime_queue": {"enabled": runtime.enabled, "event": event_summary(succeeded_event)},
    }


def run_worker(*, once: bool = False, max_messages: int = 0) -> dict[str, Any]:
    queue = RedisWorkQueue()
    queue_name = settings.nlp_queue_name
    timeout = int(settings.nlp_queue_worker_block_timeout_seconds)
    idle_exit = float(settings.nlp_queue_worker_idle_exit_seconds)
    processed = 0
    empty_since = time.monotonic()
    last_result: dict[str, Any] | None = None

    while True:
        message = queue.dequeue(queue_name, timeout_seconds=timeout)
        if message is None:
            if once or time.monotonic() - empty_since >= idle_exit:
                break
            continue
        empty_since = time.monotonic()
        last_result = process_message(message, queue=queue)
        processed += 1
        logger.info(json.dumps({"event": "nlp_queue_message_processed", **last_result}, default=str))
        if once or (max_messages and processed >= max_messages):
            break

    return {
        "source": JOB_NAME,
        "queue": queue_name,
        "queue_key": queue.queue_key(queue_name),
        "processed_messages": processed,
        "remaining_depth": queue.depth(queue_name),
        "last_result": last_result,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume Redis NLP queue messages.")
    parser.add_argument("--once", action="store_true", help="Process at most one queue message.")
    parser.add_argument("--max-messages", type=int, default=settings.nlp_queue_worker_max_messages)
    args = parser.parse_args()
    print(json.dumps(run_worker(once=args.once, max_messages=args.max_messages), default=str), flush=True)


if __name__ == "__main__":
    main()
