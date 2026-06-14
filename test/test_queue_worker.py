from types import SimpleNamespace

from backend.processing import queue_worker


class FakeRuntime:
    enabled = True

    def __init__(self):
        self.events = []

    def emit_event(self, job_name, status, payload=None, **kwargs):
        event = {
            "event_id": f"event-{len(self.events)}",
            "run_id": kwargs.get("run_id"),
            "job_name": job_name,
            "status": status,
            "payload": payload or {},
            "duration_ms": kwargs.get("duration_ms"),
            "error": kwargs.get("error"),
        }
        self.events.append(event)
        return event


class FakeQueue:
    def __init__(self):
        self.enqueued = []
        self.dead_letters = []

    def enqueue(self, queue_name, payload):
        self.enqueued.append((queue_name, payload))
        return True

    def dead_letter(self, message, *, reason, error=None, queue_name=None):
        self.dead_letters.append(
            {
                "message": message,
                "reason": reason,
                "error": error,
                "queue_name": queue_name,
            }
        )
        return True


def test_queue_worker_records_events_and_requeues_backlog(monkeypatch):
    monkeypatch.setattr(
        queue_worker,
        "settings",
        SimpleNamespace(nlp_batch_size=10, nlp_queue_name="nlp", nlp_queue_max_attempts=3),
    )
    monkeypatch.setattr(
        queue_worker,
        "process_batch",
        lambda: {"claimed": 10, "processed": 10, "discarded": 0, "error": 0},
    )
    runtime = FakeRuntime()
    queue = FakeQueue()

    result = queue_worker.process_message(
        {"message_id": "message-1", "kind": "nlp_batch"},
        runtime=runtime,
        queue=queue,
    )

    assert result["processed"] == 10
    assert [event["status"] for event in runtime.events] == ["started", "succeeded"]
    assert queue.enqueued[0][0] == "nlp"
    assert queue.enqueued[0][1]["reason"] == "continue_backlog"


def test_queue_worker_requeues_failed_message_with_retry_budget(monkeypatch):
    monkeypatch.setattr(
        queue_worker,
        "settings",
        SimpleNamespace(nlp_batch_size=10, nlp_queue_name="nlp", nlp_queue_max_attempts=3),
    )

    def fail_batch():
        raise RuntimeError("temporary elasticsearch outage")

    monkeypatch.setattr(queue_worker, "process_batch", fail_batch)
    runtime = FakeRuntime()
    queue = FakeQueue()

    result = queue_worker.process_message(
        {"message_id": "message-2", "kind": "nlp_batch"},
        runtime=runtime,
        queue=queue,
    )

    assert result["status"] == "retry_scheduled"
    assert result["attempts"] == 1
    assert queue.enqueued[0][0] == "nlp"
    assert queue.enqueued[0][1]["attempts"] == 1
    assert "temporary elasticsearch outage" in queue.enqueued[0][1]["last_error"]
    assert [event["status"] for event in runtime.events] == ["started", "retry_scheduled"]


def test_queue_worker_dead_letters_after_retry_budget(monkeypatch):
    monkeypatch.setattr(
        queue_worker,
        "settings",
        SimpleNamespace(
            nlp_batch_size=10,
            nlp_queue_name="nlp",
            nlp_queue_max_attempts=3,
            nlp_queue_dead_letter_name="nlp:dead-letter",
        ),
    )

    def fail_batch():
        raise RuntimeError("persistent mapping error")

    monkeypatch.setattr(queue_worker, "process_batch", fail_batch)
    runtime = FakeRuntime()
    queue = FakeQueue()

    result = queue_worker.process_message(
        {"message_id": "message-3", "kind": "nlp_batch", "attempts": 2},
        runtime=runtime,
        queue=queue,
    )

    assert result["status"] == "dead_lettered"
    assert result["reason"] == "max_attempts_exceeded"
    assert result["attempts"] == 3
    assert queue.enqueued == []
    assert queue.dead_letters[0]["message"]["attempts"] == 3
    assert queue.dead_letters[0]["reason"] == "max_attempts_exceeded"
    assert "persistent mapping error" in queue.dead_letters[0]["error"]
    assert [event["status"] for event in runtime.events] == ["started", "dead_lettered"]


def test_queue_worker_dead_letters_poison_message_without_processing(monkeypatch):
    monkeypatch.setattr(
        queue_worker,
        "settings",
        SimpleNamespace(
            nlp_batch_size=10,
            nlp_queue_name="nlp",
            nlp_queue_max_attempts=3,
            nlp_queue_dead_letter_name="nlp:dead-letter",
        ),
    )
    monkeypatch.setattr(
        queue_worker,
        "process_batch",
        lambda: (_ for _ in ()).throw(AssertionError("process_batch should not run")),
    )
    runtime = FakeRuntime()
    queue = FakeQueue()

    result = queue_worker.process_message(
        {"message_id": "message-4", "kind": "unexpected"},
        runtime=runtime,
        queue=queue,
    )

    assert result["status"] == "dead_lettered"
    assert result["reason"] == "poison_message"
    assert queue.dead_letters[0]["reason"] == "poison_message"
    assert "unsupported_message_kind" in queue.dead_letters[0]["error"]
