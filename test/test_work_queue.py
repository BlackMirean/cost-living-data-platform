import json
from types import SimpleNamespace

from backend.common.work_queue import RedisWorkQueue, enqueue_nlp_work


class FakeRedis:
    def __init__(self):
        self.lists = {}

    def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)
        return len(self.lists[key])

    def brpop(self, key, timeout=0):
        values = self.lists.get(key, [])
        if not values:
            return None
        return key, values.pop()

    def llen(self, key):
        return len(self.lists.get(key, []))


def queue_settings():
    return SimpleNamespace(
        redis_enabled=True,
        redis_url="redis://redis:6379/0",
        redis_queue_prefix="test_pipeline",
        nlp_queue_name="nlp",
        nlp_queue_dead_letter_name="nlp:dead-letter",
        nlp_batch_size=100,
        nlp_queue_max_messages_per_sync=10,
    )


def test_enqueue_nlp_work_creates_batch_messages(monkeypatch):
    fake = FakeRedis()
    queue = RedisWorkQueue(settings_obj=queue_settings(), client=fake)
    monkeypatch.setattr("backend.common.work_queue.settings", queue_settings())

    result = enqueue_nlp_work(reason="test", document_count=250, queue=queue)

    assert result["queued_messages"] == 3
    assert result["depth"] == 3
    payload = json.loads(fake.lists["test_pipeline:queue:nlp"][0])
    assert payload["kind"] == "nlp_batch"
    assert payload["reason"] == "test"


def test_dequeue_returns_fifo_work_item():
    fake = FakeRedis()
    queue = RedisWorkQueue(settings_obj=queue_settings(), client=fake)
    queue.enqueue("nlp", {"kind": "nlp_batch", "number": 1})
    queue.enqueue("nlp", {"kind": "nlp_batch", "number": 2})

    first = queue.dequeue("nlp")
    second = queue.dequeue("nlp")

    assert first["number"] == 1
    assert second["number"] == 2


def test_dequeue_returns_poison_message_for_invalid_json():
    fake = FakeRedis()
    queue = RedisWorkQueue(settings_obj=queue_settings(), client=fake)
    fake.lpush(queue.queue_key("nlp"), "{not-json")

    payload = queue.dequeue("nlp")

    assert payload["kind"] == "poison_message"
    assert payload["attempts"] == 0
    assert "JSONDecodeError" in payload["error"]
    assert queue.available is True


def test_dead_letter_records_original_message():
    fake = FakeRedis()
    queue = RedisWorkQueue(settings_obj=queue_settings(), client=fake)

    ok = queue.dead_letter(
        {"message_id": "message-1", "kind": "nlp_batch", "attempts": 3},
        reason="max_attempts_exceeded",
        error="RuntimeError: boom",
    )

    assert ok is True
    payload = json.loads(fake.lists["test_pipeline:queue:nlp:dead-letter"][0])
    assert payload["kind"] == "dead_letter"
    assert payload["reason"] == "max_attempts_exceeded"
    assert payload["original_message_id"] == "message-1"
    assert payload["original_message"]["attempts"] == 3


def test_status_includes_dead_letter_depth():
    fake = FakeRedis()
    queue = RedisWorkQueue(settings_obj=queue_settings(), client=fake)
    queue.enqueue("nlp", {"kind": "nlp_batch"})
    queue.dead_letter({"message_id": "message-2", "kind": "poison_message"}, reason="poison_message")

    status = queue.status()

    assert status["depth"] == 1
    assert status["dead_letter_queue"] == "nlp:dead-letter"
    assert status["dead_letter_depth"] == 1
