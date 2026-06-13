import json
from types import SimpleNamespace

import pytest

from backend.common.runtime_queue import RedisRuntime, run_pipeline_job


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.lists = {}
        self.ttl = {}

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.values:
            return False
        self.values[key] = value
        if ex:
            self.ttl[key] = ex
        return True

    def eval(self, _script, _count, key, token):
        if self.values.get(key) == token:
            del self.values[key]
            return 1
        return 0

    def rpush(self, key, value):
        self.lists.setdefault(key, []).append(value)
        return len(self.lists[key])

    def ltrim(self, key, start, end):
        values = self.lists.get(key, [])
        self.lists[key] = values[start : end + 1 if end != -1 else None]
        return True

    def expire(self, key, ttl):
        self.ttl[key] = ttl
        return True

    def llen(self, key):
        return len(self.lists.get(key, []))


def queue_settings(enabled=True):
    return SimpleNamespace(
        redis_enabled=enabled,
        redis_url="redis://redis:6379/0",
        redis_queue_prefix="test_pipeline",
        redis_lock_ttl_seconds=60,
        redis_event_ttl_seconds=3600,
        redis_event_queue_max_length=10,
    )


def test_pipeline_job_emits_events_and_releases_lock():
    client = FakeRedis()
    runtime = RedisRuntime(client=client, settings_obj=queue_settings())

    result = run_pipeline_job("job-a", lambda: {"ok": True}, runtime=runtime)

    assert result["ok"] is True
    assert result["runtime_queue"]["enabled"] is True
    assert result["runtime_queue"]["event"]["status"] == "succeeded"
    json.dumps(result)
    assert client.values == {}
    events = [json.loads(item) for item in client.lists["test_pipeline:events"]]
    assert [event["status"] for event in events] == ["started", "succeeded"]
    assert events[1]["payload"]["result"] == {"ok": True}


def test_pipeline_job_skips_when_lock_is_held():
    client = FakeRedis()
    runtime = RedisRuntime(client=client, settings_obj=queue_settings())
    client.set("test_pipeline:lock:job-a", "other-token")

    result = run_pipeline_job("job-a", lambda: {"should_not": "run"}, runtime=runtime)

    assert result["skipped"] is True
    assert result["reason"] == "lock_held"
    events = [json.loads(item) for item in client.lists["test_pipeline:events"]]
    assert events[0]["status"] == "skipped"


def test_pipeline_job_records_failure_event():
    client = FakeRedis()
    runtime = RedisRuntime(client=client, settings_obj=queue_settings())

    with pytest.raises(ValueError):
        run_pipeline_job("job-a", lambda: (_ for _ in ()).throw(ValueError("bad input")), runtime=runtime)

    events = [json.loads(item) for item in client.lists["test_pipeline:events"]]
    assert [event["status"] for event in events] == ["started", "failed"]
    assert "ValueError" in events[-1]["error"]


def test_runtime_status_when_disabled():
    runtime = RedisRuntime(client=None, settings_obj=queue_settings(enabled=False))

    status = runtime.status()

    assert status["configured"] is False
    assert status["enabled"] is False
    assert status["available"] is False
    assert status["queue_key"] == "test_pipeline:events"
