from types import SimpleNamespace

from backend.common.api_cache import ApiResponseCache


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.ttl = {}

    def get(self, key):
        return self.values.get(key)

    def setex(self, key, ttl, value):
        self.values[key] = value
        self.ttl[key] = ttl
        return True


def cache_settings(redis_enabled=False):
    return SimpleNamespace(
        api_cache_ttl_seconds=30,
        redis_enabled=redis_enabled,
        redis_url="redis://redis:6379/0",
        redis_queue_prefix="test_pipeline",
    )


def test_api_cache_uses_memory_fallback():
    cache = ApiResponseCache(settings_obj=cache_settings(redis_enabled=False))
    calls = {"count": 0}

    def produce():
        calls["count"] += 1
        return {"rows": [{"value": calls["count"]}]}

    first = cache.get_or_set(("route", "a"), produce)
    second = cache.get_or_set(("route", "a"), produce)

    assert first == second
    assert calls["count"] == 1
    assert cache.status()["backend"] == "memory"
    assert cache.status()["hits"] == 1
    assert cache.status()["misses"] == 1


def test_api_cache_uses_redis_when_available():
    client = FakeRedis()
    cache = ApiResponseCache(settings_obj=cache_settings(redis_enabled=True), client=client)
    calls = {"count": 0}

    def produce():
        calls["count"] += 1
        return {"rows": [{"value": calls["count"]}]}

    first = cache.get_or_set(("route", "b"), produce)
    second = cache.get_or_set(("route", "b"), produce)

    assert first == second
    assert calls["count"] == 1
    assert len(client.values) == 1
    assert cache.status()["backend"] == "redis"
