from types import SimpleNamespace

from backend.common.rate_limiter import ApiRateLimiter


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.ttl_values = {}

    def incr(self, key):
        self.values[key] = int(self.values.get(key, 0)) + 1
        return self.values[key]

    def expire(self, key, ttl):
        self.ttl_values[key] = ttl
        return True

    def ttl(self, key):
        return self.ttl_values.get(key, 60)


def limiter_settings(*, enabled=True, redis_enabled=False):
    return SimpleNamespace(
        api_rate_limit_enabled=enabled,
        api_rate_limit_requests_per_minute=2,
        api_rate_limit_window_seconds=60,
        redis_enabled=redis_enabled,
        redis_url="redis://redis:6379/0",
        redis_queue_prefix="test_pipeline",
    )


def test_rate_limiter_allows_until_limit_then_blocks_with_memory_backend():
    limiter = ApiRateLimiter(settings_obj=limiter_settings())

    first = limiter.check(client_id="client-a", route_key="/api/stats")
    second = limiter.check(client_id="client-a", route_key="/api/stats")
    third = limiter.check(client_id="client-a", route_key="/api/stats")

    assert first.allowed is True
    assert second.allowed is True
    assert third.allowed is False
    assert third.remaining == 0
    assert third.backend == "memory"


def test_rate_limiter_uses_redis_when_available():
    limiter = ApiRateLimiter(
        settings_obj=limiter_settings(redis_enabled=True),
        client=FakeRedis(),
    )

    decision = limiter.check(client_id="client-a", route_key="/api/stats")

    assert decision.allowed is True
    assert decision.backend == "redis"
    assert limiter.status()["redis_available"] is True
