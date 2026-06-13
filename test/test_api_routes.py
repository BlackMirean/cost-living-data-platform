from fastapi.testclient import TestClient

from backend.api.main import app


def test_openapi_includes_cost_of_living_routes():
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    paths = schema["paths"]
    assert "/api/pipeline/status" in paths
    assert "/api/pipeline/runtime" in paths
    assert "/api/pipeline/events" in paths
    assert "/api/platforms/plugins" in paths
    assert "/api/stats/overview" in paths
    assert "/api/categories/counts" in paths
    assert "/api/categories/sentiment" in paths
    assert "/api/data-quality/summary" in paths
    assert "/api/data-quality/comparison" in paths
    assert "/api/media/coverage" in paths
    assert "/api/platforms/categories" in paths
    assert "/api/trends/categories" in paths
    assert "/api/trends/sentiment" in paths
    assert "/api/official/comparison" in paths

    counts_params = {
        param["name"]
        for param in paths["/api/categories/counts"]["get"].get("parameters", [])
    }
    assert "quality" in counts_params
    assert "exclude_quality_flags" in counts_params


def test_cost_living_prefix_rewrites_to_api_routes(monkeypatch):
    monkeypatch.setattr("backend.api.main.analytics_store.health", lambda: {"status": "ok"})
    client = TestClient(app)

    response = client.get("/api/cost-living/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_platform_plugins_route_exposes_current_sources():
    client = TestClient(app)

    response = client.get("/api/platforms/plugins")

    assert response.status_code == 200
    payload = response.json()
    assert [plugin["name"] for plugin in payload["plugins"]] == ["bluesky", "mastodon", "gdelt"]
    assert payload["groups"] == {"social": ["bluesky", "mastodon"], "media": ["gdelt"]}
    mastodon = next(plugin for plugin in payload["plugins"] if plugin["name"] == "mastodon")
    assert len(mastodon["fission_handlers"]) == 3
    assert len(mastodon["schedules"]) == 3
