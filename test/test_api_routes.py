from fastapi.testclient import TestClient

from backend.api.main import app


def test_openapi_includes_cost_of_living_routes():
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    paths = schema["paths"]
    assert "/api/pipeline/status" in paths
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
