"""Tests for health probes and middleware."""

from starlette.testclient import TestClient


def test_health_live_endpoint(client: TestClient):
    """Verify /health/live returns HTTP 200 with alive status."""
    resp = client.get("/health/live")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "alive"
    assert data["service"] == "nexusflow-api"
    assert "timestamp" in data


def test_health_ready_endpoint(client: TestClient):
    """Verify /health/ready returns HTTP 200 and subsystem operational indicators."""
    resp = client.get("/health/ready")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ready"
    assert "queue_pending" in data
    assert "dlq_size" in data


def test_request_id_middleware_propagation(client: TestClient):
    """Verify X-Request-ID is generated and returned when not provided."""
    resp = client.get("/health/live")
    assert "x-request-id" in resp.headers
    generated_id = resp.headers["x-request-id"]
    assert len(generated_id) > 10


def test_request_id_middleware_preserves_caller_id(client: TestClient):
    """Verify incoming X-Request-ID is preserved and mirrored in response."""
    custom_id = "client-trace-12345-abcdef"
    resp = client.get("/health/live", headers={"X-Request-ID": custom_id})
    assert resp.headers.get("x-request-id") == custom_id
