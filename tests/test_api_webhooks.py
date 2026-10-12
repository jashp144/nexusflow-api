"""Integration tests for Webhook subscription and dispatch endpoints."""

from starlette.testclient import TestClient


def test_subscribe_and_list_webhooks(client: TestClient):
    """Verify registering and listing webhook subscriptions via API."""
    sub_payload = {
        "url": "https://consumer.example.com/events",
        "secret": "my-shared-secret-123",
        "event_types": ["payment.completed", "refund.issued"],
    }
    resp = client.post("/webhooks/subscribe", json=sub_payload)
    assert resp.status_code == 201
    sub_data = resp.json()

    assert "id" in sub_data
    assert sub_data["url"] == "https://consumer.example.com/events"
    assert sub_data["secret"] == "my-shared-secret-123"
    assert sub_data["event_types"] == ["payment.completed", "refund.issued"]
    assert sub_data["is_active"] is True

    # List registered webhooks
    list_resp = client.get("/webhooks")
    assert list_resp.status_code == 200
    subs = list_resp.json()
    assert len(subs) >= 1
    assert any(s["id"] == sub_data["id"] for s in subs)


def test_dispatch_endpoint_no_subscribers(client: TestClient):
    """Verify dispatching an event when no subscribers exist returns empty attempt list."""
    event_payload = {
        "event_type": "system.maintenance",
        "payload": {"scheduled_time": "2026-10-15T00:00:00Z"},
    }
    resp = client.post("/webhooks/dispatch", json=event_payload)
    assert resp.status_code == 200
    attempts = resp.json()
    assert isinstance(attempts, list)
    assert len(attempts) == 0
