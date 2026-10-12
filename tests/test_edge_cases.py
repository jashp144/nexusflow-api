"""Edge case tests covering failure modes, degraded health, and error middleware."""

from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from nexusflow.api.app import create_app
from nexusflow.models.task import Task
from nexusflow.models.webhook import DispatchEvent, WebhookSubscription
from nexusflow.queue.priority_queue import PriorityTaskQueue, QueueFullError


def test_queue_full_endpoint_503(client: TestClient):
    """Verify POST /tasks returns 503 when queue capacity is exhausted."""
    with patch.object(
        client.app.state.queue, "enqueue", side_effect=QueueFullError("Queue capacity full")
    ):
        resp = client.post("/tasks", json={"priority": 1, "payload": {}})
        assert resp.status_code == 503
        assert "saturated" in resp.json()["detail"].lower()


def test_health_ready_degraded_when_uninitialized():
    """Verify /health/ready returns 503 when queue subsystems are None."""
    app = create_app(enable_worker=False)
    with TestClient(app) as test_client:
        test_client.app.state.queue = None
        resp = test_client.get("/health/ready")
        assert resp.status_code == 503
        assert resp.json()["status"] == "degraded"


def test_unhandled_exception_middleware(client: TestClient):
    """Verify StructuredLoggingMiddleware catches unhandled exceptions and formats 500 JSON response."""
    with patch.object(
        client.app.state.queue, "stats", side_effect=RuntimeError("Simulated unhandled crash")
    ):
        resp = client.get("/queue/stats")
        assert resp.status_code == 500
        data = resp.json()
        assert data["error"] == "Internal Server Error"
        assert "Simulated unhandled crash" in data["detail"]
        assert "x-request-id" in resp.headers


@pytest.mark.asyncio
async def test_priority_queue_remove_middle_item():
    """Verify remove_task correctly handles removing middle item in queue."""
    q = PriorityTaskQueue()
    await q.enqueue(Task(id="first", priority=1))
    await q.enqueue(Task(id="middle", priority=2))
    await q.enqueue(Task(id="last", priority=3))

    removed = await q.remove_task("middle")
    assert removed is not None
    assert removed.id == "middle"
    assert q.qsize() == 2


@pytest.mark.asyncio
async def test_dispatcher_empty_broadcast():
    """Verify broadcast with no subscriptions returns empty list."""
    from nexusflow.dispatch.dispatcher import WebhookDispatcher

    dispatcher = WebhookDispatcher()
    evt = DispatchEvent(event_type="any", payload={})
    results = await dispatcher.broadcast(evt, subscriptions=[])
    assert results == []


def test_webhook_matches_event_inactive():
    """Verify inactive webhook subscription does not match events."""
    sub = WebhookSubscription(
        url="https://inactive.com",
        secret="s",
        is_active=False,
        event_types=["*"],
    )
    assert sub.matches_event("user.login") is False
