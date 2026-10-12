"""Integration tests for Task API routes and worker lifecycle."""

import time

from starlette.testclient import TestClient

from nexusflow.api.app import create_app


def test_post_task_success(client: TestClient):
    """Verify POST /tasks creates a task in pending state."""
    payload = {
        "priority": 2,
        "payload": {"job_type": "email.digest", "recipient": "user@example.com"},
        "max_retries": 3,
        "timeout_seconds": 15.0,
    }
    response = client.post("/tasks", json=payload)
    assert response.status_code == 201
    data = response.json()

    assert "id" in data
    assert data["priority"] == 2
    assert data["status"] == "pending"
    assert data["retry_count"] == 0
    assert data["payload"]["job_type"] == "email.digest"


def test_post_task_validation_error(client: TestClient):
    """Verify POST /tasks rejects invalid priority out of range [1, 4]."""
    payload = {
        "priority": 99,  # Invalid
        "payload": {},
    }
    response = client.post("/tasks", json=payload)
    assert response.status_code == 422


def test_get_task_not_found(client: TestClient):
    """Verify GET /tasks/{id} returns 404 for unknown task."""
    response = client.get("/tasks/non-existent-uuid")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_get_queue_stats(client: TestClient):
    """Verify GET /queue/stats returns operational queue telemetry."""
    # Enqueue a task
    client.post("/tasks", json={"priority": 1, "payload": {"foo": "bar"}})

    response = client.get("/queue/stats")
    assert response.status_code == 200
    stats = response.json()

    assert "queue" in stats
    assert "dlq" in stats
    assert stats["queue"]["total_enqueued"] >= 1


def test_worker_end_to_end_lifecycle():
    """Verify background worker processes queued task to COMPLETED state."""
    app = create_app(enable_worker=True, aging_window_seconds=1.0)
    with TestClient(app) as test_client:
        # Enqueue standard task
        resp = test_client.post(
            "/tasks",
            json={"priority": 1, "payload": {"compute": "sqrt", "x": 144}},
        )
        assert resp.status_code == 201
        task_id = resp.json()["id"]

        # Poll briefly for worker completion
        completed = False
        for _ in range(50):
            time.sleep(0.05)
            fetch = test_client.get(f"/tasks/{task_id}")
            if fetch.status_code == 200 and fetch.json()["status"] == "completed":
                completed = True
                assert fetch.json()["result"]["status"] == "success"
                break

        assert completed is True, "Task was not completed by worker within timeout"


def test_worker_retries_and_dlq_transition():
    """Verify simulated task failures trigger retries and transition into DLQ."""
    app = create_app(enable_worker=True, aging_window_seconds=1.0)
    with TestClient(app) as test_client:
        # Enqueue task with simulate_failure=True and max_retries=1
        resp = test_client.post(
            "/tasks",
            json={
                "priority": 1,
                "payload": {"simulate_failure": True, "error_message": "Database write timeout"},
                "max_retries": 1,
            },
        )
        assert resp.status_code == 201
        task_id = resp.json()["id"]

        # Poll for DLQ transition
        in_dlq = False
        for _ in range(60):
            time.sleep(0.05)
            fetch = test_client.get(f"/tasks/{task_id}")
            if fetch.status_code == 200 and fetch.json()["status"] == "dead_letter":
                in_dlq = True
                assert "Exhausted" in fetch.json()["error"]
                break

        assert in_dlq is True, "Poisoned task was not routed to DLQ"
