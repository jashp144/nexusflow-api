"""Task lifecycle and queue management endpoints."""

from typing import Any

from fastapi import APIRouter, HTTPException, Request, status

from nexusflow.models.task import Task, TaskRequest, TaskResponse
from nexusflow.queue.priority_queue import QueueFullError

router = APIRouter(tags=["Tasks"])


@router.post("/tasks", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
async def create_task(task_req: TaskRequest, request: Request) -> TaskResponse:
    """Submit a task to the asynchronous priority queue."""
    queue = request.app.state.queue
    task = Task(
        priority=task_req.priority,
        payload=task_req.payload,
        max_retries=task_req.max_retries,
        timeout_seconds=task_req.timeout_seconds,
    )

    try:
        await queue.enqueue(task)
    except QueueFullError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Queue capacity saturated: {exc}",
        ) from exc

    request.app.state.active_tasks[task.id] = task

    return TaskResponse(
        id=task.id,
        priority=task.priority,
        payload=task.payload,
        status=task.status,
        retry_count=task.retry_count,
        enqueued_at=task.enqueued_at,
        started_at=task.started_at,
        completed_at=task.completed_at,
        error=task.error,
        result=task.result,
    )


@router.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(task_id: str, request: Request) -> TaskResponse:
    """Retrieve the real-time status and result of a task."""
    # 1. Check in completed tasks
    task = request.app.state.completed_tasks.get(task_id)

    # 2. Check in active tasks
    if task is None:
        task = request.app.state.active_tasks.get(task_id)

    # 3. Check in dead letter queue
    if task is None:
        task = await request.app.state.dlq.get_by_id(task_id)

    if task is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task '{task_id}' not found",
        )

    return TaskResponse(
        id=task.id,
        priority=task.priority,
        payload=task.payload,
        status=task.status,
        retry_count=task.retry_count,
        enqueued_at=task.enqueued_at,
        started_at=task.started_at,
        completed_at=task.completed_at,
        error=task.error,
        result=task.result,
    )


@router.get("/queue/stats", response_model=dict[str, Any])
async def get_queue_stats(request: Request) -> dict[str, Any]:
    """Retrieve operational metrics for the priority queue and DLQ."""
    queue = request.app.state.queue
    dlq = request.app.state.dlq

    return {
        "queue": queue.stats(),
        "dlq": dlq.stats(),
        "active_tasks_count": len(request.app.state.active_tasks),
        "completed_tasks_count": len(request.app.state.completed_tasks),
    }
