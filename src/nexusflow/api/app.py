"""FastAPI application factory with lifespan management and worker orchestration."""

import asyncio
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI

from nexusflow.api.middleware import RequestIdMiddleware, StructuredLoggingMiddleware
from nexusflow.api.routes.health import router as health_router
from nexusflow.api.routes.tasks import router as tasks_router
from nexusflow.api.routes.webhooks import router as webhooks_router
from nexusflow.dispatch.dispatcher import WebhookDispatcher
from nexusflow.models.task import TaskStatus
from nexusflow.queue.dlq import DeadLetterQueue
from nexusflow.queue.priority_queue import PriorityTaskQueue, QueueEmptyError


async def _worker_loop(app: FastAPI) -> None:
    """Background worker daemon processing queued tasks asynchronously."""
    queue: PriorityTaskQueue = app.state.queue
    dlq: DeadLetterQueue = app.state.dlq

    while True:
        try:
            task = await queue.dequeue(timeout=0.5)
            try:
                # Simulated workload execution or payload processing
                if task.payload.get("simulate_failure"):
                    raise RuntimeError(task.payload.get("error_message", "Simulated task failure"))

                # Brief simulated async computation
                await asyncio.sleep(0.005)

                task.status = TaskStatus.COMPLETED
                task.completed_at = time.time()
                task.result = {
                    "status": "success",
                    "processed_at": task.completed_at,
                    "task_id": task.id,
                }
                app.state.active_tasks.pop(task.id, None)
                app.state.completed_tasks[task.id] = task

            except Exception as exc:
                task.retry_count += 1
                task.error = str(exc)
                if task.retry_count <= task.max_retries:
                    task.status = TaskStatus.PENDING
                    await queue.enqueue(task)
                else:
                    app.state.active_tasks.pop(task.id, None)
                    await dlq.add(
                        task,
                        reason=f"Exhausted {task.retry_count}/{task.max_retries} retries: {exc}",
                    )

        except (asyncio.TimeoutError, QueueEmptyError):
            continue
        except asyncio.CancelledError:
            break


def create_app(
    enable_worker: bool = True,
    aging_window_seconds: float = 5.0,
) -> FastAPI:
    """Create and configure production FastAPI microservice application."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        # Startup: initialize shared queue subsystems
        app.state.queue = PriorityTaskQueue(aging_window_seconds=aging_window_seconds)
        app.state.dlq = DeadLetterQueue()
        app.state.dispatcher = WebhookDispatcher(dlq=app.state.dlq)
        app.state.active_tasks = {}
        app.state.completed_tasks = {}

        worker_task = None
        if enable_worker:
            worker_task = asyncio.create_task(_worker_loop(app))
            app.state.worker_task = worker_task

        yield

        # Shutdown: cancel and drain background worker
        if worker_task is not None:
            worker_task.cancel()
            with suppress(asyncio.CancelledError):
                await worker_task

    app = FastAPI(
        title="NexusFlow API",
        description="Production asynchronous priority task queue and secure webhook dispatch microservice",
        version="1.0.0",
        lifespan=lifespan,
    )

    # Middleware
    app.add_middleware(StructuredLoggingMiddleware)
    app.add_middleware(RequestIdMiddleware)

    # Routers
    app.include_router(health_router)
    app.include_router(tasks_router)
    app.include_router(webhooks_router)

    return app
