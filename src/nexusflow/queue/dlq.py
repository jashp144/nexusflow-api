"""Dead Letter Queue (DLQ) for poisoned and failed tasks."""

import asyncio
import time
from typing import Any

from nexusflow.models.task import Task, TaskStatus


class DeadLetterQueue:
    """Asynchronous concurrency-safe storage for poisoned or exhausted tasks.

    Tasks that exceed maximum retry attempts or encounter fatal unrecoverable exceptions
    are routed to the DLQ for diagnostic inspection, audit retention, and manual or
    automated replay.
    """

    def __init__(self, max_capacity: int = 10000) -> None:
        """Initialize DeadLetterQueue.

        Args:
            max_capacity: Maximum number of dead letter tasks retained before oldest are evicted.
        """
        self.max_capacity = max_capacity
        self._dlq_tasks: dict[str, Task] = {}
        self._lock = asyncio.Lock()
        self._total_added = 0
        self._total_replayed = 0

    async def add(self, task: Task, reason: str) -> None:
        """Add a poisoned or failed task to the DLQ."""
        async with self._lock:
            # Enforce max capacity eviction if needed
            if len(self._dlq_tasks) >= self.max_capacity and task.id not in self._dlq_tasks:
                oldest_id = next(iter(self._dlq_tasks))
                del self._dlq_tasks[oldest_id]

            task.status = TaskStatus.DEAD_LETTER
            task.error = reason
            task.completed_at = time.time()
            self._dlq_tasks[task.id] = task
            self._total_added += 1

    async def get_all(self) -> list[Task]:
        """Return all tasks currently in the DLQ."""
        async with self._lock:
            return list(self._dlq_tasks.values())

    async def get_by_id(self, task_id: str) -> Task | None:
        """Lookup a specific task in the DLQ."""
        async with self._lock:
            return self._dlq_tasks.get(task_id)

    async def replay(self, task_id: str, queue: Any) -> Task | None:
        """Re-enqueue a dead-lettered task back into the active priority queue.

        Resets retry count, clears previous errors, and marks status as PENDING.

        Args:
            task_id: ID of the dead-lettered task to re-queue.
            queue: The destination PriorityTaskQueue instance.

        Returns:
            The re-enqueued Task, or None if task_id not found in DLQ.
        """
        async with self._lock:
            task = self._dlq_tasks.pop(task_id, None)
            if task is None:
                return None

            task.status = TaskStatus.PENDING
            task.retry_count = 0
            task.error = None
            task.enqueued_at = time.time()
            task.started_at = None
            task.completed_at = None
            self._total_replayed += 1

        await queue.enqueue(task)
        return task

    async def purge(self) -> int:
        """Remove all tasks from the DLQ and return the number of cleared tasks."""
        async with self._lock:
            count = len(self._dlq_tasks)
            self._dlq_tasks.clear()
            return count

    def size(self) -> int:
        """Return current count of tasks in the DLQ."""
        return len(self._dlq_tasks)

    def stats(self) -> dict[str, Any]:
        """Return operational DLQ statistics."""
        return {
            "dlq_size": len(self._dlq_tasks),
            "total_added": self._total_added,
            "total_replayed": self._total_replayed,
            "max_capacity": self.max_capacity,
        }
