"""Asynchronous concurrency-safe priority task queue with starvation-free aging."""

import asyncio
import time
from typing import Any

from nexusflow.models.task import Task, TaskStatus


class QueueEmptyError(Exception):
    """Raised when non-blocking dequeue is attempted on an empty queue."""


class QueueFullError(Exception):
    """Raised when queue capacity is exceeded."""


class PriorityTaskQueue:
    """Asynchronous concurrency-safe priority task queue.

    Implements a dynamic starvation-free aging algorithm:
    Low-priority tasks accumulate wait time in the queue and gradually
    improve their effective priority tier until promoted to critical priority,
    guaranteeing bounded wait times and preventing indefinite starvation under high load.
    """

    def __init__(
        self,
        aging_window_seconds: float = 5.0,
        maxsize: int = 0,
    ) -> None:
        """Initialize PriorityTaskQueue.

        Args:
            aging_window_seconds: Time interval in seconds after which a waiting
                task's effective priority score improves by 1 tier.
            maxsize: Maximum queue capacity (0 or negative indicates unbounded).
        """
        self.aging_window_seconds = float(aging_window_seconds)
        self.maxsize = maxsize
        self._tasks: list[Task] = []
        self._lock = asyncio.Lock()
        self._has_items = asyncio.Event()
        self._total_enqueued = 0
        self._total_dequeued = 0
        self._total_wait_time = 0.0

    async def enqueue(self, task: Task) -> None:
        """Enqueue a task with concurrency protection."""
        async with self._lock:
            if self.maxsize > 0 and len(self._tasks) >= self.maxsize:
                raise QueueFullError(f"PriorityTaskQueue maxsize ({self.maxsize}) reached")

            task.status = TaskStatus.PENDING
            task.enqueued_at = time.time()
            self._tasks.append(task)
            self._total_enqueued += 1
            self._has_items.set()

    async def put(self, task: Task) -> None:
        """Standard queue alias for enqueue."""
        await self.enqueue(task)

    async def dequeue(self, timeout: float | None = None) -> Task:
        """Dequeue the task with the highest effective priority.

        Evaluates effective priority of all waiting tasks using the aging formula:
        effective_score = max(1.0, base_priority - floor(wait_time / age_window))
        Selects the minimum effective score, tie-breaking by arrival timestamp (FIFO).

        Args:
            timeout: Maximum wait seconds for a task to become available. None means wait indefinitely.

        Returns:
            The Task selected for execution.
        """
        start_wait = time.time()

        while True:
            # Check for available items
            async with self._lock:
                if self._tasks:
                    now = time.time()
                    # Find candidate with best (lowest) effective priority score,
                    # tie-breaking by enqueued_at (earliest first), then task id
                    best_index = 0
                    best_score = self._tasks[0].effective_priority(now, self.aging_window_seconds)
                    best_enqueued = self._tasks[0].enqueued_at

                    for i in range(1, len(self._tasks)):
                        t = self._tasks[i]
                        score = t.effective_priority(now, self.aging_window_seconds)
                        if score < best_score or (
                            score == best_score and t.enqueued_at < best_enqueued
                        ):
                            best_index = i
                            best_score = score
                            best_enqueued = t.enqueued_at

                    task = self._tasks.pop(best_index)
                    if not self._tasks:
                        self._has_items.clear()

                    wait_duration = max(0.0, now - task.enqueued_at)
                    self._total_dequeued += 1
                    self._total_wait_time += wait_duration
                    task.status = TaskStatus.RUNNING
                    task.started_at = now
                    return task

            # Queue was empty, wait for an item
            if timeout is not None:
                elapsed = time.time() - start_wait
                remaining = timeout - elapsed
                if remaining <= 0:
                    raise asyncio.TimeoutError("Timeout waiting for task from PriorityTaskQueue")
                try:
                    await asyncio.wait_for(self._has_items.wait(), timeout=remaining)
                except asyncio.TimeoutError as exc:
                    raise asyncio.TimeoutError(
                        "Timeout waiting for task from PriorityTaskQueue"
                    ) from exc
            else:
                await self._has_items.wait()

    async def get(self, timeout: float | None = None) -> Task:
        """Standard queue alias for dequeue."""
        return await self.dequeue(timeout=timeout)

    def dequeue_nowait(self) -> Task:
        """Synchronously pop the best task if available, or raise QueueEmptyError."""
        if not self._tasks:
            raise QueueEmptyError("PriorityTaskQueue is empty")

        now = time.time()
        best_index = 0
        best_score = self._tasks[0].effective_priority(now, self.aging_window_seconds)
        best_enqueued = self._tasks[0].enqueued_at

        for i in range(1, len(self._tasks)):
            t = self._tasks[i]
            score = t.effective_priority(now, self.aging_window_seconds)
            if score < best_score or (score == best_score and t.enqueued_at < best_enqueued):
                best_index = i
                best_score = score
                best_enqueued = t.enqueued_at

        task = self._tasks.pop(best_index)
        if not self._tasks:
            self._has_items.clear()

        wait_duration = max(0.0, now - task.enqueued_at)
        self._total_dequeued += 1
        self._total_wait_time += wait_duration
        task.status = TaskStatus.RUNNING
        task.started_at = now
        return task

    def qsize(self) -> int:
        """Return the current number of tasks queued."""
        return len(self._tasks)

    def empty(self) -> bool:
        """Return True if the queue currently has no tasks."""
        return len(self._tasks) == 0

    def peek(self) -> Task | None:
        """Inspect the task that currently has highest effective priority without removing it."""
        if not self._tasks:
            return None
        now = time.time()
        best_task = self._tasks[0]
        best_score = best_task.effective_priority(now, self.aging_window_seconds)

        for t in self._tasks[1:]:
            score = t.effective_priority(now, self.aging_window_seconds)
            if score < best_score or (
                score == best_score and t.enqueued_at < best_task.enqueued_at
            ):
                best_task = t
                best_score = score

        return best_task

    async def remove_task(self, task_id: str) -> Task | None:
        """Remove a pending task by ID."""
        async with self._lock:
            for i, t in enumerate(self._tasks):
                if t.id == task_id:
                    removed = self._tasks.pop(i)
                    if not self._tasks:
                        self._has_items.clear()
                    return removed
            return None

    async def clear(self) -> None:
        """Flush all pending tasks from the queue."""
        async with self._lock:
            self._tasks.clear()
            self._has_items.clear()

    def stats(self) -> dict[str, Any]:
        """Return real-time operational statistics of the queue."""
        avg_wait = self._total_wait_time / self._total_dequeued if self._total_dequeued > 0 else 0.0
        return {
            "pending_count": len(self._tasks),
            "total_enqueued": self._total_enqueued,
            "total_dequeued": self._total_dequeued,
            "average_wait_seconds": round(avg_wait, 4),
            "aging_window_seconds": self.aging_window_seconds,
            "maxsize": self.maxsize,
        }
