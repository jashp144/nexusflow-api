"""Tests for PriorityTaskQueue and starvation-free aging."""

import asyncio
import time

import pytest

from nexusflow.models.task import Task, TaskPriority, TaskStatus
from nexusflow.queue.priority_queue import (
    PriorityTaskQueue,
    QueueEmptyError,
    QueueFullError,
)


@pytest.mark.asyncio
async def test_priority_ordering_default():
    """Verify tasks are dequeued in strict priority order (1=CRITICAL, 4=LOW)."""
    queue = PriorityTaskQueue(aging_window_seconds=100.0)

    t_low = Task(id="low", priority=TaskPriority.LOW)
    t_med = Task(id="med", priority=TaskPriority.MEDIUM)
    t_high = Task(id="high", priority=TaskPriority.HIGH)
    t_crit = Task(id="crit", priority=TaskPriority.CRITICAL)

    # Enqueue in reverse priority
    await queue.enqueue(t_low)
    await queue.enqueue(t_med)
    await queue.enqueue(t_high)
    await queue.enqueue(t_crit)

    assert queue.qsize() == 4

    out1 = await queue.dequeue()
    out2 = await queue.dequeue()
    out3 = await queue.dequeue()
    out4 = await queue.dequeue()

    assert out1.id == "crit"
    assert out2.id == "high"
    assert out3.id == "med"
    assert out4.id == "low"
    assert queue.empty()


@pytest.mark.asyncio
async def test_starvation_free_aging():
    """Verify that an aged low-priority task is promoted over a freshly queued critical task."""
    age_window = 2.0
    queue = PriorityTaskQueue(aging_window_seconds=age_window)

    now = time.time()
    # Simulate a low-priority task enqueued 10 seconds ago (5 age windows elapsed)
    old_low = Task(id="old_low", priority=TaskPriority.LOW)
    old_low.enqueued_at = now - 10.0

    # Simulate a freshly enqueued critical task
    fresh_crit = Task(id="fresh_crit", priority=TaskPriority.CRITICAL)
    fresh_crit.enqueued_at = now

    await queue.enqueue(old_low)
    # Reset old_low.enqueued_at because enqueue overwrites it
    old_low.enqueued_at = now - 10.0
    await queue.enqueue(fresh_crit)

    # old_low effective priority = 4 - floor(10 / 2) = 4 - 5 = 1.0 (tied with CRITICAL 1.0)
    # Tie-break: old_low.enqueued_at is earlier, so old_low MUST be dequeued first!
    first = await queue.dequeue()
    second = await queue.dequeue()

    assert first.id == "old_low"
    assert second.id == "fresh_crit"


@pytest.mark.asyncio
async def test_dequeue_nowait():
    """Verify synchronous dequeue_nowait behavior."""
    queue = PriorityTaskQueue()
    with pytest.raises(QueueEmptyError):
        queue.dequeue_nowait()

    t = Task(id="t1", priority=1)
    await queue.enqueue(t)
    popped = queue.dequeue_nowait()
    assert popped.id == "t1"
    assert popped.status == TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_queue_maxsize_enforcement():
    """Verify QueueFullError when maxsize is exceeded."""
    queue = PriorityTaskQueue(maxsize=2)
    await queue.enqueue(Task(id="1", priority=1))
    await queue.enqueue(Task(id="2", priority=2))

    with pytest.raises(QueueFullError):
        await queue.enqueue(Task(id="3", priority=3))


@pytest.mark.asyncio
async def test_dequeue_timeout():
    """Verify asyncio.TimeoutError when waiting for empty queue."""
    queue = PriorityTaskQueue()
    with pytest.raises(asyncio.TimeoutError):
        await queue.dequeue(timeout=0.05)


@pytest.mark.asyncio
async def test_peek_and_remove():
    """Verify peek inspects best task and remove_task deletes candidate."""
    queue = PriorityTaskQueue()
    t1 = Task(id="t1", priority=3)
    t2 = Task(id="t2", priority=1)
    await queue.enqueue(t1)
    await queue.enqueue(t2)

    peeked = queue.peek()
    assert peeked is not None
    assert peeked.id == "t2"
    assert queue.qsize() == 2

    removed = await queue.remove_task("t2")
    assert removed is not None
    assert removed.id == "t2"
    assert queue.qsize() == 1

    remaining = await queue.dequeue()
    assert remaining.id == "t1"

    assert await queue.remove_task("nonexistent") is None


@pytest.mark.asyncio
async def test_clear_and_stats():
    """Verify clear flushes tasks and stats reports accurate telemetry."""
    queue = PriorityTaskQueue()
    assert queue.peek() is None

    await queue.enqueue(Task(id="a", priority=2))
    await queue.enqueue(Task(id="b", priority=4))

    stats_before = queue.stats()
    assert stats_before["total_enqueued"] == 2
    assert stats_before["pending_count"] == 2

    await queue.dequeue()
    stats_mid = queue.stats()
    assert stats_mid["total_dequeued"] == 1
    assert stats_mid["pending_count"] == 1

    await queue.clear()
    assert queue.empty()
    assert queue.stats()["pending_count"] == 0


@pytest.mark.asyncio
async def test_concurrency_race():
    """Verify queue integrity under concurrent enqueue and dequeue calls."""
    queue = PriorityTaskQueue(aging_window_seconds=1.0)
    task_count = 50

    async def producer():
        for i in range(task_count):
            p = (i % 4) + 1
            await queue.enqueue(Task(id=f"concurrent_{i}", priority=p))
            await asyncio.sleep(0.001)

    consumed = []

    async def consumer():
        while len(consumed) < task_count:
            try:
                task = await queue.dequeue(timeout=0.2)
                consumed.append(task)
            except asyncio.TimeoutError:
                break

    await asyncio.gather(producer(), consumer())

    assert len(consumed) == task_count
    assert queue.empty()
