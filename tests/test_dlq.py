"""Tests for DeadLetterQueue (DLQ)."""

import pytest

from nexusflow.models.task import Task, TaskStatus
from nexusflow.queue.dlq import DeadLetterQueue
from nexusflow.queue.priority_queue import PriorityTaskQueue


@pytest.mark.asyncio
async def test_dlq_add_and_retrieve():
    """Verify tasks are recorded in DLQ with DEAD_LETTER status and error reason."""
    dlq = DeadLetterQueue()
    task = Task(id="poison_1", priority=1, payload={"fail": True})

    await dlq.add(task, reason="Unrecoverable runtime exception")

    assert dlq.size() == 1
    stored = await dlq.get_by_id("poison_1")
    assert stored is not None
    assert stored.status == TaskStatus.DEAD_LETTER
    assert stored.error == "Unrecoverable runtime exception"

    all_tasks = await dlq.get_all()
    assert len(all_tasks) == 1
    assert all_tasks[0].id == "poison_1"


@pytest.mark.asyncio
async def test_dlq_replay():
    """Verify dead-lettered task replay resets status and re-enqueues into priority queue."""
    dlq = DeadLetterQueue()
    queue = PriorityTaskQueue()

    task = Task(id="replay_1", priority=2, retry_count=3)
    await dlq.add(task, reason="Max retries exceeded")
    assert dlq.size() == 1

    replayed = await dlq.replay("replay_1", queue)
    assert replayed is not None
    assert dlq.size() == 0
    assert replayed.status == TaskStatus.PENDING
    assert replayed.retry_count == 0
    assert replayed.error is None
    assert queue.qsize() == 1

    # Verify task can be dequeued from queue
    popped = await queue.dequeue()
    assert popped.id == "replay_1"

    # Non-existent task replay returns None
    assert await dlq.replay("non_existent", queue) is None


@pytest.mark.asyncio
async def test_dlq_capacity_eviction():
    """Verify oldest task is evicted when max_capacity is reached."""
    dlq = DeadLetterQueue(max_capacity=3)

    await dlq.add(Task(id="first"), "error 1")
    await dlq.add(Task(id="second"), "error 2")
    await dlq.add(Task(id="third"), "error 3")
    assert dlq.size() == 3

    # Adding a 4th task should evict 'first'
    await dlq.add(Task(id="fourth"), "error 4")
    assert dlq.size() == 3
    assert await dlq.get_by_id("first") is None
    assert await dlq.get_by_id("fourth") is not None


@pytest.mark.asyncio
async def test_dlq_purge_and_stats():
    """Verify DLQ purge clears tasks and stats returns accurate counts."""
    dlq = DeadLetterQueue()
    await dlq.add(Task(id="a"), "err")
    await dlq.add(Task(id="b"), "err")

    stats = dlq.stats()
    assert stats["dlq_size"] == 2
    assert stats["total_added"] == 2

    cleared = await dlq.purge()
    assert cleared == 2
    assert dlq.size() == 0
    assert await dlq.get_by_id("a") is None
