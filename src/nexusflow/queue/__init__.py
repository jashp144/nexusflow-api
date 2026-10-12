"""Queue module providing priority scheduling with starvation-free aging and DLQ."""

from nexusflow.queue.dlq import DeadLetterQueue
from nexusflow.queue.priority_queue import (
    PriorityTaskQueue,
    QueueEmptyError,
    QueueFullError,
)

__all__ = [
    "DeadLetterQueue",
    "PriorityTaskQueue",
    "QueueEmptyError",
    "QueueFullError",
]
