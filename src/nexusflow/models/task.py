"""Task models and schemas for nexusflow priority queue."""

import math
import time
import uuid
from enum import Enum, IntEnum
from typing import Any

from pydantic import BaseModel, Field


class TaskPriority(IntEnum):
    """Priority levels where lower integer denotes higher scheduling priority."""

    CRITICAL = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4


class TaskStatus(str, Enum):
    """Lifecycle statuses for asynchronous queue tasks."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


class Task(BaseModel):
    """Core domain model representing a queueable task."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    priority: int = Field(default=TaskPriority.MEDIUM, ge=1, le=4)
    payload: dict[str, Any] = Field(default_factory=dict)
    status: TaskStatus = Field(default=TaskStatus.PENDING)
    max_retries: int = Field(default=3, ge=0)
    retry_count: int = Field(default=0, ge=0)
    timeout_seconds: float = Field(default=30.0, gt=0.0)
    enqueued_at: float = Field(default_factory=time.time)
    started_at: float | None = None
    completed_at: float | None = None
    error: str | None = None
    result: Any | None = None

    def effective_priority(self, current_time: float, age_window_seconds: float = 5.0) -> float:
        """Calculate dynamic starvation-free effective priority.

        Lower numeric values indicate higher priority (1 is CRITICAL, 4 is LOW).
        As waiting duration accumulates, effective priority improves by 1 tier
        per elapsed age window, clamped at 1.0 (CRITICAL).
        """
        wait_duration = max(0.0, current_time - self.enqueued_at)
        age_bonus = math.floor(wait_duration / age_window_seconds) if age_window_seconds > 0 else 0
        return max(1.0, float(self.priority) - float(age_bonus))


class TaskRequest(BaseModel):
    """API request schema for enqueuing a new task."""

    priority: int = Field(
        default=TaskPriority.MEDIUM,
        ge=1,
        le=4,
        description="Priority tier from 1 (Critical) to 4 (Low)",
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary JSON payload for task execution",
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum execution retry attempts before moving to DLQ",
    )
    timeout_seconds: float = Field(
        default=30.0,
        gt=0.0,
        le=3600.0,
        description="Execution timeout threshold in seconds",
    )


class TaskResponse(BaseModel):
    """API response schema representing task status."""

    id: str
    priority: int
    payload: dict[str, Any]
    status: TaskStatus
    retry_count: int
    enqueued_at: float
    started_at: float | None = None
    completed_at: float | None = None
    error: str | None = None
    result: Any | None = None
