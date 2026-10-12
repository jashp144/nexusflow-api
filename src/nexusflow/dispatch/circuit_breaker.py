"""Circuit breaker pattern implementation for downstream webhook endpoints."""

import time
from collections.abc import Callable
from enum import Enum
from typing import Any


class CircuitState(str, Enum):
    """Finite states of the Circuit Breaker state machine."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpenError(Exception):
    """Raised when an operation is attempted while the circuit breaker is in the OPEN state."""

    def __init__(
        self, message: str = "Circuit breaker is OPEN: downstream endpoint is unhealthy"
    ) -> None:
        super().__init__(message)


class CircuitBreaker:
    """Production circuit breaker protecting against cascading failures.

    State transitions:
    - CLOSED -> OPEN: Triggered when consecutive failures reach `failure_threshold`.
    - OPEN -> HALF_OPEN: Automatic transition after `recovery_timeout` seconds elapse.
    - HALF_OPEN -> CLOSED: Triggered when `success_threshold` consecutive probe requests succeed.
    - HALF_OPEN -> OPEN: Triggered immediately if any probe request fails.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        success_threshold: int = 2,
    ) -> None:
        """Initialize CircuitBreaker.

        Args:
            failure_threshold: Number of consecutive failures before tripping to OPEN.
            recovery_timeout: Seconds to remain OPEN before probing via HALF_OPEN.
            success_threshold: Number of consecutive successes in HALF_OPEN to close the circuit.
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = float(recovery_timeout)
        self.success_threshold = success_threshold

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._opened_at: float | None = None
        self._last_failure_time: float | None = None

    @property
    def state(self) -> CircuitState:
        """Get the current circuit state, evaluating recovery timeout transition dynamically."""
        if (
            self._state == CircuitState.OPEN
            and self._opened_at is not None
            and time.time() - self._opened_at >= self.recovery_timeout
        ):
            self._state = CircuitState.HALF_OPEN
            self._success_count = 0
        return self._state

    def can_execute(self) -> bool:
        """Determine whether requests are currently allowed through."""
        return self.state in (CircuitState.CLOSED, CircuitState.HALF_OPEN)

    def record_success(self) -> None:
        """Record a successful operation and handle state transitions."""
        current_state = self.state
        if current_state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.success_threshold:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._success_count = 0
                self._opened_at = None
        elif current_state == CircuitState.CLOSED:
            self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed operation and handle state transitions."""
        current_state = self.state
        now = time.time()
        self._last_failure_time = now

        if current_state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            self._opened_at = now
            self._success_count = 0
        elif current_state == CircuitState.CLOSED:
            self._failure_count += 1
            if self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                self._opened_at = now

    def reset(self) -> None:
        """Manually reset the circuit breaker back to initial CLOSED state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._opened_at = None

    async def execute(self, coro_fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute an asynchronous callable guarded by the circuit breaker."""
        if not self.can_execute():
            raise CircuitBreakerOpenError(
                f"Circuit breaker is OPEN (cooldown: {self.recovery_timeout}s)"
            )

        try:
            result = await coro_fn(*args, **kwargs)
            self.record_success()
            return result
        except Exception:
            self.record_failure()
            raise

    def stats(self) -> dict[str, Any]:
        """Return operational state metrics."""
        return {
            "state": self.state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "opened_at": self._opened_at,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
        }
