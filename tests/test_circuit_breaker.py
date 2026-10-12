"""Tests for downstream endpoint CircuitBreaker state transitions."""

import asyncio

import pytest

from nexusflow.dispatch.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
)


def test_circuit_initial_closed_state():
    """Verify circuit breaker initializes in CLOSED state."""
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1.0)
    assert cb.state == CircuitState.CLOSED
    assert cb.can_execute() is True


def test_circuit_trips_to_open_on_threshold_failures():
    """Verify circuit trips to OPEN when failure threshold is reached."""
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=5.0)

    cb.record_failure()
    assert cb.state == CircuitState.CLOSED
    cb.record_failure()
    assert cb.state == CircuitState.CLOSED

    # 3rd failure trips circuit
    cb.record_failure()
    assert cb.state == CircuitState.OPEN
    assert cb.can_execute() is False


@pytest.mark.asyncio
async def test_circuit_open_rejects_execution():
    """Verify execute raises CircuitBreakerOpenError while OPEN."""
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=10.0)
    cb.record_failure()
    assert cb.state == CircuitState.OPEN

    async def sample_coro():
        return "ok"

    with pytest.raises(CircuitBreakerOpenError):
        await cb.execute(sample_coro)


@pytest.mark.asyncio
async def test_circuit_transition_to_half_open_and_close():
    """Verify recovery timeout enables HALF_OPEN probe and consecutive successes close circuit."""
    recovery_sec = 0.1
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=recovery_sec, success_threshold=2)

    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitState.OPEN

    # Wait for recovery timeout to elapse
    await asyncio.sleep(0.15)
    assert cb.state == CircuitState.HALF_OPEN
    assert cb.can_execute() is True

    # 1st success in HALF_OPEN
    cb.record_success()
    assert cb.state == CircuitState.HALF_OPEN

    # 2nd success closes circuit
    cb.record_success()
    assert cb.state == CircuitState.CLOSED
    assert cb.can_execute() is True


@pytest.mark.asyncio
async def test_circuit_half_open_failure_reopens():
    """Verify any failure in HALF_OPEN trips circuit immediately back to OPEN."""
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05, success_threshold=2)
    cb.record_failure()
    assert cb.state == CircuitState.OPEN

    await asyncio.sleep(0.08)
    assert cb.state == CircuitState.HALF_OPEN

    # Failure during probe re-trips to OPEN
    cb.record_failure()
    assert cb.state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_execute_success_and_failure_tracking():
    """Verify execute helper wraps coroutines and records metrics."""
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1.0)

    async def successful_call():
        return 42

    async def failing_call():
        raise ValueError("simulated network drop")

    res = await cb.execute(successful_call)
    assert res == 42
    assert cb.state == CircuitState.CLOSED

    with pytest.raises(ValueError, match="simulated network drop"):
        await cb.execute(failing_call)

    assert cb._failure_count == 1
    assert cb.state == CircuitState.CLOSED


def test_circuit_manual_reset_and_stats():
    """Verify manual reset and stats reporting."""
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=5.0)
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitState.OPEN

    stats = cb.stats()
    assert stats["state"] == "open"
    assert stats["failure_count"] == 2

    cb.reset()
    assert cb.state == CircuitState.CLOSED
    assert cb._failure_count == 0
