"""Tests for RetryPolicy exponential backoff and jitter calculation."""

from nexusflow.dispatch.retry import RetryPolicy


def test_retry_policy_exponential_growth_without_jitter():
    """Verify exponential backoff progression when jitter is disabled."""
    policy = RetryPolicy(
        max_attempts=4,
        base_delay=1.0,
        max_delay=10.0,
        backoff_factor=2.0,
        jitter=False,
    )

    # attempt 0: 1.0 * 2^0 = 1.0
    assert policy.calculate_delay(0) == 1.0
    # attempt 1: 1.0 * 2^1 = 2.0
    assert policy.calculate_delay(1) == 2.0
    # attempt 2: 1.0 * 2^2 = 4.0
    assert policy.calculate_delay(2) == 4.0
    # attempt 3: 1.0 * 2^3 = 8.0
    assert policy.calculate_delay(3) == 8.0
    # attempt 4: 1.0 * 2^4 = 16.0 capped at 10.0
    assert policy.calculate_delay(4) == 10.0


def test_retry_policy_with_full_jitter():
    """Verify full jitter produces values between 0.0 and calculated ceiling."""
    policy = RetryPolicy(
        max_attempts=3,
        base_delay=0.5,
        max_delay=5.0,
        backoff_factor=2.0,
        jitter=True,
    )

    for attempt in range(5):
        delay = policy.calculate_delay(attempt)
        ceiling = min(5.0, 0.5 * (2.0**attempt))
        assert 0.0 <= delay <= ceiling


def test_should_retry_boundary():
    """Verify should_retry adheres strictly to max_attempts threshold."""
    policy = RetryPolicy(max_attempts=3)

    assert policy.should_retry(1) is True
    assert policy.should_retry(2) is True
    assert policy.should_retry(3) is False
    assert policy.should_retry(4) is False
