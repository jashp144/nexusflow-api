"""Retry policy implementing exponential backoff with full jitter."""

import random


class RetryPolicy:
    """Configurable retry policy with exponential backoff and decorrelated full jitter.

    Mitigates thundering herd problems and synchronized retry storms against downstream
    endpoints during partial outages.
    """

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 0.2,
        max_delay: float = 10.0,
        backoff_factor: float = 2.0,
        jitter: bool = True,
    ) -> None:
        """Initialize RetryPolicy.

        Args:
            max_attempts: Maximum total delivery attempts allowed (initial + retries).
            base_delay: Initial retry backoff delay in seconds.
            max_delay: Upper ceiling on backoff delay in seconds.
            backoff_factor: Exponential multiplier applied per consecutive attempt.
            jitter: If True, applies full jitter Uniform(0, calculated_delay).
        """
        self.max_attempts = max(1, max_attempts)
        self.base_delay = max(0.001, float(base_delay))
        self.max_delay = max(self.base_delay, float(max_delay))
        self.backoff_factor = max(1.0, float(backoff_factor))
        self.jitter = jitter

    def should_retry(self, attempt: int) -> bool:
        """Check if an operation should be retried given current attempt number (1-indexed)."""
        return attempt < self.max_attempts

    def calculate_delay(self, attempt: int) -> float:
        """Calculate wait delay in seconds for the given attempt index (0-based or 1-based).

        Formula:
        T_cap = min(max_delay, base_delay * (backoff_factor ^ attempt))
        T_delay = Uniform(0, T_cap) if jitter else T_cap
        """
        exponent = max(0, attempt)
        delay_cap = min(self.max_delay, self.base_delay * (self.backoff_factor**exponent))
        if self.jitter:
            return random.uniform(0.0, delay_cap)
        return delay_cap
