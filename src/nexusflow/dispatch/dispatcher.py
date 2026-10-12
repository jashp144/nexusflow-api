"""Asynchronous webhook dispatcher with HMAC authentication, retries, and circuit breaker."""

import asyncio
import json
import time

import httpx

from nexusflow.dispatch.circuit_breaker import CircuitBreaker
from nexusflow.dispatch.retry import RetryPolicy
from nexusflow.dispatch.signer import WebhookSigner
from nexusflow.models.task import Task, TaskPriority
from nexusflow.models.webhook import DeliveryAttempt, DispatchEvent, WebhookSubscription
from nexusflow.queue.dlq import DeadLetterQueue


class WebhookDispatcher:
    """Production webhook delivery engine.

    Features:
    - HMAC-SHA256 signature stamping and replay timestamp protection
    - Per-subscription circuit breaker isolation
    - Exponential backoff with full jitter on transient downstream failures
    - Optional Dead Letter Queue routing on exhaustion of retry attempts
    """

    def __init__(
        self,
        retry_policy: RetryPolicy | None = None,
        dlq: DeadLetterQueue | None = None,
        default_timeout: float = 10.0,
        circuit_failure_threshold: int = 5,
        circuit_recovery_timeout: float = 30.0,
    ) -> None:
        """Initialize WebhookDispatcher."""
        self.retry_policy = retry_policy or RetryPolicy()
        self.dlq = dlq
        self.default_timeout = default_timeout
        self.circuit_failure_threshold = circuit_failure_threshold
        self.circuit_recovery_timeout = circuit_recovery_timeout

        self._subscriptions: dict[str, WebhookSubscription] = {}
        self._circuit_breakers: dict[str, CircuitBreaker] = {}
        self._delivery_history: list[DeliveryAttempt] = []
        self._lock = asyncio.Lock()

    def add_subscription(self, subscription: WebhookSubscription) -> None:
        """Register a webhook subscription destination."""
        self._subscriptions[subscription.id] = subscription
        if subscription.id not in self._circuit_breakers:
            self._circuit_breakers[subscription.id] = CircuitBreaker(
                failure_threshold=self.circuit_failure_threshold,
                recovery_timeout=self.circuit_recovery_timeout,
            )

    def get_subscription(self, subscription_id: str) -> WebhookSubscription | None:
        """Retrieve subscription by identifier."""
        return self._subscriptions.get(subscription_id)

    def list_subscriptions(self) -> list[WebhookSubscription]:
        """List all registered webhook subscriptions."""
        return list(self._subscriptions.values())

    def remove_subscription(self, subscription_id: str) -> bool:
        """Remove a webhook subscription."""
        removed = self._subscriptions.pop(subscription_id, None) is not None
        self._circuit_breakers.pop(subscription_id, None)
        return removed

    def get_circuit_breaker(self, subscription_id: str) -> CircuitBreaker:
        """Get or create the CircuitBreaker instance for a given subscription."""
        if subscription_id not in self._circuit_breakers:
            self._circuit_breakers[subscription_id] = CircuitBreaker(
                failure_threshold=self.circuit_failure_threshold,
                recovery_timeout=self.circuit_recovery_timeout,
            )
        return self._circuit_breakers[subscription_id]

    async def dispatch(
        self,
        subscription: WebhookSubscription,
        event: DispatchEvent,
        client: httpx.AsyncClient | None = None,
    ) -> DeliveryAttempt:
        """Dispatch a single event to a specific webhook subscription.

        Executes delivery with HMAC signature stamping, circuit breaker protection,
        and configured exponential backoff retries.
        """
        if not subscription.matches_event(event.event_type):
            return DeliveryAttempt(
                event_id=event.event_id,
                subscription_id=subscription.id,
                url=subscription.url,
                status_code=None,
                success=False,
                error="Subscription filtered event type",
                attempt_number=0,
                duration_ms=0.0,
            )

        cb = self.get_circuit_breaker(subscription.id)
        if not cb.can_execute():
            err_msg = f"Delivery blocked: Circuit breaker for {subscription.url} is OPEN"
            attempt = DeliveryAttempt(
                event_id=event.event_id,
                subscription_id=subscription.id,
                url=subscription.url,
                status_code=None,
                success=False,
                error=err_msg,
                attempt_number=0,
                duration_ms=0.0,
            )
            async with self._lock:
                self._delivery_history.append(attempt)
            if self.dlq is not None:
                await self.dlq.add(
                    Task(
                        id=f"webhook_{event.event_id}",
                        priority=TaskPriority.HIGH,
                        payload={"event": event.model_dump(), "subscription_id": subscription.id},
                    ),
                    reason=err_msg,
                )
            return attempt

        # Prepare payload and cryptographic headers
        timestamp = int(event.timestamp)
        payload_json = json.dumps(event.payload, separators=(",", ":"), sort_keys=True)
        signature = WebhookSigner.sign(subscription.secret, timestamp, payload_json)

        headers = {
            "Content-Type": "application/json",
            "X-Nexus-Signature": signature,
            "X-Nexus-Timestamp": str(timestamp),
            "X-Nexus-Event-Id": event.event_id,
            "X-Nexus-Event-Type": event.event_type,
        }

        should_close_client = False
        if client is None:
            client = httpx.AsyncClient(timeout=self.default_timeout)
            should_close_client = True

        last_error: str | None = None
        last_status_code: int | None = None
        attempt_count = 0
        total_duration_ms = 0.0

        try:
            for attempt_idx in range(1, self.retry_policy.max_attempts + 1):
                attempt_count = attempt_idx
                t_start = time.time()
                try:
                    resp = await client.post(
                        subscription.url,
                        content=payload_json,
                        headers=headers,
                    )
                    duration_ms = (time.time() - t_start) * 1000.0
                    total_duration_ms += duration_ms
                    last_status_code = resp.status_code

                    if 200 <= resp.status_code < 300:
                        cb.record_success()
                        success_attempt = DeliveryAttempt(
                            event_id=event.event_id,
                            subscription_id=subscription.id,
                            url=subscription.url,
                            status_code=resp.status_code,
                            success=True,
                            error=None,
                            attempt_number=attempt_count,
                            duration_ms=total_duration_ms,
                        )
                        async with self._lock:
                            self._delivery_history.append(success_attempt)
                        return success_attempt

                    # Non-2xx response
                    err_msg = f"HTTP {resp.status_code}: {resp.text[:200]}"
                    last_error = err_msg

                    # Treat 5xx as server failures for circuit breaker
                    if resp.status_code >= 500:
                        cb.record_failure()

                    # 4xx client errors (e.g. 400 Bad Request, 401 Unauthorized, 404 Not Found)
                    # are typically not transient; don't retry 4xx errors
                    if 400 <= resp.status_code < 500:
                        break

                except (httpx.RequestError, asyncio.TimeoutError) as exc:
                    duration_ms = (time.time() - t_start) * 1000.0
                    total_duration_ms += duration_ms
                    last_error = f"Network error: {exc!s}"
                    cb.record_failure()

                # If we have attempts left, sleep backoff with jitter
                if self.retry_policy.should_retry(attempt_idx):
                    delay = self.retry_policy.calculate_delay(attempt_idx)
                    await asyncio.sleep(delay)

        finally:
            if should_close_client:
                await client.aclose()

        # All retry attempts exhausted or unretryable error
        failed_attempt = DeliveryAttempt(
            event_id=event.event_id,
            subscription_id=subscription.id,
            url=subscription.url,
            status_code=last_status_code,
            success=False,
            error=last_error,
            attempt_number=attempt_count,
            duration_ms=total_duration_ms,
        )

        async with self._lock:
            self._delivery_history.append(failed_attempt)

        if self.dlq is not None:
            await self.dlq.add(
                Task(
                    id=f"webhook_{event.event_id}",
                    priority=TaskPriority.HIGH,
                    payload={"event": event.model_dump(), "subscription_id": subscription.id},
                ),
                reason=f"Exhausted {attempt_count} attempts: {last_error}",
            )

        return failed_attempt

    async def broadcast(
        self,
        event: DispatchEvent,
        subscriptions: list[WebhookSubscription] | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> list[DeliveryAttempt]:
        """Broadcast an event to all matching active webhook subscriptions concurrently."""
        target_subs = (
            subscriptions
            if subscriptions is not None
            else [s for s in self._subscriptions.values() if s.is_active]
        )

        if not target_subs:
            return []

        should_close_client = False
        if client is None:
            client = httpx.AsyncClient(timeout=self.default_timeout)
            should_close_client = True

        try:
            tasks = [self.dispatch(sub, event, client=client) for sub in target_subs]
            results = await asyncio.gather(*tasks, return_exceptions=False)
            return list(results)
        finally:
            if should_close_client:
                await client.aclose()

    def get_history(self, limit: int = 100) -> list[DeliveryAttempt]:
        """Return recent delivery attempt records."""
        return self._delivery_history[-limit:]
