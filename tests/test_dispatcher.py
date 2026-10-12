"""Tests for WebhookDispatcher, mock transport delivery, and retry behavior."""

import httpx
import pytest

from nexusflow.dispatch.dispatcher import WebhookDispatcher
from nexusflow.dispatch.retry import RetryPolicy
from nexusflow.dispatch.signer import WebhookSigner
from nexusflow.models.webhook import DispatchEvent, WebhookSubscription
from nexusflow.queue.dlq import DeadLetterQueue


@pytest.mark.asyncio
async def test_dispatcher_successful_delivery():
    """Verify webhook is signed and delivered successfully via mock HTTP transport."""
    received_headers = {}
    received_body = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal received_headers, received_body
        received_headers = dict(request.headers)
        received_body = request.read().decode("utf-8")
        return httpx.Response(200, json={"status": "received"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        dispatcher = WebhookDispatcher()
        sub = WebhookSubscription(
            id="sub_1",
            url="https://example.com/webhook",
            secret="super-secret-key",
            event_types=["*"],
        )
        dispatcher.add_subscription(sub)

        event = DispatchEvent(
            event_id="evt_100",
            event_type="order.created",
            payload={"order_id": 12345, "total": 99.50},
        )

        attempt = await dispatcher.dispatch(sub, event, client=client)

        assert attempt.success is True
        assert attempt.status_code == 200
        assert attempt.attempt_number == 1

        # Verify HMAC signature received on target
        sig = received_headers.get("x-nexus-signature")
        ts = received_headers.get("x-nexus-timestamp")
        assert sig is not None
        assert ts is not None
        assert WebhookSigner.verify(sub.secret, float(ts), received_body, sig) is True


@pytest.mark.asyncio
async def test_dispatcher_retry_on_transient_failure():
    """Verify dispatcher retries on HTTP 500 and succeeds on subsequent attempt."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(503, text="Service Temporarily Unavailable")
        return httpx.Response(200, json={"status": "ok"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        retry_policy = RetryPolicy(max_attempts=3, base_delay=0.01, jitter=False)
        dispatcher = WebhookDispatcher(retry_policy=retry_policy)

        sub = WebhookSubscription(
            id="sub_retry",
            url="https://example.com/transient",
            secret="secret",
        )
        event = DispatchEvent(event_type="test.event", payload={"data": 1})

        attempt = await dispatcher.dispatch(sub, event, client=client)

        assert attempt.success is True
        assert attempt.status_code == 200
        assert attempt.attempt_number == 2
        assert call_count == 2


@pytest.mark.asyncio
async def test_dispatcher_exhausted_retries_routes_to_dlq():
    """Verify permanent 500 failures exhaust retries and route to DLQ."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="Internal Server Error")

    transport = httpx.MockTransport(handler)
    dlq = DeadLetterQueue()
    async with httpx.AsyncClient(transport=transport) as client:
        retry_policy = RetryPolicy(max_attempts=3, base_delay=0.01, jitter=False)
        dispatcher = WebhookDispatcher(retry_policy=retry_policy, dlq=dlq)

        sub = WebhookSubscription(
            id="sub_fail",
            url="https://example.com/permanent-failure",
            secret="secret",
        )
        event = DispatchEvent(event_type="test.fail", payload={"order": "broken"})

        attempt = await dispatcher.dispatch(sub, event, client=client)

        assert attempt.success is False
        assert attempt.status_code == 500
        assert attempt.attempt_number == 3
        assert dlq.size() == 1

        stored = await dlq.get_by_id(f"webhook_{event.event_id}")
        assert stored is not None
        assert "Exhausted 3 attempts" in stored.error


@pytest.mark.asyncio
async def test_dispatcher_non_retryable_client_error():
    """Verify 4xx client errors (e.g. 400 Bad Request) are not retried indefinitely."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(400, text="Bad Request Payload")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        retry_policy = RetryPolicy(max_attempts=5, base_delay=0.01, jitter=False)
        dispatcher = WebhookDispatcher(retry_policy=retry_policy)

        sub = WebhookSubscription(
            id="sub_client_err",
            url="https://example.com/400",
            secret="secret",
        )
        event = DispatchEvent(event_type="test.bad", payload={})

        attempt = await dispatcher.dispatch(sub, event, client=client)

        assert attempt.success is False
        assert attempt.status_code == 400
        # Fails immediately without running through all 5 attempts
        assert call_count == 1


@pytest.mark.asyncio
async def test_dispatcher_circuit_breaker_blocks_when_open():
    """Verify tripped circuit breaker blocks subsequent dispatches immediately."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="Downstream Server Dead")

    transport = httpx.MockTransport(handler)
    dlq = DeadLetterQueue()
    async with httpx.AsyncClient(transport=transport) as client:
        retry_policy = RetryPolicy(max_attempts=1, base_delay=0.01, jitter=False)
        dispatcher = WebhookDispatcher(
            retry_policy=retry_policy,
            dlq=dlq,
            circuit_failure_threshold=2,
            circuit_recovery_timeout=10.0,
        )

        sub = WebhookSubscription(id="sub_cb", url="https://example.com/cb", secret="s")
        dispatcher.add_subscription(sub)

        # 1st failure
        await dispatcher.dispatch(sub, DispatchEvent(event_type="e1", payload={}), client=client)
        # 2nd failure trips circuit
        await dispatcher.dispatch(sub, DispatchEvent(event_type="e2", payload={}), client=client)

        cb = dispatcher.get_circuit_breaker(sub.id)
        assert cb.can_execute() is False

        # 3rd dispatch should be blocked by circuit breaker without network calls
        attempt3 = await dispatcher.dispatch(
            sub, DispatchEvent(event_type="e3", payload={}), client=client
        )
        assert attempt3.success is False
        assert "Circuit breaker for https://example.com/cb is OPEN" in attempt3.error


@pytest.mark.asyncio
async def test_dispatcher_broadcast_and_filtering():
    """Verify broadcast dispatches concurrently and respects event_type filters."""
    delivered_urls = []

    def handler(request: httpx.Request) -> httpx.Response:
        delivered_urls.append(str(request.url))
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        dispatcher = WebhookDispatcher()

        sub_all = WebhookSubscription(
            id="sub_all", url="https://endpoint-a.com/hook", secret="s1", event_types=["*"]
        )
        sub_orders = WebhookSubscription(
            id="sub_orders",
            url="https://endpoint-b.com/hook",
            secret="s2",
            event_types=["order.completed"],
        )
        sub_users = WebhookSubscription(
            id="sub_users",
            url="https://endpoint-c.com/hook",
            secret="s3",
            event_types=["user.created"],
        )

        dispatcher.add_subscription(sub_all)
        dispatcher.add_subscription(sub_orders)
        dispatcher.add_subscription(sub_users)

        # Broadcast order.completed event: sub_all and sub_orders should receive it, sub_users skipped
        event = DispatchEvent(event_type="order.completed", payload={"id": 99})
        attempts = await dispatcher.broadcast(event, client=client)

        assert len(attempts) == 3
        # 2 successful deliveries
        successful = [a for a in attempts if a.success]
        assert len(successful) == 2

        # 1 filtered out
        filtered = [a for a in attempts if not a.success and a.attempt_number == 0]
        assert len(filtered) == 1
        assert filtered[0].subscription_id == "sub_users"


def test_dispatcher_subscription_crud():
    """Verify subscription add, list, get, and delete operations."""
    dispatcher = WebhookDispatcher()
    sub = WebhookSubscription(id="s1", url="https://test.com", secret="k")

    dispatcher.add_subscription(sub)
    assert len(dispatcher.list_subscriptions()) == 1
    assert dispatcher.get_subscription("s1") == sub

    assert dispatcher.remove_subscription("s1") is True
    assert dispatcher.get_subscription("s1") is None
    assert dispatcher.remove_subscription("s1") is False
