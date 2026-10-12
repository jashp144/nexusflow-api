"""Webhook subscription management and event dispatch endpoints."""

from fastapi import APIRouter, Request, status

from nexusflow.models.webhook import (
    DeliveryAttempt,
    DispatchEvent,
    WebhookSubscribeRequest,
    WebhookSubscription,
)

router = APIRouter(tags=["Webhooks"])


@router.post(
    "/webhooks/subscribe",
    response_model=WebhookSubscription,
    status_code=status.HTTP_201_CREATED,
)
async def subscribe_webhook(
    sub_req: WebhookSubscribeRequest,
    request: Request,
) -> WebhookSubscription:
    """Register a new webhook subscription destination."""
    dispatcher = request.app.state.dispatcher
    subscription = WebhookSubscription(
        url=sub_req.url,
        secret=sub_req.secret,
        event_types=sub_req.event_types,
    )
    dispatcher.add_subscription(subscription)
    return subscription


@router.get("/webhooks", response_model=list[WebhookSubscription])
async def list_webhooks(request: Request) -> list[WebhookSubscription]:
    """List all currently active webhook subscriptions."""
    dispatcher = request.app.state.dispatcher
    return dispatcher.list_subscriptions()


@router.post("/webhooks/dispatch", response_model=list[DeliveryAttempt])
async def dispatch_webhook_event(
    event: DispatchEvent,
    request: Request,
) -> list[DeliveryAttempt]:
    """Trigger an event broadcast to all subscribed webhook destinations."""
    dispatcher = request.app.state.dispatcher
    return await dispatcher.broadcast(event)
