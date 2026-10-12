"""Webhook domain models and schemas for nexusflow."""

import time
import uuid
from typing import Any

from pydantic import BaseModel, Field


class WebhookSubscription(BaseModel):
    """Model representing an active or inactive webhook subscription destination."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    url: str
    secret: str
    event_types: list[str] = Field(default_factory=lambda: ["*"])
    is_active: bool = True
    created_at: float = Field(default_factory=time.time)

    def matches_event(self, event_type: str) -> bool:
        """Check if subscription is interested in a given event type."""
        if not self.is_active:
            return False
        return "*" in self.event_types or event_type in self.event_types


class DispatchEvent(BaseModel):
    """Event payload to be dispatched to subscribed webhooks."""

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: float = Field(default_factory=time.time)


class DeliveryAttempt(BaseModel):
    """Telemetry record for an individual HTTP webhook delivery attempt."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_id: str
    subscription_id: str
    url: str
    status_code: int | None = None
    success: bool
    error: str | None = None
    attempt_number: int = 1
    duration_ms: float = 0.0
    timestamp: float = Field(default_factory=time.time)


class WebhookSubscribeRequest(BaseModel):
    """API request schema for registering a webhook subscription."""

    url: str = Field(description="Target HTTPS/HTTP webhook destination URL")
    secret: str = Field(description="Shared cryptographic secret for HMAC-SHA256 signing")
    event_types: list[str] = Field(
        default_factory=lambda: ["*"],
        description="Event types to subscribe to (use '*' for all events)",
    )
