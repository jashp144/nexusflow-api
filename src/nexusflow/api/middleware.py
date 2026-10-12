"""FastAPI middleware for request tracing, structured logging, and error handling."""

import logging
import time
import uuid
from collections.abc import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

logger = logging.getLogger("nexusflow.access")


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Propagate or generate unique X-Request-ID correlation headers for every HTTP call."""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Response]
    ) -> Response:
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        request.state.request_id = request_id

        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response


class StructuredLoggingMiddleware(BaseHTTPMiddleware):
    """Record structured operational access logs with latency telemetry."""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Response]
    ) -> Response:
        start_time = time.time()
        request_id = getattr(request.state, "request_id", "unknown")

        try:
            response = await call_next(request)
            duration_ms = (time.time() - start_time) * 1000.0
            logger.info(
                "request_completed method=%s path=%s status=%d duration_ms=%.2f request_id=%s",
                request.method,
                request.url.path,
                response.status_code,
                duration_ms,
                request_id,
            )
            return response
        except Exception as exc:
            duration_ms = (time.time() - start_time) * 1000.0
            logger.error(
                "request_failed method=%s path=%s error=%s duration_ms=%.2f request_id=%s",
                request.method,
                request.url.path,
                str(exc),
                duration_ms,
                request_id,
            )
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal Server Error",
                    "detail": str(exc),
                    "request_id": request_id,
                },
                headers={"X-Request-ID": request_id},
            )
