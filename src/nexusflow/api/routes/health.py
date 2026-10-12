"""Health check probes for container orchestration and load balancers."""

import time
from typing import Any

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

router = APIRouter(tags=["Health"])


@router.get("/health/live", response_model=dict[str, Any])
async def liveness_probe() -> dict[str, Any]:
    """Liveness probe verifying the API event loop is responsive."""
    return {
        "status": "alive",
        "service": "nexusflow-api",
        "timestamp": time.time(),
    }


@router.get("/health/ready", response_model=dict[str, Any])
async def readiness_probe(request: Request) -> JSONResponse:
    """Readiness probe verifying background queue and worker operational readiness."""
    is_ready = True
    details: dict[str, Any] = {
        "service": "nexusflow-api",
        "timestamp": time.time(),
    }

    try:
        queue = getattr(request.app.state, "queue", None)
        dlq = getattr(request.app.state, "dlq", None)
        dispatcher = getattr(request.app.state, "dispatcher", None)

        if queue is None or dlq is None or dispatcher is None:
            is_ready = False
            details["error"] = "Subsystems not initialized"
        else:
            details["queue_pending"] = queue.qsize()
            details["dlq_size"] = dlq.size()
            details["subscriptions_count"] = len(dispatcher.list_subscriptions())
    except Exception as exc:
        is_ready = False
        details["error"] = str(exc)

    status_code = status.HTTP_200_OK if is_ready else status.HTTP_503_SERVICE_UNAVAILABLE
    details["status"] = "ready" if is_ready else "degraded"
    return JSONResponse(status_code=status_code, content=details)
