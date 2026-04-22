"""
FastAPI router.

Endpoints (all under /v1/auditmanager):
  POST   /events          Ingest a single CloudEvent.
  POST   /events/batch    Ingest up to `ingest.max_batch_size` CloudEvents.
  GET    /health          Liveness+readiness rolled into one.
  GET    /version         Service version & build metadata.
  GET    /config          Effective non-sensitive configuration.

The ingest endpoints return 202 Accepted immediately after the event is handed
to the in-process queue — the HTTP caller never waits on Kafka or Postgres.
"""

from __future__ import annotations

import asyncio
import importlib.metadata
import os

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from ..config import get_settings
from ..schema.cloud_event import CloudEvent, EventBatch
from .schema import make_error_response, make_response

router = APIRouter(prefix="/v1/auditmanager")


# -----------------------------------------------------------------------------
# POST /events — ingest one event
# -----------------------------------------------------------------------------
@router.post("/events", status_code=202)
async def ingest_event(event: CloudEvent):
    from ..main import get_producer, is_startup_complete

    if not is_startup_complete():
        return JSONResponse(
            status_code=503,
            content=make_error_response("AUD-005", "Service not ready"),
        )

    try:
        get_producer().enqueue(event)
    except asyncio.QueueFull:
        return JSONResponse(
            status_code=503,
            content=make_error_response(
                "AUD-004", "Audit ingest queue full — backpressure"
            ),
        )

    return JSONResponse(
        status_code=202, content=make_response({"accepted": event.id})
    )


# -----------------------------------------------------------------------------
# POST /events/batch — ingest many
# -----------------------------------------------------------------------------
@router.post("/events/batch", status_code=202)
async def ingest_batch(batch: EventBatch):
    from ..main import get_producer, is_startup_complete

    if not is_startup_complete():
        return JSONResponse(
            status_code=503,
            content=make_error_response("AUD-005", "Service not ready"),
        )

    settings = get_settings()
    limit = settings.audit_manager.ingest.max_batch_size
    if len(batch.events) > limit:
        return JSONResponse(
            status_code=400,
            content=make_error_response(
                "AUD-007",
                f"Batch size {len(batch.events)} exceeds limit {limit}",
            ),
        )

    producer = get_producer()
    accepted: list[str] = []
    try:
        for ev in batch.events:
            producer.enqueue(ev)
            accepted.append(ev.id)
    except asyncio.QueueFull:
        return JSONResponse(
            status_code=503,
            content=make_error_response(
                "AUD-004",
                f"Queue full after accepting {len(accepted)}/{len(batch.events)}",
            ),
        )

    return JSONResponse(
        status_code=202,
        content=make_response({"accepted": accepted, "count": len(accepted)}),
    )


# -----------------------------------------------------------------------------
# GET /health
# -----------------------------------------------------------------------------
@router.get("/health")
async def health():
    from sqlalchemy import text

    from ..db import get_engine
    from ..main import is_startup_complete

    if not is_startup_complete():
        return JSONResponse(
            status_code=503,
            content=make_error_response(
                "AUD-005", "Service not ready: startup not complete"
            ),
        )

    try:
        engine = get_engine()
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as e:  # noqa: BLE001
        return JSONResponse(
            status_code=503,
            content=make_error_response(
                "AUD-006", f"Database health check failed: {e}"
            ),
        )

    return JSONResponse(
        status_code=200, content=make_response({"status": "UP"})
    )


# -----------------------------------------------------------------------------
# GET /version
# -----------------------------------------------------------------------------
@router.get("/version")
async def version():
    try:
        svc_version = importlib.metadata.version("audit-manager")
    except importlib.metadata.PackageNotFoundError:
        svc_version = "0.1.0-dev"

    return JSONResponse(
        status_code=200,
        content=make_response(
            {
                "service_version": svc_version,
                "build_time": os.environ.get("BUILD_TIME", "dev"),
                "git_commit": os.environ.get("GIT_COMMIT", "dev"),
            }
        ),
    )


# -----------------------------------------------------------------------------
# GET /config — non-sensitive effective config
# -----------------------------------------------------------------------------
@router.get("/config")
async def config_view():
    cfg = get_settings().audit_manager
    return JSONResponse(
        status_code=200,
        content=make_response(
            {
                "service_id": cfg.service_id,
                "api_version": cfg.api_version,
                "ingest": cfg.ingest.model_dump(),
                "kafka": {
                    "topic": cfg.kafka.topic,
                    "dlq_topic": cfg.kafka.dlq_topic,
                    "consumer_group": cfg.kafka.consumer_group,
                },
                "database": cfg.database.model_dump(),
            }
        ),
    )
