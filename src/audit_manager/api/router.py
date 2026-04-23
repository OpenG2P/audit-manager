"""
FastAPI router — API contract is documented in-place so the generated
OpenAPI spec (and whatever renders it — Swagger UI, ReDoc, GitBook's
OpenAPI plugin) carries the full story without any parallel markdown.

Endpoints (all under /v1/auditmanager):
  POST   /events          Ingest a single CloudEvent.
  POST   /events/batch    Ingest up to `ingest.max_batch_size` CloudEvents.
  GET    /health          Liveness+readiness rolled into one.
  GET    /version         Service version & build metadata.
  GET    /config          Effective non-sensitive configuration.

The ingest endpoints return 202 Accepted immediately after the event is
enqueued — the HTTP caller never waits on Kafka or Postgres.
"""

from __future__ import annotations

import asyncio
import importlib.metadata
import os
from datetime import datetime, timezone

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from ..config import get_settings
from ..schema.cloud_event import CloudEvent, EventBatch
from .models import (
    AcceptedPayload,
    AcceptedResponse,
    BatchAcceptedPayload,
    BatchAcceptedResponse,
    ConfigPayload,
    ConfigResponse,
    ErrorResponse,
    HealthPayload,
    HealthResponse,
    VersionPayload,
    VersionResponse,
)
from .schema import make_error_response

router = APIRouter(prefix="/v1/auditmanager")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# Shared `responses=` description fragments for the ingest endpoints.
_INGEST_503_DESCRIPTION = (
    "Service not ready or backpressure.\n\n"
    "- `AUD-004` — ingest queue full. Retry with exponential backoff + jitter.\n"
    "- `AUD-005` — service startup not complete.\n"
    "- `AUD-006` — database health check failed (surfaces via `/health`, but "
    "also blocks ingest readiness indirectly)."
)
_VALIDATION_422_DESCRIPTION = (
    "Malformed CloudEvent — failed schema validation (missing required field, "
    "invalid `outcome` enum, unparseable `time`, etc.)."
)


# -----------------------------------------------------------------------------
# POST /events — ingest one event
# -----------------------------------------------------------------------------
@router.post(
    "/events",
    status_code=202,
    summary="Ingest a single CloudEvent",
    description=(
        "Accepts one CloudEvent, validates its schema, and enqueues it for "
        "durable publication to Kafka.\n\n"
        "Returns **202 Accepted** the moment the event lands in the in-process "
        "queue — Kafka and Postgres latency are fully hidden from the caller. "
        "Delivery from there is at-least-once with idempotent inserts on "
        "(`id`, `occurred_at`), so replays after a crash never produce duplicate "
        "rows in the audit store."
    ),
    response_model=AcceptedResponse,
    responses={
        202: {
            "model": AcceptedResponse,
            "description": "Event accepted into the ingest queue. The `id` you submitted is echoed back.",
        },
        422: {"model": ErrorResponse, "description": _VALIDATION_422_DESCRIPTION},
        503: {"model": ErrorResponse, "description": _INGEST_503_DESCRIPTION},
    },
)
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

    return AcceptedResponse(
        responsetime=_now_iso(),
        response=AcceptedPayload(accepted=event.id),
    )


# -----------------------------------------------------------------------------
# POST /events/batch — ingest many
# -----------------------------------------------------------------------------
@router.post(
    "/events/batch",
    status_code=202,
    summary="Ingest a batch of CloudEvents",
    description=(
        "Accepts up to `ingest.max_batch_size` CloudEvents in a single request. "
        "Each event is validated and enqueued independently — same 202 semantics "
        "as `/events`. Events in the batch are not atomic: on queue overflow "
        "part way through, the response reports the count actually accepted.\n\n"
        "Events in a batch become **separate rows** in Postgres (one per "
        "CloudEvent). Batching is a transport optimization — not a storage "
        "concept."
    ),
    response_model=BatchAcceptedResponse,
    responses={
        202: {
            "model": BatchAcceptedResponse,
            "description": "All events (or as many as fit before backpressure) accepted. The response echoes accepted ids.",
        },
        400: {
            "model": ErrorResponse,
            "description": (
                "`AUD-007` — batch payload exceeds the configured "
                "`ingest.max_batch_size`. Split into smaller batches."
            ),
        },
        422: {"model": ErrorResponse, "description": _VALIDATION_422_DESCRIPTION},
        503: {"model": ErrorResponse, "description": _INGEST_503_DESCRIPTION},
    },
)
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

    return BatchAcceptedResponse(
        responsetime=_now_iso(),
        response=BatchAcceptedPayload(accepted=accepted, count=len(accepted)),
    )


# -----------------------------------------------------------------------------
# GET /health
# -----------------------------------------------------------------------------
@router.get(
    "/health",
    summary="Health / readiness probe",
    description=(
        "Combined liveness + readiness probe. Returns **200** with "
        "`status: UP` only when service startup is complete **and** the "
        "Postgres connection is healthy. Kubernetes probes in the shipped "
        "Helm chart point at this endpoint."
    ),
    response_model=HealthResponse,
    responses={
        200: {"model": HealthResponse, "description": "Service is ready to serve traffic."},
        503: {
            "model": ErrorResponse,
            "description": (
                "Not ready.\n\n"
                "- `AUD-005` — service startup not yet complete.\n"
                "- `AUD-006` — Postgres health check failed."
            ),
        },
    },
)
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

    return HealthResponse(
        responsetime=_now_iso(),
        response=HealthPayload(status="UP"),
    )


# -----------------------------------------------------------------------------
# GET /version
# -----------------------------------------------------------------------------
@router.get(
    "/version",
    summary="Service version and build metadata",
    description=(
        "Returns the running service version plus the git commit and build "
        "timestamp baked into the Docker image. Useful from probes and smoke "
        "tests to confirm which image is actually deployed."
    ),
    response_model=VersionResponse,
    responses={
        200: {"model": VersionResponse, "description": "Version metadata."},
    },
)
async def version():
    try:
        svc_version = importlib.metadata.version("audit-manager")
    except importlib.metadata.PackageNotFoundError:
        svc_version = "0.1.0-dev"

    return VersionResponse(
        responsetime=_now_iso(),
        response=VersionPayload(
            service_version=svc_version,
            build_time=os.environ.get("BUILD_TIME", "dev"),
            git_commit=os.environ.get("GIT_COMMIT", "dev"),
        ),
    )


# -----------------------------------------------------------------------------
# GET /config — non-sensitive effective config
# -----------------------------------------------------------------------------
@router.get(
    "/config",
    summary="Effective non-sensitive configuration",
    description=(
        "Returns the non-sensitive portion of the effective service "
        "configuration — ingest tunables, Kafka topic/group names, and "
        "partition-maintenance settings. Secrets and Kafka bootstrap URLs "
        "are deliberately excluded."
    ),
    response_model=ConfigResponse,
    responses={
        200: {"model": ConfigResponse, "description": "Effective configuration snapshot."},
    },
)
async def config_view():
    cfg = get_settings().audit_manager
    return ConfigResponse(
        responsetime=_now_iso(),
        response=ConfigPayload(
            service_id=cfg.service_id,
            api_version=cfg.api_version,
            ingest=cfg.ingest.model_dump(),
            kafka={
                "topic": cfg.kafka.topic,
                "dlq_topic": cfg.kafka.dlq_topic,
                "consumer_group": cfg.kafka.consumer_group,
            },
            database=cfg.database.model_dump(),
        ),
    )
