"""
Pydantic response models for the Audit Manager API.

These are the typed shapes every endpoint returns. FastAPI picks them up via
`response_model=` and `responses={}` so the generated OpenAPI spec carries:

  * a ready-to-read schema for the response envelope,
  * a concrete example payload per endpoint,
  * a full description of every documented status code (202/400/422/503),
  * the OpenG2P error-code catalog (AUD-004, AUD-005, AUD-006, AUD-007).

Narrative docs elsewhere (GitBook) stay thin — everything a consumer needs
to integrate is in the spec.
"""

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


# -----------------------------------------------------------------------------
# Envelope building blocks
# -----------------------------------------------------------------------------
class ErrorDetail(BaseModel):
    """One error entry in the `errors[]` array of the response envelope."""

    errorCode: str = Field(
        ...,
        description=(
            "OpenG2P-assigned error code. Catalog: "
            "`AUD-004` (ingest queue full — backpressure), "
            "`AUD-005` (service not ready — startup incomplete), "
            "`AUD-006` (database health check failed), "
            "`AUD-007` (batch payload exceeds configured max size)."
        ),
        examples=["AUD-004"],
    )
    message: str = Field(
        ...,
        description="Human-readable message describing the failure.",
        examples=["Audit ingest queue full — backpressure"],
    )


class _EnvelopeBase(BaseModel):
    """Shared top-level envelope fields across every response."""

    id: str = Field(
        default="openg2p.auditmanager",
        description="Constant — identifies the service that produced this envelope.",
        examples=["openg2p.auditmanager"],
    )
    version: str = Field(
        default="1.0",
        description="Envelope schema version (not the service version).",
        examples=["1.0"],
    )
    responsetime: str = Field(
        ...,
        description="RFC3339 timestamp at which this response was produced.",
        examples=["2026-04-23T10:00:00.000Z"],
    )


# -----------------------------------------------------------------------------
# Per-endpoint success payloads
# -----------------------------------------------------------------------------
class AcceptedPayload(BaseModel):
    accepted: str = Field(
        ...,
        description="The `id` of the CloudEvent that was accepted into the ingest queue.",
        examples=["01HXQ9R2V3K8Y2ZP7N5FJ6M4A1"],
    )


class BatchAcceptedPayload(BaseModel):
    accepted: List[str] = Field(
        ...,
        description="CloudEvent ids accepted into the ingest queue, in the order supplied.",
        examples=[["01HXQ9R2V...", "01HXQ9R2W...", "01HXQ9R2X..."]],
    )
    count: int = Field(
        ...,
        description="Number of events accepted (matches `len(accepted)`).",
        examples=[3],
    )


class HealthPayload(BaseModel):
    status: str = Field(
        ...,
        description="`UP` when ready to serve traffic; the envelope is 503 otherwise.",
        examples=["UP"],
    )


class VersionPayload(BaseModel):
    service_version: str = Field(..., examples=["0.1.0"])
    build_time: str = Field(
        ...,
        description="Image build timestamp baked in at `docker build` time.",
        examples=["2026-04-23T08:30:00.000Z"],
    )
    git_commit: str = Field(
        ...,
        description="Short git commit hash baked in at `docker build` time.",
        examples=["a1b2c3d"],
    )


class ConfigPayload(BaseModel):
    """Non-sensitive effective configuration. Shape follows `AuditManagerConfig`."""

    model_config = ConfigDict(extra="allow")

    service_id: str = Field(..., examples=["openg2p.auditmanager"])
    api_version: str = Field(..., examples=["1.0"])
    ingest: dict = Field(
        ...,
        description="Ingest-side config — queue bound, batch limit.",
        examples=[{"queue_max_size": 10000, "max_batch_size": 500}],
    )
    kafka: dict = Field(
        ...,
        description="Kafka-side non-sensitive config — topic, DLQ topic, consumer group.",
        examples=[{
            "topic": "openg2p.audit.events",
            "dlq_topic": "openg2p.audit.dlq",
            "consumer_group": "openg2p-audit-consumer",
        }],
    )
    database: dict = Field(
        ...,
        description="Postgres-side config — partition pre-create and retention.",
        examples=[{
            "partition_pre_create_months": 3,
            "partition_retention_months": 84,
            "partition_check_interval_seconds": 3600,
        }],
    )


# -----------------------------------------------------------------------------
# Full response envelopes (one per endpoint, plus a shared error envelope)
# -----------------------------------------------------------------------------
class AcceptedResponse(_EnvelopeBase):
    response: AcceptedPayload
    errors: List[ErrorDetail] = Field(default_factory=list, examples=[[]])


class BatchAcceptedResponse(_EnvelopeBase):
    response: BatchAcceptedPayload
    errors: List[ErrorDetail] = Field(default_factory=list, examples=[[]])


class HealthResponse(_EnvelopeBase):
    response: HealthPayload
    errors: List[ErrorDetail] = Field(default_factory=list, examples=[[]])


class VersionResponse(_EnvelopeBase):
    response: VersionPayload
    errors: List[ErrorDetail] = Field(default_factory=list, examples=[[]])


class ConfigResponse(_EnvelopeBase):
    response: ConfigPayload
    errors: List[ErrorDetail] = Field(default_factory=list, examples=[[]])


class ErrorResponse(_EnvelopeBase):
    """Shared shape for every non-2xx response — `response` is null, `errors` is populated."""

    response: Optional[dict] = Field(
        default=None,
        description="Always `null` on an error response.",
        examples=[None],
    )
    errors: List[ErrorDetail] = Field(
        ...,
        description="One entry per failure. Always at least one for a non-2xx response.",
        examples=[[{
            "errorCode": "AUD-004",
            "message": "Audit ingest queue full — backpressure",
        }]],
    )
