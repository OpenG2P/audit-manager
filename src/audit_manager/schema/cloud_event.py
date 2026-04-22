"""
CloudEvents 1.0 envelope for OpenG2P audit events.

Callers POST a CloudEvent. The envelope is validated by FastAPI via pydantic
before we accept the event; the `data` payload is event-type-specific and kept
as an open dict (callers own their schemas). Three sub-fields inside `data` are
standardized across all OpenG2P events: actor, action, outcome.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


# -----------------------------------------------------------------------------
# Standardized sub-fields inside `data`
# -----------------------------------------------------------------------------
class Actor(BaseModel):
    """Who (or what system) triggered the event."""

    type: Literal["user", "system", "service", "anonymous"] = "user"
    id: str
    name: Optional[str] = None
    roles: list[str] = Field(default_factory=list)
    ip: Optional[str] = None
    session_id: Optional[str] = None


class Resource(BaseModel):
    """The primary object acted upon (optional for plain logins)."""

    type: str
    id: Optional[str] = None
    model_config = ConfigDict(extra="allow")


class AuditData(BaseModel):
    """OpenG2P-specific `data` shape. Open — extra fields allowed per event type."""

    actor: Actor
    action: str
    outcome: Literal["success", "failure", "denied"]
    resource: Optional[Resource] = None
    reason: Optional[str] = None

    model_config = ConfigDict(extra="allow")


# -----------------------------------------------------------------------------
# CloudEvents 1.0 envelope
# -----------------------------------------------------------------------------
class CloudEvent(BaseModel):
    """
    CloudEvents v1.0 envelope.

    Spec: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md
    """

    specversion: Literal["1.0"] = "1.0"
    id: str = Field(default_factory=lambda: str(uuid4()))
    source: str  # e.g. "/openg2p/beneficiary-service"
    type: str  # e.g. "org.openg2p.beneficiary.updated"
    subject: Optional[str] = None  # e.g. "beneficiary/b_1029384756"
    time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    datacontenttype: str = "application/json"
    traceparent: Optional[str] = None  # W3C trace context

    data: AuditData

    # CloudEvents allows arbitrary context attributes at the top level.
    model_config = ConfigDict(extra="allow")

    @field_validator("time", mode="before")
    @classmethod
    def _parse_time(cls, v: Any) -> Any:
        if isinstance(v, str):
            # Accept RFC3339 with or without 'Z'
            if v.endswith("Z"):
                v = v[:-1] + "+00:00"
        return v

    def to_record(self) -> dict[str, Any]:
        """Flatten to a dict matching the audit_events table columns."""
        d = self.data
        return {
            "id": self.id,
            "occurred_at": self.time,
            "source": self.source,
            "type": self.type,
            "subject": self.subject,
            "actor_type": d.actor.type,
            "actor_id": d.actor.id,
            "resource_type": d.resource.type if d.resource else None,
            "resource_id": d.resource.id if d.resource else None,
            "action": d.action,
            "outcome": d.outcome,
            "trace_id": _trace_id_from_parent(self.traceparent),
            "envelope": self.model_dump(mode="json"),
        }


def _trace_id_from_parent(traceparent: Optional[str]) -> Optional[str]:
    """Extract the 16-byte trace id from a W3C traceparent header."""
    if not traceparent:
        return None
    parts = traceparent.split("-")
    if len(parts) >= 3:
        return parts[1]
    return None


class EventBatch(BaseModel):
    """Batched ingest payload."""

    events: list[CloudEvent]
