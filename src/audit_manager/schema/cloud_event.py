"""
CloudEvents 1.0 envelope for OpenG2P audit events.

Callers POST a CloudEvent. The envelope is validated by FastAPI via pydantic
before we accept the event; the `data` payload is event-type-specific and kept
as an open dict (callers own their schemas). Three sub-fields inside `data` are
standardized across all OpenG2P events: actor, action, outcome.
"""

from __future__ import annotations

import json
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
        """Flatten to a dict matching the audit_events table columns.

        Promoted columns (flat, indexed) come from the CloudEvents envelope
        and the standardized `data` sub-fields (actor/action/outcome/reason).
        Anything else inside `data` — resource extras like amount/currency,
        changes[] diffs, context{} — goes into the `details` JSONB column.
        Events with no extras (e.g. plain logins) get details = NULL.
        """
        d = self.data
        details = _compute_details(d)
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
            "reason": d.reason,
            "trace_id": _trace_id_from_parent(self.traceparent),
            # asyncpg wants a JSON string (or None) for JSONB casts — not a dict.
            "details": json.dumps(details) if details is not None else None,
        }


def _trace_id_from_parent(traceparent: Optional[str]) -> Optional[str]:
    """Extract the 16-byte trace id from a W3C traceparent header."""
    if not traceparent:
        return None
    parts = traceparent.split("-")
    if len(parts) >= 3:
        return parts[1]
    return None


# `data` sub-fields that are FULLY promoted to flat columns — drop entirely
# from details (no information left to keep).
_FULLY_PROMOTED_DATA_FIELDS = {"action", "outcome", "reason"}

# `data` sub-fields where ONLY a couple of attributes are promoted to flat
# columns. The remaining attributes (e.g. actor.name/roles/ip,
# resource.amount/currency/program_id) are preserved under `details.<field>`.
_PARTIALLY_PROMOTED_SUB_ATTRS = {
    "actor": {"id", "type"},
    "resource": {"id", "type"},
}


def _is_meaningful(value: Any) -> bool:
    """True if value carries information beyond emptiness.

    Filters out None and empty collections/strings — `roles: []` after
    stripping `id`/`type` from actor shouldn't make `details.actor` materialise.
    """
    if value is None:
        return False
    if isinstance(value, (list, dict, str)) and len(value) == 0:
        return False
    return True


def _compute_details(d: "AuditData") -> Optional[dict[str, Any]]:
    """Return event-type-specific extras from `data`, or None if there aren't any.

    For each `data` sub-field:
      * `action`, `outcome`, `reason`  → fully promoted → dropped
      * `actor`     → keep all attrs except {id, type} (those are flat columns).
                      Empty extras like `roles: []` are also dropped so we don't
                      materialise `details.actor` for events that have no real
                      actor info beyond id + type.
      * `resource`  → keep all attrs except {id, type}; same emptiness filter
      * everything else (`changes`, `context`, custom keys) → pass through
        (None values already filtered by `exclude_none=True`)
    """
    raw = d.model_dump(mode="json", exclude_none=True)
    extras: dict[str, Any] = {}
    for key, value in raw.items():
        if key in _FULLY_PROMOTED_DATA_FIELDS:
            continue
        if key in _PARTIALLY_PROMOTED_SUB_ATTRS:
            promoted = _PARTIALLY_PROMOTED_SUB_ATTRS[key]
            if isinstance(value, dict):
                rest = {
                    k: v for k, v in value.items()
                    if k not in promoted and _is_meaningful(v)
                }
                if rest:
                    extras[key] = rest
            continue
        extras[key] = value
    return extras if extras else None


class EventBatch(BaseModel):
    """Batched ingest payload."""

    events: list[CloudEvent]
