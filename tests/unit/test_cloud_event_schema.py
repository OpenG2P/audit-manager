"""
Unit tests for the CloudEvents schema.

Pure pydantic validation — no DB, no Kafka, no network. Runs in <100ms.

    pytest tests/unit/ -v
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from audit_manager.schema.cloud_event import (
    Actor,
    AuditData,
    CloudEvent,
    EventBatch,
)


SAMPLES_DIR = Path(__file__).parent.parent / "sample-events"


# -----------------------------------------------------------------------------
# Sample files round-trip — every shipped sample must parse cleanly.
# -----------------------------------------------------------------------------
def _iter_valid_sample_files():
    for path in sorted(SAMPLES_DIR.glob("*.json")):
        # 08-batch.json is an EventBatch, not a single CloudEvent
        # 99-* are intentionally invalid
        if path.name.startswith(("08-", "99-")):
            continue
        yield path


@pytest.mark.parametrize(
    "sample_path",
    list(_iter_valid_sample_files()),
    ids=lambda p: p.name,
)
def test_sample_event_parses(sample_path: Path) -> None:
    payload = json.loads(sample_path.read_text())
    ev = CloudEvent.model_validate(payload)
    assert ev.id == payload["id"]
    assert ev.type == payload["type"]
    assert ev.data.action == payload["data"]["action"]
    assert ev.data.outcome == payload["data"]["outcome"]


def test_sample_batch_parses() -> None:
    payload = json.loads((SAMPLES_DIR / "08-batch.json").read_text())
    batch = EventBatch.model_validate(payload)
    assert len(batch.events) == 3
    assert {e.id for e in batch.events} == {
        "smoke-08-batch-a",
        "smoke-08-batch-b",
        "smoke-08-batch-c",
    }


def test_invalid_sample_missing_actor_is_rejected() -> None:
    payload = json.loads(
        (SAMPLES_DIR / "99-invalid-missing-actor.json").read_text()
    )
    with pytest.raises(ValidationError):
        CloudEvent.model_validate(payload)


# -----------------------------------------------------------------------------
# Required field enforcement
# -----------------------------------------------------------------------------
def _base_event() -> dict:
    return {
        "specversion": "1.0",
        "id": "test-id",
        "source": "/test",
        "type": "org.openg2p.test.event",
        "time": "2026-04-22T14:00:00Z",
        "data": {
            "actor": {"type": "user", "id": "u_1"},
            "action": "login",
            "outcome": "success",
        },
    }


@pytest.mark.parametrize(
    "missing_top",
    ["source", "type"],
)
def test_missing_required_envelope_field_rejected(missing_top: str) -> None:
    payload = _base_event()
    del payload[missing_top]
    with pytest.raises(ValidationError):
        CloudEvent.model_validate(payload)


def test_missing_id_gets_generated() -> None:
    # CloudEvents requires `id`, but we auto-generate a UUIDv4 if missing
    # so that emitters without a dedup strategy still land in the store.
    # Production emitters should supply their own stable id for idempotency.
    payload = _base_event()
    del payload["id"]
    ev = CloudEvent.model_validate(payload)
    assert len(ev.id) >= 32  # uuid4 string


@pytest.mark.parametrize(
    "missing_data",
    ["actor", "action", "outcome"],
)
def test_missing_required_data_field_rejected(missing_data: str) -> None:
    payload = _base_event()
    del payload["data"][missing_data]
    with pytest.raises(ValidationError):
        CloudEvent.model_validate(payload)


def test_invalid_outcome_rejected() -> None:
    payload = _base_event()
    payload["data"]["outcome"] = "maybe-it-worked"
    with pytest.raises(ValidationError):
        CloudEvent.model_validate(payload)


def test_invalid_actor_type_rejected() -> None:
    payload = _base_event()
    payload["data"]["actor"]["type"] = "superuser"  # not in enum
    with pytest.raises(ValidationError):
        CloudEvent.model_validate(payload)


def test_invalid_specversion_rejected() -> None:
    payload = _base_event()
    payload["specversion"] = "0.3"
    with pytest.raises(ValidationError):
        CloudEvent.model_validate(payload)


# -----------------------------------------------------------------------------
# Time parsing
# -----------------------------------------------------------------------------
def test_time_parses_rfc3339_with_z() -> None:
    payload = _base_event()
    payload["time"] = "2026-04-22T14:00:00Z"
    ev = CloudEvent.model_validate(payload)
    assert ev.time.tzinfo is not None
    assert ev.time.astimezone(timezone.utc) == datetime(
        2026, 4, 22, 14, 0, 0, tzinfo=timezone.utc
    )


def test_time_parses_rfc3339_with_offset() -> None:
    payload = _base_event()
    payload["time"] = "2026-04-22T14:00:00+00:00"
    ev = CloudEvent.model_validate(payload)
    assert ev.time.astimezone(timezone.utc) == datetime(
        2026, 4, 22, 14, 0, 0, tzinfo=timezone.utc
    )


# -----------------------------------------------------------------------------
# Flexibility — `data` allows extra fields per event-type
# -----------------------------------------------------------------------------
def test_extra_fields_in_data_accepted() -> None:
    payload = _base_event()
    payload["data"]["changes"] = [{"field": "x", "from": "a", "to": "b"}]
    payload["data"]["context"] = {"custom": "value", "nested": {"k": 1}}
    ev = CloudEvent.model_validate(payload)
    assert ev.data.model_dump()["changes"][0]["field"] == "x"
    assert ev.data.model_dump()["context"]["nested"]["k"] == 1


def test_extra_fields_at_envelope_level_accepted() -> None:
    # CloudEvents spec explicitly allows custom context attributes at the
    # envelope level. We preserve them rather than rejecting.
    payload = _base_event()
    payload["partitionkey"] = "custom"
    ev = CloudEvent.model_validate(payload)
    assert ev.model_dump()["partitionkey"] == "custom"


# -----------------------------------------------------------------------------
# to_record() — the shape inserted into Postgres
# -----------------------------------------------------------------------------
def test_to_record_shape_for_beneficiary_updated() -> None:
    payload = json.loads(
        (SAMPLES_DIR / "04-beneficiary-updated.json").read_text()
    )
    ev = CloudEvent.model_validate(payload)
    rec = ev.to_record()

    # Flat promoted columns
    assert rec["id"] == "smoke-04-beneficiary-updated"
    assert rec["source"] == "/openg2p/beneficiary-service"
    assert rec["type"] == "org.openg2p.beneficiary.updated"
    assert rec["subject"] == "beneficiary/b_1029384756"
    assert rec["actor_type"] == "user"
    assert rec["actor_id"] == "u_4421"
    assert rec["resource_type"] == "beneficiary"
    assert rec["resource_id"] == "b_1029384756"
    assert rec["action"] == "update"
    assert rec["outcome"] == "success"
    assert rec["reason"] is None   # no reason on a successful update
    # traceparent -> trace_id extracted from W3C header
    assert rec["trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"

    # details is a JSON string (asyncpg/JSONB contract). Parse and verify:
    #  - action/outcome/reason are NOT in details (fully promoted to flat columns)
    #  - actor/resource keep all attrs EXCEPT id/type (those are flat columns)
    #  - changes[] from the CloudEvent `data` IS in details
    details = json.loads(rec["details"])
    assert "action" not in details
    assert "outcome" not in details
    # actor.id and actor.type are flat — but actor.* extras are preserved
    assert "id" not in details.get("actor", {})
    assert "type" not in details.get("actor", {})
    # resource.id and resource.type are flat — but resource.* extras are preserved
    assert "id" not in details.get("resource", {})
    assert "type" not in details.get("resource", {})
    # changes[] passes through unchanged
    assert details["changes"][0]["field"] == "phone"
    assert len(details["changes"]) == 2


def test_to_record_no_resource_for_login() -> None:
    payload = json.loads((SAMPLES_DIR / "01-login-success.json").read_text())
    ev = CloudEvent.model_validate(payload)
    rec = ev.to_record()

    assert rec["resource_type"] is None
    assert rec["resource_id"] is None
    assert rec["action"] == "login"
    details = json.loads(rec["details"])
    # No resource on a login event
    assert "resource" not in details
    # Context passes through
    assert details["context"]["mfa"] == "totp"
    # Actor extras (name, roles, ip, session_id) preserved under details.actor
    assert details["actor"]["name"] == "fatima.k"
    assert "program.operator" in details["actor"]["roles"]


def test_to_record_system_actor() -> None:
    payload = json.loads(
        (SAMPLES_DIR / "06-payment-reversed-system.json").read_text()
    )
    ev = CloudEvent.model_validate(payload)
    rec = ev.to_record()

    assert rec["actor_type"] == "system"
    assert rec["actor_id"] == "reconciliation-job"
    assert rec["outcome"] == "success"
    # reason is promoted to a flat column
    assert rec["reason"] == "bank_rejection"
    # bank_code lives in details.context
    details = json.loads(rec["details"])
    assert details["context"]["bank_code"] == "E102"


def test_to_record_reason_promoted_for_failure() -> None:
    payload = json.loads((SAMPLES_DIR / "02-login-failed.json").read_text())
    ev = CloudEvent.model_validate(payload)
    rec = ev.to_record()

    assert rec["outcome"] == "failure"
    assert rec["reason"] == "invalid_password"
    # reason should NOT also be in details (it's been promoted)
    details = json.loads(rec["details"])
    assert "reason" not in details


def test_to_record_actor_extras_preserved_in_details() -> None:
    """actor.{name, roles, ip, session_id} must land in details.actor.* — only id/type are stripped."""
    payload = _base_event()
    payload["data"]["actor"] = {
        "type": "user",
        "id": "u_abc",
        "name": "Admin User",
        "roles": ["operator", "approver"],
        "ip": "10.0.0.1",
        "session_id": "sess_xyz",
    }
    ev = CloudEvent.model_validate(payload)
    rec = ev.to_record()

    # Flat columns
    assert rec["actor_id"] == "u_abc"
    assert rec["actor_type"] == "user"

    # Extras preserved under details.actor
    details = json.loads(rec["details"])
    assert details["actor"]["name"] == "Admin User"
    assert details["actor"]["roles"] == ["operator", "approver"]
    assert details["actor"]["ip"] == "10.0.0.1"
    assert details["actor"]["session_id"] == "sess_xyz"
    # id/type stripped
    assert "id" not in details["actor"]
    assert "type" not in details["actor"]


def test_actor_extra_attributes_pass_through() -> None:
    """Actor has extra='allow' — emitters can add arbitrary actor fields
    (e.g. username, session_state) without an audit-manager schema change."""
    payload = _base_event()
    payload["data"]["actor"] = {
        "type": "user",
        "id": "u_x",
        "name": "Display Name",
        "username": "loginname",       # custom field, not in Actor schema
        "session_state": "sess_abc",   # another custom field
    }
    ev = CloudEvent.model_validate(payload)
    rec = ev.to_record()
    details = json.loads(rec["details"])
    assert details["actor"]["username"] == "loginname"
    assert details["actor"]["session_state"] == "sess_abc"
    assert details["actor"]["name"] == "Display Name"


def test_to_record_resource_extras_preserved_in_details() -> None:
    """resource.{amount, currency, ...} must land in details.resource.* — only id/type are stripped."""
    payload = _base_event()
    payload["data"]["resource"] = {
        "type": "payment",
        "id": "pay_999",
        "amount": 2500,
        "currency": "INR",
        "beneficiary_id": "b_x",
    }
    ev = CloudEvent.model_validate(payload)
    rec = ev.to_record()

    assert rec["resource_id"] == "pay_999"
    assert rec["resource_type"] == "payment"

    details = json.loads(rec["details"])
    assert details["resource"]["amount"] == 2500
    assert details["resource"]["currency"] == "INR"
    assert details["resource"]["beneficiary_id"] == "b_x"
    assert "id" not in details["resource"]
    assert "type" not in details["resource"]


def test_to_record_details_is_none_when_no_extras() -> None:
    # A minimal event with only the 3 core data fields has nothing to
    # put into details — it should come out as None (→ SQL NULL).
    payload = _base_event()
    ev = CloudEvent.model_validate(payload)
    rec = ev.to_record()
    assert rec["details"] is None


def test_to_record_trace_id_none_without_traceparent() -> None:
    payload = _base_event()
    ev = CloudEvent.model_validate(payload)
    assert ev.to_record()["trace_id"] is None


# -----------------------------------------------------------------------------
# Actor / AuditData direct checks
# -----------------------------------------------------------------------------
def test_actor_defaults() -> None:
    a = Actor(id="u_1")
    assert a.type == "user"  # default
    assert a.roles == []


def test_audit_data_requires_three_core_fields() -> None:
    with pytest.raises(ValidationError):
        AuditData.model_validate({})
    with pytest.raises(ValidationError):
        AuditData.model_validate(
            {"actor": {"id": "u_1"}, "action": "login"}  # missing outcome
        )
