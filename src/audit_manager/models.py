"""
PostgreSQL schema management for audit events.

The design:
  * One logical table `audit_events`, RANGE-partitioned by `occurred_at` per month.
  * Parent table is created at startup via CREATE TABLE IF NOT EXISTS (matching
    the OpenG2P id-generator pattern — DDL lives in the service, not Alembic).
  * A background maintainer creates N future monthly partitions and optionally
    drops partitions older than the retention window.
  * Primary key includes occurred_at so it can coexist with RANGE partitioning
    (Postgres requirement). Idempotent inserts use ON CONFLICT (id, occurred_at).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

# Parent table DDL — partitioned by RANGE on occurred_at.
_PARENT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS audit_events (
    id              TEXT         NOT NULL,
    occurred_at     TIMESTAMPTZ  NOT NULL,
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    source          TEXT         NOT NULL,
    type            TEXT         NOT NULL,
    subject         TEXT,
    actor_type      TEXT         NOT NULL,
    actor_id        TEXT         NOT NULL,
    resource_type   TEXT,
    resource_id     TEXT,
    action          TEXT         NOT NULL,
    outcome         TEXT         NOT NULL,
    trace_id        TEXT,
    envelope        JSONB        NOT NULL,
    PRIMARY KEY (id, occurred_at)
) PARTITION BY RANGE (occurred_at)
"""

# Indexes are defined on the parent — Postgres propagates them to partitions.
_PARENT_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_audit_events_occurred_at "
    "  ON audit_events (occurred_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_audit_events_actor "
    "  ON audit_events (actor_id, occurred_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_audit_events_resource "
    "  ON audit_events (resource_type, resource_id, occurred_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_audit_events_type "
    "  ON audit_events (type, occurred_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_audit_events_trace "
    "  ON audit_events (trace_id) WHERE trace_id IS NOT NULL",
]


def _month_start(d: datetime) -> datetime:
    return datetime(d.year, d.month, 1, tzinfo=timezone.utc)


def _next_month(d: datetime) -> datetime:
    year, month = (d.year + 1, 1) if d.month == 12 else (d.year, d.month + 1)
    return datetime(year, month, 1, tzinfo=timezone.utc)


def _prev_month(d: datetime) -> datetime:
    year, month = (d.year - 1, 12) if d.month == 1 else (d.year, d.month - 1)
    return datetime(year, month, 1, tzinfo=timezone.utc)


def _partition_name(start: datetime) -> str:
    return f"audit_events_{start.year:04d}_{start.month:02d}"


async def create_parent_table(engine: AsyncEngine) -> None:
    """Create the parent table and indexes if they don't exist."""
    async with engine.begin() as conn:
        await conn.execute(text(_PARENT_TABLE_DDL))
        for ddl in _PARENT_INDEXES:
            await conn.execute(text(ddl))
    logger.info("audit_events parent table ensured")


async def ensure_partitions(
    engine: AsyncEngine,
    pre_create_months: int,
    retention_months: int,
) -> None:
    """
    Ensure the current month and the next `pre_create_months - 1` monthly
    partitions exist. Drop partitions older than `retention_months` if that
    retention is enabled (> 0).
    """
    now = datetime.now(timezone.utc)
    current = _month_start(now)

    async with engine.begin() as conn:
        # Create current + future partitions
        start = current
        for _ in range(pre_create_months):
            end = _next_month(start)
            name = _partition_name(start)
            stmt = (
                f"CREATE TABLE IF NOT EXISTS {name} "
                f"PARTITION OF audit_events "
                f"FOR VALUES FROM ('{start.isoformat()}') "
                f"TO ('{end.isoformat()}')"
            )
            await conn.execute(text(stmt))
            start = end

        # Drop partitions older than retention
        if retention_months > 0:
            cutoff = current
            for _ in range(retention_months):
                cutoff = _prev_month(cutoff)

            rows = (
                await conn.execute(
                    text(
                        "SELECT inhrelid::regclass::text AS child "
                        "FROM pg_inherits "
                        "WHERE inhparent = 'audit_events'::regclass"
                    )
                )
            ).fetchall()

            prefix = "audit_events_"
            cutoff_key = f"{cutoff.year:04d}_{cutoff.month:02d}"
            for (child,) in rows:
                if not child.startswith(prefix):
                    continue
                key = child[len(prefix) :]
                if key < cutoff_key:
                    logger.info("Dropping retired audit partition %s", child)
                    await conn.execute(text(f"DROP TABLE IF EXISTS {child}"))

    logger.info(
        "Partition maintenance done (pre_create=%d, retention_months=%d)",
        pre_create_months,
        retention_months,
    )
