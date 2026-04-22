"""
Kafka consumer — drains the audit topic and batch-inserts into Postgres.

Delivery model: at-least-once with idempotent writes.
  * We commit offsets only after a successful Postgres COMMIT.
  * Inserts use ON CONFLICT (id, occurred_at) DO NOTHING, so replays of the
    same offset (after a crash) are safe.

Horizontal scaling: every replica of the service joins the same consumer group
(`openg2p-audit-consumer`). Kafka assigns partitions; adding pods auto-rebalances.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.structs import ConsumerRecord
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from ..config import KafkaConfig
from ..schema.cloud_event import CloudEvent

logger = logging.getLogger(__name__)


_INSERT_SQL = text(
    """
    INSERT INTO audit_events (
        id, occurred_at, source, type, subject,
        actor_type, actor_id, resource_type, resource_id,
        action, outcome, trace_id, envelope
    ) VALUES (
        :id, :occurred_at, :source, :type, :subject,
        :actor_type, :actor_id, :resource_type, :resource_id,
        :action, :outcome, :trace_id, CAST(:envelope AS JSONB)
    )
    ON CONFLICT (id, occurred_at) DO NOTHING
    """
)


class AuditConsumer:
    """Single-task Kafka consumer that flushes batches to Postgres."""

    def __init__(self, cfg: KafkaConfig, engine: AsyncEngine,
                 batch_max_records: int, flush_interval_ms: int) -> None:
        self._cfg = cfg
        self._engine = engine
        self._batch_max = batch_max_records
        self._flush_interval_s = flush_interval_ms / 1000.0
        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task | None = None

    # ---- lifecycle ----------------------------------------------------------
    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            self._cfg.topic,
            bootstrap_servers=self._cfg.bootstrap_servers,
            group_id=self._cfg.consumer_group,
            client_id=self._cfg.client_id,
            enable_auto_commit=self._cfg.consumer.enable_auto_commit,
            auto_offset_reset=self._cfg.consumer.auto_offset_reset,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await self._consumer.start()
        self._task = asyncio.create_task(self._run(), name="audit-consumer")
        logger.info(
            "Kafka consumer started (group=%s, topic=%s)",
            self._cfg.consumer_group,
            self._cfg.topic,
        )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer is not None:
            await self._consumer.stop()
        logger.info("Kafka consumer stopped")

    # ---- main loop ----------------------------------------------------------
    async def _run(self) -> None:
        assert self._consumer is not None

        batch: list[ConsumerRecord] = []
        deadline = time.monotonic() + self._flush_interval_s

        while True:
            timeout_s = max(deadline - time.monotonic(), 0.0)
            timeout_ms = max(int(timeout_s * 1000), 1)

            # getmany returns a dict {TopicPartition: [records...]}
            result = await self._consumer.getmany(
                timeout_ms=timeout_ms, max_records=self._batch_max
            )
            for records in result.values():
                batch.extend(records)

            now = time.monotonic()
            full = len(batch) >= self._batch_max
            timed_out = now >= deadline and batch

            if full or timed_out:
                await self._flush(batch)
                batch = []
                deadline = time.monotonic() + self._flush_interval_s
            elif not batch:
                # Nothing accumulating — reset deadline so we don't spin.
                deadline = time.monotonic() + self._flush_interval_s

    async def _flush(self, records: list[ConsumerRecord]) -> None:
        """Persist `records` to Postgres, then commit Kafka offsets."""
        assert self._consumer is not None

        rows: list[dict[str, Any]] = []
        bad: list[ConsumerRecord] = []
        for rec in records:
            try:
                ev = CloudEvent.model_validate(rec.value)
                rows.append(ev.to_record())
            except Exception as e:  # noqa: BLE001
                logger.warning("Dropping malformed event at %s:%d: %s",
                               rec.topic, rec.offset, e)
                bad.append(rec)

        if rows:
            try:
                async with self._engine.begin() as conn:
                    await conn.execute(_INSERT_SQL, rows)
            except Exception:
                # Do NOT commit offsets on failure — Kafka will re-deliver
                # these records on the next poll. Idempotent inserts make
                # this safe.
                logger.exception(
                    "Postgres insert failed for %d rows — offsets not committed",
                    len(rows),
                )
                return

        # Compute max offset per partition across the whole batch (including bad
        # messages — we want to advance past them, not re-consume).
        offsets: dict[TopicPartition, int] = {}
        for rec in records:
            tp = TopicPartition(rec.topic, rec.partition)
            offsets[tp] = max(offsets.get(tp, -1), rec.offset)

        # aiokafka commit takes {tp: offset_to_commit}, which is next-to-read
        # (i.e. last_offset + 1).
        commit_map = {tp: off + 1 for tp, off in offsets.items()}
        await self._consumer.commit(commit_map)

        logger.info(
            "Persisted %d events (bad=%d) across %d partitions",
            len(rows),
            len(bad),
            len(offsets),
        )
