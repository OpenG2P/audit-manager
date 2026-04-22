"""
Kafka producer and the in-process drain worker.

Flow:
  HTTP handler  --put_nowait-->  asyncio.Queue  --drain_worker-->  Kafka
The HTTP handler never awaits Kafka; it only blocks on an O(1) queue put. The
drain worker batches and ships items. A bounded queue means overload surfaces
as 503 rather than silent loss.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

from aiokafka import AIOKafkaProducer

from ..config import KafkaConfig
from ..schema.cloud_event import CloudEvent

logger = logging.getLogger(__name__)


def _normalize_acks(acks: str | int) -> int | str:
    """aiokafka accepts 0 | 1 | -1 | "all" (ints must be real ints, not strings)."""
    if isinstance(acks, int):
        return acks
    s = str(acks).strip().lower()
    if s == "all":
        return "all"
    return int(s)   # "0" | "1" | "-1"


class AuditProducer:
    """Wraps an AIOKafkaProducer plus the in-process ingest queue."""

    def __init__(self, cfg: KafkaConfig, queue_max_size: int) -> None:
        self._cfg = cfg
        self._queue: asyncio.Queue[CloudEvent] = asyncio.Queue(maxsize=queue_max_size)
        self._producer: Optional[AIOKafkaProducer] = None
        self._drain_task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()

    # ---- lifecycle ----------------------------------------------------------
    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._cfg.bootstrap_servers,
            client_id=self._cfg.client_id,
            acks=_normalize_acks(self._cfg.producer.acks),
            linger_ms=self._cfg.producer.linger_ms,
            compression_type=self._cfg.producer.compression_type,
            max_batch_size=self._cfg.producer.max_batch_size,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: v.encode("utf-8") if v else None,
        )
        await self._producer.start()
        self._drain_task = asyncio.create_task(
            self._drain_worker(), name="audit-producer-drain"
        )
        logger.info(
            "Kafka producer started (bootstrap=%s, topic=%s)",
            self._cfg.bootstrap_servers,
            self._cfg.topic,
        )

    async def stop(self) -> None:
        self._stopping.set()
        if self._drain_task is not None:
            self._drain_task.cancel()
            try:
                await self._drain_task
            except asyncio.CancelledError:
                pass
        if self._producer is not None:
            await self._producer.stop()
        logger.info("Kafka producer stopped")

    # ---- public API ---------------------------------------------------------
    def enqueue(self, event: CloudEvent) -> None:
        """Non-blocking enqueue. Raises QueueFull if overloaded."""
        self._queue.put_nowait(event)

    def queue_depth(self) -> int:
        return self._queue.qsize()

    # ---- internal -----------------------------------------------------------
    async def _drain_worker(self) -> None:
        """Pull from the queue and publish to Kafka one-by-one.

        aiokafka batches internally via `linger_ms` + `max_batch_size`, so we
        don't need our own batching loop. Using `send_and_wait` per event gives
        back-pressure (if the broker is slow, the queue fills, HTTP callers
        start getting 503 — which is the right behavior).
        """
        assert self._producer is not None
        topic = self._cfg.topic
        while True:
            event = await self._queue.get()
            try:
                payload = event.model_dump(mode="json")
                # Key by subject (or actor_id) so all events for a given
                # entity land on the same partition in order.
                key = event.subject or event.data.actor.id
                await self._producer.send_and_wait(topic, value=payload, key=key)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # The event has already been accepted (202); we can't fail the
                # caller now. Log loudly and continue. Re-enqueue would risk an
                # infinite loop if the broker is unreachable for a long time.
                logger.exception(
                    "Failed to publish audit event %s: %s", event.id, e
                )
            finally:
                self._queue.task_done()
