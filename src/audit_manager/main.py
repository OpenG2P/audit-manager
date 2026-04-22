"""
FastAPI application entry point for OpenG2P Audit Manager.

Lifespan:
  Startup:
    1. Init Postgres engine.
    2. Create parent `audit_events` table + indexes (idempotent).
    3. Pre-create monthly partitions for current + future months.
    4. Start Kafka producer + in-process drain worker.
    5. Start Kafka consumer (Postgres batch writer).
    6. Start partition-maintenance loop.
    7. Mark startup complete → /health returns 200.
  Shutdown:
    Reverse order — cancel workers, stop Kafka, dispose DB engine.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI

from .api.router import router
from .config import get_settings
from .db import dispose_engine, init_engine
from .kafka.consumer import AuditConsumer
from .kafka.producer import AuditProducer
from .models import create_parent_table, ensure_partitions
from .worker.partitions import partition_maintainer_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_startup_complete = False
_producer: Optional[AuditProducer] = None
_consumer: Optional[AuditConsumer] = None
_partition_task: Optional[asyncio.Task] = None


def is_startup_complete() -> bool:
    return _startup_complete


def get_producer() -> AuditProducer:
    if _producer is None:
        raise RuntimeError("Producer not initialised")
    return _producer


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _startup_complete, _producer, _consumer, _partition_task

    settings = get_settings().audit_manager

    logger.info("Initialising database engine...")
    engine = init_engine()

    logger.info("Ensuring audit_events parent table + indexes...")
    await create_parent_table(engine)

    logger.info("Pre-creating monthly partitions...")
    await ensure_partitions(
        engine,
        pre_create_months=settings.database.partition_pre_create_months,
        retention_months=settings.database.partition_retention_months,
    )

    logger.info("Starting Kafka producer + drain worker...")
    _producer = AuditProducer(
        cfg=settings.kafka, queue_max_size=settings.ingest.queue_max_size
    )
    await _producer.start()

    logger.info("Starting Kafka consumer → Postgres batch writer...")
    _consumer = AuditConsumer(
        cfg=settings.kafka,
        engine=engine,
        batch_max_records=settings.kafka.consumer.batch_max_records,
        flush_interval_ms=settings.kafka.consumer.flush_interval_ms,
    )
    await _consumer.start()

    logger.info("Starting partition maintenance loop...")
    _partition_task = asyncio.create_task(
        partition_maintainer_loop(engine, settings.database),
        name="audit-partition-maintainer",
    )

    _startup_complete = True
    logger.info("Startup complete. /health will now return 200.")

    yield

    logger.info("Shutting down...")
    _startup_complete = False

    if _partition_task is not None:
        _partition_task.cancel()
        try:
            await _partition_task
        except asyncio.CancelledError:
            pass

    if _consumer is not None:
        await _consumer.stop()

    if _producer is not None:
        await _producer.stop()

    await dispose_engine()
    logger.info("Shutdown complete.")


app = FastAPI(
    title="OpenG2P Audit Manager",
    description=(
        "CloudEvents audit ingest for OpenG2P. Accepts events over HTTP, "
        "buffers in Kafka, persists to PostgreSQL. Callers receive 202 "
        "Accepted immediately — Kafka and Postgres are never on the hot path."
    ),
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.include_router(router)
