"""Background loop that maintains monthly partitions of audit_events."""

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncEngine

from ..config import DatabaseConfig
from ..models import ensure_partitions

logger = logging.getLogger(__name__)


async def partition_maintainer_loop(engine: AsyncEngine, cfg: DatabaseConfig) -> None:
    """Run `ensure_partitions` every `partition_check_interval_seconds`."""
    while True:
        try:
            await ensure_partitions(
                engine,
                pre_create_months=cfg.partition_pre_create_months,
                retention_months=cfg.partition_retention_months,
            )
        except Exception:
            logger.exception("Partition maintenance failed; will retry")

        try:
            await asyncio.sleep(cfg.partition_check_interval_seconds)
        except asyncio.CancelledError:
            return
