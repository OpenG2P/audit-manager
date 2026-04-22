"""Async PostgreSQL engine and session management."""

import os

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

_engine: AsyncEngine | None = None


def _build_database_url() -> str:
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    name = os.environ.get("DB_NAME", "auditmanager")
    user = os.environ.get("DB_USER", "postgres")
    password = os.environ.get("DB_PASSWORD", "postgres")
    return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{name}"


def init_engine() -> AsyncEngine:
    global _engine
    _engine = create_async_engine(
        _build_database_url(),
        pool_size=10,
        max_overflow=5,
        pool_pre_ping=True,
    )
    return _engine


def get_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("Database engine not initialized. Call init_engine() first.")
    return _engine


def get_session() -> AsyncSession:
    return AsyncSession(get_engine(), expire_on_commit=False)


async def dispose_engine() -> None:
    global _engine
    if _engine is not None:
        await _engine.dispose()
        _engine = None
