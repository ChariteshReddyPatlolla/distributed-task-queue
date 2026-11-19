import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Optional
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from services.common.config import settings
from services.common.models import Base

_engine: Optional[AsyncEngine] = None
_engine_loop: Optional[asyncio.AbstractEventLoop] = None
_session_factory: Optional[async_sessionmaker[AsyncSession]] = None


def get_engine() -> AsyncEngine:
    global _engine, _engine_loop, _session_factory
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    if _engine is None or (_engine_loop is not None and _engine_loop is not current_loop):
        if _engine is not None:
            try:
                _engine.sync_engine.dispose()
            except Exception:
                pass
        _engine = create_async_engine(
            settings.DATABASE_URL,
            pool_size=20,
            max_overflow=10,
            pool_pre_ping=True,
            pool_timeout=10,
            echo=False,
        )
        _engine_loop = current_loop
        _session_factory = None
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        engine = get_engine()
        _session_factory = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
    return _session_factory


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@asynccontextmanager
async def get_session_context() -> AsyncGenerator[AsyncSession, None]:
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def init_db(engine: Optional[AsyncEngine] = None) -> None:
    target_engine = engine or get_engine()
    async with target_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def check_db_health(timeout: float = 3.0) -> bool:
    try:
        engine = get_engine()
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            return result.scalar() == 1
    except Exception:
        return False


async def close_db() -> None:
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
