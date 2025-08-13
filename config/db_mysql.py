import contextlib
from typing import Any, AsyncIterator
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
    AsyncConnection,
)
from sqlalchemy.orm import DeclarativeBase
from config.settings import settings
from fastapi import Depends
from typing import Annotated


class Base(DeclarativeBase):
    pass


class DatabaseSessionManager:
    def __init__(self, host: str, engine_kwargs: dict[str, Any] = {}):
        self._engine = create_async_engine(host, **engine_kwargs)
        self._sessionmaker = async_sessionmaker(
            autocommit=False, autoflush=False, bind=self._engine , class_=AsyncSession
        )

    async def close(self):
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")
        await self._engine.dispose()

        self._engine = None
        self._sessionmaker = None

    @contextlib.asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")

        async with self._engine.begin() as connection:
            try:
                yield connection
            except Exception:
                await connection.rollback()
                raise

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        if self._sessionmaker is None:
            raise Exception("DatabaseSessionManager is not initialized")

        session = self._sessionmaker()
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
            await self._engine.dispose()


sessionmanager = DatabaseSessionManager(
    str(settings.DATABASE_MYSQL_URL),
    {
        "echo": False,  # Sin logging en producción para mejor performance
        "future": True,
        "pool_size": 8,
        "max_overflow": 4,
        "pool_recycle": 1800,
        "connect_args": {
            "connect_timeout": 10,
            "charset": "utf8mb4",  # Charset moderno
        },
    },
)


async def get_db() -> AsyncIterator[AsyncSession]:
    async with sessionmanager.session() as session:
        yield session


DB = Annotated[AsyncSession, Depends(get_db)]
