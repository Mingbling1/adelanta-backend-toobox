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


class BaseAdministrativo(DeclarativeBase):
    pass


class DatabaseSessionManagerAdministrativo:
    def __init__(self, host: str, engine_kwargs: dict[str, Any] = {}):
        self._engine = create_async_engine(
            host,
            **engine_kwargs,
        )
        self._sessionmaker = async_sessionmaker(
            autocommit=False,
            bind=self._engine,
            expire_on_commit=False,
        )

    async def close(self):
        if self._engine is None:
            raise Exception("DatabaseSessionManagerAdministrativo is not initialized")
        await self._engine.dispose()

        self._engine = None
        self._sessionmaker = None

    @contextlib.asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        if self._engine is None:
            raise Exception("DatabaseSessionManagerAdministrativo is not initialized")

        async with self._engine.begin() as connection:
            try:
                yield connection
            except Exception:
                await connection.rollback()
                raise
            finally:
                await connection.close()

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        if self._sessionmaker is None:
            raise Exception("DatabaseSessionManagerAdministrativo is not initialized")

        session = self._sessionmaker()
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


sessionmanager_administrativo = DatabaseSessionManagerAdministrativo(
    str(settings.DATABASE_MYSQL_URL_ADMINISTRATIVO),
    {
        "echo": True,
        "future": True,
        "pool_size": 100,
        "max_overflow": 20,
        "pool_recycle": 200,
        "connect_args": {"connect_timeout": 10},
    },
)


async def get_db_administrativo() -> AsyncIterator[AsyncSession]:
    async with sessionmanager_administrativo.session() as session:
        yield session


DB_ADMINISTRATIVO = Annotated[AsyncSession, Depends(get_db_administrativo)]
