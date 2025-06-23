# conftest.py
from typing import AsyncIterator
import pytest
import httpx
from httpx import ASGITransport, AsyncClient
from main import app
from config.cronjob import cronjob_manager
from cronjobs.BaseCronjob import BaseCronjob
import asyncio
from config.logger import logger
from config.redis import redis_client_manager


@pytest.fixture(scope="function", autouse=True)
async def setup_cronjobs():
    # Forzar que Redis se "despierte" haciendo un PING antes de arrancar los cronjobs
    try:
        # await redis_client_manager.start()
        pong = await redis_client_manager.connect()
        logger.info("Redis ping response: %s", pong)
    except Exception as e:
        logger.error("Error pinging Redis: %s", e)
    # Iniciar el scheduler y registrar todos los cronjobs
    loop = asyncio.get_running_loop()
    asyncio.set_event_loop(loop)
    await cronjob_manager.start()
    BaseCronjob.register_all_cronjobs()
    yield
    await cronjob_manager.shutdown()


@pytest.fixture(params=["asyncio"], scope="session")
def anyio_backend(request):
    return request.param


@pytest.fixture(scope="function")
async def client() -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver", follow_redirects=True
    ) as client:
        yield client
