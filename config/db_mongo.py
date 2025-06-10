import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from config.settings import settings


connection = AsyncIOMotorClient(
    f"mongodb+srv://{settings.MONGO_USERNAME}:{settings.MONGO_PASSWORD}@{settings.MONGO_HOST}/?retryWrites=true&w=majority",
    io_loop=asyncio.get_event_loop(),
)
