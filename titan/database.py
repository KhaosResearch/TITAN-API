from motor.motor_asyncio import AsyncIOMotorClient

from titan.config import settings
from titan.logger import get_logger

logger = get_logger(__name__)


class DataBase:
    client: AsyncIOMotorClient = None


db = DataBase()


async def create_db_connection():
    logger.debug("Connecting to database for the first time")
    db.client = AsyncIOMotorClient(settings.MONGO_DNS)


async def close_db_connection():
    logger.debug("Closing connection with database")
    db.client.close()


async def get_connection() -> AsyncIOMotorClient:
    return db.client.titan
