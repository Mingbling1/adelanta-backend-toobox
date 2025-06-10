from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.redis import RedisJobStore
from typing import Annotated
from fastapi import Depends
from config.logger import logger
from config.settings import settings


class CronJobManager(AsyncIOScheduler):
    def __init__(self, timezone="America/Lima"):
        jobstores = {
            "default": RedisJobStore(
                jobs_key="apscheduler.jobs",
                run_times_key="apscheduler.run_times",
                host=settings.REDIS_HOST,
                port=6379,
                db=0,
            )
        }
        super().__init__(jobstores=jobstores, timezone=timezone)

    def start(self):
        super().start()
        logger.debug("Scheduler started.")

    def wakeup(self):
        super().wakeup()
        logger.debug("Scheduler waked up.")

    def close(self):
        self.shutdown()
        logger.debug("Scheduler stopped.")


cronjob_manager = CronJobManager()


def get_cronjob_manager() -> AsyncIOScheduler:
    return cronjob_manager


CRONJOB = Annotated[AsyncIOScheduler, Depends(get_cronjob_manager)]
