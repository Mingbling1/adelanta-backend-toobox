from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
from typing import Annotated
from fastapi import Depends
from config.logger import logger
from config.settings import settings
# from contextlib import asynccontextmanager


class CronJobManager:
    def __init__(self):
        self._scheduler = None
        self._is_running = False

    def _create_scheduler(self):
        """Crear el scheduler con configuración optimizada"""
        jobstores = {
            "default": RedisJobStore(
                jobs_key="apscheduler.jobs",
                run_times_key="apscheduler.run_times",
                host=settings.REDIS_HOST,
                port=6379,
                db=0,
            )
        }
        
        executors = {
            "default": AsyncIOExecutor()
        }
        
        job_defaults = {
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 30
        }
        
        return AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone="America/Lima"
        )

    async def start(self):
        """Iniciar el scheduler"""
        if not self._scheduler:
            self._scheduler = self._create_scheduler()
        
        if not self._is_running:
            self._scheduler.start()
            self._is_running = True
            logger.info("Scheduler started successfully")

    async def shutdown(self):
        """Detener el scheduler limpiamente"""
        if self._scheduler and self._is_running:
            self._scheduler.shutdown(wait=True)
            self._is_running = False
            logger.info("Scheduler stopped")

    def get_scheduler(self) -> AsyncIOScheduler:
        """Obtener el scheduler (para dependency injection)"""
        if not self._scheduler:
            self._scheduler = self._create_scheduler()
        return self._scheduler

    @property
    def is_running(self) -> bool:
        return self._is_running


# Instancia global
cronjob_manager = CronJobManager()

def get_cronjob_manager() -> AsyncIOScheduler:
    return cronjob_manager.get_scheduler()

CRONJOB = Annotated[AsyncIOScheduler, Depends(get_cronjob_manager)]