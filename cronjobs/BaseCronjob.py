import pkgutil
import importlib
from functools import wraps
from config.db_mysql import sessionmanager
from config.cronjob import cronjob_manager
from config.logger import logger
from datetime import datetime, timedelta
import pytz


class BaseCronjob:
    _registry = {}
    peru_tz = pytz.timezone("America/Lima")

    def __init__(self, description: str = "No description available") -> None:
        self.description = description
        self._register()

    def _register(self):
        """Register the cron job in the registry."""
        job_id = self.__class__.__name__.lower()
        BaseCronjob._registry[job_id] = {
            "callable": self.run,
            "description": self.description,
            "class_name": self.__class__.__name__,
        }
        logger.info(f"Registered job: {job_id}")

    @staticmethod
    def with_db_session(func):
        """Decorator to provide a database session to the cron job."""

        @wraps(func)
        async def wrapper(*args, **kwargs):
            logger.info("Opening database session")
            async with sessionmanager.session() as session:
                try:
                    result = await func(*args, db=session, **kwargs)
                finally:
                    logger.info("Closing database session")
                return result

        return wrapper

    @staticmethod
    def start_all():
        """Start all registered cron jobs."""
        logger.info("Starting all cronjobs...")
        for job_id, job_info in BaseCronjob._registry.items():
            cronjob_manager.add_job(job_info["callable"], id=job_id)
            logger.info(f"Added job to scheduler: {job_id}")
        logger.info("All cronjobs registered successfully.")

    @staticmethod
    def register_all_cronjobs():
        """Register all cron jobs by instantiating them."""
        logger.info("Registering all cronjobs...")
        BaseCronjob._find_and_register_subclasses("cronjobs.datamart")
        logger.info("All cronjobs registered.")

    @staticmethod
    def _find_and_register_subclasses(package_name):
        """Find and register all subclasses of CronJob in the given package."""
        package = importlib.import_module(package_name)
        for _, module_name, _ in pkgutil.walk_packages(
            package.__path__, package.__name__ + "."
        ):
            module = importlib.import_module(module_name)
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, BaseCronjob)
                    and attr is not BaseCronjob
                ):
                    logger.info(f"Found subclass: {attr.__name__}")
                    attr()  # Instantiate the subclass to register it

    @staticmethod
    def obtener_datetime_fecha_inicio() -> datetime:
        return datetime(2019, 7, 1, tzinfo=BaseCronjob.peru_tz)

    @staticmethod
    def obtener_datetime_fecha_fin(dias: int = 0) -> datetime:
        # Retornar la fecha actual
        return datetime.now(BaseCronjob.peru_tz) - timedelta(days=dias)

    @staticmethod
    def obtener_string_fecha_inicio(tipo: int = 0) -> str:
        if tipo == 0:
            return BaseCronjob.obtener_datetime_fecha_inicio().strftime("%Y%m%d")
        elif tipo == 1:
            return BaseCronjob.obtener_datetime_fecha_inicio().strftime("%Y-%m-%d")

    @staticmethod
    def obtener_string_fecha_fin(dias: int = 0, tipo: int = 0) -> str:
        """
        Obtener la fecha fin en formato string
        """
        if tipo == 0:
            return BaseCronjob.obtener_datetime_fecha_fin(dias).strftime("%Y%m%d")
        elif tipo == 1:
            return BaseCronjob.obtener_datetime_fecha_fin(dias).strftime("%Y-%m-%d")
