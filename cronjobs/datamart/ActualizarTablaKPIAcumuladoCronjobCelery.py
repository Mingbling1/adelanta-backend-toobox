"""
🔄 Wrapper Celery para ActualizarTablaKPIAcumuladoCronjob
Mantiene compatibilidad con la interfaz existente
"""

from typing import Dict, Any
from config.celery_tasks import actualizar_kpi_acumulado_task
from config.logger import logger


class ActualizarTablaKPIAcumuladoCronjobCelery:
    """
    Wrapper de compatibilidad para migrar a Celery
    Mantiene la misma interfaz que el cronjob original
    """

    def __init__(self):
        self.description = "Actualiza la tabla de KPI acumulado usando Celery"

    async def run(self, **kwargs) -> Dict[str, Any]:
        """
        Ejecutar task usando Celery
        Mantiene compatibilidad con la interfaz original
        """
        try:
            logger.info("🔄 Enviando KPI Acumulado task a Celery...")

            # Enviar task a Celery de forma asíncrona
            result = actualizar_kpi_acumulado_task.delay()

            logger.info(f"✅ Task enviada a Celery con ID: {result.id}")

            return {
                "status": "enqueued",
                "task_id": result.id,
                "message": "Task enviada a Celery exitosamente",
            }

        except Exception as e:
            logger.error(f"❌ Error enviando task a Celery: {str(e)}")
            raise e

    def run_sync(self) -> Dict[str, Any]:
        """
        Ejecutar de forma síncrona (para testing o casos especiales)
        """
        try:
            logger.info("🔄 Ejecutando KPI Acumulado task síncronamente...")

            # Ejecutar task directamente (sin Celery)
            result = actualizar_kpi_acumulado_task()

            logger.info("✅ Task ejecutada síncronamente")
            return result

        except Exception as e:
            logger.error(f"❌ Error ejecutando task síncronamente: {str(e)}")
            raise e

    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        Obtener estado de una task de Celery
        """
        try:
            from config.celery_config import celery_app

            result = celery_app.AsyncResult(task_id)

            return {
                "task_id": task_id,
                "status": result.status,
                "result": result.result,
                "ready": result.ready(),
                "successful": result.successful() if result.ready() else None,
                "failed": result.failed() if result.ready() else None,
            }

        except Exception as e:
            logger.error(f"❌ Error obteniendo estado de task: {str(e)}")
            return {"task_id": task_id, "status": "ERROR", "error": str(e)}
