# background/processors/toolbox/tablas_reportes_processor.py
"""
🔄 Wrapper Celery para ActualizarTablasReportesCronjob
Mantiene compatibilidad con la interfaz existente
"""

from typing import Dict, Any
from background.tasks.toolbox.tablas_reportes_task import tablas_reportes_task
from background.processors.base_processor import BaseProcessor
from config.logger import logger


class TablasReportesProcessor(BaseProcessor):
    """
    Wrapper de compatibilidad para migrar a Celery
    Mantiene la misma interfaz que el cronjob original
    ✅ HEREDA: get_task_status() de BaseProcessor (elimina duplicación)
    """

    def __init__(self):
        super().__init__()  # Llamar al constructor de BaseProcessor
        self.description = "Actualiza las tablas de reportes: KPI, NuevosClientesNuevosPagadores y Saldos usando Celery"
        self.status_key = "ActualizarTablasReportesCronjob_status"

    async def process(self, **kwargs) -> Dict[str, Any]:
        """
        Ejecutar task usando Celery
        Mantiene compatibilidad con la interfaz original
        """
        try:
            logger.info("🔄 Enviando Tablas Reportes task a Celery...")

            # Enviar task a Celery de forma asíncrona
            result = tablas_reportes_task.delay()

            logger.info(f"✅ Task enviada a Celery con ID: {result.id}")

            return {
                "status": "enqueued",
                "task_id": result.id,
                "message": "Task enviada a Celery exitosamente",
            }

        except Exception as e:
            logger.error(f"❌ Error enviando task a Celery: {str(e)}")
            raise e

    def process_sync(self) -> Dict[str, Any]:
        """
        Ejecutar de forma síncrona (para testing o casos especiales)
        """
        try:
            logger.info("🔄 Ejecutando Tablas Reportes task síncronamente...")

            # Ejecutar task directamente (sin Celery)
            result = tablas_reportes_task()

            logger.info("✅ Task ejecutada síncronamente")
            return result

        except Exception as e:
            logger.error(f"❌ Error ejecutando task síncronamente: {str(e)}")
            raise e
