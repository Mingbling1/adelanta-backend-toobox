"""
🔄 Wrapper Celery para ActualizarTablaKPIAcumuladoCronjob
Mantiene compatibilidad con la interfaz existente
"""

from typing import Dict, Any
from background.tasks.toolbox.kpi_acumulado_task import actualizar_kpi_acumulado_task
from background.processors.base_processor import BaseProcessor
from config.logger import logger


class KPIAcumuladoProcessor(BaseProcessor):
    """
    Wrapper de compatibilidad para migrar a Celery
    Mantiene la misma interfaz que el cronjob original
    ✅ HEREDA: get_task_status() de BaseProcessor (elimina duplicación)
    """

    def __init__(self):
        super().__init__()  # Llamar al constructor de BaseProcessor
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

    # ✅ MÉTODO ELIMINADO: get_task_status() ahora se hereda de BaseProcessor
    # Esto elimina ~50 líneas de código duplicado
