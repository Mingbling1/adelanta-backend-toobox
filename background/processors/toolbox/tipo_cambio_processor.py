"""
🔄 Processor: Tipo de Cambio SUNAT
🎯 Wrapper de compatibilidad para tipo_cambio_task
🔧 Patrón: OPCIONAL - Solo wrapper del task Celery
"""

from background.processors.base_processor import BaseProcessor
from background.tasks.toolbox.tipo_cambio_task import tipo_cambio_task
from config.logger import logger


class TipoCambioProcessor(BaseProcessor):
    """
    🔄 Wrapper de compatibilidad para migrar a Celery
    Mantiene la misma interfaz que el cronjob original
    ✅ HEREDA: get_task_status() de BaseProcessor (elimina duplicación)
    """

    def __init__(self):
        super().__init__()  # Llamar al constructor de BaseProcessor
        self.description = (
            "Actualización automática de tipos de cambio desde API SUNAT usando Celery"
        )

    async def run(self, batch_size: int = 1) -> dict:
        """
        🚀 Ejecutar task de actualización Tipo de Cambio

        Args:
            batch_size: Tamaño del lote para procesamiento (1-10)

        Returns:
            dict: Respuesta formateada con task_id y detalles
        """
        try:
            logger.info(
                f"🔄 Enviando Tipo de Cambio task a Celery con parámetros: batch_size={batch_size}"
            )

            # 🎯 Ejecutar Celery task
            task_result = tipo_cambio_task.delay(batch_size=batch_size)

            logger.info(f"✅ Task enviado a Celery con ID: {task_result.id}")

            return {
                "status": "enqueued",
                "task_id": task_result.id,
                "message": "Task Tipo de Cambio SUNAT enviado a Celery exitosamente",
                "parameters": {"batch_size": batch_size},
            }

        except Exception as e:
            logger.error(f"❌ Error enviando task a Celery: {str(e)}")
            raise e

    def run_sync(self, batch_size: int = 1) -> dict:
        """
        Ejecutar de forma síncrona (para testing o casos especiales)
        """
        try:
            logger.info("🔄 Ejecutando Tipo de Cambio task síncronamente...")

            # Ejecutar task directamente (sin Celery)
            result = tipo_cambio_task(batch_size=batch_size)

            logger.info("✅ Task ejecutado síncronamente")
            return result

        except Exception as e:
            logger.error(f"❌ Error ejecutando task síncronamente: {str(e)}")
            raise e
