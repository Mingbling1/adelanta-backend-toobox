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
    🔄 Processor para actualización de Tipo de Cambio SUNAT

    Hereda de BaseProcessor para formateo centralizado y status management.
    Es un wrapper opcional que facilita la ejecución del task Celery.
    """

    def __init__(self):
        super().__init__(
            task_name="toolbox.tipo_cambio",
            description="Actualización automática de tipos de cambio desde API SUNAT",
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
                f"🔄 TipoCambioProcessor ejecutando task con parámetros: batch_size={batch_size}"
            )

            # 🎯 Ejecutar Celery task
            task_result = tipo_cambio_task.delay(batch_size=batch_size)

            # 📊 Formatear respuesta usando BaseProcessor
            response = self.format_task_response(task_result.id)

            # 🔧 Agregar información adicional específica
            response.update(
                {
                    "task_name": self.task_name,
                    "description": self.description,
                    "parameters": {"batch_size": batch_size},
                }
            )

            logger.info(f"✅ Task iniciado exitosamente: {task_result.id}")
            return response

        except Exception as e:
            error_msg = f"❌ Error en TipoCambioProcessor: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "message": error_msg, "task_name": self.task_name}
