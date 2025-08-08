"""
🎮 Router: Tipo de Cambio SUNAT
🎯 Endpoints: POST /execute ÚNICAMENTE
🔧 Patrón: Router → Celery Task (+ Repository Factory) → Database
"""

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import ORJSONResponse
from background.processors.toolbox.tipo_cambio_processor import TipoCambioProcessor
from background.schemas.task_schema import TaskExecuteResponse
from config.logger import logger

router = APIRouter(
    responses={404: {"description": "Task not found"}},
)


@router.post(
    "/execute",
    response_model=TaskExecuteResponse,
    response_class=ORJSONResponse,
    summary="🚀 Ejecutar Tipo de Cambio SUNAT",
    description="Ejecuta la actualización de tipos de cambio desde API SUNAT usando Celery",
)
async def execute_tipo_cambio_task(
    batch_size: int = Query(
        1, ge=1, le=10, description="Tamaño del lote para procesamiento (1-10)"
    ),
):
    """🎯 Control remoto para ejecutar Tipo de Cambio SUNAT bajo demanda"""
    try:
        logger.info("🎮 API: Iniciando ejecución remota de Tipo de Cambio SUNAT...")

        logger.info(
            f"🚀 Ejecutando tipo_cambio_task con parámetros: batch_size={batch_size}"
        )

        # Instanciar el wrapper de Celery
        tipo_cambio_celery = TipoCambioProcessor()

        # Ejecutar task
        result = await tipo_cambio_celery.run(batch_size=batch_size)

        logger.info(f"✅ API: Task enviada exitosamente - ID: {result['task_id']}")

        return TaskExecuteResponse(
            success=True,
            task_id=result["task_id"],
            message="Task Tipo de Cambio SUNAT enviada a Celery exitosamente",
            task_name="toolbox.tipo_cambio",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ API: Error ejecutando Tipo de Cambio: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error ejecutando task: {str(e)}")


