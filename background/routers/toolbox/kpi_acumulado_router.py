from fastapi import APIRouter, HTTPException
from fastapi.responses import ORJSONResponse
from config.logger import logger
from background.processors.toolbox.kpi_acumulado_processor import (
    KPIAcumuladoProcessor,
)
from background.schemas.task_schema import (
    TaskExecuteResponse,
    TaskStatusResponse,
)


router = APIRouter(
    responses={404: {"description": "Task not found"}},
    tags=["Background Tasks - KPI Acumulado"],
)


@router.post(
    "/execute",
    response_model=TaskExecuteResponse,
    response_class=ORJSONResponse,
    summary="🚀 Ejecutar KPI Acumulado",
    description="Ejecuta la actualización de tabla KPI Acumulado usando Celery",
)
async def execute_kpi_acumulado_task():
    """
    🎯 Control remoto para ejecutar KPI Acumulado bajo demanda
    """
    try:
        logger.info("🎮 API: Iniciando ejecución remota de KPI Acumulado...")

        # Instanciar el wrapper de Celery
        kpi_celery = KPIAcumuladoProcessor()

        # Ejecutar task
        result = await kpi_celery.run()

        logger.info(f"✅ API: Task enviada exitosamente - ID: {result['task_id']}")

        return TaskExecuteResponse(
            success=True,
            task_id=result["task_id"],
            message="Task KPI Acumulado enviada a Celery exitosamente",
            task_name="actualizar_kpi_acumulado_task",
        )

    except Exception as e:
        logger.error(f"❌ API: Error ejecutando KPI Acumulado: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error ejecutando task: {str(e)}")
