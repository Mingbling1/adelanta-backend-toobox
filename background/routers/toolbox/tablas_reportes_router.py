# background/routers/toolbox/tablas_reportes_router.py
"""🎮 API Endpoints para tablas reportes - delega a Celery"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import ORJSONResponse
from config.logger import logger
from background.processors.toolbox.tablas_reportes_processor import (
    TablasReportesProcessor,
)
from background.schemas.task_schema import TaskExecuteResponse


router = APIRouter(
    responses={404: {"description": "Task not found"}},
    tags=["Background Tasks - Tablas Reportes"],
)


@router.post(
    "/execute",
    response_model=TaskExecuteResponse,
    response_class=ORJSONResponse,
    summary="🚀 Ejecutar Tablas Reportes",
    description="Ejecuta la actualización de tablas de reportes (KPI, NuevosClientes, Saldos) usando Celery",
)
async def execute_tablas_reportes_task():
    """🎯 Control remoto para ejecutar Tablas Reportes bajo demanda"""
    try:
        logger.info("🎮 API: Iniciando ejecución remota de Tablas Reportes...")

        # Instanciar el wrapper de Celery
        reportes_celery = TablasReportesProcessor()

        # Ejecutar task (sin await - Celery delay es síncrono)
        result = reportes_celery.run_sync()

        logger.info(f"✅ API: Task enviada exitosamente - ID: {result['task_id']}")

        return TaskExecuteResponse(
            success=True,
            task_id=result["task_id"],
            message="Task Tablas Reportes enviada a Celery exitosamente",
            task_name="tablas_reportes_task",
        )

    except Exception as e:
        logger.error(f"❌ API: Error ejecutando Tablas Reportes: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error ejecutando task: {str(e)}")
