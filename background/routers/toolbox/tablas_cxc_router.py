# background/routers/toolbox/tablas_cxc_router.py
"""🎮 API Endpoints para tablas CXC - delega a Celery"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import ORJSONResponse
from config.logger import logger
from background.processors.toolbox.tablas_cxc_processor import TablasCXCProcessor
from background.schemas.task_schema import TaskExecuteResponse


router = APIRouter(
    responses={404: {"description": "Task not found"}},
    tags=["Background Tasks - Tablas CXC"],
)


@router.post(
    "/execute",
    response_model=TaskExecuteResponse,
    response_class=ORJSONResponse,
    summary="🚀 Ejecutar Tablas CXC",
    description="Ejecuta la actualización de tablas CXC con ETL Power BI completo usando Celery",
)
async def execute_tablas_cxc_task():
    """🎯 Control remoto para ejecutar Tablas CXC bajo demanda"""
    try:
        logger.info("🎮 API: Iniciando ejecución remota de Tablas CXC...")

        # Instanciar el wrapper de Celery
        cxc_celery = TablasCXCProcessor()

        # Ejecutar task
        result = await cxc_celery.run()

        logger.info(f"✅ API: Task enviada exitosamente - ID: {result['task_id']}")

        return TaskExecuteResponse(
            success=True,
            task_id=result["task_id"],
            message="Task Tablas CXC enviada a Celery exitosamente",
            task_name="tablas_cxc_task",
        )

    except Exception as e:
        logger.error(f"❌ API: Error ejecutando Tablas CXC: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error ejecutando task: {str(e)}")
