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
from datetime import datetime
from typing import Optional

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
    start_date: Optional[str] = Query(
        None,
        description="Fecha inicio formato YYYY-MM-DD (opcional, default: hace 30 días)",
    ),
    end_date: Optional[str] = Query(
        None, description="Fecha fin formato YYYY-MM-DD (opcional, default: hoy)"
    ),
    batch_size: int = Query(
        1, ge=1, le=10, description="Tamaño del lote para procesamiento (1-10)"
    ),
):
    """🎯 Control remoto para ejecutar Tipo de Cambio SUNAT bajo demanda"""
    try:
        logger.info("🎮 API: Iniciando ejecución remota de Tipo de Cambio SUNAT...")

        # 📅 Validar fechas si se proporcionan
        if start_date:
            try:
                datetime.strptime(start_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(
                    status_code=400, detail="start_date debe tener formato YYYY-MM-DD"
                )

        if end_date:
            try:
                datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(
                    status_code=400, detail="end_date debe tener formato YYYY-MM-DD"
                )

        # 📅 Validar que start_date <= end_date
        if start_date and end_date:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            if start_dt > end_dt:
                raise HTTPException(
                    status_code=400,
                    detail="start_date debe ser menor o igual a end_date",
                )

        logger.info(
            f"🚀 Ejecutando tipo_cambio_task con parámetros: start_date={start_date}, end_date={end_date}, batch_size={batch_size}"
        )

        # Instanciar el wrapper de Celery
        tipo_cambio_celery = TipoCambioProcessor()

        # Ejecutar task
        result = await tipo_cambio_celery.run(
            start_date=start_date, end_date=end_date, batch_size=batch_size
        )

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
