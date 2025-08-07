from fastapi import APIRouter
from fastapi.responses import ORJSONResponse
from config.logger import logger
from background.schemas.task_schema import TaskStatusResponse
from background.processors.base_processor import BaseProcessor

router = APIRouter(
    prefix="/tasks",
    responses={404: {"description": "Task not found"}},
    tags=["Background Tasks - Centralized"],
)


@router.get("/status/{task_id}", response_model=TaskStatusResponse)
async def get_status(task_id: str):
    """🔍 Consultar estado de task de tablas reportes - FORMATEO CENTRALIZADO"""
    return BaseProcessor.format_task_response(task_id)


@router.get(
    "/available",
    response_class=ORJSONResponse,
    summary="📋 Listar Tasks Disponibles - AUTOMATIZADO",
    description="Lista automáticamente todas las tasks disponibles desde el registro de Celery",
)
async def list_available_tasks():
    """
    📋 Listar todas las tasks disponibles - AUTOMATIZACIÓN INTELIGENTE
    ✅ Extrae automáticamente desde celery_app.tasks
    ✅ Incluye fallback manual en caso de error
    """
    return BaseProcessor.get_available_tasks()


@router.get(
    "/scheduled",
    response_class=ORJSONResponse,
    summary="🕐 Listar Tareas Programadas - BEAT SCHEDULE",
    description="Obtiene información detallada de todas las tareas programadas automáticamente en Celery Beat",
)
async def list_scheduled_tasks():
    """
    🕐 Listar todas las tareas programadas automáticamente
    ✅ Extrae información desde celery_app.conf.beat_schedule
    ✅ Incluye horarios, próximas ejecuciones y configuración
    ✅ Información perfecta para mostrar en frontend
    """
    logger.info("🕐 API: Consultando tareas programadas automáticamente...")

    try:
        result = BaseProcessor.get_scheduled_tasks()

        logger.info(
            f"📊 API: Encontradas {result.get('total_scheduled', 0)} tareas programadas"
        )

        return result

    except Exception as e:
        logger.error(f"❌ API: Error obteniendo tareas programadas: {str(e)}")
        return {
            "success": False,
            "scheduled_tasks": {},
            "total_scheduled": 0,
            "error": str(e),
            "message": "Error en el endpoint de tareas programadas",
        }
