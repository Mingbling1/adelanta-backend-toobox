"""
🎮 Router de Control Remoto para Tasks de Celery
API para ejecutar cronjobs bajo demanda desde Swagger
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import ORJSONResponse
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
from config.logger import logger
from cronjobs.datamart.ActualizarTablaKPIAcumuladoCronjobCelery import (
    ActualizarTablaKPIAcumuladoCronjobCelery,
)


# 📋 Schemas para el control remoto
class TaskExecuteRequest(BaseModel):
    task_name: str = Field(..., description="Nombre de la task a ejecutar")
    parameters: Dict[str, Any] = Field(
        default_factory=dict, description="Parámetros opcionales"
    )


class TaskStatusRequest(BaseModel):
    task_id: str = Field(..., description="ID de la task para consultar estado")


class TaskExecuteResponse(BaseModel):
    success: bool
    task_id: str
    message: str
    task_name: str


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    result: Optional[Any] = None
    ready: bool
    successful: Optional[bool] = None
    failed: Optional[bool] = None
    error: Optional[str] = None  # Agregar campo para errores


# 🎮 Router de control remoto
router = APIRouter(
    prefix="/tasks",
    responses={404: {"description": "Task not found"}},
)


@router.post(
    "/execute/kpi-acumulado",
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
        kpi_celery = ActualizarTablaKPIAcumuladoCronjobCelery()

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


@router.get(
    "/status/{task_id}",
    response_model=TaskStatusResponse,
    response_class=ORJSONResponse,
    summary="📊 Consultar Estado de Task",
    description="Consulta el estado y resultado de una task de Celery",
)
async def get_task_status(task_id: str):
    """
    🔍 Consultar estado de una task específica
    """
    try:
        logger.info(f"🔍 API: Consultando estado de task: {task_id}")

        # Usar el wrapper para consultar estado
        kpi_celery = ActualizarTablaKPIAcumuladoCronjobCelery()
        status_info = kpi_celery.get_task_status(task_id)

        logger.info(f"📊 API: Estado de task {task_id}: {status_info['status']}")

        # Asegurar que todos los campos opcionales estén presentes
        response_data = {
            "task_id": status_info.get("task_id", task_id),
            "status": status_info.get("status", "UNKNOWN"),
            "result": status_info.get("result"),
            "ready": status_info.get("ready", False),
            "successful": status_info.get("successful"),
            "failed": status_info.get("failed"),
            "error": status_info.get("error"),
        }

        return TaskStatusResponse(**response_data)

    except Exception as e:
        logger.error(f"❌ API: Error consultando estado: {str(e)}")

        # Devolver respuesta estructurada en caso de error
        error_response = {
            "task_id": task_id,
            "status": "API_ERROR",
            "result": None,
            "ready": False,
            "successful": None,
            "failed": None,
            "error": str(e),
        }

        return TaskStatusResponse(**error_response)


@router.get(
    "/available",
    response_class=ORJSONResponse,
    summary="📋 Listar Tasks Disponibles",
    description="Lista todas las tasks disponibles para ejecución remota",
)
async def list_available_tasks():
    """
    📋 Listar todas las tasks disponibles
    """
    try:
        available_tasks = {
            "kpi_acumulado": {
                "name": "actualizar_kpi_acumulado_task",
                "description": "Actualiza la tabla de KPI acumulado",
                "endpoint": "/tasks/execute/kpi-acumulado",
                "method": "POST",
            },
            # Aquí podrás agregar más tasks en el futuro
        }

        return {
            "success": True,
            "available_tasks": available_tasks,
            "total_tasks": len(available_tasks),
        }

    except Exception as e:
        logger.error(f"❌ API: Error listando tasks: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error listando tasks: {str(e)}")


@router.post(
    "/execute/sync/kpi-acumulado",
    response_class=ORJSONResponse,
    summary="⚡ Ejecutar KPI Acumulado (Síncrono)",
    description="Ejecuta KPI Acumulado de forma síncrona para testing",
)
async def execute_kpi_acumulado_sync():
    """
    ⚡ Ejecución síncrona para testing (sin Celery)
    """
    try:
        logger.info("⚡ API: Ejecutando KPI Acumulado síncronamente...")

        kpi_celery = ActualizarTablaKPIAcumuladoCronjobCelery()
        result = kpi_celery.run_sync()

        logger.info("✅ API: Ejecución síncrona completada")

        return {
            "success": True,
            "message": "Task ejecutada síncronamente",
            "result": result,
        }

    except Exception as e:
        logger.error(f"❌ API: Error en ejecución síncrona: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Error en ejecución síncrona: {str(e)}"
        )
