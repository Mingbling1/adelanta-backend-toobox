from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field


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
    model_config = {"arbitrary_types_allowed": True}

    task_id: str
    status: str
    result: Optional[Any] = None
    ready: bool
    successful: Optional[bool] = None
    failed: Optional[bool] = None
    error: Optional[str] = None


# 🆕 Schemas específicos para Tipo de Cambio SUNAT
class TipoCambioTaskRequest(BaseModel):
    """📋 Request para task de actualización Tipo de Cambio"""

    start_date: Optional[str] = Field(
        None,
        description="Fecha inicio formato YYYY-MM-DD (opcional, default: hace 30 días)",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    )
    end_date: Optional[str] = Field(
        None,
        description="Fecha fin formato YYYY-MM-DD (opcional, default: hoy)",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    )
    batch_size: int = Field(
        1, ge=1, le=10, description="Tamaño del lote para procesamiento (1-10)"
    )


class TipoCambioTaskResult(BaseModel):
    """📊 Resultado de task de actualización Tipo de Cambio"""

    status: str = Field(..., description="Estado de la ejecución")
    message: str = Field(..., description="Mensaje descriptivo")
    dates_processed: int = Field(
        ..., description="Cantidad de fechas procesadas exitosamente"
    )
    dates_failed: int = Field(..., description="Cantidad de fechas que fallaron")
    failed_dates: List[str] = Field(
        default_factory=list, description="Lista de fechas que fallaron"
    )
    task_id: str = Field(..., description="ID del task Celery")
    date_range: str = Field(..., description="Rango de fechas procesado")


class TipoCambioTaskResponse(BaseModel):
    """📤 Respuesta completa de task Tipo de Cambio"""

    success: bool = Field(..., description="Indicador de éxito")
    task_id: str = Field(..., description="ID del task Celery")
    task_name: str = Field(..., description="Nombre del task")
    description: str = Field(..., description="Descripción del task")
    message: str = Field(..., description="Mensaje de estado")
    status: str = Field(..., description="Estado actual del task")
    parameters: TipoCambioTaskRequest = Field(..., description="Parámetros utilizados")
