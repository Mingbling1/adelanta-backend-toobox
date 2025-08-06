from typing import Dict, Any, Optional
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
