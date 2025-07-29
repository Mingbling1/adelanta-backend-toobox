"""Retomas schemas for v2 architecture."""

from pydantic import BaseModel, Field
from typing import Optional


class RetomasCalcularRequest(BaseModel):
    """Request schema for retomas calculations."""
    fecha_corte: str = Field(..., description="Cut-off date for retomas calculation (ISO format)")


class RetomasCalcularResponse(BaseModel):
    """Response schema for retomas calculations."""
    RUCPagador: Optional[str] = Field(None, description="RUC del pagador")
    RazonSocialPagador: Optional[str] = Field(None, description="Razón social del pagador")
    Cobranzas_MontoPagoSoles: float = Field(..., description="Monto de cobranzas en soles")
    Desembolsos_MontoDesembolsoSoles: float = Field(..., description="Monto de desembolsos en soles")
    PorRetomar: float = Field(..., description="Monto por retomar (diferencia cobranzas - desembolsos)")


class RetomasCalcularListResponse(BaseModel):
    """List response schema for retomas calculations."""
    retomas: list[RetomasCalcularResponse] = Field(..., description="Lista de retomas calculadas")
    total_records: int = Field(..., description="Total number of records")
