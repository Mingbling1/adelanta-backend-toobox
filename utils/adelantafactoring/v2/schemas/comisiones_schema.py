"""
📊 Schemas Pydantic V2 - Comisiones

Mantiene compatibilidad con v1 mientras mejora validación financiera
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, Literal
from datetime import datetime
import pandas as pd
from decimal import Decimal


class ComisionesSchema(BaseModel):
    """Schema principal para datos de comisiones"""

    RUCCliente: Optional[str] = Field(None, description="RUC del cliente")
    RUCPagador: str = Field(..., description="RUC del pagador", min_length=1)
    Tipo: Literal["Nuevo", "Recurrente"] = Field(..., description="Tipo de operación")
    Detalle: str = Field(..., description="Detalle de la operación", min_length=1)
    Mes: str = Field(..., description="Mes de la operación", min_length=1)
    TipoOperacion: Literal["Factoring", "Confirming", "Capital de Trabajo"] = Field(
        ..., description="Tipo de operación financiera"
    )
    Ejecutivo: str = Field(..., description="Ejecutivo responsable", min_length=1)

    # Campos adicionales para cálculos
    MontoComision: Optional[Decimal] = Field(
        None, description="Monto de comisión calculado"
    )
    FechaOperacion: Optional[datetime] = Field(
        None, description="Fecha de la operación"
    )

    @field_validator("RUCCliente", mode="before")
    @classmethod
    def validate_ruc_cliente(cls, v):
        """Convierte NaN o valores missing de pandas en None"""
        if (
            pd.isna(v) if hasattr(pd, "isna") else v != v
        ):  # Compatibilidad con/sin pandas
            return None
        return str(v).strip() if v else None

    @field_validator("RUCPagador", mode="before")
    @classmethod
    def validate_ruc_pagador(cls, v):
        """Normaliza RUC del pagador"""
        if v is None:
            raise ValueError("RUC Pagador no puede ser None")
        return str(v).strip()

    @field_validator("MontoComision", mode="before")
    @classmethod
    def validate_monto_comision(cls, v):
        """Convierte a Decimal para precisión financiera"""
        if v is None:
            return None
        return Decimal(str(v))


class PromocionSchema(BaseModel):
    """Schema para promociones de ejecutivos"""

    Ejecutivo: str = Field(..., description="Ejecutivo en promoción", min_length=1)
    TipoOperacion: Literal["Factoring", "Confirming", "Capital de Trabajo"] = Field(
        ..., description="Tipo de operación en promoción"
    )
    FechaExpiracion: datetime = Field(
        ..., description="Fecha de expiración de la promoción"
    )

    @field_validator("FechaExpiracion", mode="before")
    @classmethod
    def validate_fecha_expiracion(cls, v):
        """Normaliza fecha de expiración"""
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v


class RegistroComisionSchema(BaseModel):
    """Schema para registro individual de comisión (compatibilidad v1)"""

    RUCCliente: Optional[str] = Field(None, description="RUC del cliente")
    RUCPagador: str = Field(..., description="RUC del pagador")
    Tipo: Literal["Nuevo", "Recurrente"] = Field(..., description="Tipo de operación")
    Detalle: str = Field(..., description="Detalle de la operación")
    Mes: str = Field(..., description="Mes de la operación")
    TipoOperacion: Literal["Factoring", "Confirming", "Capital de Trabajo"] = Field(
        ..., description="Tipo de operación financiera"
    )
    Ejecutivo: str = Field(..., description="Ejecutivo responsable")

    @field_validator("RUCCliente", mode="before")
    @classmethod
    def validate_ruc_cliente(cls, v):
        """Convierte NaN o valores missing de pandas en None"""
        if pd.isna(v) if hasattr(pd, "isna") else v != v:
            return None
        return str(v).strip() if v else None


# Alias para compatibilidad con v1
ComisionesCalcularSchema = ComisionesSchema
RegistroComisionSchemaV1 = RegistroComisionSchema
PromocionSchemaV1 = PromocionSchema
