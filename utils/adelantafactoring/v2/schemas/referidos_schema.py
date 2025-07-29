"""
🏷️ Referidos Schema V2 - Adelanta Factoring Financial ETL

Schemas optimizados para el sistema de referidos con validación RUST-powered.
Mantiene compatibilidad completa con el sistema legacy.
"""

from pydantic import BaseModel, Field, field_validator, ConfigDict
from datetime import datetime
from typing import List


class ReferidosBaseSchema(BaseModel):
    """Schema base para datos de referidos"""

    Referencia: str = Field(..., description="Referencia del referido")
    CodigoLiquidacion: str = Field(..., description="Código de liquidación")
    Ejecutivo: str = Field(..., description="Ejecutivo asignado")
    Mes: datetime = Field(..., description="Mes de referencia")

    @field_validator("Mes", mode="before")
    @classmethod
    def parsear_mes(cls, v):
        """
        🗓️ Acepta cadena 'dd/mm/yyyy' y la convierte a datetime.
        Mantiene compatibilidad con el formato original.
        """
        if isinstance(v, str):
            return datetime.strptime(v, "%d/%m/%Y")
        if isinstance(v, datetime):
            return v
        raise ValueError(f"Formato de fecha no soportado: {type(v)}")

    @field_validator("Referencia", "CodigoLiquidacion", "Ejecutivo", mode="before")
    @classmethod
    def validar_strings(cls, v):
        """🔧 Valida y limpia campos de texto"""
        if not v or not isinstance(v, str):
            raise ValueError("Campo de texto no puede estar vacío")
        return v.strip()

    model_config = ConfigDict(arbitrary_types_allowed=True)


class ReferidosCalcularSchema(ReferidosBaseSchema):
    """
    📊 Schema principal para datos calculados de referidos.
    Compatible 100% con el schema legacy.
    """

    pass


class ReferidosRawSchema(BaseModel):
    """
    📥 Schema para datos RAW del webservice.
    Normaliza nombres de columnas y formatos.
    """

    REFERENCIA: str = Field(..., alias="referencia")
    LIQUIDACION: str = Field(..., alias="liquidacion")
    EJECUTIVO: str = Field(..., alias="ejecutivo")
    MES: str = Field(..., alias="mes")

    model_config = ConfigDict(arbitrary_types_allowed=True)


class ReferidosProcessedSchema(BaseModel):
    """
    📤 Schema para datos procesados y listos para usar.
    Incluye metadatos adicionales del procesamiento.
    """

    data: List[ReferidosCalcularSchema]
    total_records: int = Field(..., description="Total de registros procesados")
    duplicates_removed: int = Field(default=0, description="Duplicados eliminados")
    processing_timestamp: datetime = Field(default_factory=datetime.now)

    model_config = ConfigDict(arbitrary_types_allowed=True)
