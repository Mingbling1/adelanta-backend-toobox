"""
Schemas de dominio para cálculos de sector pagadores
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional


class SectorPagadoresRequest(BaseModel):
    """Request para cálculo de sector pagadores (sin parámetros adicionales)"""

    # Para este módulo no se requieren parámetros específicos
    force_refresh: bool = Field(
        default=False, description="Forzar actualización de datos"
    )

    class Config:
        arbitrary_types_allowed = True


class SectorPagadorRecord(BaseModel):
    """Schema para un registro de sector pagador procesado"""

    ruc_pagador: str = Field(..., description="RUC del pagador")
    sector: str = Field(..., description="Sector económico")
    grupo_eco: Optional[str] = Field(None, description="Grupo económico")


class SectorPagadoresResult(BaseModel):
    """Resultado del procesamiento de sector pagadores"""

    records: list[SectorPagadorRecord] = Field(
        ..., description="Lista de registros procesados"
    )
    records_count: int = Field(..., description="Cantidad de registros procesados")
    unique_sectors: list[str] = Field(..., description="Lista de sectores únicos")
    unique_grupos: list[str] = Field(
        ..., description="Lista de grupos económicos únicos"
    )

    class Config:
        arbitrary_types_allowed = True


class SectorPagadoresCalcularSchema(BaseModel):
    """Schema para validación de datos de sector pagadores (compatibilidad)"""

    RUCPagador: str = Field(..., description="RUC del pagador")
    Sector: str = Field(..., description="Sector económico")
    GrupoEco: Optional[str] = Field(None, description="Grupo económico")

    @field_validator("GrupoEco", mode="before")
    @classmethod
    def convertir_valores_vacios_a_none(cls, v):
        """Convierte valores vacíos a None"""
        if v == "" or v is None:
            return None
        return v

    @field_validator("RUCPagador", "Sector", mode="before")
    @classmethod
    def limpiar_strings(cls, v):
        """Limpia strings de espacios"""
        if isinstance(v, str):
            return v.strip()
        return v
