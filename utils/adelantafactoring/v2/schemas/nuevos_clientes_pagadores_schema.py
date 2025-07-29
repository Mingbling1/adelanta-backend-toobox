"""
🏷️ Nuevos Clientes Nuevos Pagadores Schema V2 - Adelanta Factoring

Schemas optimizados para análisis de nuevos clientes y pagadores con validación RUST-powered.
Arquitectura hexagonal pura sin dependencias legacy.
"""

from pydantic import BaseModel, Field, ConfigDict, field_validator
from datetime import datetime, date
from typing import List, Optional
import math


class NuevosClientesNuevosPagadoresBaseSchema(BaseModel):
    """Schema base para nuevos clientes y pagadores"""

    FechaOperacion: Optional[date] = Field(None, description="Fecha de operación")
    Ejecutivo: Optional[str] = Field(None, description="Ejecutivo responsable")
    RUCCliente: Optional[str] = Field(None, description="RUC del cliente")
    RUCPagador: Optional[str] = Field(None, description="RUC del pagador")
    TipoOperacion: Optional[str] = Field(None, description="Tipo de operación")
    RazonSocial: Optional[str] = Field(None, description="Razón social")
    Mes: Optional[str] = Field(None, description="Mes de análisis (YYYY-MM)")

    @field_validator("RUCCliente", "RUCPagador", mode="before")
    @classmethod
    def validate_ruc_fields(cls, value):
        """
        🔍 Valida campos RUC, convirtiendo números y manejando valores especiales.
        """
        try:
            # Manejar valores NaN y None
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return None

            # Convertir números a string
            if isinstance(value, (int, float)):
                return str(int(value))

            # Si ya es string, limpiar y retornar
            if isinstance(value, str):
                cleaned = value.strip()
                return cleaned if cleaned else None

            return None
        except Exception:
            return None

    @field_validator("Ejecutivo", "TipoOperacion", "RazonSocial", "Mes", mode="before")
    @classmethod
    def validate_string_fields(cls, value):
        """
        📝 Valida campos de texto, manejando valores NaN y None.
        """
        try:
            # Manejar valores NaN y None
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return None

            # Si es string, limpiar y retornar
            if isinstance(value, str):
                cleaned = value.strip()
                return cleaned if cleaned else None

            # Convertir otros tipos a string
            return str(value).strip() if value is not None else None
        except Exception:
            return None

    @field_validator("FechaOperacion", mode="before")
    @classmethod
    def validate_fecha_operacion(cls, value):
        """
        📅 Validador de fechas optimizado - SIN TIMEZONE para Excel.

        Convierte datetime con timezone a date sin timezone para:
        ✅ Evitar error: "Excel does not support datetimes with timezones"
        ✅ Mantener solo la fecha (sin hora/timezone)
        ✅ Compatibilidad total con Excel export
        """
        try:
            # Manejar valores NaN y None
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return None

            if isinstance(value, str):
                # Intentar convertir string a fecha
                if value.strip() == "":
                    return None
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return dt.date()

            elif isinstance(value, datetime):
                return value.date()

            elif isinstance(value, date):
                return value

            return None
        except Exception:
            return None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class NuevosClientesNuevosPagadoresCalcularSchema(
    NuevosClientesNuevosPagadoresBaseSchema
):
    """
    📊 Schema principal para nuevos clientes y pagadores calculados.
    Compatible 100% con el schema legacy.
    """

    pass


class NuevosClientesNuevosPagadoresProcessedSchema(BaseModel):
    """
    📤 Schema para datos procesados con metadatos del procesamiento.
    """

    data: List[NuevosClientesNuevosPagadoresCalcularSchema]
    total_records: int = Field(..., description="Total de registros procesados")
    nuevos_clientes: int = Field(default=0, description="Cantidad de nuevos clientes")
    nuevos_pagadores: int = Field(default=0, description="Cantidad de nuevos pagadores")
    ejecutivos_unicos: int = Field(
        default=0, description="Cantidad de ejecutivos únicos"
    )
    tipos_operacion: List[str] = Field(
        default_factory=list, description="Tipos de operación encontrados"
    )
    periodo_analisis: str = Field(
        ..., description="Período de análisis (start_date - end_date)"
    )
    processing_timestamp: datetime = Field(default_factory=datetime.now)

    model_config = ConfigDict(arbitrary_types_allowed=True)  #


class ProcessingMetadata(BaseModel):
    """
    📊 Metadatos de procesamiento.
    """

    total_registros: int = Field(default=0, description="Total de registros procesados")
    nuevos_clientes: int = Field(default=0, description="Cantidad de nuevos clientes")
    nuevos_pagadores: int = Field(default=0, description="Cantidad de nuevos pagadores")
    ejecutivos_unicos: int = Field(
        default=0, description="Cantidad de ejecutivos únicos"
    )
    tipos_operacion: dict = Field(
        default_factory=dict, description="Count por tipo de operación"
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)  #


class NuevosClientesNuevosPagadoresRequestSchema(BaseModel):
    """
    📥 Schema para request de análisis de nuevos clientes y pagadores.
    """

    inicio: str = Field(..., description="Fecha de inicio (YYYY-MM-DD)")
    fin: str = Field(..., description="Fecha de fin (YYYY-MM-DD)")
    columnas: Optional[dict] = Field(
        default_factory=dict, description="Configuración de columnas"
    )

    @field_validator("inicio", "fin", mode="before")
    @classmethod
    def validate_date_format(cls, value):
        """
        📅 Valida formato de fechas YYYY-MM-DD.
        """
        if isinstance(value, str):
            try:
                datetime.strptime(value, "%Y-%m-%d")
                return value
            except ValueError:
                raise ValueError(f"Formato de fecha inválido: {value}. Use YYYY-MM-DD")
        return value


class NuevosClientesNuevosPagadoresResponseSchema(BaseModel):
    """
    📤 Schema para respuesta de análisis de nuevos clientes y pagadores.
    """

    registros: List[dict] = Field(
        default_factory=list, description="Registros procesados"
    )
    metadata: ProcessingMetadata = Field(..., description="Metadatos del procesamiento")
    errores: Optional[List[str]] = Field(
        default=None, description="Errores encontrados durante el procesamiento"
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)  #
