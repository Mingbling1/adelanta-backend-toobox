"""
📋 Schema CXC ETL Processor V2 - Orquestación completa de schemas
Coordina validación de Acumulado + Pagos + Dev + KPI en un pipeline unificado
"""

from typing import Dict, List, Optional
from pydantic import BaseModel, Field, field_validator
from datetime import datetime, date


class CXCETLRawInputSchema(BaseModel):
    """Schema de entrada para datos raw del ETL CXC"""

    # Configuración del procesamiento
    fecha_corte: Optional[str] = None
    include_fuera_sistema: bool = Field(
        default=True, description="Incluir operaciones fuera del sistema"
    )
    apply_kpi_processing: bool = Field(
        default=True, description="Aplicar procesamiento KPI"
    )
    apply_power_bi_etl: bool = Field(default=True, description="Aplicar ETL Power BI")

    # Configuración de tipo cambio
    tipo_cambio_default: float = Field(default=3.8, ge=1.0, le=10.0)

    @field_validator("fecha_corte", mode="before")
    @classmethod
    def validate_fecha_corte(cls, v):
        if v is None:
            return None

        if isinstance(v, str):
            try:
                datetime.strptime(v, "%Y-%m-%d")
                return v
            except ValueError:
                raise ValueError("fecha_corte debe estar en formato YYYY-MM-DD")

        if isinstance(v, (datetime, date)):
            return v.strftime("%Y-%m-%d")

        return v


class CXCETLProcessedSchema(BaseModel):
    """Schema para datos procesados del ETL CXC"""

    # Metadatos del procesamiento
    proceso_exitoso: bool
    fecha_procesamiento: datetime
    total_registros_acumulado: int = Field(ge=0)
    total_registros_pagos: int = Field(ge=0)
    total_registros_dev: int = Field(ge=0)

    # Estadísticas de procesamiento
    registros_fuera_sistema: int = Field(default=0, ge=0)
    duplicados_eliminados: int = Field(default=0, ge=0)
    ids_artificiales_generados: int = Field(default=0, ge=0)

    # Configuración aplicada
    etl_config: CXCETLRawInputSchema


class CXCETLOutputSchema(BaseModel):
    """Schema de salida completo del ETL CXC"""

    # Metadatos del procesamiento
    metadata: CXCETLProcessedSchema

    # Datos procesados (serán listas de diccionarios)
    acumulado_data: List[Dict] = Field(default_factory=list)
    pagos_data: List[Dict] = Field(default_factory=list)
    dev_data: List[Dict] = Field(default_factory=list)

    # Métricas agregadas
    metricas: Optional[Dict] = None

    # Datos de tipo cambio utilizados
    tipo_cambio_aplicado: float = Field(gt=0)

    @field_validator("acumulado_data", "pagos_data", "dev_data", mode="before")
    @classmethod
    def validate_data_lists(cls, v):
        """Validar que las listas de datos sean válidas"""
        if not isinstance(v, list):
            return []
        return v


class CXCETLErrorSchema(BaseModel):
    """Schema para errores en el procesamiento ETL"""

    error_type: str
    error_message: str
    error_timestamp: datetime

    # Contexto del error
    modulo_origen: Optional[str] = None
    datos_contexto: Optional[Dict] = None

    # Datos parciales salvados (si es posible)
    datos_parciales: Optional[Dict] = None


class CXCETLHealthCheckSchema(BaseModel):
    """Schema para health check del ETL CXC"""

    status: str  # "healthy", "degraded", "unhealthy"
    version: str = "v2"

    # Estado de componentes
    acumulado_status: str
    pagos_status: str
    dev_status: str
    kpi_status: str

    # Estadísticas
    ultimo_procesamiento: Optional[datetime] = None
    promedio_registros_dia: Optional[int] = None

    # Dependencias externas
    webservice_status: str
    database_status: Optional[str] = None


class CXCETLMetricsSchema(BaseModel):
    """Schema para métricas del procesamiento ETL"""

    # Tiempos de procesamiento (en segundos)
    tiempo_extraccion: float = Field(ge=0)
    tiempo_transformacion: float = Field(ge=0)
    tiempo_validacion: float = Field(ge=0)
    tiempo_total: float = Field(ge=0)

    # Volumen de datos
    mb_procesados: float = Field(ge=0)
    registros_por_segundo: float = Field(ge=0)

    # Calidad de datos
    porcentaje_validacion_exitosa: float = Field(ge=0, le=100)
    errores_validacion: int = Field(default=0, ge=0)

    # Memoria utilizada
    memoria_pico_mb: Optional[float] = None
