"""
📊 Schemas V2 - Ventas Autodetracción

Esquemas de validación para ventas con autodetracción
"""

from pydantic import BaseModel, Field, model_validator, ConfigDict
import json
import re
import pandas as pd
from datetime import datetime
from typing import Optional, Any


class VentasAutodetraccionesRequestSchema(BaseModel):
    """Schema para request de cálculo de autodetracción de ventas"""

    hasta: str = Field(
        ...,
        description="Mes hasta el cual calcular (formato YYYY-MM)",
        pattern=r"^\d{4}-\d{2}$",
    )

    @model_validator(mode="before")
    @classmethod
    def validate_to_json(cls, value):
        """Validador para convertir string JSON a dict si es necesario"""
        if isinstance(value, str):
            return cls(**json.loads(value))
        return value

    @model_validator(mode="after")
    def validate_hasta_format(self):
        """Validar que el formato de fecha sea correcto"""
        if not re.match(r"^\d{4}-\d{2}$", self.hasta):
            raise ValueError("hasta debe tener formato YYYY-MM")

        # Validar que el mes esté en rango válido
        year, month = self.hasta.split("-")
        month_int = int(month)
        if month_int < 1 or month_int > 12:
            raise ValueError("El mes debe estar entre 01 y 12")

        return self


class VentasAutodetraccionesResponseSchema(BaseModel):
    """Schema para response de cálculo de autodetracción de ventas"""

    excel_filename: str = Field(description="Nombre del archivo Excel generado")
    registro_ventas_count: int = Field(description="Cantidad de registros de ventas")
    autodetraccion_count: int = Field(
        description="Cantidad de registros con autodetracción"
    )
    total_autodetraccion: float = Field(description="Total de autodetracción calculada")
    hasta: str = Field(description="Período procesado")
    mensaje: str = Field(default="Cálculo completado exitosamente")

    model_config = ConfigDict(arbitrary_types_allowed=True)


# Schemas adicionales para compatibilidad con V1
class VentasAutodetraccionesRequest(BaseModel):
    """Request para cálculo de autodetracción de ventas (V1 compatibility)"""

    hasta: str = Field(..., description="Formato YYYY-MM")
    comprobantes_df: pd.DataFrame = Field(
        ..., description="DataFrame con comprobantes (pd.DataFrame)"
    )
    tipo_cambio_df: pd.DataFrame = Field(
        ..., description="DataFrame con tipo de cambio (pd.DataFrame)"
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)


class VentasAutodetraccionesResult(BaseModel):
    """Resultado del cálculo de autodetracción de ventas (V1 compatibility)"""

    excel_buffer: Any = Field(..., description="Buffer del archivo Excel (BytesIO)")
    registro_ventas_count: int = Field(
        ..., description="Cantidad de registros de ventas"
    )
    autodetraccion_count: int = Field(
        ..., description="Cantidad de registros con autodetracción"
    )
    hasta: str = Field(..., description="Período procesado")
    total_autodetraccion: float = Field(
        ..., description="Total de autodetracción calculada"
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)


class RegistroVenta(BaseModel):
    """Schema para un registro de venta procesado"""

    estado_doc_tributario: str = Field(
        ..., description="Estado del documento tributario"
    )
    fecha_emision: datetime = Field(..., description="Fecha de emisión")
    monto_base: float = Field(..., description="Monto base para cálculo")
    autodetraccion_aplicable: bool = Field(..., description="Si aplica autodetracción")
    monto_autodetraccion: Optional[float] = Field(
        None, description="Monto de autodetracción calculado"
    )
    tipo_comprobante: str = Field(..., description="Tipo de comprobante")
    comprobante: str = Field(..., description="Número de comprobante")
    documento: str = Field(..., description="Documento del cliente")
    razon_social: str = Field(..., description="Razón social del cliente")
    valor_venta: float = Field(..., description="Valor de venta")
    importe: float = Field(..., description="Importe total")
    moneda: str = Field(..., description="Moneda")
    fuente: str = Field(..., description="Fuente del registro")
    igv: float = Field(..., description="IGV")
    valor_venta_soles: float = Field(..., description="Valor de venta en soles")
    importe_soles: float = Field(..., description="Importe en soles")
    igv_soles: float = Field(..., description="IGV en soles")


class AutodetraccionRecord(BaseModel):
    """Schema para un registro de autodetracción"""

    fecha_emision: datetime = Field(..., description="Fecha de emisión")
    tipo_comprobante: str = Field(..., description="Tipo de comprobante")
    comprobante: str = Field(..., description="Número de comprobante")
    documento: str = Field(..., description="Documento del cliente")
    razon_social: str = Field(..., description="Razón social del cliente")
    valor_venta_soles: float = Field(..., description="Valor de venta en soles")
    importe_soles: float = Field(..., description="Importe en soles")
    igv_soles: float = Field(..., description="IGV en soles")
    autodetraccion_soles: float = Field(..., description="Autodetracción en soles")
    moneda: str = Field(..., description="Moneda")
