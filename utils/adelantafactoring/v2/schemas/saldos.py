"""
Schemas para Saldos v2
"""

from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import List, Optional
import pandas as pd


class SaldosRequest(BaseModel):
    """Request para procesamiento de saldos"""

    force_refresh: bool = Field(
        default=False, description="Forzar actualización de datos"
    )


class SaldoRecord(BaseModel):
    """Registro individual de saldo procesado"""

    fecha_operacion: Optional[datetime] = Field(None, description="Fecha de operación")
    evolucion_caja: float = Field(..., description="Evolución de caja")
    costo_exceso_caja: float = Field(..., description="Costo exceso de caja")
    ingreso_no_recibido_exceso_caja: float = Field(
        ..., description="Ingreso no recibido por exceso de caja"
    )
    monto_overnight: float = Field(..., description="Monto overnight")
    ingresos_overnight_neto: float = Field(..., description="Ingresos overnight neto")
    saldo_total_caja: float = Field(..., description="Saldo total de caja")

    @field_validator("fecha_operacion", mode="before")
    @classmethod
    def parse_fecha(cls, value):
        """Convierte fechas desde string DD/MM/YYYY"""
        if value is None or value == "" or value == "null":
            return None
        if isinstance(value, str):
            try:
                return datetime.strptime(value, "%d/%m/%Y")
            except ValueError:
                try:
                    return datetime.strptime(value, "%Y-%m-%d")
                except ValueError:
                    raise ValueError(f"Fecha '{value}' no está en formato válido")
        return value

    @field_validator(
        "evolucion_caja",
        "costo_exceso_caja",
        "ingreso_no_recibido_exceso_caja",
        "monto_overnight",
        "ingresos_overnight_neto",
        "saldo_total_caja",
        mode="before",
    )
    @classmethod
    def parse_numeric_fields(cls, value):
        """Convierte campos numéricos con manejo de NaN y strings"""
        if pd.isna(value) or value == "" or value in ["-", None]:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                # Limpiar string y convertir
                clean_value = value.replace(",", "").replace("$", "").strip()
                return float(clean_value) if clean_value else 0.0
            except ValueError:
                return 0.0
        return 0.0


class SaldosResult(BaseModel):
    """Resultado del procesamiento de saldos"""

    records: List[SaldoRecord] = Field(
        ..., description="Registros de saldos procesados"
    )
    records_count: int = Field(..., description="Cantidad de registros")
    total_saldo_caja: float = Field(..., description="Total saldo de caja")
    fecha_ultimo_saldo: Optional[datetime] = Field(
        None, description="Fecha del último saldo"
    )


# Schema de compatibilidad con la versión original
class SaldosCalcularSchema(BaseModel):
    """Schema de compatibilidad con SaldosCalcular original"""

    FechaOperacion: datetime = Field(..., description="Fecha de operación")
    EvolucionCaja: Optional[float] = Field(None, description="Evolución de caja")
    CostoExcesoCaja: Optional[float] = Field(None, description="Costo exceso de caja")
    IngresoNoRecibidoPorExcesoCaja: Optional[float] = Field(
        None, description="Ingreso no recibido por exceso de caja"
    )
    MontoOvernight: Optional[float] = Field(None, description="Monto overnight")
    IngresosOvernightNeto: Optional[float] = Field(
        None, description="Ingresos overnight neto"
    )
    SaldoTotalCaja: Optional[float] = Field(None, description="Saldo total de caja")

    @field_validator("FechaOperacion", mode="before")
    @classmethod
    def parse_fecha(cls, value):
        if isinstance(value, str):
            try:
                return datetime.strptime(value, "%d/%m/%Y")
            except ValueError:
                raise ValueError(f"Fecha '{value}' no está en el formato DD/MM/YYYY")
        return value

    @field_validator(
        "EvolucionCaja",
        "CostoExcesoCaja",
        "IngresoNoRecibidoPorExcesoCaja",
        "MontoOvernight",
        "IngresosOvernightNeto",
        "SaldoTotalCaja",
        mode="before",
    )
    @classmethod
    def parse_numerics(cls, value):
        if value in ["", "-", None] or pd.isna(value):
            return 0.0
        if isinstance(value, float):
            return value
        try:
            return float(str(value).replace(",", ""))
        except ValueError:
            return 0.0
