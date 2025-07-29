"""
📊 Schema V2 - Diferido

Validación Pydantic para datos de diferidos internos y externos
"""

from pydantic import BaseModel, field_validator, ConfigDict
from typing import Dict, Any, Optional, List
from datetime import datetime
from decimal import Decimal


class DiferidoBaseSchema(BaseModel):
    """Schema base para datos de diferido"""

    model_config = ConfigDict(
        validate_assignment=True, str_strip_whitespace=True, use_enum_values=True
    )

    CodigoLiquidacion: str
    NroDocumento: str
    FechaOperacion: datetime
    FechaConfirmado: datetime
    Moneda: str
    Interes: Decimal
    DiasEfectivo: int

    @field_validator("Moneda")
    @classmethod
    def validate_currency(cls, v):
        """Validar que la moneda sea válida"""
        allowed_currencies = ["PEN", "USD", "EUR"]
        if v not in allowed_currencies:
            raise ValueError(f"Moneda debe ser una de: {allowed_currencies}")
        return v

    @field_validator("Interes", mode="before")
    @classmethod
    def validate_interes(cls, v):
        """Preservar precisión financiera del interés"""
        if v is None:
            return Decimal("0.00")
        try:
            return Decimal(str(v))
        except (ValueError, TypeError):
            raise ValueError("Interes debe ser un número válido")

    @field_validator("DiasEfectivo")
    @classmethod
    def validate_dias_efectivo(cls, v):
        """Validar días efectivos"""
        if v < 0:
            raise ValueError("DiasEfectivo no puede ser negativo")
        return v


class DiferidoExternoSchema(DiferidoBaseSchema):
    """Schema para diferidos desde fuente externa (Excel)"""

    # Campos dinámicos para columnas de fechas (mes-YYYY)
    date_columns: Optional[Dict[str, Decimal]] = None

    @field_validator("date_columns", mode="before")
    @classmethod
    def validate_date_columns(cls, v):
        """Validar columnas de fechas dinámicas"""
        if v is None:
            return {}

        # Convertir valores a Decimal para precisión financiera
        validated = {}
        for key, value in v.items():
            try:
                validated[key] = (
                    Decimal(str(value)) if value is not None else Decimal("0.00")
                )
            except (ValueError, TypeError):
                validated[key] = Decimal("0.00")
        return validated


class DiferidoInternoSchema(DiferidoBaseSchema):
    """Schema para diferidos calculados internamente"""

    # Campos calculados internamente
    monto_diferido_calculado: Decimal
    fecha_calculo: datetime

    @field_validator("monto_diferido_calculado", mode="before")
    @classmethod
    def validate_monto_diferido(cls, v):
        """Preservar precisión del monto diferido"""
        if v is None:
            return Decimal("0.00")
        try:
            return Decimal(str(v))
        except (ValueError, TypeError):
            return Decimal("0.00")


class DiferidoComparacionSchema(BaseModel):
    """Schema para el resultado de comparación de diferidos"""

    model_config = ConfigDict(validate_assignment=True)

    CodigoLiquidacion: str
    Moneda: str

    # Datos externos
    externo_nro_documento: str
    externo_interes: Decimal
    externo_fecha_operacion: datetime
    externo_fecha_confirmado: datetime
    externo_dias_efectivo: int

    # Datos calculados
    calculado_nro_documento: str
    calculado_interes: Decimal
    calculado_fecha_operacion: datetime
    calculado_fecha_confirmado: datetime
    calculado_dias_efectivo: int

    # Diferencias por columnas de fechas
    diferencias_monto: Dict[str, Decimal]
    diferencia_significativa: bool

    @field_validator("diferencias_monto", mode="before")
    @classmethod
    def validate_diferencias(cls, v):
        """Validar diferencias monetarias"""
        if v is None:
            return {}

        validated = {}
        for key, value in v.items():
            try:
                validated[key] = (
                    Decimal(str(value)) if value is not None else Decimal("0.00")
                )
            except (ValueError, TypeError):
                validated[key] = Decimal("0.00")
        return validated


class DiferidoRequestSchema(BaseModel):
    """Schema para requests de diferido"""

    hasta: str  # Formato YYYY-MM
    file_path_externo: Optional[str] = None
    df_interno_data: Optional[List[Dict[str, Any]]] = None

    @field_validator("hasta")
    @classmethod
    def validate_hasta_format(cls, v):
        """Validar formato de fecha hasta"""
        import re

        if not re.match(r"^\d{4}-\d{2}$", v):
            raise ValueError("El parámetro 'hasta' debe tener el formato 'YYYY-MM'")
        return v


class DiferidoResponseSchema(BaseModel):
    """Schema para respuesta de diferido"""

    model_config = ConfigDict(validate_assignment=True)

    total_registros: int
    registros_con_diferencias: int
    diferencias_significativas: int
    periodo_analizado: str
    fecha_procesamiento: datetime
    comparaciones: List[DiferidoComparacionSchema]

    @field_validator("fecha_procesamiento", mode="before")
    @classmethod
    def set_default_fecha(cls, v):
        """Establecer fecha actual si no se proporciona"""
        return v if v is not None else datetime.now()
