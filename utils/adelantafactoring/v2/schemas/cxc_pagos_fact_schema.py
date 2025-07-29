"""
🏦 Schemas para CXC Pagos Fact - Validación Pydantic con precisión financiera
"""

from pydantic import BaseModel, Field, ConfigDict, field_validator
from datetime import date, datetime
from typing import Optional
from decimal import Decimal


class CXCPagosFactBaseSchema(BaseModel):
    """Schema base para pagos de facturas CXC"""

    IdLiquidacionPago: int = Field(..., description="ID de liquidación de pago")
    IdLiquidacionDet: int = Field(..., description="ID de detalle de liquidación")
    FechaPago: date = Field(..., description="Fecha del pago")
    DiasMora: int = Field(..., description="Días de mora")
    MontoCobrarPago: Decimal = Field(..., description="Monto a cobrar del pago")
    MontoPago: Decimal = Field(..., description="Monto del pago")
    ObservacionPago: Optional[str] = Field(None, description="Observación del pago")
    InteresPago: Decimal = Field(..., description="Interés del pago")
    GastosPago: Decimal = Field(..., description="Gastos del pago")
    TipoPago: str = Field(..., description="Tipo de pago")
    SaldoDeuda: Decimal = Field(..., description="Saldo de la deuda")
    ExcesoPago: Decimal = Field(..., description="Exceso del pago")
    FechaPagoCreacion: Optional[datetime] = Field(
        None, description="Fecha de creación del pago"
    )
    FechaPagoModificacion: Optional[datetime] = Field(
        None, description="Fecha de modificación del pago"
    )

    @field_validator("IdLiquidacionPago", "IdLiquidacionDet", mode="before")
    @classmethod
    def validate_required_integer_fields(cls, v, info):
        """Convierte floats/strings a enteros para campos OBLIGATORIOS - NO acepta None"""
        field_name = info.field_name if info else "campo_desconocido"

        # Campos obligatorios NO pueden ser None
        if v is None:
            raise ValueError(f"Campo obligatorio '{field_name}' no puede ser None")

        if v == "" or v == "null":
            raise ValueError(f"Campo obligatorio '{field_name}' no puede estar vacío")

        if isinstance(v, int):
            return v

        if isinstance(v, float):
            # Si es un float que representa un entero, convertir a int
            if v.is_integer():
                return int(v)
            else:
                # Si tiene decimales, redondear hacia abajo
                return int(v)

        if isinstance(v, str):
            # Limpiar string y convertir
            v = v.strip().replace(",", "")
            if v == "":
                raise ValueError(
                    f"Campo obligatorio '{field_name}' no puede estar vacío"
                )
            try:
                # Intentar como float primero, luego convertir a int
                float_val = float(v)
                return int(float_val)
            except ValueError:
                raise ValueError(
                    f"No se puede convertir '{v}' a entero para campo '{field_name}'"
                )

        # Fallback para otros tipos
        try:
            return int(float(v))
        except (ValueError, TypeError):
            raise ValueError(
                f"No se puede convertir '{v}' (tipo: {type(v)}) a entero para campo obligatorio '{field_name}'"
            )

    @field_validator("DiasMora", mode="before")
    @classmethod
    def validate_optional_integer_fields(cls, v):
        """Convierte floats/strings a enteros para campos OPCIONALES - acepta None/vacío como 0"""
        if v is None or v == "" or v == "null":
            return 0

        if isinstance(v, int):
            return v

        if isinstance(v, float):
            # Si es un float que representa un entero, convertir a int
            if v.is_integer():
                return int(v)
            else:
                # Si tiene decimales, redondear hacia abajo
                return int(v)

        if isinstance(v, str):
            # Limpiar string y convertir
            v = v.strip().replace(",", "")
            if v == "":
                return 0
            try:
                # Intentar como float primero, luego convertir a int
                float_val = float(v)
                return int(float_val)
            except ValueError:
                return 0

        # Fallback para otros tipos
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return 0

    @field_validator(
        "MontoCobrarPago",
        "MontoPago",
        "InteresPago",
        "GastosPago",
        "SaldoDeuda",
        "ExcesoPago",
        mode="before",
    )
    @classmethod
    def validate_decimal_fields(cls, v, info):
        """Convierte a Decimal preservando precisión financiera"""
        field_name = info.field_name if info else "campo_desconocido"

        if v is None or v == "" or v == "null":
            return Decimal("0.00")

        if isinstance(v, Decimal):
            return v

        if isinstance(v, (int, float)):
            return Decimal(str(v))

        if isinstance(v, str):
            v = v.strip().replace(",", "")
            if v == "":
                return Decimal("0.00")
            try:
                return Decimal(v)
            except Exception:
                return Decimal("0.00")

        return Decimal("0.00")

    @field_validator("ObservacionPago", "TipoPago", mode="before")
    @classmethod
    def validate_string_fields(cls, v):
        """Valida campos de string, convirtiendo NaN y None a string vacío"""
        if v is None or v == "null" or str(v).lower() == "nan":
            return ""
        return str(v).strip()

    @field_validator("FechaPago", mode="before")
    @classmethod
    def validate_date_fields(cls, v):
        """Valida y convierte fechas"""
        if v is None or v == "" or v == "null":
            raise ValueError("FechaPago es obligatorio")

        if isinstance(v, date):
            return v

        if isinstance(v, datetime):
            return v.date()

        if isinstance(v, str):
            try:
                # Intentar varios formatos de fecha
                from dateutil.parser import parse

                return parse(v).date()
            except Exception:
                raise ValueError(f"No se puede convertir '{v}' a fecha")

        raise ValueError(f"Formato de fecha inválido: {v}")

    @field_validator("FechaPagoCreacion", "FechaPagoModificacion", mode="before")
    @classmethod
    def validate_datetime_fields(cls, v):
        """Valida campos datetime opcionales"""
        if v is None or v == "" or v == "null":
            return None

        if isinstance(v, datetime):
            return v

        if isinstance(v, str):
            try:
                from dateutil.parser import parse

                return parse(v)
            except Exception:
                return None

        return None

    model_config = ConfigDict(arbitrary_types_allowed=True) #
        # Habilitar validación estricta
        validate_assignment = True
        # Preservar decimales
        json_encoders = {Decimal: lambda v: float(v)}


class CXCPagosFactSchema(CXCPagosFactBaseSchema):
    """Schema completo para pagos de facturas CXC"""

    pass
