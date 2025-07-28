from pydantic import BaseModel, Field, field_validator
from datetime import date, datetime
from typing import Optional


class CXCPagosFactCalcularSchema(BaseModel):
    IdLiquidacionPago: int = Field(..., description="ID de liquidación de pago")
    IdLiquidacionDet: int = Field(..., description="ID de detalle de liquidación")
    FechaPago: date = Field(..., description="Fecha del pago")
    DiasMora: int = Field(..., description="Días de mora")
    MontoCobrarPago: float = Field(..., description="Monto a cobrar del pago")
    MontoPago: float = Field(..., description="Monto del pago")
    ObservacionPago: Optional[str] = Field(None, description="Observación del pago")
    InteresPago: float = Field(..., description="Interés del pago")
    GastosPago: float = Field(..., description="Gastos del pago")
    TipoPago: str = Field(..., description="Tipo de pago")
    SaldoDeuda: float = Field(..., description="Saldo de la deuda")
    ExcesoPago: float = Field(..., description="Exceso del pago")
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

    @field_validator("ObservacionPago", "TipoPago", mode="before")
    @classmethod
    def validate_string_fields(cls, v):
        """Valida campos de string, convirtiendo NaN y None a string vacío"""
        import math

        # Manejar None o strings vacíos
        if v is None or v == "" or v == "null":
            return ""

        # Manejar valores NaN (float)
        if isinstance(v, float) and math.isnan(v):
            return ""

        # Si ya es string, retornar tal como está
        if isinstance(v, str):
            return v.strip()

        # Convertir otros tipos a string
        return str(v).strip()

    class Config:
        from_attributes = True
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
            datetime: lambda v: v.isoformat() if v else None,
        }
