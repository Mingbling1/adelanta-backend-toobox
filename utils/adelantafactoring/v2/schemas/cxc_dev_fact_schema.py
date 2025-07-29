"""
📊 CXC Dev Fact Schema V2 - Validación con Pydantic 2.0+
Preserva integridad de datos financieros para devoluciones de facturas CXC
"""

from pydantic import BaseModel, Field, ConfigDict, field_validator
from datetime import date, datetime
from typing import Optional
from decimal import Decimal
import math


class CXCDevFactBaseSchema(BaseModel):
    """Schema base para validación de devoluciones de facturas CXC"""

    IdLiquidacionDevolucion: int = Field(
        ..., description="ID de liquidación de devolución"
    )
    IdLiquidacionDet: int = Field(..., description="ID de detalle de liquidación")
    FechaDesembolso: Optional[date] = Field(None, description="Fecha de desembolso")
    MontoDevolucion: Decimal = Field(..., description="Monto de devolución")
    DescuentoDevolucion: Decimal = Field(..., description="Descuento de devolución")
    EstadoDevolucion: int = Field(..., description="Estado de devolución")
    ObservacionDevolucion: Optional[str] = Field(
        None, description="Observación de devolución"
    )

    @field_validator("IdLiquidacionDevolucion", "IdLiquidacionDet", mode="before")
    @classmethod
    def validate_required_integer_fields(cls, v, info):
        """
        Convierte floats/strings a enteros para campos OBLIGATORIOS - NO acepta None.
        Preserva integridad de datos financieros.
        """
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

    @field_validator("EstadoDevolucion", mode="before")
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

    @field_validator("MontoDevolucion", "DescuentoDevolucion", mode="before")
    @classmethod
    def validate_decimal_fields(cls, v, info):
        """
        Convierte valores numéricos a Decimal preservando precisión financiera.
        NUNCA modifica datos financieros originales.
        """
        field_name = info.field_name if info else "campo_desconocido"

        if v is None or v == "" or v == "null":
            return Decimal("0.00")

        # Manejar NaN
        if isinstance(v, float) and math.isnan(v):
            return Decimal("0.00")

        # Si ya es Decimal, preservar tal como está
        if isinstance(v, Decimal):
            return v

        # Convertir números a Decimal manteniendo precisión
        if isinstance(v, (int, float)):
            return Decimal(str(v))

        # Limpiar strings y convertir
        if isinstance(v, str):
            v = v.strip().replace(",", "")
            if v == "":
                return Decimal("0.00")
            try:
                return Decimal(v)
            except Exception:
                raise ValueError(
                    f"No se puede convertir '{v}' a Decimal para campo '{field_name}'"
                )

        # Fallback
        try:
            return Decimal(str(v))
        except Exception:
            raise ValueError(
                f"Tipo no soportado para campo Decimal '{field_name}': {type(v)}"
            )

    @field_validator("ObservacionDevolucion", mode="before")
    @classmethod
    def validate_string_fields(cls, v):
        """Valida campos de string, convirtiendo NaN y None a string vacío"""
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

    @field_validator("FechaDesembolso", mode="before")
    @classmethod
    def validate_fecha_desembolso(cls, v):
        """Convierte fechas en formato dd/mm/yyyy a date object"""
        if v is None or v == "" or v == "null":
            return None

        if isinstance(v, date):
            return v

        if isinstance(v, datetime):
            return v.date()

        if isinstance(v, str):
            # Intentar diferentes formatos
            formats = [
                "%d/%m/%Y",  # 15/10/2019
                "%Y-%m-%d",  # 2019-10-15
                "%d-%m-%Y",  # 15-10-2019
                "%Y/%m/%d",  # 2019/10/15
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue

            # Si ningún formato funciona, lanzar error específico
            raise ValueError(
                f"Formato de fecha no reconocido: {v}. Formatos soportados: dd/mm/yyyy, yyyy-mm-dd"
            )

        raise ValueError(f"Tipo de fecha no soportado: {type(v)}")

    model_config = ConfigDict(arbitrary_types_allowed=True) #
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
            Decimal: lambda v: float(v) if v else None,
        }


# Alias para compatibilidad con nomenclatura existente
CXCDevFactCalcularSchema = CXCDevFactBaseSchema
