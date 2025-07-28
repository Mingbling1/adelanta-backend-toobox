from pydantic import BaseModel, Field, field_validator
from datetime import date, datetime
from typing import Optional


class CXCDevFactCalcularSchema(BaseModel):
    IdLiquidacionDevolucion: int = Field(
        ..., description="ID de liquidación de devolución"
    )
    IdLiquidacionDet: int = Field(..., description="ID de detalle de liquidación")
    FechaDesembolso: Optional[date] = Field(None, description="Fecha de desembolso")
    MontoDevolucion: float = Field(..., description="Monto de devolución")
    DescuentoDevolucion: float = Field(..., description="Descuento de devolución")
    EstadoDevolucion: int = Field(..., description="Estado de devolución")
    ObservacionDevolucion: Optional[str] = Field(
        None, description="Observación de devolución"
    )

    @field_validator("IdLiquidacionDevolucion", "IdLiquidacionDet", mode="before")
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

    @field_validator("ObservacionDevolucion", mode="before")
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

    class Config:
        from_attributes = True
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
        }
