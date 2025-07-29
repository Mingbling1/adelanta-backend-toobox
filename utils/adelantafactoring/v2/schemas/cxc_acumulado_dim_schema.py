"""
📊 CXC Acumulado DIM Schema V2 - Validación con Pydantic 2.0+
Preserva integridad de datos financieros para CXC acumulado dimensional
Schema complejo con múltiples campos financieros y dimensionales
"""

from pydantic import BaseModel, Field, ConfigDict, field_validator
from datetime import date, datetime
from typing import Optional
from decimal import Decimal
import math


class CXCAcumuladoDIMRawSchema(BaseModel):
    """
    Schema para validar datos RAW que vienen del webservice.
    Convierte automáticamente strings a tipos apropiados.
    """

    # Campos principales de identificación
    IdLiquidacionCab: int = Field(..., description="ID de liquidación cabecera")
    IdLiquidacionDet: int = Field(..., description="ID de detalle de liquidación")
    CodigoLiquidacion: str = Field(..., description="Código de liquidación")
    CodigoSolicitud: str = Field(..., description="Código de solicitud")

    # Datos de cliente y pagador
    RUCCliente: str = Field(..., description="RUC del cliente")
    RazonSocialCliente: str = Field(..., description="Razón social del cliente")
    RUCPagador: str = Field(..., description="RUC del pagador")
    RazonSocialPagador: str = Field(..., description="Razón social del pagador")

    # Datos financieros básicos
    Moneda: str = Field(..., description="Moneda")
    DeudaAnterior: Decimal = Field(..., description="Deuda anterior")

    # Observaciones
    ObservacionLiquidacion: Optional[str] = Field(
        None, description="Observación de liquidación"
    )
    ObservacionSolicitud: Optional[str] = Field(
        None, description="Observación de solicitud"
    )

    # Flags y configuraciones
    FlagPagoInteresConfirming: str = Field(
        ..., description="Flag de pago interés confirming"
    )
    FechaInteresConfirming: Optional[date] = Field(
        None, description="Fecha interés confirming"
    )

    # Tipo de operación
    TipoOperacion: str = Field(..., description="Tipo de operación")
    TipoOperacionDetalle: str = Field(..., description="Detalle tipo de operación")
    Estado: str = Field(..., description="Estado")
    NroDocumento: str = Field(..., description="Número de documento")

    # Tasas y porcentajes
    TasaNominalMensualPorc: Decimal = Field(
        ..., description="Tasa nominal mensual porcentaje"
    )
    FinanciamientoPorc: Decimal = Field(..., description="Financiamiento porcentaje")

    # Fechas críticas
    FechaConfirmado: date = Field(..., description="Fecha confirmado")
    FechaOperacion: date = Field(..., description="Fecha de operación")

    # Días y montos principales
    DiasEfectivo: int = Field(..., description="Días efectivo")
    NetoConfirmado: Decimal = Field(..., description="Neto confirmado")
    FondoResguardo: Decimal = Field(..., description="Fondo resguardo")

    # Comisiones y descuentos
    ComisionEstructuracionPorc: Decimal = Field(
        ..., description="Comisión estructuración porcentaje"
    )

    @field_validator(
        "IdLiquidacionCab", "IdLiquidacionDet", "DiasEfectivo", mode="before"
    )
    @classmethod
    def validate_required_integer_fields(cls, v, info):
        """
        Convierte floats/strings a enteros para campos OBLIGATORIOS - NO acepta None.
        Preserva integridad de datos financieros.
        """
        field_name = info.field_name if info else "campo_desconocido"

        if v is None:
            raise ValueError(f"Campo obligatorio '{field_name}' no puede ser None")

        if v == "" or v == "null":
            raise ValueError(f"Campo obligatorio '{field_name}' no puede estar vacío")

        if isinstance(v, int):
            return v

        if isinstance(v, float):
            if v.is_integer():
                return int(v)
            else:
                return int(v)

        if isinstance(v, str):
            v = v.strip().replace(",", "")
            if v == "":
                raise ValueError(
                    f"Campo obligatorio '{field_name}' no puede estar vacío"
                )
            try:
                float_val = float(v)
                return int(float_val)
            except ValueError:
                raise ValueError(
                    f"No se puede convertir '{v}' a entero para campo '{field_name}'"
                )

        try:
            return int(float(v))
        except (ValueError, TypeError):
            raise ValueError(
                f"No se puede convertir '{v}' (tipo: {type(v)}) a entero para campo obligatorio '{field_name}'"
            )

    @field_validator(
        "DeudaAnterior",
        "TasaNominalMensualPorc",
        "FinanciamientoPorc",
        "NetoConfirmado",
        "FondoResguardo",
        "ComisionEstructuracionPorc",
        mode="before",
    )
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

    @field_validator(
        "CodigoLiquidacion",
        "CodigoSolicitud",
        "RUCCliente",
        "RazonSocialCliente",
        "RUCPagador",
        "RazonSocialPagador",
        "Moneda",
        "FlagPagoInteresConfirming",
        "TipoOperacion",
        "TipoOperacionDetalle",
        "Estado",
        "NroDocumento",
        "ObservacionLiquidacion",
        "ObservacionSolicitud",
        mode="before",
    )
    @classmethod
    def validate_string_fields(cls, v):
        """Valida campos de string, convirtiendo NaN y None a string vacío cuando es opcional"""
        # Para campos obligatorios como CodigoLiquidacion, validar que no sean None
        if v is None:
            return ""  # Permitir vacío y luego validar en validación específica si es necesario

        # Manejar valores NaN (float)
        if isinstance(v, float) and math.isnan(v):
            return ""

        # Si ya es string, retornar tal como está
        if isinstance(v, str):
            return v.strip()

        # Convertir otros tipos a string
        return str(v).strip()

    @field_validator(
        "FechaConfirmado", "FechaOperacion", "FechaInteresConfirming", mode="before"
    )
    @classmethod
    def validate_fecha_fields(cls, v):
        """Convierte fechas en diferentes formatos a date object"""
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
                "%Y-%m-%d %H:%M:%S",  # Con timestamp
                "%d/%m/%Y %H:%M:%S",  # Con timestamp español
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


class CXCAcumuladoDIMCalcularSchema(BaseModel):
    """
    Schema final para datos procesados con ETL completo.
    Incluye campos calculados y dimensionales adicionales.
    """

    # Heredar todos los campos del schema raw
    IdLiquidacionCab: int
    IdLiquidacionDet: int
    CodigoLiquidacion: str
    CodigoSolicitud: str
    RUCCliente: str
    RazonSocialCliente: str
    RUCPagador: str
    RazonSocialPagador: str
    Moneda: str
    DeudaAnterior: Decimal
    ObservacionLiquidacion: Optional[str] = None
    ObservacionSolicitud: Optional[str] = None
    FlagPagoInteresConfirming: str
    FechaInteresConfirming: Optional[date] = None
    TipoOperacion: str
    TipoOperacionDetalle: str
    Estado: str
    NroDocumento: str
    TasaNominalMensualPorc: Decimal
    FinanciamientoPorc: Decimal
    FechaConfirmado: date
    FechaOperacion: date
    DiasEfectivo: int
    NetoConfirmado: Decimal
    FondoResguardo: Decimal
    ComisionEstructuracionPorc: Decimal

    # Campos calculados por ETL Power BI
    EstadoReal: Optional[str] = Field(None, description="Estado real calculado")
    Sector: Optional[str] = Field(None, description="Sector del pagador")
    GrupoEco: Optional[str] = Field(None, description="Grupo económico")
    MoraMayo: Optional[bool] = Field(None, description="Flag mora mayo")
    DeudaAnteriorSoles: Optional[Decimal] = Field(
        None, description="Deuda anterior en soles"
    )
    NetoConfirmadoSoles: Optional[Decimal] = Field(
        None, description="Neto confirmado en soles"
    )
    FondoResguardoSoles: Optional[Decimal] = Field(
        None, description="Fondo resguardo en soles"
    )

    model_config = ConfigDict(arbitrary_types_allowed=True) #
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
            Decimal: lambda v: float(v) if v else None,
        }


# Alias para compatibilidad con nomenclatura existente
CXCAcumuladoDIMBaseSchema = CXCAcumuladoDIMCalcularSchema
