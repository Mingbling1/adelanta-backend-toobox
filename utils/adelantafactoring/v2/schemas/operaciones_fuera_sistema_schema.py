"""
🏷️ Operaciones Fuera Sistema Schema V2 - Adelanta Factoring Financial ETL

Schemas optimizados para operaciones financieras fuera del sistema con validación RUST-powered.
Mantiene compatibilidad completa con el sistema legacy y maneja datos PEN/USD.
"""

from pydantic import BaseModel, Field, ConfigDict, field_validator
from datetime import datetime, date
from typing import List, Optional, Union


class OperacionesFueraSistemaBaseSchema(BaseModel):
    """Schema base para operaciones fuera del sistema"""

    CodigoLiquidacion: str = Field(..., description="Código de liquidación")
    NroDocumento: str = Field(..., description="Número de documento")
    RazonSocialCliente: str = Field(..., description="Razón social del cliente")
    RUCCliente: str = Field(..., description="RUC del cliente")
    RazonSocialPagador: str = Field(..., description="Razón social del pagador")
    RUCPagador: str = Field(..., description="RUC del pagador")
    TasaNominalMensualPorc: Optional[float] = Field(
        None, description="Tasa nominal mensual porcentaje"
    )
    FinanciamientoPorc: Optional[float] = Field(
        None, description="Financiamiento porcentaje"
    )
    FechaOperacion: Optional[date] = Field(None, description="Fecha de operación")
    FechaConfirmado: Optional[date] = Field(None, description="Fecha confirmado")
    DiasEfectivo: int = Field(default=0, description="Días efectivo")
    Moneda: str = Field(..., description="Moneda")
    NetoConfirmado: Optional[float] = Field(None, description="Neto confirmado")
    MontoComisionEstructuracion: Optional[float] = Field(
        None, description="Monto comisión estructuración"
    )
    ComisionEstructuracionIGV: Optional[float] = Field(
        None, description="Comisión estructuración IGV"
    )
    ComisionEstructuracionConIGV: Optional[float] = Field(
        None, description="Comisión estructuración con IGV"
    )
    FondoResguardo: Optional[float] = Field(None, description="Fondo resguardo")
    MontoCobrar: float = Field(..., description="Monto a cobrar")
    Interes: Optional[float] = Field(None, description="Interés")
    InteresConIGV: Optional[float] = Field(None, description="Interés con IGV")
    GastosContrato: Optional[float] = Field(None, description="Gastos de contrato")
    ServicioCustodia: Optional[float] = Field(None, description="Servicio custodia")
    ServicioCobranza: Optional[float] = Field(None, description="Servicio cobranza")
    GastoVigenciaPoder: Optional[float] = Field(
        None, description="Gasto vigencia poder"
    )
    GastosDiversosSinIGV: Optional[float] = Field(
        None, description="Gastos diversos sin IGV"
    )
    GastosDiversosIGV: Optional[float] = Field(None, description="Gastos diversos IGV")
    GastosDiversosConIGV: Optional[float] = Field(
        None, description="Gastos diversos con IGV"
    )
    MontoTotalFacturado: Optional[float] = Field(
        None, description="Monto total facturado"
    )
    FacturasGeneradas: Optional[str] = Field(None, description="Facturas generadas")
    MontoDesembolso: Optional[float] = Field(None, description="Monto desembolso")
    FechaPago: Optional[date] = Field(None, description="Fecha de pago")
    Estado: Optional[str] = Field(None, description="Estado")
    DiasMora: Optional[int] = Field(None, description="Días de mora")
    InteresPago: Optional[float] = Field(None, description="Interés pago")
    GastosPago: Optional[float] = Field(None, description="Gastos pago")
    MontoCobrarPago: Optional[float] = Field(None, description="Monto a cobrar pago")
    MontoPago: Optional[float] = Field(None, description="Monto pago")
    ExcesoPago: Optional[float] = Field(None, description="Exceso pago")
    FechaDesembolso: Optional[date] = Field(None, description="Fecha desembolso")
    Ejecutivo: Optional[str] = Field(None, description="Ejecutivo")
    TipoOperacion: Optional[str] = Field(None, description="Tipo de operación")

    @field_validator("RUCCliente", "RUCPagador", "NroDocumento", mode="before")
    @classmethod
    def validate_ruc_documents(cls, value):
        """🔍 Valida RUCs y documentos, convirtiendo números a strings"""
        try:
            if isinstance(value, (int, float)):
                return str(int(value))  # Convierte a string sin decimales
            if isinstance(value, str) and value.strip():
                return value.strip()
            return None
        except Exception:
            return None

    @field_validator(
        "MontoComisionEstructuracion",
        "ComisionEstructuracionIGV",
        "GastosContrato",
        mode="before",
    )
    @classmethod
    def validate_comision_fields(cls, value):
        """💰 Valida campos de comisión, maneja valores especiales"""
        try:
            if value in ["", "-"]:
                return None
            return float(value)
        except Exception:
            return None

    @field_validator(
        "ServicioCustodia",
        "ServicioCobranza",
        "GastoVigenciaPoder",
        "ComisionEstructuracionConIGV",
        "MontoPago",
        "TasaNominalMensualPorc",
        "FinanciamientoPorc",
        "NetoConfirmado",
        "ExcesoPago",
        "MontoCobrarPago",
        "GastosPago",
        "InteresPago",
        "DiasMora",
        "MontoDesembolso",
        "MontoTotalFacturado",
        "GastosDiversosConIGV",
        "GastosDiversosIGV",
        "GastosDiversosSinIGV",
        "InteresConIGV",
        "Interes",
        "MontoCobrar",
        "FondoResguardo",
        "DiasEfectivo",
        mode="before",
    )
    @classmethod
    def validate_financial_fields(cls, value):
        """💸 Valida campos financieros, convierte valores especiales a 0"""
        try:
            if value in ["", "-", "<"]:
                return 0
            return float(value) if value is not None else 0
        except Exception:
            return 0

    @field_validator(
        "FechaDesembolso",
        "FechaPago",
        "FechaOperacion",
        "FechaConfirmado",
        mode="before",
    )
    @classmethod
    def validate_date_fields(cls, value):
        """
        📅 Validador de fechas optimizado - SIN TIMEZONE para Excel

        Convierte datetime con timezone a date sin timezone para:
        ✅ Evitar error: "Excel does not support datetimes with timezones"
        ✅ Mantener solo la fecha (sin hora/timezone)
        ✅ Compatibilidad total con Excel export
        """
        if isinstance(value, dict) and "f_jsdate__java_util_Date" in value:
            try:
                dt = datetime.fromisoformat(value["f_jsdate__java_util_Date"])
                return dt.date()
            except Exception:
                return None

        if value == "" or value is None:
            return None

        try:
            if isinstance(value, datetime):
                return value.date()
            elif isinstance(value, date):
                return value
            else:
                dt = datetime.fromisoformat(str(value))
                return dt.date()
        except Exception:
            return None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class OperacionesFueraSistemaCalcularSchema(OperacionesFueraSistemaBaseSchema):
    """
    📊 Schema principal para operaciones fuera del sistema calculadas.
    Compatible 100% con el schema legacy.
    """

    pass


class OperacionesFueraSistemaRawSchema(BaseModel):
    """
    📥 Schema para datos RAW de Google Sheets (PEN/USD).
    Maneja el mapeo de columnas del Excel original.
    """

    # Mapeo de columnas originales del Excel
    Liquidacion: Optional[str] = Field(None, alias="Liquidación")
    NumeroFacturaLetra: Optional[str] = Field(None, alias="N° Factura/ Letra")
    NombreCliente: Optional[str] = Field(None, alias="Nombre Cliente")
    RUCCliente: Optional[str] = Field(None, alias="RUC Cliente")
    NombreDeudor: Optional[str] = Field(None, alias="Nombre Deudor")
    RUCDeudor: Optional[str] = Field(None, alias="RUC DEUDOR")
    TNMOp: Optional[float] = Field(None, alias="TNM Op")
    PorcentajeFinan: Optional[float] = Field(None, alias="% Finan")
    FechaOp: Optional[Union[str, date]] = Field(None, alias="Fecha de Op")
    FPagoConfirmada: Optional[Union[str, date]] = Field(None, alias="F.Pago Confirmada")
    DiasEfect: Optional[int] = Field(None, alias="Días Efect")
    Moneda: Optional[str] = Field(None, alias="Moneda")
    NetoConfirmado: Optional[float] = Field(None, alias="Neto Confirmado")
    ComisionEstructuracion: Optional[float] = Field(
        None, alias="Comisión de Estructuracion"
    )
    IGVComision: Optional[float] = Field(None, alias="IGV Comisión")
    ComisionConIGV: Optional[float] = Field(None, alias="Comision Con IGV")
    FondoResguardo: Optional[float] = Field(None, alias="Fondo Resguardo")
    NetoAFinanciar: Optional[float] = Field(None, alias="Neto a Financiar")
    Ejecutivo: Optional[str] = Field(None, alias="EJECUTIVO")
    TipoOperacion: Optional[str] = Field(None, alias="TIPO DE OPERACIÓN")

    model_config = ConfigDict(arbitrary_types_allowed=True)  #


class OperacionesFueraSistemaProcessedSchema(BaseModel):
    """
    📤 Schema para datos procesados con metadatos del procesamiento.
    """

    data: List[OperacionesFueraSistemaCalcularSchema]
    total_records: int = Field(..., description="Total de registros procesados")
    pen_records: int = Field(default=0, description="Registros en PEN")
    usd_records: int = Field(default=0, description="Registros en USD")
    filtered_records: int = Field(
        default=0, description="Registros filtrados por validación"
    )
    processing_timestamp: datetime = Field(default_factory=datetime.now)

    model_config = ConfigDict(arbitrary_types_allowed=True)  #
