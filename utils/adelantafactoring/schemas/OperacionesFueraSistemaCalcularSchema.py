from pydantic import BaseModel, field_validator
from datetime import datetime, date


class OperacionesFueraSistemaCalcularSchema(BaseModel):
    CodigoLiquidacion: str
    NroDocumento: str
    RazonSocialCliente: str
    RUCCliente: str
    RazonSocialPagador: str
    RUCPagador: str
    TasaNominalMensualPorc: float | None
    FinanciamientoPorc: float | None
    FechaOperacion: date | None
    FechaConfirmado: date | None
    DiasEfectivo: int
    Moneda: str
    NetoConfirmado: float | None
    MontoComisionEstructuracion: float | None
    ComisionEstructuracionIGV: float | None
    ComisionEstructuracionConIGV: float | None
    FondoResguardo: float | None
    MontoCobrar: float
    Interes: float | None
    InteresConIGV: float | None
    GastosContrato: float | None
    ServicioCustodia: float | None
    ServicioCobranza: float | None
    GastoVigenciaPoder: float | None
    GastosDiversosSinIGV: float | None
    GastosDiversosIGV: float | None
    GastosDiversosConIGV: float | None
    MontoTotalFacturado: float | None
    FacturasGeneradas: str | None
    MontoDesembolso: float | None
    FechaPago: date | None
    Estado: str | None
    DiasMora: int | None
    InteresPago: float | None
    GastosPago: float | None
    MontoCobrarPago: float | None
    MontoPago: float | None
    ExcesoPago: float | None
    FechaDesembolso: date | None
    Ejecutivo: str | None
    TipoOperacion: str | None

    @field_validator("RUCCliente", "RUCPagador", "NroDocumento", mode="before")
    @classmethod
    def validate_ruc(cls, value):
        try:
            if isinstance(value, (int, float)):  # Acepta enteros y flotantes
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
    def validate_comision(cls, value):
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
        "MontoTotalFacturado",
        mode="before",
    )
    @classmethod
    def validate_gastos(cls, value):
        try:
            if value in ["", "-", "<"]:
                return 0
            return float(value)
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
    def validate_date(cls, value):
        """
        🎯 VALIDADOR DE FECHAS MEJORADO - SIN TIMEZONE PARA EXCEL

        Convierte datetime con timezone a date sin timezone para:
        ✅ Evitar error: "Excel does not support datetimes with timezones"
        ✅ Mantener solo la fecha (sin hora/timezone)
        ✅ Compatibilidad total con Excel export
        """
        if isinstance(value, dict) and "f_jsdate__java_util_Date" in value:
            try:
                dt = datetime.fromisoformat(value["f_jsdate__java_util_Date"])
                return dt.date()  # Convertir a date (sin timezone)
            except Exception:
                return None

        if value == "" or value is None:
            return None

        try:
            if isinstance(value, datetime):
                return value.date()  # Convertir datetime a date (sin timezone)
            elif isinstance(value, date):
                return value  # Ya es date, no necesita conversión
            else:
                # String ISO format
                dt = datetime.fromisoformat(str(value))
                return dt.date()  # Convertir a date (sin timezone)
        except Exception:
            return None
