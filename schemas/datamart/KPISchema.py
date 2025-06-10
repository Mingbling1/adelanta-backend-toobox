from pydantic import BaseModel, field_validator
from datetime import datetime
import pandas as pd


class KPISchema(BaseModel):
    CodigoLiquidacion: str
    CodigoSolicitud: str | None
    RUCCliente: str
    RazonSocialCliente: str
    RUCPagador: str
    RazonSocialPagador: str
    Moneda: str
    DeudaAnterior: float
    ObservacionLiquidacion: str | None
    ObservacionSolicitud: str | None
    FlagPagoInteresConfirming: str | None
    TipoOperacion: str
    Estado: str
    NroDocumento: str
    TasaNominalMensualPorc: float
    FinanciamientoPorc: float
    FechaConfirmado: datetime | None
    FechaOperacion: datetime
    DiasEfectivo: int
    NetoConfirmado: float
    FondoResguardo: float
    MontoComisionEstructuracion: float
    ComisionEstructuracionIGV: float
    ComisionEstructuracionConIGV: float
    MontoCobrar: float
    Interes: float
    InteresConIGV: float
    GastosContrato: float
    GastoVigenciaPoder: float
    ServicioCobranza: float
    ServicioCustodia: float
    GastosDiversosIGV: float
    GastosDiversosConIGV: float
    MontoTotalFacturado: float
    MontoDesembolso: float
    FacturasGeneradas: str
    Ejecutivo: str
    FechaPago: datetime | None
    MontoPago: float
    MontoPagoSoles: float
    ExcesoPago: float
    FechaDesembolso: datetime | None
    MontoDevolucion: float
    EstadoDevolucion: str | None
    Anticipo: str | None
    TramoAnticipo: str | None
    Mes: str
    Año: str
    MesAño: str
    tcFecha: datetime | None
    tcCompra: float | None
    tcVenta: float | None
    ColocacionSoles: float
    MontoDesembolsoSoles: float
    Ingresos: float
    IngresosSoles: float
    MesSemana: str
    CostosFondo: float
    TotalIngresos: float
    CostosFondoSoles: float
    TotalIngresosSoles: float
    Utilidad: float
    Sector: str | None
    GrupoEco: str | None
    FueraSistema: str | None

    @field_validator(
        "FechaDesembolso",
        "FechaConfirmado",
        "FechaOperacion",
        "tcFecha",
        "Sector",
        "CodigoSolicitud",
        "ObservacionLiquidacion",
        "ObservacionSolicitud",
        "FlagPagoInteresConfirming",
        "EstadoDevolucion",
        "Anticipo",
        "TramoAnticipo",
        "Sector",
        "GrupoEco",
        "FueraSistema",
        mode="before",
    )
    @classmethod
    def convert_nan_to_none(cls, value):
        if pd.isna(value):
            return None
        return value

    @field_validator(
        "MontoComisionEstructuracion",
        "ComisionEstructuracionIGV",
        "GastosContrato",
        "GastoVigenciaPoder",
        "ServicioCobranza",
        "ServicioCustodia",
        "MontoPago",
        "MontoPagoSoles",
        "ExcesoPago",
        "DeudaAnterior",
        "MontoDevolucion",
        "DeudaAnterior",
        "ColocacionSoles",
        "IngresosSoles",
        "CostosFondoSoles",
        "TotalIngresosSoles",
        "Utilidad",
        "MontoDesembolsoSoles",
        "tcCompra",
        "tcVenta",
        mode="before",
    )
    @classmethod
    def convert_string_to_zero(cls, value):
        if pd.isna(value):
            return 0

        if value in ["", "-"]:
            return 0

        return value
