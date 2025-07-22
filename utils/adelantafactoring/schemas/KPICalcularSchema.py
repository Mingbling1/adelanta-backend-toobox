from pydantic import BaseModel, field_validator, computed_field
from datetime import datetime
import pandas as pd


class KPICalcularSchema(BaseModel):
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
    Referencia: str | None
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
    TipoCambioFecha: datetime | None
    TipoCambioCompra: float | None
    TipoCambioVenta: float
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
    FechaPago: datetime | None

    @computed_field
    @property
    def MontoNegociado(self) -> float:
        return self.NetoConfirmado - self.FondoResguardo

    @computed_field
    @property
    def MontoNegociadoSoles(self) -> float | None:
        return (
            (self.NetoConfirmado - self.FondoResguardo) * (self.TipoCambioVenta)
            if self.Moneda == "USD"
            else self.MontoNegociado
        )

    @field_validator(
        "FechaDesembolso",
        "FechaConfirmado",
        "FechaOperacion",
        "FechaPago",
        "TipoCambioFecha",
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
        "TipoCambioCompra",
        "TipoCambioVenta",
        mode="before",
    )
    @classmethod
    def convert_string_to_zero(cls, value):
        if pd.isna(value):
            return 0

        if value in ["", "-"]:
            return 0

        return value

    @field_validator(
        "FechaDesembolso",
        "FechaConfirmado",
        "FechaOperacion",
        "FechaPago",
        "TipoCambioFecha",
        mode="before",
    )
    @classmethod
    def convert_string_to_datetime(cls, value):
        if isinstance(value, str):
            if value in ["", "-"]:
                return None
            try:
                return datetime.strptime(value, "%d/%m/%Y")
            except ValueError:
                raise ValueError(f"Invalid date format: {value}")
        return value


class KPIAcumuladoCalcularSchema(BaseModel):
    """
    Schema para KPI Acumulado cuando tipo_reporte = 0.
    Incluye todos los campos de KPICalcularSchema más campos adicionales específicos para reportes acumulados.
    """

    # Campos base existentes de KPICalcularSchema
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

    # Nuevos campos específicos para acumulado
    FechaInteresConfirming: datetime | None
    TipoOperacionDetalle: str | None

    # Campos base continuados
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

    # Campos de pago extendidos para acumulado
    FechaPago: datetime | None
    FechaPagoCreacion: datetime | None
    FechaPagoModificacion: datetime | None
    DiasMora: int | None
    MontoCobrarPago: float | None
    MontoPago: float
    FacturasMoraGeneradas: str | None
    InteresPago: float | None
    GastosPago: float | None
    TipoPago: str | None
    SaldoDeuda: float | None
    ExcesoPago: float
    ObservacionPago: str | None

    # Campos de desembolso y devolución extendidos
    FechaDesembolso: datetime | None
    MontoDevolucion: float
    DescuentoDevolucion: float | None
    EstadoDevolucion: str | None
    ObservacionDevolucion: str | None
    Anticipo: str | None
    TramoAnticipo: str | None

    # Campos calculados base (mantenidos del esquema original)
    Referencia: str | None
    MontoPagoSoles: float
    Mes: str
    Año: str
    MesAño: str
    TipoCambioFecha: datetime | None
    TipoCambioCompra: float | None
    TipoCambioVenta: float
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

    @computed_field
    @property
    def MontoNegociado(self) -> float:
        return self.NetoConfirmado - self.FondoResguardo

    @computed_field
    @property
    def MontoNegociadoSoles(self) -> float | None:
        return (
            (self.NetoConfirmado - self.FondoResguardo) * (self.TipoCambioVenta)
            if self.Moneda == "USD"
            else self.MontoNegociado
        )

    # Reutilizando field_validators de KPICalcularSchema con campos adicionales
    @field_validator(
        "FechaDesembolso",
        "FechaConfirmado",
        "FechaOperacion",
        "FechaPago",
        "FechaPagoCreacion",
        "FechaPagoModificacion",
        "FechaInteresConfirming",
        "TipoCambioFecha",
        "Sector",
        "CodigoSolicitud",
        "ObservacionLiquidacion",
        "ObservacionSolicitud",
        "FlagPagoInteresConfirming",
        "EstadoDevolucion",
        "ObservacionDevolucion",
        "Anticipo",
        "TramoAnticipo",
        "GrupoEco",
        "FueraSistema",
        "TipoOperacionDetalle",
        "FacturasMoraGeneradas",
        "TipoPago",
        "ObservacionPago",
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
        "MontoCobrarPago",
        "InteresPago",
        "GastosPago",
        "SaldoDeuda",
        "ExcesoPago",
        "DeudaAnterior",
        "MontoDevolucion",
        "DescuentoDevolucion",
        "ColocacionSoles",
        "IngresosSoles",
        "CostosFondoSoles",
        "TotalIngresosSoles",
        "Utilidad",
        "MontoDesembolsoSoles",
        "TipoCambioCompra",
        "TipoCambioVenta",
        mode="before",
    )
    @classmethod
    def convert_string_to_zero(cls, value):
        if pd.isna(value):
            return 0

        if value in ["", "-"]:
            return 0

        return value

    @field_validator(
        "FechaDesembolso",
        "FechaConfirmado",
        "FechaOperacion",
        "FechaPago",
        "FechaPagoCreacion",
        "FechaPagoModificacion",
        "FechaInteresConfirming",
        "TipoCambioFecha",
        mode="before",
    )
    @classmethod
    def convert_string_to_datetime(cls, value):
        if isinstance(value, str):
            if value in ["", "-", "No Aplica"]:
                return None
            try:
                return datetime.strptime(value, "%d/%m/%Y")
            except ValueError:
                raise ValueError(f"Invalid date format: {value}")
        return value

    @field_validator(
        "DiasMora",
        mode="before",
    )
    @classmethod
    def convert_dias_mora(cls, value):
        if pd.isna(value):
            return None
        if value in ["", "-"]:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
