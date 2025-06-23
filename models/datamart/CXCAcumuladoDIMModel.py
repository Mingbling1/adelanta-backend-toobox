from sqlalchemy import String, Integer, Numeric, Date, Float
from sqlalchemy.orm import Mapped, mapped_column
from models.datamart.DatamartBaseModel import BaseModel


class CXCAcumuladoDIMModel(BaseModel):
    __tablename__ = "CXCAcumuladoDim"

    # Campos originales existentes
    IdLiquidacionCab: Mapped[int] = mapped_column(
        "IdLiquidacionCab", Integer, primary_key=True
    )
    IdLiquidacionDet: Mapped[int] = mapped_column(
        "IdLiquidacionDet", Integer, nullable=False, primary_key=True
    )
    CodigoLiquidacion: Mapped[str] = mapped_column(
        "CodigoLiquidacion", String(20), nullable=False
    )
    CodigoSolicitud: Mapped[str] = mapped_column(
        "CodigoSolicitud", String(20), nullable=False
    )
    RUCCliente: Mapped[str] = mapped_column("RUCCliente", String(11), nullable=False)
    RazonSocialCliente: Mapped[str] = mapped_column(
        "RazonSocialCliente", String(200), nullable=False
    )
    RUCPagador: Mapped[str] = mapped_column("RUCPagador", String(11), nullable=False)
    RazonSocialPagador: Mapped[str] = mapped_column(
        "RazonSocialPagador", String(200), nullable=False
    )
    Moneda: Mapped[str] = mapped_column("Moneda", String(3), nullable=False)
    DeudaAnterior: Mapped[float] = mapped_column(
        "DeudaAnterior", Numeric(18, 2), nullable=False
    )
    ObservacionLiquidacion: Mapped[str] = mapped_column(
        "ObservacionLiquidacion", String(500), nullable=True
    )
    ObservacionSolicitud: Mapped[str] = mapped_column(
        "ObservacionSolicitud", String(500), nullable=True
    )
    FlagPagoInteresConfirming: Mapped[str] = mapped_column(
        "FlagPagoInteresConfirming", String(20), nullable=False
    )
    FechaInteresConfirming: Mapped[Date] = mapped_column(
        "FechaInteresConfirming", Date, nullable=True
    )
    TipoOperacion: Mapped[str] = mapped_column(
        "TipoOperacion", String(100), nullable=False
    )
    TipoOperacionDetalle: Mapped[str] = mapped_column(
        "TipoOperacionDetalle", String(200), nullable=False
    )
    Estado: Mapped[str] = mapped_column("Estado", String(100), nullable=False)
    NroDocumento: Mapped[str] = mapped_column(
        "NroDocumento", String(50), nullable=False
    )
    TasaNominalMensualPorc: Mapped[float] = mapped_column(
        "TasaNominalMensualPorc", Numeric(18, 6), nullable=False
    )
    FinanciamientoPorc: Mapped[float] = mapped_column(
        "FinanciamientoPorc", Numeric(18, 6), nullable=False
    )
    FechaConfirmado: Mapped[Date] = mapped_column(
        "FechaConfirmado", Date, nullable=False
    )
    FechaOperacion: Mapped[Date] = mapped_column("FechaOperacion", Date, nullable=False)
    DiasEfectivo: Mapped[int] = mapped_column("DiasEfectivo", Integer, nullable=False)
    NetoConfirmado: Mapped[float] = mapped_column(
        "NetoConfirmado", Numeric(18, 2), nullable=False
    )
    FondoResguardo: Mapped[float] = mapped_column(
        "FondoResguardo", Numeric(18, 2), nullable=False
    )
    ComisionEstructuracionPorc: Mapped[float] = mapped_column(
        "ComisionEstructuracionPorc", Numeric(18, 6), nullable=False
    )
    MontoComisionEstructuracion: Mapped[float] = mapped_column(
        "MontoComisionEstructuracion", Numeric(18, 2), nullable=False
    )
    ComisionEstructuracionIGV: Mapped[float] = mapped_column(
        "ComisionEstructuracionIGV", Numeric(18, 6), nullable=False
    )
    ComisionEstructuracionConIGV: Mapped[float] = mapped_column(
        "ComisionEstructuracionConIGV", Numeric(18, 2), nullable=False
    )
    FacturasGeneradas: Mapped[str] = mapped_column(
        "FacturasGeneradas", String(50), nullable=True
    )
    MontoCobrar: Mapped[float] = mapped_column(
        "MontoCobrar", Numeric(18, 2), nullable=False
    )
    Interes: Mapped[float] = mapped_column("Interes", Numeric(18, 2), nullable=False)
    InteresIGV: Mapped[float] = mapped_column(
        "InteresIGV", Numeric(18, 6), nullable=False
    )
    InteresConIGV: Mapped[float] = mapped_column(
        "InteresConIGV", Numeric(18, 2), nullable=False
    )
    GastosContrato: Mapped[float] = mapped_column(
        "GastosContrato", Numeric(18, 2), nullable=False
    )
    GastoVigenciaPoder: Mapped[float] = mapped_column(
        "GastoVigenciaPoder", Numeric(18, 2), nullable=False
    )
    ServicioCobranza: Mapped[float] = mapped_column(
        "ServicioCobranza", Numeric(18, 2), nullable=False
    )
    ServicioCustodia: Mapped[float] = mapped_column(
        "ServicioCustodia", Numeric(18, 2), nullable=False
    )
    GastosDiversos: Mapped[float] = mapped_column(
        "GastosDiversos", Numeric(18, 2), nullable=False
    )
    GastosDiversosIGV: Mapped[float] = mapped_column(
        "GastosDiversosIGV", Numeric(18, 6), nullable=False
    )
    GastosDiversosConIGV: Mapped[float] = mapped_column(
        "GastosDiversosConIGV", Numeric(18, 2), nullable=False
    )
    MontoTotalFacturado: Mapped[float] = mapped_column(
        "MontoTotalFacturado", Numeric(18, 2), nullable=False
    )
    MontoDesembolso: Mapped[float] = mapped_column(
        "MontoDesembolso", Numeric(18, 2), nullable=False
    )
    Ejecutivo: Mapped[str] = mapped_column("Ejecutivo", String(200), nullable=False)

    # === COLUMNAS ETL CALCULADAS ===
    SaldoTotal: Mapped[float] = mapped_column("SaldoTotal", Numeric(18, 2), nullable=False)
    SaldoTotalPen: Mapped[float] = mapped_column("SaldoTotalPen", Numeric(18, 2), nullable=False)
    TipoPagoReal: Mapped[str] = mapped_column("TipoPagoReal", String(50), nullable=False)
    EstadoCuenta: Mapped[str] = mapped_column("EstadoCuenta", String(20), nullable=False)
    EstadoReal: Mapped[str] = mapped_column("EstadoReal", String(50), nullable=False)
    Sector: Mapped[str] = mapped_column("Sector", String(200), nullable=True)
    GrupoEco: Mapped[str] = mapped_column("GrupoEco", String(200), nullable=True)