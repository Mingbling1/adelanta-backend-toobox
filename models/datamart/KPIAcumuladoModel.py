from sqlalchemy import String, Float, Integer, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime
from config.db_mysql import Base


class KPIAcumuladoModel(Base):
    __tablename__ = "kpi_acumulado"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    CodigoLiquidacion: Mapped[str] = mapped_column(String(255))
    CodigoSolicitud: Mapped[str | None] = mapped_column(String(255), nullable=True)
    RUCCliente: Mapped[str] = mapped_column(String(255), nullable=False)
    RazonSocialCliente: Mapped[str] = mapped_column(String(255), nullable=False)
    RUCPagador: Mapped[str] = mapped_column(String(255), nullable=False)
    RazonSocialPagador: Mapped[str] = mapped_column(String(255), nullable=False)
    Moneda: Mapped[str] = mapped_column(String(255), nullable=False)
    DeudaAnterior: Mapped[float] = mapped_column(Float, nullable=False)
    ObservacionLiquidacion: Mapped[str | None] = mapped_column(
        String(255), nullable=True
    )
    ObservacionSolicitud: Mapped[str | None] = mapped_column(String(255), nullable=True)
    FlagPagoInteresConfirming: Mapped[str | None] = mapped_column(
        String(255), nullable=True
    )
    FechaInteresConfirming: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    TipoOperacion: Mapped[str] = mapped_column(String(255), nullable=False)
    Estado: Mapped[str] = mapped_column(String(255), nullable=False)
    NroDocumento: Mapped[str] = mapped_column(String(255), nullable=False)
    TasaNominalMensualPorc: Mapped[float] = mapped_column(Float, nullable=False)
    FinanciamientoPorc: Mapped[float] = mapped_column(Float, nullable=False)
    FechaConfirmado: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    FechaOperacion: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    DiasEfectivo: Mapped[int] = mapped_column(Integer, nullable=False)
    NetoConfirmado: Mapped[float] = mapped_column(Float, nullable=False)
    FondoResguardo: Mapped[float] = mapped_column(Float, nullable=False)
    MontoComisionEstructuracion: Mapped[float] = mapped_column(Float, nullable=False)
    ComisionEstructuracionIGV: Mapped[float] = mapped_column(Float, nullable=False)
    ComisionEstructuracionConIGV: Mapped[float] = mapped_column(Float, nullable=False)
    MontoCobrar: Mapped[float] = mapped_column(Float, nullable=False)
    Interes: Mapped[float] = mapped_column(Float, nullable=False)
    InteresConIGV: Mapped[float] = mapped_column(Float, nullable=False)
    GastosContrato: Mapped[float] = mapped_column(Float, nullable=False)
    GastoVigenciaPoder: Mapped[float] = mapped_column(Float, nullable=False)
    ServicioCobranza: Mapped[float] = mapped_column(Float, nullable=False)
    ServicioCustodia: Mapped[float] = mapped_column(Float, nullable=False)
    GastosDiversosIGV: Mapped[float] = mapped_column(Float, nullable=False)
    GastosDiversosConIGV: Mapped[float] = mapped_column(Float, nullable=False)
    MontoTotalFacturado: Mapped[float] = mapped_column(Float, nullable=False)
    MontoDesembolso: Mapped[float] = mapped_column(Float, nullable=False)
    FacturasGeneradas: Mapped[str] = mapped_column(String(255), nullable=False)
    Ejecutivo: Mapped[str] = mapped_column(String(255), nullable=False)
    FechaPago: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # ✅ AGREGAR COLUMNAS FALTANTES:
    FechaPagoCreacion: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    FechaPagoModificacion: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    TipoOperacionDetalle: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # ✅ FIN COLUMNAS AGREGADAS
    DiasMora: Mapped[int | None] = mapped_column(Integer, nullable=True)
    MontoCobrarPago: Mapped[float | None] = mapped_column(Float, nullable=True)
    MontoPago: Mapped[float] = mapped_column(Float, nullable=False)
    FacturasMoraGeneradas: Mapped[str | None] = mapped_column(
        String(255), nullable=True
    )
    InteresPago: Mapped[float | None] = mapped_column(Float, nullable=True)
    GastosPago: Mapped[float | None] = mapped_column(Float, nullable=True)
    TipoPago: Mapped[str | None] = mapped_column(String(255), nullable=True)
    SaldoDeuda: Mapped[float | None] = mapped_column(Float, nullable=True)
    ExcesoPago: Mapped[float] = mapped_column(Float, nullable=False)
    ObservacionPago: Mapped[str | None] = mapped_column(String(255), nullable=True)
    FechaDesembolso: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    MontoDevolucion: Mapped[float] = mapped_column(Float, nullable=False)
    DescuentoDevolucion: Mapped[float | None] = mapped_column(Float, nullable=True)
    EstadoDevolucion: Mapped[str | None] = mapped_column(String(255), nullable=True)
    ObservacionDevolucion: Mapped[str | None] = mapped_column(
        String(255), nullable=True
    )
    Anticipo: Mapped[str | None] = mapped_column(String(255), nullable=True)
    TramoAnticipo: Mapped[str | None] = mapped_column(String(255), nullable=True)
    FueraSistema: Mapped[str | None] = mapped_column(String(255), nullable=True)
    GastosDiversosSinIGV: Mapped[float | None] = mapped_column(Float, nullable=True)
    Mes: Mapped[str] = mapped_column(String(255), nullable=False)
    Año: Mapped[str] = mapped_column(String(255), nullable=False)
    MesAño: Mapped[str] = mapped_column(String(255), nullable=False)
    Sector: Mapped[str | None] = mapped_column(String(255), nullable=True)
    GrupoEco: Mapped[str | None] = mapped_column(String(255), nullable=True)
    Referencia: Mapped[str | None] = mapped_column(String(255), nullable=True)
    TipoCambioFecha: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    TipoCambioCompra: Mapped[float | None] = mapped_column(Float, nullable=True)
    TipoCambioVenta: Mapped[float | None] = mapped_column(Float, nullable=True)
    ColocacionSoles: Mapped[float] = mapped_column(Float, nullable=False)
    MontoDesembolsoSoles: Mapped[float] = mapped_column(Float, nullable=False)
    Ingresos: Mapped[float] = mapped_column(Float, nullable=False)
    IngresosSoles: Mapped[float] = mapped_column(Float, nullable=False)
    MesSemana: Mapped[str] = mapped_column(String(255), nullable=False)
    CostosFondo: Mapped[float] = mapped_column(Float, nullable=False)
    TotalIngresos: Mapped[float] = mapped_column(Float, nullable=False)
    CostosFondoSoles: Mapped[float] = mapped_column(Float, nullable=False)
    TotalIngresosSoles: Mapped[float] = mapped_column(Float, nullable=False)
    MontoPagoSoles: Mapped[float] = mapped_column(Float, nullable=False)
    MontoNegociado: Mapped[float] = mapped_column(Float, nullable=False)
    MontoNegociadoSoles: Mapped[float] = mapped_column(Float, nullable=False)
    Utilidad: Mapped[float] = mapped_column(Float, nullable=False)

    def to_dict(self) -> dict:
        data = self.__dict__.copy()
        data.pop("id", None)
        data.pop("_sa_instance_state", None)
        return data
