from sqlalchemy import String, Float, Integer, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime
from config.db_mysql import Base


class KPIModel(Base):
    __tablename__ = "kpi"
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
    TipoOperacion: Mapped[str] = mapped_column(String(255), nullable=False)
    Estado: Mapped[str] = mapped_column(String(255), nullable=False)
    NroDocumento: Mapped[str] = mapped_column(String(255), nullable=False)
    TasaNominalMensualPorc: Mapped[float] = mapped_column(Float, nullable=False)
    FinanciamientoPorc: Mapped[float] = mapped_column(Float, nullable=False)
    FechaConfirmado: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    FechaOperacion: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    DiasEfectivo: Mapped[int] = mapped_column(Integer, nullable=False)
    NetoConfirmado: Mapped[float] = mapped_column(Float, nullable=False)
    FondoResguardo: Mapped[float] = mapped_column(Float, nullable=False)
    MontoNegociado: Mapped[float] = mapped_column(Float, nullable=False)
    MontoNegociadoSoles: Mapped[float | None] = mapped_column(Float, nullable=True)
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
    Referencia: Mapped[str | None] = mapped_column(String(255), nullable=True)
    FechaPago: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    MontoPago: Mapped[float] = mapped_column(Float, nullable=False)
    MontoPagoSoles: Mapped[float] = mapped_column(Float, nullable=False)
    ExcesoPago: Mapped[float] = mapped_column(Float, nullable=False)
    FechaDesembolso: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    MontoDevolucion: Mapped[float] = mapped_column(Float, nullable=False)
    EstadoDevolucion: Mapped[str | None] = mapped_column(String(255), nullable=True)
    Anticipo: Mapped[str | None] = mapped_column(String(255), nullable=True)
    TramoAnticipo: Mapped[str | None] = mapped_column(String(255), nullable=True)
    Mes: Mapped[str] = mapped_column(String(255), nullable=False)
    Año: Mapped[str] = mapped_column(String(255), nullable=False)
    MesAño: Mapped[str] = mapped_column(String(255), nullable=False)
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
    Utilidad: Mapped[float] = mapped_column(Float, nullable=False)
    Sector: Mapped[str | None] = mapped_column(String(255), nullable=True)
    GrupoEco: Mapped[str | None] = mapped_column(String(255), nullable=True)
    FueraSistema: Mapped[str | None] = mapped_column(String(255), nullable=True)

    def to_dict(self) -> dict:
        """
        Devuelve un dict con los campos en el mismo orden
        que las columnas definidas en __table__.columns.
        """
        # Recorremos las columnas tal como están declaradas en el modelo
        data = {
            col.name: getattr(self, col.name)
            for col in self.__table__.columns
            if col.name != "id"  # si no quieres incluir el id
        }
        return data
