from sqlalchemy import String, Integer, Numeric, Date, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column
from models.datamart.DatamartBaseModel import BaseModel


class CXCPagosFactModel(BaseModel):
    __tablename__ = "CXCPagosFact"
    __table_args__ = (Index("ix_CXCPagosFact_FechaPago", "FechaPago"),)

    IdLiquidacionPago: Mapped[int] = mapped_column(
        "IdLiquidacionPago", Integer, primary_key=True
    )
    IdLiquidacionDet: Mapped[int] = mapped_column(
        "IdLiquidacionDet", Integer, nullable=False
    )
    FechaPago: Mapped[Date] = mapped_column("FechaPago", Date, nullable=False)
    DiasMora: Mapped[int] = mapped_column("DiasMora", Integer, nullable=False)
    MontoCobrarPago: Mapped[float] = mapped_column(
        "MontoCobrarPago", Numeric(18, 2), nullable=False
    )
    MontoPago: Mapped[float] = mapped_column(
        "MontoPago", Numeric(18, 2), nullable=False
    )
    ObservacionPago: Mapped[str] = mapped_column(
        "ObservacionPago", String(500), nullable=True
    )
    InteresPago: Mapped[float] = mapped_column(
        "InteresPago", Numeric(18, 2), nullable=False
    )
    GastosPago: Mapped[float] = mapped_column(
        "GastosPago", Numeric(18, 2), nullable=False
    )
    TipoPago: Mapped[str] = mapped_column("TipoPago", String(50), nullable=False)
    SaldoDeuda: Mapped[float] = mapped_column(
        "SaldoDeuda", Numeric(18, 2), nullable=False
    )
    ExcesoPago: Mapped[float] = mapped_column(
        "ExcesoPago", Numeric(18, 2), nullable=False
    )
    FechaPagoCreacion: Mapped[DateTime] = mapped_column(
        "FechaPagoCreacion", DateTime, nullable=True
    )
    FechaPagoModificacion: Mapped[DateTime] = mapped_column(
        "FechaPagoModificacion", DateTime, nullable=True
    )
