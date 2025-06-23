from sqlalchemy import Integer, Numeric, Date, Index, String
from sqlalchemy.orm import Mapped, mapped_column
from models.datamart.DatamartBaseModel import BaseModel


class CXCDevFactModel(BaseModel):
    __tablename__ = "CXCDevFact"
    __table_args__ = (Index("ix_CXCDevFact_FechaDesembolso", "FechaDesembolso"),)

    IdLiquidacionDevolucion: Mapped[int] = mapped_column(
        "IdLiquidacionDevolucion", Integer, primary_key=True
    )
    IdLiquidacionDet: Mapped[int] = mapped_column(
        "IdLiquidacionDet", Integer, nullable=False
    )
    FechaDesembolso: Mapped[Date] = mapped_column(
        "FechaDesembolso", Date, nullable=True
    )
    MontoDevolucion: Mapped[float] = mapped_column(
        "MontoDevolucion", Numeric(18, 2), nullable=False
    )
    DescuentoDevolucion: Mapped[float] = mapped_column(
        "DescuentoDevolucion", Numeric(18, 2), nullable=False
    )
    EstadoDevolucion: Mapped[int] = mapped_column(
        "EstadoDevolucion", Integer, nullable=False
    )
    ObservacionDevolucion: Mapped[str] = mapped_column(
        "ObservacionDevolucion", String(500), nullable=True
    )
