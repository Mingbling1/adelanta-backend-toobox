from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, Float, DateTime
from datetime import datetime
from config.db_mysql import Base


class SaldosModel(Base):
    __tablename__ = "saldos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    FechaOperacion: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    EvolucionCaja: Mapped[float | None] = mapped_column(Float, nullable=True)
    CostoExcesoCaja: Mapped[float | None] = mapped_column(Float, nullable=True)
    IngresoNoRecibidoPorExcesoCaja: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )
    MontoOvernight: Mapped[float | None] = mapped_column(Float, nullable=True)
    IngresosOvernightNeto: Mapped[float | None] = mapped_column(Float, nullable=True)
    SaldoTotalCaja: Mapped[float | None] = mapped_column(Float, nullable=True)
