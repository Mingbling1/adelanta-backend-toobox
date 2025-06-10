from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Integer, DateTime
from datetime import datetime
from config.db_mysql import Base


class ReferidosModel(Base):
    __tablename__ = "referidos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    Referencia: Mapped[str] = mapped_column(String(255), nullable=True)
    Moneda: Mapped[str] = mapped_column(String(255), nullable=False)
    CodigoLiquidacion: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False
    )
    NroDocumento: Mapped[str | None] = mapped_column(String(255), nullable=True)
    RazonSocialCliente: Mapped[str] = mapped_column(String(255), nullable=False)
    Ejecutivo: Mapped[str] = mapped_column(String(255), nullable=False)
    FechaOperacion: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    def to_dict(self) -> dict:
        data = self.__dict__.copy()
        data.pop("id", None)
        data.pop("_sa_instance_state", None)
        return data
