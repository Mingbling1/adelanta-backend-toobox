from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, Float, String
from config.db_mysql import Base


class TipoCambioModel(Base):
    __tablename__ = "tipo_cambio"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True, unique=True
    )
    TipoCambioFecha: Mapped[str] = mapped_column(
        String(255), nullable=False, unique=True
    )
    TipoCambioCompra: Mapped[float] = mapped_column(Float, nullable=False)
    TipoCambioVenta: Mapped[float] = mapped_column(Float, nullable=False)

    def to_dict(self) -> dict:
        data = self.__dict__.copy()
        data.pop("id", None)
        data.pop("_sa_instance_state", None)
        return data
