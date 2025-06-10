from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Integer, Float
from config.db_mysql import Base


class RetomasModel(Base):
    __tablename__ = "retomas"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    RUCPagador: Mapped[str | None] = mapped_column(String(255), nullable=True)
    RazonSocialPagador: Mapped[str | None] = mapped_column(String(255), nullable=True)
    Cobranzas_MontoPagoSoles: Mapped[float] = mapped_column(Float, nullable=False)
    Desembolsos_MontoDesembolsoSoles: Mapped[float] = mapped_column(
        Float, nullable=False
    )
    PorRetomar: Mapped[float] = mapped_column(Float, nullable=False)

    def to_dict(self) -> dict:
        data = self.__dict__.copy()
        data.pop("id", None)
        data.pop("_sa_instance_state", None)
        return data
