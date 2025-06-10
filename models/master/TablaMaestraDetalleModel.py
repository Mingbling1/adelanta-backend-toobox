from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import ForeignKey
from sqlalchemy import Integer, String
from models.master.MasterBaseModel import MasterBaseModel
from uuid import UUID, uuid4


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from models.master.TablaMaestraModel import TablaMaestraModel


class TablaMaestraDetalleModel(MasterBaseModel):
    __tablename__ = "tabla_maestra_detalle"

    tabla_maestra_detalle_id: Mapped[UUID] = mapped_column(
        primary_key=True, default=uuid4
    )

    tabla_maestra_id: Mapped[UUID] = mapped_column(
        ForeignKey("tabla_maestra.tabla_maestra_id"), nullable=False
    )
    codigo: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    valor: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    descripcion: Mapped[str] = mapped_column(String(250), nullable=False, unique=True)

    tabla_maestra: Mapped["TablaMaestraModel"] = relationship(back_populates="detalles")
