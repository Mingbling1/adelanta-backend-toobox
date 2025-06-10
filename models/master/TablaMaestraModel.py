from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import String
from models.master.MasterBaseModel import MasterBaseModel
from uuid import UUID, uuid4
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from models.master.TablaMaestraDetalleModel import TablaMaestraDetalleModel


class TablaMaestraModel(MasterBaseModel):
    __tablename__ = "tabla_maestra"

    tabla_maestra_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)

    tabla_nombre: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    tipo: Mapped[str] = mapped_column(String(250), nullable=False, index=True)

    detalles: Mapped[list["TablaMaestraDetalleModel"]] = relationship(
        back_populates="tabla_maestra"
    )
