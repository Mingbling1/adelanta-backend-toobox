from sqlalchemy import String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.auth.AuthBaseModel import AuthBaseModel
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

if TYPE_CHECKING:
    from models.auth.PermisoModel import PermisoModel


class PermisoAccionModel(AuthBaseModel):
    __tablename__ = "permiso_accion"

    permiso_accion_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    permiso_id: Mapped[UUID] = mapped_column(ForeignKey("permiso.permiso_id"))
    accion: Mapped[str] = mapped_column(String(50), nullable=False)
    descripcion: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Relación con el permiso padre
    permiso: Mapped["PermisoModel"] = relationship(back_populates="acciones")
