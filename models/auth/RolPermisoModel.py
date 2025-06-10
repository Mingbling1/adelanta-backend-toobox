from sqlalchemy import ForeignKey
from config.db_mysql_administrativo import BaseAdministrativo
from sqlalchemy.orm import relationship, Mapped, mapped_column
from uuid import uuid4, UUID
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from models.auth.RolModel import RolModel
    from models.auth.PermisoModel import PermisoModel


class RolPermisoModel(BaseAdministrativo):
    __tablename__ = "rol_permiso"

    rol_permiso_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    rol_id: Mapped[UUID] = mapped_column(ForeignKey("rol.rol_id"), nullable=False)
    permiso_id: Mapped[UUID] = mapped_column(
        ForeignKey("permiso.permiso_id"), nullable=False
    )

    rol: Mapped["RolModel"] = relationship(back_populates="permisos")
    permiso: Mapped["PermisoModel"] = relationship(back_populates="roles")
