from models.auth.AuthBaseModel import AuthBaseModel
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import String
from typing import TYPE_CHECKING
from uuid import UUID, uuid4


if TYPE_CHECKING:
    from models.auth.RolPermisoModel import RolPermisoModel


class RolModel(AuthBaseModel):
    __tablename__ = "rol"
    rol_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    nombre: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)

    permisos: Mapped[list["RolPermisoModel"]] = relationship(back_populates="rol")
