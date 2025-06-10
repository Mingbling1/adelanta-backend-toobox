from sqlalchemy import String, Boolean, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.auth.AuthBaseModel import AuthBaseModel
from typing import TYPE_CHECKING, List, Optional
from uuid import UUID, uuid4


if TYPE_CHECKING:
    from models.auth.RolPermisoModel import RolPermisoModel
    from models.auth.PermisoAccionModel import PermisoAccionModel


class PermisoModel(AuthBaseModel):
    __tablename__ = "permiso"
    permiso_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    codigo: Mapped[str] = mapped_column(String(50), nullable=False)
    nombre: Mapped[str] = mapped_column(String(100), nullable=False)
    descripcion: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # Quitar es_modulo como sugeriste
    # es_modulo: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Nuevo campo para la jerarquía
    modulo_padre_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("permiso.permiso_id"), nullable=True
    )

    # Relaciones
    # Relación auto-referencial para jerarquía de módulos
    modulo_padre: Mapped[Optional["PermisoModel"]] = relationship(
        "PermisoModel",
        remote_side=[permiso_id],
        back_populates="submodulos",
        foreign_keys=[modulo_padre_id],
    )
    submodulos: Mapped[List["PermisoModel"]] = relationship(
        back_populates="modulo_padre", foreign_keys=[modulo_padre_id]
    )

    # Relaciones existentes
    roles: Mapped[List["RolPermisoModel"]] = relationship(back_populates="permiso")
    acciones: Mapped[List["PermisoAccionModel"]] = relationship(
        back_populates="permiso"
    )
