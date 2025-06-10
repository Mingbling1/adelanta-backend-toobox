from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy import String, ForeignKey, Boolean
from uuid import UUID, uuid4
from models.auth.AuthBaseModel import AuthBaseModel
from typing import TYPE_CHECKING
from sqlalchemy import Uuid

if TYPE_CHECKING:
    from models.auth.RolPermisoModel import RolModel


class UsuarioModel(AuthBaseModel):
    __tablename__ = "usuario"

    usuario_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    username: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=True)
    token: Mapped[UUID | None] = mapped_column(Uuid, unique=True, nullable=True)
    token_verified: Mapped[bool | None] = mapped_column(
        Boolean, default=False, nullable=True
    )
    rol_id: Mapped[UUID | None] = mapped_column(ForeignKey("rol.rol_id"), nullable=True)

    rol: Mapped["RolModel"] = relationship("RolModel")
