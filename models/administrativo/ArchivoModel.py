from sqlalchemy import String, BigInteger, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Mapped, mapped_column
from models.administrativo.AdministrativoBaseModel import AdministrativoBaseModel
from uuid import UUID, uuid4
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from models.administrativo.GastoModel import GastoModel


class ArchivoModel(AdministrativoBaseModel):
    __tablename__ = "archivo"

    archivo_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    google_drive_archivo_id: Mapped[str] = mapped_column(String(225), nullable=False)
    webViewLink: Mapped[str] = mapped_column(String(225), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    path: Mapped[str] = mapped_column(String(255), nullable=False)
    path_id: Mapped[str] = mapped_column(String(255), nullable=False)
    size: Mapped[int] = mapped_column(BigInteger)
    content_type: Mapped[str] = mapped_column(String(225), nullable=False)
    parent_folder_id: Mapped[str] = mapped_column(String(225), nullable=False)

    # Foreign Key
    gasto_id: Mapped[UUID] = mapped_column(ForeignKey("gasto.gasto_id"))

    # Relationship with Gasto
    gasto: Mapped["GastoModel"] = relationship(back_populates="archivos")
