from sqlalchemy import String
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Mapped, mapped_column
from models.administrativo.AdministrativoBaseModel import AdministrativoBaseModel
from typing import TYPE_CHECKING
from uuid import UUID, uuid4


if TYPE_CHECKING:
    from models.administrativo.GastoModel import GastoModel
    from models.administrativo.CuentaBancariaModel import CuentaBancariaModel


class ProveedorModel(AdministrativoBaseModel):
    __tablename__ = "proveedor"

    proveedor_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    nombre_proveedor: Mapped[str] = mapped_column(String(255))
    tipo_proveedor: Mapped[str] = mapped_column(String(255))
    tipo_documento: Mapped[str] = mapped_column(String(255))
    numero_documento: Mapped[str] = mapped_column(String(255), index=True, unique=True)

    # Relationships
    gasto: Mapped["GastoModel"] = relationship(back_populates="proveedor")
    cuentas_bancarias: Mapped[list["CuentaBancariaModel"]] = relationship(
        back_populates="proveedor"
    )
