from sqlalchemy import String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Mapped, mapped_column
from models.administrativo.AdministrativoBaseModel import AdministrativoBaseModel
from uuid import UUID, uuid4
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from models.administrativo.ProveedorModel import ProveedorModel
    from models.administrativo.PagoModel import PagoModel


class CuentaBancariaModel(AdministrativoBaseModel):
    __tablename__ = "cuenta_bancaria"

    cuenta_bancaria_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    banco: Mapped[str] = mapped_column(String(255))
    moneda: Mapped[str] = mapped_column(String(255))
    tipo_cuenta: Mapped[str] = mapped_column(String(255))
    cc: Mapped[str | None] = mapped_column(String(255), nullable=True)
    cci: Mapped[str | None] = mapped_column(String(255), nullable=True)
    nota: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # Foreign key
    proveedor_id: Mapped[UUID] = mapped_column(ForeignKey("proveedor.proveedor_id"))

    # Relationship
    proveedor: Mapped["ProveedorModel"] = relationship(
        back_populates="cuentas_bancarias"
    )
    pago: Mapped["PagoModel"] = relationship(back_populates="cuenta_bancaria")
