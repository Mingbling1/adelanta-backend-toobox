from sqlalchemy import Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column
from models.administrativo.AdministrativoBaseModel import AdministrativoBaseModel
from typing import TYPE_CHECKING
from uuid import UUID, uuid4


if TYPE_CHECKING:
    from models.administrativo.GastoModel import GastoModel
    from models.administrativo.CuentaBancariaModel import CuentaBancariaModel


class PagoModel(AdministrativoBaseModel):
    __tablename__ = "pago"

    pago_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    pago_fecha: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    pago_monto: Mapped[float] = mapped_column(Float)

    # Foreign key
    cuenta_bancaria_id: Mapped[UUID] = mapped_column(
        ForeignKey("cuenta_bancaria.cuenta_bancaria_id")
    )

    gasto_id: Mapped[UUID] = mapped_column(ForeignKey("gasto.gasto_id"))
    # Relationship
    gasto: Mapped["GastoModel"] = relationship(back_populates="pagos")
    cuenta_bancaria: Mapped["CuentaBancariaModel"] = relationship(back_populates="pago")
