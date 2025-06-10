from sqlalchemy import String, Float, DateTime, ForeignKey, Text, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime
from models.administrativo.AdministrativoBaseModel import AdministrativoBaseModel
from uuid import UUID, uuid4

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from models.administrativo.ArchivoModel import ArchivoModel
    from models.administrativo.PagoModel import PagoModel
    from models.administrativo.ProveedorModel import ProveedorModel


class GastoModel(AdministrativoBaseModel):
    __tablename__ = "gasto"

    gasto_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    tipo_gasto: Mapped[str] = mapped_column(String(255), nullable=False)
    tipo_CDP: Mapped[str] = mapped_column(String(255))
    numero_CDP: Mapped[str | None] = mapped_column(String(255), nullable=True)
    fecha_emision: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    importe: Mapped[float] = mapped_column(Float)
    moneda: Mapped[str] = mapped_column(String(255))
    porcentaje_descuento: Mapped[float] = mapped_column(Float)
    gasto_estado: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    motivo: Mapped[str] = mapped_column(Text)
    naturaleza_gasto: Mapped[str | None] = mapped_column(String(255), nullable=True)
    centro_costos: Mapped[str] = mapped_column(String(255))
    fecha_pago_tentativa: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    fecha_contable: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    monto_neto: Mapped[float] = mapped_column(Float)

    # Foreign key
    proveedor_id: Mapped[UUID] = mapped_column(ForeignKey("proveedor.proveedor_id"))

    # Relationships
    archivos: Mapped[list["ArchivoModel"]] = relationship(back_populates="gasto")
    pagos: Mapped[list["PagoModel"]] = relationship(back_populates="gasto")
    proveedor: Mapped["ProveedorModel"] = relationship(back_populates="gasto")
