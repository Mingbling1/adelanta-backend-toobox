from uuid import UUID, uuid4
from datetime import datetime
from sqlalchemy import String, DateTime
from sqlalchemy.orm import mapped_column, Mapped
from config.db_mysql import Base


# from datetime import datetime
class ActualizacionReportesModel(Base):
    __tablename__ = "actualizacion_reportes"

    actualizacion_reporte_id: Mapped[UUID] = mapped_column(
        primary_key=True, default=uuid4
    )
    ultima_actualizacion: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    estado: Mapped[str] = mapped_column(String(50), nullable=False)
    detalle: Mapped[str] = mapped_column(String(255), nullable=True)

    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "ultima_actualizacion": self.ultima_actualizacion.isoformat(),
            "estado": self.estado,
            "detalle": self.detalle,
        }
