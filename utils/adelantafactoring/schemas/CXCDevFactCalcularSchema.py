from pydantic import BaseModel, Field, field_validator
from datetime import date, datetime
from typing import Optional


class CXCDevFactCalcularSchema(BaseModel):
    IdLiquidacionDevolucion: int = Field(
        ..., description="ID de liquidación de devolución"
    )
    IdLiquidacionDet: int = Field(..., description="ID de detalle de liquidación")
    FechaDesembolso: Optional[date] = Field(None, description="Fecha de desembolso")
    MontoDevolucion: float = Field(..., description="Monto de devolución")
    DescuentoDevolucion: float = Field(..., description="Descuento de devolución")
    EstadoDevolucion: int = Field(..., description="Estado de devolución")
    ObservacionDevolucion: Optional[str] = Field(
        None, description="Observación de devolución"
    )

    @field_validator("FechaDesembolso", mode="before")
    @classmethod
    def validate_fecha_desembolso(cls, v):
        """Convierte fechas en formato dd/mm/yyyy a date object"""
        if v is None or v == "" or v == "null":
            return None

        if isinstance(v, date):
            return v

        if isinstance(v, datetime):
            return v.date()

        if isinstance(v, str):
            # Intentar diferentes formatos
            formats = [
                "%d/%m/%Y",  # 15/10/2019
                "%Y-%m-%d",  # 2019-10-15
                "%d-%m-%Y",  # 15-10-2019
                "%Y/%m/%d",  # 2019/10/15
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue

            # Si ningún formato funciona, lanzar error específico
            raise ValueError(
                f"Formato de fecha no reconocido: {v}. Formatos soportados: dd/mm/yyyy, yyyy-mm-dd"
            )

        raise ValueError(f"Tipo de fecha no soportado: {type(v)}")

    class Config:
        from_attributes = True
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
        }
