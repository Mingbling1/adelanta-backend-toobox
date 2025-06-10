from pydantic import BaseModel, field_validator
from datetime import datetime


class ReferidosCalcularSchema(BaseModel):
    Referencia: str
    CodigoLiquidacion: str
    Ejecutivo: str
    Mes: datetime

    @field_validator(
        "Mes",
        mode="before",
    )
    @classmethod
    def parsear_mes(cls, v):
        """
        Acepta cadena 'dd/mm/yyyy' y la convierte a datetime.
        """
        if isinstance(v, str):
            return datetime.strptime(v, "%d/%m/%Y")
        return v
