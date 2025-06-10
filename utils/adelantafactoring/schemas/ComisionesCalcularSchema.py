from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Literal, Tuple
import pandas as pd


class RegistroComisionSchema(BaseModel):
    RUCCliente: str | None
    RUCPagador: str
    Tipo: Literal["Nuevo", "Recurrente"]
    Detalle: str
    Mes: str
    TipoOperacion: Literal["Factoring", "Confirming", "Capital de Trabajo"]
    Ejecutivo: str

    @field_validator("RUCCliente", mode="before")
    @classmethod
    def _nan_to_none(cls, v):
        # convierte NaN (float) o valores missing de pandas en None
        return None if pd.isna(v) else v


class PromocionSchema(BaseModel):
    Ejecutivo: str
    TipoOperacion: Literal["Factoring", "Confirming", "Capital de Trabajo"]
    FechaExpiracion: datetime
