from pydantic import BaseModel, field_validator
from datetime import datetime
import pandas as pd


class NuevosClientesNuevosPagadoresCalcularSchema(BaseModel):
    FechaOperacion: datetime | None
    Ejecutivo: str | None
    RUCCliente: str | None
    RUCPagador: str | None
    TipoOperacion: str | None
    RazonSocial: str | None

    @field_validator("RUCCliente", "RUCPagador", mode="before")
    @classmethod
    def validate_ruc(cls, value):
        if isinstance(value, int):
            return str(value)
        return value

    @field_validator(
        "FechaOperacion",
        "Ejecutivo",
        "RUCCliente",
        "RUCPagador",
        "TipoOperacion",
        "RazonSocial",
        mode="before",
    )
    @classmethod
    def convert_nan_to_none(cls, value):
        if pd.isna(value):
            return None
        return value
