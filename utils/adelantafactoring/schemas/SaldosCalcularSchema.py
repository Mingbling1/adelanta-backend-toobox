from pydantic import BaseModel, field_validator
from datetime import datetime
import pandas as pd


class SaldosCalcularSchema(BaseModel):
    FechaOperacion: datetime
    EvolucionCaja: float | None
    CostoExcesoCaja: float | None
    IngresoNoRecibidoPorExcesoCaja: float | None
    MontoOvernight: float | None
    IngresosOvernightNeto: float | None
    SaldoTotalCaja: float | None

    @field_validator("FechaOperacion", mode="before")
    @classmethod
    def parse_fecha(cls, value):
        if isinstance(value, str):
            try:
                return datetime.strptime(value, "%d/%m/%Y")
            except ValueError:
                raise ValueError(f"Fecha '{value}' no está en el formato DD/MM/YYYY")
        return value

    @field_validator(
        "FechaOperacion",
        "EvolucionCaja",
        "CostoExcesoCaja",
        "IngresoNoRecibidoPorExcesoCaja",
        "MontoOvernight",
        "IngresosOvernightNeto",
        "SaldoTotalCaja",
        mode="before",
    )
    @classmethod
    def convert_nan_to_none(cls, value):
        if pd.isna(value) or value == "":
            return None
        return value

    @field_validator(
        "EvolucionCaja",
        "CostoExcesoCaja",
        "IngresoNoRecibidoPorExcesoCaja",
        "MontoOvernight",
        "IngresosOvernightNeto",
        "SaldoTotalCaja",
        mode="before",
    )
    @classmethod
    def parse_numerics(cls, value):
        if value in ["", "-", None]:
            return 0.0
        if isinstance(value, float):
            return value
        try:
            return float(value.replace(",", ""))
        except ValueError:
            return 0.0

    @field_validator(
        "EvolucionCaja",
        "CostoExcesoCaja",
        "IngresoNoRecibidoPorExcesoCaja",
        "MontoOvernight",
        "IngresosOvernightNeto",
        "SaldoTotalCaja",
        mode="after",
    )
    @classmethod
    def none_to_zero(cls, value):
        if value is None:
            return 0.0
        return value
