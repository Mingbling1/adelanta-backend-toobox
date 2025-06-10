from pydantic import BaseModel, field_validator
from fastapi import Query
from datetime import datetime
import pandas as pd


class ReferidosCreateSchema(BaseModel):
    fecha_corte: str | None = (
        Query(
            None,
            regex=r"^\d{4}-\d{2}-\d{2}$",
            description="Formato YYYY-MM-DD (opcional)",
        ),
    )


class ReferidosPostRequestSchema(BaseModel):
    Referencia: str
    Moneda: str
    CodigoLiquidacion: str
    NroDocumento: str | None = None
    RazonSocialCliente: str
    Ejecutivo: str
    FechaOperacion: datetime

    @field_validator(
        "Referencia",
        "Moneda",
        "CodigoLiquidacion",
        "NroDocumento",
        "RazonSocialCliente",
        "Ejecutivo",
        "FechaOperacion",
        mode="before",
    )
    @classmethod
    def convert_nan_to_none(cls, value):
        if pd.isna(value):
            return None
        return value

    # @field_validator(
    #     "MontoComisionEstructuracion",
    #     "ComisionEstructuracionIGV",
    #     "GastosContrato",
    #     "GastoVigenciaPoder",
    #     "ServicioCobranza",
    #     "ServicioCustodia",
    #     "MontoPago",
    #     "MontoPagoSoles",
    #     "ExcesoPago",
    #     "DeudaAnterior",
    #     "MontoDevolucion",
    #     "DeudaAnterior",
    #     "ColocacionSoles",
    #     "IngresosSoles",
    #     "CostosFondoSoles",
    #     "TotalIngresosSoles",
    #     "Utilidad",
    #     "MontoDesembolsoSoles",
    #     "tcCompra",
    #     "tcVenta",
    #     mode="before",
    # )
    # @classmethod
    # def convert_string_to_zero(cls, value):
    #     if pd.isna(value):
    #         return 0

    #     if value in ["", "-"]:
    #         return 0

    #     return value


class ReferidosSchema(ReferidosPostRequestSchema):
    id: int
