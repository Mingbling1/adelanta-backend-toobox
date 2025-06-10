from schemas.BaseSchema import BaseCreateSchema, BaseOutputSchema, BaseUpdateSchema
from uuid import UUID
from datetime import datetime
from pydantic import model_validator, Field, BaseModel, computed_field
import json
from enum import Enum
from typing import Literal, Union
import math
from schemas.administrativo.ProveedorSchema import ProveedorOutputSchema


class GastoTipo(str, Enum):
    gastosAdministracion = "gastosAdministración"
    gastosMobilidad = "gastosMovilidad"
    gastosFijos = "gastosFijos"
    gastosExtraordinarios = "gastosExtraordinarios"
    gastosOtros = "gastosOtros"


class GastoTipoCDP(str, Enum):
    recibo = "recibo"
    factura = "factura"
    ticket = "ticket"
    boleta = "boleta"
    reciboHonorarios = "reciboHonorarios"


class GastoCreateOtrosSchema(BaseCreateSchema):
    tipo_gasto: Literal[GastoTipo.gastosOtros]
    tipo_CDP: GastoTipoCDP
    fecha_emision: datetime
    importe: float
    moneda: str
    porcentaje_descuento: float
    gasto_estado: int = 0
    motivo: str
    naturaleza_gasto: str | None = None
    centro_costos: str
    fecha_pago_tentativa: datetime
    proveedor_id: UUID

    @computed_field
    @property
    def monto_neto(self) -> float:
        monto_neto = self.importe * (1 - self.porcentaje_descuento)
        return math.ceil(monto_neto * 100) / 100


class GastoCreateExtraordinarioSchema(BaseCreateSchema):
    tipo_gasto: Literal[GastoTipo.gastosExtraordinarios]
    tipo_CDP: GastoTipoCDP
    numero_CDP: str
    fecha_emision: datetime
    importe: float
    moneda: str
    porcentaje_descuento: float
    gasto_estado: int = 0
    motivo: str
    naturaleza_gasto: str | None = None
    centro_costos: str
    fecha_pago_tentativa: datetime
    fecha_contable: datetime
    proveedor_id: UUID

    @computed_field
    @property
    def monto_neto(self) -> float:
        monto_neto = self.importe * (1 - self.porcentaje_descuento)
        return math.ceil(monto_neto * 100) / 100


class GastoCreateFijoSchema(BaseCreateSchema):
    tipo_gasto: Literal[GastoTipo.gastosFijos]
    tipo_CDP: GastoTipoCDP
    numero_CDP: str
    fecha_emision: datetime
    importe: float
    moneda: str
    porcentaje_descuento: float
    motivo: str
    gasto_estado: int = 0
    naturaleza_gasto: str | None = None
    centro_costos: str
    fecha_pago_tentativa: datetime
    fecha_contable: datetime
    proveedor_id: UUID

    @computed_field
    @property
    def monto_neto(self) -> float:
        monto_neto = self.importe * (1 - self.porcentaje_descuento)
        return math.ceil(monto_neto * 100) / 100


class GastoCreateSchema(BaseModel):
    tipo_gasto: Union[
        GastoCreateOtrosSchema,
        GastoCreateExtraordinarioSchema,
        GastoCreateFijoSchema,
    ] = Field(..., discriminator="tipo_gasto")

    @model_validator(mode="before")
    @classmethod
    def validate_to_json(cls, value):
        if isinstance(value, str):
            return cls(**json.loads(value))
        return {"tipo_gasto": {**value}}


class GastoOutputSchema(BaseOutputSchema):
    gasto_id: UUID
    tipo_gasto: str
    tipo_CDP: str
    numero_CDP: str | None = None
    fecha_emision: datetime
    importe: float
    moneda: str
    porcentaje_descuento: float
    gasto_estado: str | int
    motivo: str
    naturaleza_gasto: str | None = None
    centro_costos: str
    fecha_pago_tentativa: datetime
    fecha_contable: datetime | None = None
    monto_neto: float
    proveedor_id: UUID


class GastoSearchOutputSchema(GastoOutputSchema):
    proveedor: ProveedorOutputSchema | None = None
    total_pages: int


class GastoUpdateOtrosSchema(BaseUpdateSchema):
    tipo_gasto: Literal[GastoTipo.gastosOtros] = GastoTipo.gastosOtros
    tipo_CDP: GastoTipoCDP | None = None
    fecha_emision: datetime | None = None
    importe: float | None = None
    moneda: str | None = None
    porcentaje_descuento: float | None = None
    motivo: str | None = None
    naturaleza_gasto: str | None = None
    centro_costos: str | None = None
    fecha_pago_tentativa: datetime | None = None

    @computed_field
    @property
    def monto_neto(self) -> float:
        if self.importe and self.porcentaje_descuento is not None:
            monto_neto = self.importe * (1 - self.porcentaje_descuento)
            return math.ceil(monto_neto * 100) / 100
        return None


class GastoUpdateExtraordinarioSchema(BaseUpdateSchema):
    tipo_gasto: Literal[GastoTipo.gastosExtraordinarios] = (
        GastoTipo.gastosExtraordinarios
    )
    tipo_CDP: GastoTipoCDP | None = None
    numero_CDP: str | None = None
    fecha_emision: datetime | None = None
    importe: float | None = None
    moneda: str | None = None
    porcentaje_descuento: float | None = None
    motivo: str | None = None
    naturaleza_gasto: str | None = None
    centro_costos: str | None = None
    fecha_pago_tentativa: datetime | None = None
    fecha_contable: datetime | None = None

    @computed_field
    @property
    def monto_neto(self) -> float:
        if self.importe and self.porcentaje_descuento is not None:
            monto_neto = self.importe * (1 - self.porcentaje_descuento)
            return math.ceil(monto_neto * 100) / 100
        return None


class GastoUpdateFijoSchema(BaseUpdateSchema):
    tipo_gasto: Literal[GastoTipo.gastosFijos] = GastoTipo.gastosFijos
    tipo_CDP: GastoTipoCDP | None = None
    numero_CDP: str | None = None
    fecha_emision: datetime | None = None
    importe: float | None = None
    moneda: str | None = None
    porcentaje_descuento: float | None = None
    motivo: str | None = None
    naturaleza_gasto: str | None = None
    centro_costos: str | None = None
    fecha_pago_tentativa: datetime | None = None
    fecha_contable: datetime | None = None

    @computed_field
    @property
    def monto_neto(self) -> float:
        if self.importe and self.porcentaje_descuento is not None:
            monto_neto = self.importe * (1 - self.porcentaje_descuento)
            return math.ceil(monto_neto * 100) / 100
        return None


class GastoUpdateSchema(BaseModel):
    tipo_gasto: Union[
        GastoUpdateOtrosSchema,
        GastoUpdateExtraordinarioSchema,
        GastoUpdateFijoSchema,
    ] = Field(..., discriminator="tipo_gasto")

    @model_validator(mode="before")
    @classmethod
    def validate_to_json(cls, value):
        if isinstance(value, str):
            return cls(**json.loads(value))
        return {"tipo_gasto": {**value}}
