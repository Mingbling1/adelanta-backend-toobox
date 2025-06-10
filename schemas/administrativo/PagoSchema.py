from schemas.BaseSchema import BaseCreateSchema, BaseOutputSchema, BaseUpdateSchema
from uuid import UUID
from datetime import datetime


class PagoCreateSchema(BaseCreateSchema):
    pago_monto: float
    pago_fecha: str
    gasto_id: UUID
    cuenta_bancaria_id: UUID


class PagoOutputSchema(BaseOutputSchema):
    pago_id: UUID
    pago_fecha: datetime
    pago_monto: float
    gasto_id: UUID
    cuenta_bancaria_id: UUID


class PagoSearchOutputSchema(PagoOutputSchema):
    total_pages: int


class PagoUpdateSchema(BaseUpdateSchema):
    pago_fecha: str = None
    pago_monto: float = None
    pago_estado: str = None
    cuenta_bancaria_id: UUID
