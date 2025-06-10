from schemas.BaseSchema import BaseCreateSchema, BaseOutputSchema, BaseUpdateSchema
from uuid import UUID
from schemas.administrativo.ProveedorSchema import ProveedorOutputSchema


class CuentaBancariaCreateSchema(BaseCreateSchema):
    banco: str
    moneda: str
    tipo_cuenta: str
    cc: str
    cci: str
    nota: str
    proveedor_id: UUID


class CuentaBancariaOutputSchema(BaseOutputSchema):
    cuenta_bancaria_id: UUID
    banco: str
    moneda: str
    tipo_cuenta: str
    cc: str
    cci: str
    nota: str | None
    proveedor_id: UUID


class CuentaBancariaSearchOutputSchema(CuentaBancariaOutputSchema):
    total_pages: int | None
    proveedor: ProveedorOutputSchema


class CuentaBancariaUpdateSchema(BaseUpdateSchema):
    banco: str | None
    moneda: str | None
    tipo_cuenta: str | None
    cc: str | None
    cci: str | None
    nota: str | None
