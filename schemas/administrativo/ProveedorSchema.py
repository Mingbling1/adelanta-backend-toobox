from schemas.BaseSchema import BaseCreateSchema, BaseOutputSchema, BaseUpdateSchema
from uuid import UUID


class ProveedorCreateSchema(BaseCreateSchema):
    nombre_proveedor: str
    tipo_proveedor: str
    tipo_documento: str
    numero_documento: str


class ProveedorOutputSchema(BaseOutputSchema):
    proveedor_id: UUID
    nombre_proveedor: str
    tipo_proveedor: str
    tipo_documento: str
    numero_documento: str


class ProveedorSearchOutputSchema(ProveedorOutputSchema):
    total_pages: int


class ProveedorUpdateSchema(BaseUpdateSchema):
    nombre_proveedor: str = None
    tipo_proveedor: str = None
    tipo_documento: str = None
    numero_documento: str = None
