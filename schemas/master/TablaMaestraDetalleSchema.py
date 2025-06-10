from schemas.BaseSchema import BaseCreateSchema, BaseOutputSchema, BaseUpdateSchema
from uuid import UUID


class TablaMaestraDetalleCreateSchema(BaseCreateSchema):
    tabla_maestra_id: UUID
    codigo: int
    valor: str
    descripcion: str


class TablaMaestraDetalleOutputSchema(BaseOutputSchema):
    tabla_maestra_detalle_id: UUID
    tabla_maestra_id: UUID
    codigo: int
    valor: str
    descripcion: str


class TablaMaestraDetalleUpdateSchema(BaseUpdateSchema):
    codigo: int = None
    valor: str = None
    descripcion: str = None


class TablaMaestraDetalleSearchOutputSchema(TablaMaestraDetalleOutputSchema):
    total_pages: int
