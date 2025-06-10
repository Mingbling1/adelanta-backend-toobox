from schemas.BaseSchema import BaseCreateSchema, BaseOutputSchema, BaseUpdateSchema
from uuid import UUID


class TablaMaestraCreateSchema(BaseCreateSchema):
    tabla_nombre: str
    tipo: str


class TablaMaestraOutputSchema(BaseOutputSchema):
    tabla_maestra_id: UUID
    tabla_nombre: str
    tipo: str


class TablaMaestraUpdateSchema(BaseUpdateSchema):
    tabla_nombre: str = None
    tipo: str = None


class TablaMaestraSearchOutputSchema(TablaMaestraOutputSchema):
    total_pages: int
