from schemas.BaseSchema import BaseCreateSchema, BaseOutputSchema, BaseUpdateSchema
from schemas.auth.PermisoSchema import PermisoOutputSchema, PermisoJerarquicoSchema
from uuid import UUID


class RolCreateSchema(BaseCreateSchema):
    nombre: str


class RolOutputSchema(BaseOutputSchema):
    rol_id: UUID
    nombre: str


class RolSearchOutputSchema(RolOutputSchema):
    total_pages: int


class RolUpdateSchema(BaseUpdateSchema):
    nombre: str = None


class RolPermisosOutputSchema(BaseOutputSchema):
    rol_id: UUID
    nombre: str
    permisos: list[
        PermisoJerarquicoSchema
    ]  # Cambiado de PermisoOutputSchema a PermisoJerarquicoSchema
    total_pages: int | None = None
