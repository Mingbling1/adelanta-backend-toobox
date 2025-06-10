from schemas.BaseSchema import BaseCreateSchema, BaseOutputSchema, BaseUpdateSchema
from uuid import UUID
from typing import Optional, List


class PermisoCreateSchema(BaseCreateSchema):
    codigo: str
    nombre: str
    descripcion: str | None
    modulo_padre_id: UUID | None = None


class PermisoOutputSchema(BaseOutputSchema):
    permiso_id: UUID
    codigo: str
    nombre: str
    descripcion: Optional[str] = None
    modulo_padre_id: Optional[UUID] = None


class PermisoSearchOutputSchema(PermisoOutputSchema):
    total_pages: int


class PermisoUpdateSchema(BaseUpdateSchema):
    nombre: str = None


class SubmoduloSchema(BaseOutputSchema):
    permiso_id: UUID
    codigo: str
    nombre: str
    descripcion: Optional[str] = None


class PermisoJerarquicoSchema(BaseOutputSchema):
    permiso_id: UUID
    codigo: str
    nombre: str
    descripcion: Optional[str] = None
    submodulos: List[SubmoduloSchema]


class PermisoRolSchema(BaseOutputSchema):
    permiso_id: UUID
    codigo: str
    nombre: str
    descripcion: Optional[str] = None
    submodulos: List[SubmoduloSchema] = []
