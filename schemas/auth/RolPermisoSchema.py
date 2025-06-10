from pydantic import BaseModel, field_validator
from uuid import UUID
from schemas.BaseSchema import BaseCreateSchema, BaseOutputSchema, BaseUpdateSchema
from typing import List, Optional


class RolPermisoCreateSchema(BaseModel):
    rol_id: UUID
    permiso_id: UUID


class RolPermisoBulkCreateSchema(BaseCreateSchema):
    rol_nombre: str
    permisos: list[UUID]

    @field_validator("rol_nombre")
    @classmethod
    def capitalizar_nombre(cls, v: str) -> str:
        if not v:
            return v

        return v.capitalize()


class RolPermisoOutputSchema(BaseModel):
    rol_permiso_id: UUID
    rol_id: UUID
    permiso_id: UUID


class RolPermisoUpdateSchema(BaseModel):
    rol_id: UUID
    permisos_agregar: Optional[List[UUID]] = None
    permisos_quitar: Optional[List[UUID]] = None


# ...existing code...


class SubmoduloOutputSchema(BaseOutputSchema):
    permiso_id: UUID
    codigo: str
    nombre: str
    descripcion: Optional[str] = None
    modulo_padre_id: UUID


class RolSubmodulosOutputSchema(BaseOutputSchema):
    rol_id: UUID
    nombre: str
    submodulos: List[SubmoduloOutputSchema]
    total_pages: int | None = None
