from schemas.BaseSchema import BaseCreateSchema, BaseOutputSchema, BaseUpdateSchema
from schemas.auth.RolSchema import RolPermisosOutputSchema
from schemas.auth.RolPermisoSchema import RolSubmodulosOutputSchema
from pydantic import Field, EmailStr
from uuid import UUID
from pydantic import BaseModel


class UsuarioCreateSchema(BaseCreateSchema):
    username: str
    email: EmailStr
    password: str = Field(min_length=8, max_length=255)
    token: str | None = None
    token_verified: bool | None = False


class UsuarioOutputSchema(BaseOutputSchema):
    usuario_id: str | UUID
    username: str
    email: EmailStr
    hashed_password: str = Field(min_length=8, max_length=255)


class UsuarioOutputSearchSchema(BaseOutputSchema):
    usuario_id: str | UUID
    username: str
    email: EmailStr
    hashed_password: str = Field(min_length=8, max_length=255)
    total_pages: int | None = None
    rol: RolSubmodulosOutputSchema | None = None


class UsuarioUpdateSchema(BaseUpdateSchema):
    username: str | None = None
    email: str | None = None
    rol_permiso_id: UUID | None = None


class UsuarioAsignarRolSchema(BaseModel):
    rol_id: str
    updated_by: str


class UusuarioEliminarSchema(BaseModel):
    estado: int = Field(0, description="Nuevo estado: 0=inactivo, 1=activo")
    updated_by: str | UUID
