from repositories.auth.RolPermisoRepository import RolPermisoRepository
from repositories.auth.RolRepository import RolRepository
from schemas.auth.RolPermisoSchema import (
    RolPermisoCreateSchema,
    RolPermisoBulkCreateSchema,
    RolPermisoUpdateSchema,
)
from fastapi import Depends, HTTPException
from uuid import UUID
from schemas.auth.RolSchema import RolCreateSchema


class RolPermisoService:
    def __init__(
        self,
        rol_permiso_repository: RolPermisoRepository = Depends(),
        rol_repository: RolRepository = Depends(),
    ):
        self.rol_permiso_repository = rol_permiso_repository
        self.rol_repository = rol_repository

    async def asignar_permiso_a_rol(self, input: RolPermisoCreateSchema):
        await self.rol_permiso_repository.asignar_permiso_a_rol(
            input.rol_id, input.permiso_id
        )

    async def asignar_permisos_a_rol_masivo(self, input: RolPermisoBulkCreateSchema):
        rol_create_schema = RolCreateSchema(
            nombre=input.rol_nombre, created_by=input.created_by
        )
        rol = await self.rol_repository.create(rol_create_schema.model_dump())

        await self.rol_permiso_repository.asignar_permisos_a_rol_masivo(
            rol.rol_id, input.permisos
        )

    async def eliminar_permiso_de_rol(self, input: RolPermisoCreateSchema):
        await self.rol_permiso_repository.eliminar_permiso_de_rol(
            input.rol_id, input.permiso_id
        )

    async def obtener_permisos_por_rol(self, rol_id: str):
        return await self.rol_permiso_repository.obtener_permisos_por_rol(UUID(rol_id))

    async def obtener_roles_por_permiso(self, permiso_id: str):
        return await self.rol_permiso_repository.obtener_roles_por_permiso(
            UUID(permiso_id)
        )

    async def actualizar_permisos_rol(self, input: RolPermisoUpdateSchema):
        """
        Actualiza los permisos de un rol según los parámetros especificados.
        """
        return await self.rol_permiso_repository.actualizar_permisos_rol(
            input.rol_id, input.permisos_agregar, input.permisos_quitar
        )

    async def listar_roles_con_submodulos(self, limit: int = 10, page: int = 1):
        """
        Obtiene una lista paginada de roles con sus submódulos asignados.
        """
        return await self.rol_permiso_repository.listar_roles_con_submodulos(
            limit, page
        )
