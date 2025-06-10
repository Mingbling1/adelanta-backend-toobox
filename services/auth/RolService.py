from repositories.auth.RolRepository import RolRepository
from models.auth.RolModel import RolModel
from schemas.auth.RolSchema import (
    RolCreateSchema,
    RolUpdateSchema,
)
from fastapi import Depends
from uuid import UUID


class RolService:
    def __init__(self, service: RolRepository = Depends()):
        self.service = service

    async def get_by_id(self, rol_id: str) -> RolModel:
        return await self.service.get_by_id(UUID(rol_id), "rol_id")

    async def update(self, rol_id: str, input: RolUpdateSchema) -> RolModel:
        return await self.service.update(UUID(rol_id), input.model_dump(), "rol_id")

    async def create(self, input: RolCreateSchema) -> RolModel:
        return await self.service.create(input.model_dump())

    async def create_many_with_return(
        self, input: list[RolCreateSchema]
    ) -> list[RolModel]:
        return await self.service.create_many_with_return(
            [item.model_dump() for item in input]
        )

    async def delete_all(self):
        await self.service.delete_all()

    async def get_all(self, limit: int = 10, offset: int = 0) -> list[RolModel]:
        return await self.service.get_all(limit, offset)

    async def get_all_and_search(
        self,
        limit: int = 10,
        offset: int = 1,
        nombre: str = None,
    ) -> list[RolModel]:
        return await self.service.get_all_and_search(limit, offset, nombre)

    async def obtener_roles_con_permisos(
        self,
        limit: int = 10,
        page: int = 1,
    ) -> list[dict]:
        """
        Obtiene roles con sus permisos en estructura jerárquica.

        Args:
            limit: Número de registros por página
            page: Número de página (iniciando en 1)

        Returns:
            list: Lista de roles con permisos organizados jerárquicamente
        """
        return await self.service.obtener_roles_con_permisos(limit, page)

    async def eliminar_rol_y_permisos(self, rol_id: str) -> dict:
        """
        Elimina un rol y todos sus permisos asociados en una sola operación.

        Args:
            rol_id: ID del rol a eliminar

        Returns:
            dict: Información sobre la operación realizada
        """
        try:
            return await self.service.eliminar_rol_y_sus_permisos(UUID(rol_id))
        except Exception as e:
            # La excepción ya viene manejada del repositorio
            raise
