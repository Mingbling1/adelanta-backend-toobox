from repositories.auth.PermisoRepository import PermisoRepository
from models.auth.PermisoModel import PermisoModel
from schemas.auth.PermisoSchema import (
    PermisoCreateSchema,
    PermisoUpdateSchema,
)
from fastapi import Depends
from uuid import UUID


class PermisoService:
    def __init__(self, service: PermisoRepository = Depends()):
        self.service = service

    async def get_by_id(self, permiso_id: str) -> PermisoModel:
        return await self.service.get_by_id(UUID(permiso_id), "permiso_id")

    async def update(self, permiso_id: str, input: PermisoUpdateSchema) -> PermisoModel:
        return await self.service.update(
            UUID(permiso_id), input.model_dump(), "permiso_id"
        )

    async def create(self, input: PermisoCreateSchema) -> PermisoModel:
        return await self.service.create(input.model_dump())

    async def create_many_with_return(
        self, input: list[PermisoCreateSchema]
    ) -> list[PermisoModel]:
        return await self.service.create_many_with_return(
            [item.model_dump() for item in input]
        )

    async def delete_all(self):
        await self.service.delete_all()

    async def get_all(self, limit: int = 10, offset: int = 0) -> list[PermisoModel]:
        return await self.service.get_all(limit, offset)

    async def get_all_and_search(
        self,
        limit: int = 10,
        offset: int = 1,
        nombre: str = None,
    ) -> list[PermisoModel]:
        return await self.service.get_all_and_search(limit, offset, nombre)
