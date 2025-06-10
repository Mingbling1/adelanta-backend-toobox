from repositories.administrativo.ProveedorRepository import ProveedorRepository
from models.administrativo.ProveedorModel import ProveedorModel
from schemas.administrativo.ProveedorSchema import (
    ProveedorCreateSchema,
    ProveedorUpdateSchema,
)
from fastapi import Depends
from uuid import UUID


class ProveedorService:
    def __init__(self, proveedor_repository: ProveedorRepository = Depends()):
        self.proveedor_repository = proveedor_repository

    async def get_by_id(self, proveedor_id: str) -> ProveedorModel:
        return await self.proveedor_repository.get_by_id(
            UUID(proveedor_id), "proveedor_id"
        )

    async def update(
        self, proveedor_id: str, input: ProveedorUpdateSchema
    ) -> ProveedorModel:
        return await self.proveedor_repository.update(
            UUID(proveedor_id), input.model_dump(), "proveedor_id"
        )

    async def create(self, input: ProveedorCreateSchema) -> ProveedorModel:
        return await self.proveedor_repository.create(input.model_dump())

    async def create_many_with_return(
        self, input: list[ProveedorCreateSchema]
    ) -> list[ProveedorModel]:
        return await self.proveedor_repository.create_many_with_return(
            [item.model_dump() for item in input]
        )

    async def delete_all(self):
        await self.proveedor_repository.delete_all()

    async def get_all(self, limit: int = 10, offset: int = 0) -> list[ProveedorModel]:
        return await self.proveedor_repository.get_all(limit, offset)

    async def get_all_and_search(
        self,
        limit: int = 10,
        offset: int = 1,
        nombre_proveedor: str = None,
        tipo_proveedor: str = None,
        numero_documento: str = None,
    ) -> list[ProveedorModel]:
        return await self.proveedor_repository.get_all_and_search(
            limit, offset, nombre_proveedor, tipo_proveedor, numero_documento
        )
