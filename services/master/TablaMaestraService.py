from repositories.master.TablaMaestraRepository import TablaMaestraRepository
from models.master.TablaMaestraModel import TablaMaestraModel
from schemas.master.TablaMaestraSchema import (
    TablaMaestraCreateSchema,
    TablaMaestraUpdateSchema,
)
from fastapi import Depends
from uuid import UUID


class TablaMaestraService:
    def __init__(self, service: TablaMaestraRepository = Depends()):
        self.service = service

    async def get_by_id(self, tabla_maestra_id: str) -> TablaMaestraModel:
        return await self.service.get_by_id(UUID(tabla_maestra_id), "tabla_maestra_id")

    async def update(
        self, tabla_maestra_id: str, input: TablaMaestraUpdateSchema
    ) -> TablaMaestraModel:
        return await self.service.update(
            UUID(tabla_maestra_id), input.model_dump(), "tabla_maestra_id"
        )

    async def create(self, input: TablaMaestraCreateSchema) -> TablaMaestraModel:
        return await self.service.create(input.model_dump())

    async def create_many_with_return(
        self, input: list[TablaMaestraCreateSchema]
    ) -> list[TablaMaestraModel]:
        return await self.service.create_many_with_return(
            [item.model_dump() for item in input]
        )

    async def delete_all(self):
        await self.service.delete_all()

    async def get_all(
        self, limit: int = 10, offset: int = 0
    ) -> list[TablaMaestraModel]:
        return await self.service.get_all(limit, offset)

    async def get_all_and_search(
        self,
        limit: int = 10,
        offset: int = 1,
        tabla_nombre: str = None,
        tipo: str = None,
        
    ) -> list[TablaMaestraModel]:
        return await self.service.get_all_and_search(limit, offset, tabla_nombre, tipo)
