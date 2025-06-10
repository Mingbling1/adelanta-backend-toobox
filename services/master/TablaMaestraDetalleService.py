from repositories.master.TablaMaestraDetalleRepository import (
    TablaMaestraDetalleRepository,
)
from models.master.TablaMaestraDetalleModel import TablaMaestraDetalleModel
from schemas.master.TablaMaestraDetalleSchema import (
    TablaMaestraDetalleCreateSchema,
    TablaMaestraDetalleUpdateSchema,
)
from fastapi import Depends
from uuid import UUID


class TablaMaestraDetalleService:
    def __init__(self, service: TablaMaestraDetalleRepository = Depends()):
        self.service = service

    async def get_by_id(
        self, tabla_maestra_detalle_id: str
    ) -> TablaMaestraDetalleModel:
        return await self.service.get_by_id(
            UUID(tabla_maestra_detalle_id), "tabla_maestra_detalle_id"
        )

    async def update(
        self, tabla_maestra_detalle_id: str, input: TablaMaestraDetalleUpdateSchema
    ) -> TablaMaestraDetalleModel:
        return await self.service.update(
            UUID(tabla_maestra_detalle_id),
            input.model_dump(),
            "tabla_maestra_detalle_id",
        )

    async def create(
        self, input: TablaMaestraDetalleCreateSchema
    ) -> TablaMaestraDetalleModel:
        return await self.service.create(input.model_dump())

    async def create_many_with_return(
        self, input: list[TablaMaestraDetalleCreateSchema]
    ) -> list[TablaMaestraDetalleModel]:
        return await self.service.create_many_with_return(
            [item.model_dump() for item in input]
        )

    async def delete_all(self):
        await self.service.delete_all()

    async def get_all(
        self, limit: int = 10, offset: int = 0
    ) -> list[TablaMaestraDetalleModel]:
        return await self.service.get_all(limit, offset)

    async def get_all_and_search(
        self,
        limit: int = 10,
        offset: int = 1,
        codigo: int = None,
        valor: str = None,
        descripcion: str = None,
        tabla_maestra_id: str = None,
        tabla_nombre: str = None,
        tipo: str = None,
    ) -> list[TablaMaestraDetalleModel]:
        return await self.service.get_all_and_search(
            limit, offset, codigo, valor, descripcion
        )
