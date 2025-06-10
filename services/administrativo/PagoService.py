from repositories.administrativo.PagoRepository import PagoRepository
from repositories.administrativo.GastoRepository import GastoRepository
from models.administrativo.PagoModel import PagoModel
from models.administrativo.GastoModel import GastoModel
from schemas.administrativo.PagoSchema import (
    PagoCreateSchema,
    PagoUpdateSchema,
)
from fastapi import Depends, HTTPException
from uuid import UUID


class PagoService:
    def __init__(
        self,
        pago_repository: PagoRepository = Depends(),
        gasto_repository: GastoRepository = Depends(),
    ):
        self.pago_repository = pago_repository
        self.gasto_repository = gasto_repository

    async def get_by_id(self, pago_id: str) -> PagoModel:
        return await self.pago_repository.get_by_id(UUID(pago_id), "pago_id")

    async def update(self, pago_id: str, input: PagoUpdateSchema) -> PagoModel:
        return await self.pago_repository.update(
            UUID(pago_id), input.model_dump(), "pago_id"
        )

    async def create(self, input: PagoCreateSchema) -> PagoModel:
        return await self.pago_repository.create(input.model_dump())

    async def create_many_with_return(
        self, input: list[PagoCreateSchema]
    ) -> list[PagoModel]:
        return await self.pago_repository.create_many_with_return(
            [item.model_dump() for item in input]
        )

    async def delete_all(self):
        await self.pago_repository.delete_all()

    async def get_all(self, limit: int = 10, offset: int = 0) -> list[PagoModel]:
        return await self.pago_repository.get_all(limit, offset)

    async def get_all_and_search(
        self,
        limit: int = 10,
        offset: int = 1,
        nombre_pago: str = None,
        pago_monto: float = None,
        pago_fecha: str = None,
        gasto_id: str | None = None,
    ) -> list[PagoModel]:
        return await self.pago_repository.get_all_and_search(
            limit, offset, nombre_pago, pago_monto, pago_fecha, gasto_id
        )

    async def add_pago_to_gasto(self, gasto_id: str, input: PagoCreateSchema):
        # Obtener el gasto
        gasto: GastoModel = await self.gasto_repository.get_by_id(
            UUID(gasto_id), "gasto_id", type_result="first"
        )
        if not gasto:
            raise HTTPException(status_code=404, detail="Gasto no encontrado")

        # Obtener todos los pagos asociados al gasto
        pagos: list[PagoModel] = await self.pago_repository.get_by_id(
            UUID(gasto_id), "gasto_id", type_result="all"
        )

        # Sumar todos los pago_monto
        total_pagado = sum(pago.pago_monto for pago in pagos)

        # Verificar que la suma no supere el monto_neto del gasto
        if total_pagado + input.pago_monto > gasto.monto_neto:
            raise HTTPException(
                status_code=400,
                detail="El monto total de los pagos supera el monto neto del gasto",
            )

        # Añadir el nuevo pago
        await self.create(input)

        # Verificar si el nuevo pago hace que la suma de los pagos sea igual al monto_neto
        if total_pagado + input.pago_monto == gasto.monto_neto:
            # Actualizar el estado del gasto a "pagado"
            await self.gasto_repository.update(
                UUID(gasto_id), {"gasto_estado": 1}, "gasto_id"
            )

    async def add_pago_to_gasto_bulk(
        self, gasto_ids: list[str], pagos: list[PagoCreateSchema]
    ):
        if len(gasto_ids) != len(pagos):
            raise HTTPException(
                status_code=400, detail="La cantidad de gasto_ids y pagos no coincide"
            )

        for gasto_id, pago in zip(gasto_ids, pagos):
            await self.add_pago_to_gasto(gasto_id, pago)
