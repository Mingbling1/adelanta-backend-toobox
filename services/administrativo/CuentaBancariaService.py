from repositories.administrativo.CuentaBancariaRepository import (
    CuentaBancariaRepository,
)
from models.administrativo.CuentaBancariaModel import CuentaBancariaModel
from schemas.administrativo.CuentaBancariaSchema import (
    CuentaBancariaCreateSchema,
    CuentaBancariaUpdateSchema,
)
from fastapi import Depends
from uuid import UUID


class CuentaBancariaService:
    def __init__(
        self, cuenta_bancaria_repository: CuentaBancariaRepository = Depends()
    ):
        self.cuenta_bancaria_repository = cuenta_bancaria_repository

    async def get_by_id(self, cuenta_bancaria_id: str) -> CuentaBancariaModel:
        return await self.cuenta_bancaria_repository.get_by_id(
            UUID(cuenta_bancaria_id), "cuenta_bancaria_id"
        )

    async def update(
        self, cuenta_bancaria_id: str, input: CuentaBancariaUpdateSchema
    ) -> CuentaBancariaModel:
        return await self.cuenta_bancaria_repository.update(
            UUID(cuenta_bancaria_id), input.model_dump(), "cuenta_bancaria_id"
        )

    async def create(self, input: CuentaBancariaCreateSchema) -> CuentaBancariaModel:
        return await self.cuenta_bancaria_repository.create(input.model_dump())

    async def create_many_with_return(
        self, input: list[CuentaBancariaCreateSchema]
    ) -> list[CuentaBancariaModel]:
        return await self.cuenta_bancaria_repository.create_many_with_return(
            [item.model_dump() for item in input]
        )

    async def delete_all(self):
        await self.cuenta_bancaria_repository.delete_all()

    async def get_all(
        self, limit: int = 10, offset: int = 0
    ) -> list[CuentaBancariaModel]:
        return await self.cuenta_bancaria_repository.get_all(limit, offset)

    async def get_all_and_search(
        self,
        limit: int = 10,
        offset: int = 1,
        banco: str = None,
        moneda: str = None,
        tipo_cuenta: str = None,
        proveedor_id: str = None,
        nombre_proveedor: str = None,
        numero_documento: str = None,
        nombre_cuenta_bancaria: str = None,
    ) -> list[CuentaBancariaModel]:
        return await self.cuenta_bancaria_repository.get_all_and_search(
            limit,
            offset,
            banco,
            moneda,
            tipo_cuenta,
            proveedor_id,
            nombre_proveedor,
            numero_documento,
            nombre_cuenta_bancaria,
        )
