from repositories.datamart.SaldosRepository import SaldosRepository
from fastapi import Depends


class SaldosService:
    def __init__(self, saldos_repository: SaldosRepository = Depends()):
        self.saldos_repository = saldos_repository

    async def create_many(self, input: list[dict]):
        await self.saldos_repository.create_many(input)

    async def delete_all(self):
        await self.saldos_repository.delete_all()
