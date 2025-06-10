from repositories.datamart.KPIAcumuladoRepository import KPIAcumuladoRepository
from models.datamart.KPIAcumuladoModel import KPIAcumuladoModel
from fastapi import Depends


class KPIAcumuladoService:
    def __init__(self, kpi_acumulado_repository: KPIAcumuladoRepository = Depends()):
        self.kpi_acumulado_repository = kpi_acumulado_repository

    async def create_many(self, input: list[dict]):
        await self.kpi_acumulado_repository.create_many(input)

    async def delete_all(self):
        await self.kpi_acumulado_repository.delete_all()

    async def get_all(
        self, limit: int = 10, offset: int = 0
    ) -> list[KPIAcumuladoModel]:
        return await self.kpi_acumulado_repository.get_all(limit, offset)
