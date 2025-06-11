from repositories.datamart.TipoCambioRepository import TipoCambioRepository
from fastapi import Depends, UploadFile
from schemas.datamart.TipoCambioSchema import (
    TipoCambioPostRequestSchema,
)
from models.datamart.TipoCambioModel import TipoCambioModel
import pandas as pd

from utils.decorators import create_job
from typing import Literal
from io import BytesIO
import asyncio


class TipoCambioService:
    def __init__(self, tipo_cambio_repository: TipoCambioRepository = Depends()):
        self.tipo_cambio_repository = tipo_cambio_repository

    async def obtener_todos_con_filtro_fecha(
        self, limit: int = 10, offset: int = 0, year_month: str | None = None
    ) -> list[TipoCambioModel]:
        # Obtener registros del repositorio
        return await self.tipo_cambio_repository.obtener_todos_con_filtro_fecha(
            limit=limit, offset=offset, year_month=year_month
        )

    async def create_many(self, input: list[TipoCambioPostRequestSchema]):
        await self.tipo_cambio_repository.create_many([i.model_dump() for i in input])

    async def delete_all(self):
        await self.tipo_cambio_repository.delete_all()

    async def create_many_from_csv(self, file: UploadFile):
        df = pd.read_csv(file.file)

        records = df.to_dict(orient="records")

        validated_records = [
            TipoCambioPostRequestSchema(**record).model_dump() for record in records
        ]

        await self.tipo_cambio_repository.create_many(validated_records)

    @create_job(
        name="Generar CSV de Tipo Cambio",
        description="Generar un archivo CSV con los registros de tipo de cambio",
        expire=60 * 10,
        is_buffer=True,
        save_as=Literal["csv"],
        capture_params=True,
    )
    async def get_all_to_csv(self) -> BytesIO:
        # 1) Obtener registros
        records = await self.tipo_cambio_repository.get_all(limit=None, offset=0)

        # 2) Construir DataFrame en hilo
        def _build_df():
            return pd.DataFrame([r.to_dict() for r in records])

        df = await asyncio.to_thread(_build_df)

        # 3) Escribir a BytesIO en hilo
        def _write_buf() -> BytesIO:
            buf = BytesIO()
            df.to_csv(buf, index=False)
            buf.seek(0)
            return buf

        buffer = await asyncio.to_thread(_write_buf)
        return buffer
