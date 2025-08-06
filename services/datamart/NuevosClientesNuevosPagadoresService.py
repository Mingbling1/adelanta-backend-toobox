from repositories.datamart.NuevosClientesNuevosPagadoresRepository import (
    NuevosClientesNuevosPagadoresRepository,
)
from models.datamart.NuevosClientesNuevosPagadoresModel import (
    NuevosClientesNuevosPagadoresModel,
)
from fastapi import Depends
from utils.decorators import create_job
from io import BytesIO
from typing import Literal
import asyncio
import polars as pl
from services.BaseService import BaseService


class NuevosClientesNuevosPagadoresService(
    BaseService[NuevosClientesNuevosPagadoresModel]
):
    def __init__(
        self,
        nuevos_clientes_nuevos_pagadores_repository: NuevosClientesNuevosPagadoresRepository = Depends(),
    ):
        self.nuevos_clientes_nuevos_pagadores_repository = (
            nuevos_clientes_nuevos_pagadores_repository
        )

    @create_job(
        name="Calcular Nuevos Clientes Nuevos Pagadores",
        description="Generar un excel con los Nuevos Clientes Nuevos Pagadores calculados",
        expire=60 * 10 * 1,
        is_buffer=True,
        save_as=["excel", "csv"],
        capture_params=True,
    )
    async def get_all_to_file(self, tipo: Literal["excel", "csv"] = "excel") -> BytesIO:
        # Obtener datos de la base de datos
        data_dicts = (
            await self.nuevos_clientes_nuevos_pagadores_repository.get_all_dicts()
        )

        # Crear DataFrame con polars para mejor performance
        df = pl.DataFrame(data_dicts)

        def _write_buffer() -> BytesIO:
            """Función para escribir el buffer usando polars"""
            buf = BytesIO()

            if tipo.lower() == "csv":
                # Escribir CSV con polars
                csv_content = df.write_csv()
                buf.write(csv_content.encode("utf-8"))
            else:

                df.write_excel(workbook=buf)

            buf.seek(0)
            return buf

        # Ejecutar en thread separado para operaciones I/O
        buffer = await asyncio.to_thread(_write_buffer)
        return buffer
