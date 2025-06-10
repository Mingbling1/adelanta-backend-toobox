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
import pandas as pd


class NuevosClientesNuevosPagadoresService:
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
        # 1) Traer todos los registros
        records: list[NuevosClientesNuevosPagadoresModel] = (
            await self.nuevos_clientes_nuevos_pagadores_repository.get_all(
                limit=None, offset=0
            )
        )

        # 2) Construir el DataFrame en un hilo
        def _build_df():
            df = pd.DataFrame([r.to_dict() for r in records])
            return df

        # 3) Guardar el DataFrame en un buffer
        df = await asyncio.to_thread(_build_df)

        # 3) Escribir el buffer en un hilo
        def _write_buffer() -> BytesIO:
            buf = BytesIO()
            if tipo.lower() == "csv":
                csv_text = df.to_csv(index=False)
                buf.write(csv_text.encode("utf-8"))
            else:
                with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                    df.to_excel(writer, index=False, sheet_name="Sheet1")
            buf.seek(0)
            return buf

        buffer = await asyncio.to_thread(_write_buffer)
        return buffer
