from repositories.datamart.RetomasRepository import RetomasRepository
from fastapi import Depends
from utils.adelantafactoring.calculos.RetomasCalcular import RetomasCalcular
from datetime import datetime
from repositories.datamart.KPIAcumuladoRepository import KPIAcumuladoRepository
import pandas as pd
from utils.decorators import create_job
from io import BytesIO
import asyncio


class RetomasService:
    def __init__(
        self,
        retomas_repository: RetomasRepository = Depends(),
        kpi_acumulado_repository: KPIAcumuladoRepository = Depends(),
    ):
        self.retomas_repository = retomas_repository
        self.kpi_acumulado_repository = kpi_acumulado_repository
        from cronjobs.datamart.ActualizarTablaRetomaCronjob import (
            ActualizarTablaRetomaCronjob,
        )

        self.actualizar_tabla_retoma_cronjob = ActualizarTablaRetomaCronjob()

    async def create_many(self, input: list[dict]):
        await self.retomas_repository.create_many(input)

    async def delete_all(self):
        await self.retomas_repository.delete_all()

    @create_job(
        name="Calcular Retomas",
        description="Genera el Excel retomas",
        expire=60 * 10,  # 10 minutos
        is_buffer=True,
        save_as="excel",
        capture_params=True,
    )
    async def calcular_retomas(self, fecha_corte: datetime):
        kpi_acumulado_df = pd.DataFrame(
            await self.kpi_acumulado_repository.get_all_dicts(exclude_pk=True)
        )

        # Crear la instancia de RetomasCalcular pasando el DataFrame
        retomas_calcular = RetomasCalcular(kpi_acumulado_df)
        resultado_retomas_calcular_df = (
            await retomas_calcular.calcular_retomas_async(fecha_corte, to_df=True)
        )
        excel_buffer = BytesIO()
        await self.actualizar_tabla_retoma_cronjob.run(fecha_corte=fecha_corte)
        await asyncio.to_thread(
            resultado_retomas_calcular_df.to_excel,
            excel_buffer,
            engine="xlsxwriter",
            index=False,
        )
        excel_buffer.seek(0)
        return excel_buffer
