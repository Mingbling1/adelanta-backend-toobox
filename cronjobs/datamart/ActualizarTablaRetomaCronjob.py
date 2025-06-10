from cronjobs.BaseCronjob import BaseCronjob
from services.datamart.RetomasService import RetomasService
from utils.adelantafactoring.calculos import RetomasCalcular
import pandas as pd
from utils.timing_decorator import timing_decorator
from dependency_injector.wiring import inject, Provide
from config.container import Container
from models.datamart.KPIAcumuladoModel import KPIAcumuladoModel
from repositories.datamart.KPIAcumuladoRepository import KPIAcumuladoRepository

# from datetime import datetime
from config.logger import logger


class ActualizarTablaRetomaCronjob(BaseCronjob):
    def __init__(self):
        super().__init__(description="Actualiza la tabla de retomas")

    @timing_decorator
    async def actualizar_tablas_reportes(self, data: dict, service):
        await service.delete_all()
        if data:
            await service.create_many(data)

    @inject
    async def run(
        self,
        fecha_corte: str | None = None,
        kpi_acumulado_repository: KPIAcumuladoRepository = Provide[
            Container.kpi_acumulado_repository
        ],
        retomas_service: RetomasService = Provide[Container.retomas_service],
    ):

        kpi_acumulado_records: list[KPIAcumuladoModel] = (
            await kpi_acumulado_repository.get_all(limit=None)
        )
        kpi_df = pd.DataFrame([record.to_dict() for record in kpi_acumulado_records])

        logger.critical(f"Fecha de corte: {fecha_corte}")
        if not fecha_corte:
            fecha_corte = BaseCronjob.obtener_datetime_fecha_fin(
                90,
            )

        retomas_calcular = await RetomasCalcular(kpi_df).calcular_retomas_async(
            fecha_corte
        )

        await self.actualizar_tablas_reportes(retomas_calcular, retomas_service)
