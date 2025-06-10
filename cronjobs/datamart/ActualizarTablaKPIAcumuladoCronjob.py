from cronjobs.BaseCronjob import BaseCronjob
from utils.timing_decorator import timing_decorator
from config.container import Container
from dependency_injector.wiring import inject, Provide
from services.datamart.KPIAcumuladoService import KPIAcumuladoService
from services.datamart.TipoCambioService import TipoCambioService
from utils.adelantafactoring.calculos import KPICalcular
import pandas as pd


class ActualizarTablaKPIAcumuladoCronjob(BaseCronjob):
    def __init__(self):
        super().__init__(description="Actualiza la tabla de KPI acumulado")

    @timing_decorator
    async def actualizar_tabla(self, data: dict, service):
        await service.delete_all()
        if data:
            await service.create_many(data)

    @inject
    async def run(
        self,
        kpi_acumulado_service: KPIAcumuladoService = Provide[
            Container.kpi_acumulado_service
        ],
        tipo_cambio_service: TipoCambioService = Provide[Container.tipo_cambio_service],
    ):
        tipo_cambio_data = await tipo_cambio_service.get_all(limit=None)
        tipo_cambio_df = pd.DataFrame([i.__dict__ for i in tipo_cambio_data])
        tipo_cambio_df["TipoCambioFecha"] = pd.to_datetime(
            tipo_cambio_df["TipoCambioFecha"]
        )

        kpi_acumulado_calcular = await KPICalcular(tipo_cambio_df).calcular(
            start_date=BaseCronjob.obtener_datetime_fecha_inicio(),
            end_date=BaseCronjob.obtener_datetime_fecha_fin(),
            fecha_corte=BaseCronjob.obtener_datetime_fecha_fin(),
            tipo_reporte=0,
        )

        await self.actualizar_tabla(kpi_acumulado_calcular, kpi_acumulado_service)
