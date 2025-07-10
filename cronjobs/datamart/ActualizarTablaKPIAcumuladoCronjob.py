from cronjobs.BaseCronjob import BaseCronjob
from config.container import Container
from dependency_injector.wiring import inject, Provide
from utils.adelantafactoring.calculos import KPICalcular
import pandas as pd
from repositories.datamart.TipoCambioRepository import TipoCambioRepository
from repositories.datamart.KPIRepository import KPIRepository


class ActualizarTablaKPIAcumuladoCronjob(BaseCronjob):
    def __init__(self):
        super().__init__(description="Actualiza la tabla de KPI acumulado")

    @inject
    async def run(
        self,
        tipo_cambio_repository: TipoCambioRepository = Provide[
            Container.tipo_cambio_repository
        ],
        kpi_repository: KPIRepository = Provide[Container.kpi_repository],
    ):
        try:
            # TipoCambio
            tipo_cambio_df = pd.DataFrame(
                await tipo_cambio_repository.get_all_dicts(exclude_pk=True)
            )
            tipo_cambio_df["TipoCambioFecha"] = pd.to_datetime(
                tipo_cambio_df["TipoCambioFecha"]
            )

            # KPI Acumulado
            kpi_acumulado_calcular = await KPICalcular(tipo_cambio_df).calcular(
                start_date=BaseCronjob.obtener_datetime_fecha_inicio(),
                end_date=BaseCronjob.obtener_datetime_fecha_fin(),
                fecha_corte=BaseCronjob.obtener_datetime_fecha_fin(),
                tipo_reporte=0,
            )

            await kpi_repository.delete_all()
            await kpi_repository.create_many(kpi_acumulado_calcular)

        except Exception as e:
            raise e