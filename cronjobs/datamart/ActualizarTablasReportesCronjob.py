from cronjobs.BaseCronjob import BaseCronjob

from utils.adelantafactoring.calculos import KPICalcular
from repositories.datamart.NuevosClientesNuevosPagadoresRepository import (
    NuevosClientesNuevosPagadoresRepository,
)
from utils.adelantafactoring.calculos import NuevosClientesNuevosPagadoresCalcular

from utils.adelantafactoring.calculos import SaldosCalcular
from repositories.datamart.KPIRepository import KPIRepository
from repositories.datamart.TipoCambioRepository import TipoCambioRepository
from repositories.datamart.SaldosRepository import SaldosRepository

import pandas as pd

# from utils.timing_decorator import timing_decorator
from datetime import datetime
from dependency_injector.wiring import inject, Provide
from config.container import Container
import orjson
from config.redis import redis_client_manager
from repositories.datamart.ActualizacionReportesRepository import (
    ActualizacionReportesRepository,
)


class ActualizarTablasReportesCronjob(BaseCronjob):
    def __init__(self):
        super().__init__(
            description="Actualiza las tablas de reportes: KPI, NuevosClientesNuevosPagadores y Saldos"
        )

    # @timing_decorator
    # async def actualizar_tablas_reportes(self, data: dict, service):
    #     await service.delete_all()
    #     if data:
    #         await service.create_many(data)

    @inject
    async def run(
        self,
        tipo_cambio_repository: TipoCambioRepository = Provide[
            Container.tipo_cambio_repository
        ],
        kpi_repository: KPIRepository = Provide[Container.kpi_repository],
        nuevos_clientes_nuevos_pagadores_repository: NuevosClientesNuevosPagadoresRepository = Provide[
            Container.nuevos_clientes_nuevos_pagadores_repository
        ],
        saldos_repository: SaldosRepository = Provide[Container.saldos_repository],
        actualizacion_reportes_repository: ActualizacionReportesRepository = Provide[
            Container.actualizacion_reportes_repository
        ],
    ):
        status_key = "ActualizarTablasReportesCronjob_status"

        try:
            # TipoCambio
            tipo_cambio_df = pd.DataFrame(
                await tipo_cambio_repository.get_all_dicts(exclude_pk=True)
            )
            tipo_cambio_df["TipoCambioFecha"] = pd.to_datetime(
                tipo_cambio_df["TipoCambioFecha"]
            )
            # KPI
            kpi_calcular = await KPICalcular(tipo_cambio_df).calcular(
                start_date=BaseCronjob.obtener_datetime_fecha_inicio(),
                end_date=BaseCronjob.obtener_datetime_fecha_fin(),
                fecha_corte=BaseCronjob.obtener_datetime_fecha_fin(),
            )

            await kpi_repository.delete_all()
            await kpi_repository.create_many(kpi_calcular)

            # NuevosClientesNuevosPagadores
            nuevos_clientes_nuevos_pagadores_calcular = (
                NuevosClientesNuevosPagadoresCalcular(
                    pd.DataFrame(kpi_calcular)
                ).calcular(
                    start_date=BaseCronjob.obtener_string_fecha_inicio(tipo=1),
                    end_date=BaseCronjob.obtener_string_fecha_fin(tipo=1),
                    ruc_c_col="RUCCliente",
                    ruc_p_col="RUCPagador",
                    ruc_c_ns_col="RazonSocialCliente",
                    ruc_p_ns_col="RazonSocialPagador",
                    ejecutivo_col="Ejecutivo",
                    type_op_col="TipoOperacion",
                )
            )

            nuevos_clientes_nuevos_pagadores_repository.delete_all()
            await nuevos_clientes_nuevos_pagadores_repository.create_many(
                nuevos_clientes_nuevos_pagadores_calcular
            )
            # Saldos
            saldos_calcular = SaldosCalcular().calcular()

            await saldos_repository.delete_all()
            await saldos_repository.create_many(saldos_calcular)

            # Obtenemos el timestamp
            now = datetime.now(BaseCronjob.peru_tz)

            # Actualizamos en la base de datos mediante el repositorio utilizando el método create
            await actualizacion_reportes_repository.create(
                {"ultima_actualizacion": now, "estado": "Success", "detalle": None}
            )

            # Si todo se ejecuta sin errores, se guarda en Redis el status Active con la hora actual
            now_str = now.isoformat()
            status_value = orjson.dumps(
                {"status": "Active", "timestamp": now_str, "error": None}
            ).decode("utf-8")
            client = redis_client_manager.get_client()
            await client.set(status_key, status_value)

        except Exception as e:
            now = datetime.now(BaseCronjob.peru_tz)
            await actualizacion_reportes_repository.create(
                {"ultima_actualizacion": now, "estado": "Error", "detalle": str(e)}
            )
            now_str = now.isoformat()
            status_value = orjson.dumps(
                {"status": "Error", "timestamp": now_str, "error": str(e)}
            ).decode("utf-8")
            client = redis_client_manager.get_client()
            await client.set(status_key, status_value)
            raise e
