import pandas as pd
from dependency_injector.wiring import inject, Provide
from cronjobs.BaseCronjob import BaseCronjob
from config.logger import logger
from config.container import Container
from repositories.datamart.TipoCambioRepository import TipoCambioRepository
from repositories.datamart.CXCAcumuladoDIMRepository import CXCAcumuladoDIMRepository
from repositories.datamart.CXCPagosFactRepository import CXCPagosFactRepository
from repositories.datamart.CXCDevFactRepository import CXCDevFactRepository
from utils.adelantafactoring.calculos.CXCETLProcessor import CXCETLProcessor
import traceback


class ActualizarCXCETLCronjob(BaseCronjob):
    """
    🚀 CRONJOB CXC ETL - Procesamiento completo de las 3 tablas CXC

    FLUJO:
    1. CXCETLProcessor orquesta todo el trabajo (webservices + ETL + validación)
    2. Inserción directa de las 3 tablas procesadas
    """

    def __init__(self):
        super().__init__(description="ETL CXC completo con Pydantic optimizado")

    @inject
    async def run(
        self,
        tipo_cambio_repository: TipoCambioRepository = Provide[
            Container.tipo_cambio_repository
        ],
        cxc_acumulado_dim_repository: CXCAcumuladoDIMRepository = Provide[
            Container.cxc_acumulado_dim_repository
        ],
        cxc_pagos_fact_repository: CXCPagosFactRepository = Provide[
            Container.cxc_pagos_fact_repository
        ],
        cxc_dev_fact_repository: CXCDevFactRepository = Provide[
            Container.cxc_dev_fact_repository
        ],
    ):
        """🎯 Procesamiento ETL CXC completo"""
        try:
            # Obtener tipo de cambio
            tipo_cambio_df = pd.DataFrame(
                await tipo_cambio_repository.get_all_dicts(exclude_pk=True)
            )

            # CXCETLProcessor orquesta todo el trabajo
            logger.warning("⚡ Iniciando procesamiento ETL CXC completo...")
            processor = CXCETLProcessor(tipo_cambio_df)

            acumulado_final, pagos_final, dev_final = (
                await processor.procesar_todo_cxc()
            )

            logger.warning(
                f"✅ Procesamiento completado - Acumulado: {len(acumulado_final)}, Pagos: {len(pagos_final)}, Dev: {len(dev_final)}"
            )

            # Insertar las 3 tablas
            if acumulado_final:
                await cxc_acumulado_dim_repository.delete_and_bulk_insert_chunked(
                    acumulado_final, chunk_size=2000
                )

            if pagos_final:
                await cxc_pagos_fact_repository.delete_and_bulk_insert_chunked(
                    pagos_final, chunk_size=2000
                )

            if dev_final:
                await cxc_dev_fact_repository.delete_and_bulk_insert_chunked(
                    dev_final, chunk_size=2000
                )

            total_registros = len(acumulado_final) + len(pagos_final) + len(dev_final)
            logger.warning(
                f"🎉 ETL CXC completado - Total: {total_registros} registros"
            )

        except Exception as e:
            logger.error(traceback.format_exc())
            raise e
