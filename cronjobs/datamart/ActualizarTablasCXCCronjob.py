from cronjobs.BaseCronjob import BaseCronjob
from dependency_injector.wiring import inject, Provide
from config.container import Container
from config.logger import logger
import pandas as pd

# Importar los calculadores
from utils.adelantafactoring.calculos.CXCPagosFactCalcular import CXCPagosFactCalcular
from utils.adelantafactoring.calculos.CXCDevFactCalcular import CXCDevFactCalcular
from utils.adelantafactoring.calculos.CXCAcumuladoDIMCalcular import (
    CXCAcumuladoDIMCalcular,
)

# Importar repositorios
from repositories.datamart import (
    CXCAcumuladoDIMRepository,
    CXCDevFactRepository,
    CXCPagosFactRepository,
)

from repositories.datamart.TipoCambioRepository import TipoCambioRepository


class ActualizarTablasCXCCronjob(BaseCronjob):
    def __init__(self) -> None:
        super().__init__(
            description="Actualizar Tablas CXC con ETL Power BI completo"
        )

    @inject   
    async def run(
        self,
        cxc_acumulado_dim_repository: CXCAcumuladoDIMRepository = Provide[
            Container.cxc_acumulado_dim_repository
        ],
        cxc_pagos_fact_repository: CXCPagosFactRepository = Provide[
            Container.cxc_pagos_fact_repository
        ],
        cxc_dev_fact_repository: CXCDevFactRepository = Provide[
            Container.cxc_dev_fact_repository
        ],
        tipo_cambio_repository: TipoCambioRepository = Provide[
            Container.tipo_cambio_repository
        ],
    ):
        """
        Actualiza las tablas CXC con ETL completo replicando Power BI.
        """
        
        # Inicializar calculadores
        pagos_calculador = CXCPagosFactCalcular()
        dev_calculador = CXCDevFactCalcular()
        acumulado_calculador = CXCAcumuladoDIMCalcular()

        # === 1. OBTENER TIPO DE CAMBIO ===
        logger.info("💱 Obteniendo tipos de cambio...")

        try:
            tipo_cambio_df = pd.DataFrame(
                await tipo_cambio_repository.get_all_dicts(exclude_pk=True)
            )
            logger.info(
                f"✅ Tipos de cambio obtenidos: {len(tipo_cambio_df)} registros"
            )
        except Exception as e:
            logger.error(f"❌ Error obteniendo tipos de cambio: {e}")
            tipo_cambio_df = pd.DataFrame()

        # === 2. ACTUALIZAR CXC PAGOS FACT ===
        pagos_data = []
        try:
            logger.info("📊 Procesando datos de pagos...")
            pagos_data = await pagos_calculador.calcular()

            if pagos_data:
                logger.info(f"💾 Insertando {len(pagos_data)} registros de pagos...")
                await cxc_pagos_fact_repository.delete_and_bulk_insert_chunked(
                    pagos_data, chunk_size=2000
                )
                logger.info("✅ Pagos actualizados correctamente")
            else:
                logger.warning("⚠️ No se obtuvieron datos de pagos")

        except Exception as e:
            logger.error(f"❌ Error actualizando CXCPagosFact: {e}")

        # === 3. ACTUALIZAR CXC DEV FACT ===
        try:
            logger.info("📊 Procesando datos de devoluciones...")
            dev_data = await dev_calculador.calcular()

            if dev_data:
                logger.info(
                    f"💾 Insertando {len(dev_data)} registros de devoluciones..."
                )
                await cxc_dev_fact_repository.delete_and_bulk_insert_chunked(
                    dev_data, chunk_size=2000
                )
                logger.info("✅ Devoluciones actualizadas correctamente")

        except Exception as e:
            logger.error(f"❌ Error actualizando devoluciones: {e}")

        # === 4. ACTUALIZAR CXC ACUMULADO DIM CON ETL COMPLETO ===
        try:
            logger.info("📊 Procesando ETL Acumulado DIM (replica Power BI)...")

            pagos_df = pd.DataFrame(pagos_data) if pagos_data else pd.DataFrame()
            if pagos_df.empty:
                logger.warning("⚠️ No hay datos de pagos para procesar")
                return

            acumulado_data = await acumulado_calculador.calcular(
                cxc_pagos_fact_df=pagos_df, tipo_cambio_df=tipo_cambio_df
            )

            if acumulado_data:
                logger.info(
                    f"💾 Insertando {len(acumulado_data)} registros acumulados..."
                )
                await cxc_acumulado_dim_repository.delete_and_bulk_insert_chunked(
                    acumulado_data, chunk_size=2000
                )
                logger.info("✅ ETL Acumulado DIM completado correctamente")

        except Exception as e:
            logger.error(f"❌ Error en ETL Acumulado DIM: {e}")
            raise

        # === 5. RESUMEN FINAL ===
        total_pagos = len(pagos_data) if pagos_data else 0
        total_acum = len(acumulado_data) if "acumulado_data" in locals() else 0

        logger.info("🎉 ETL CXC completo finalizado exitosamente")
        logger.info(
            f"📈 Resumen: {total_pagos} pagos, {total_acum} acumulados procesados"
        )