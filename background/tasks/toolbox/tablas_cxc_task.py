# background/tasks/toolbox/tablas_cxc_task.py
"""🚀 Celery Task para Tablas CXC - Business Logic Completa"""

import asyncio
import pandas as pd
import gc
from datetime import datetime
from typing import Dict, Any

from config.celery_config import celery_app
from config.repository_factory import create_repository_factory
from config.logger import logger
from cronjobs.BaseCronjob import BaseCronjob

# Importar los calculadores
from utils.adelantafactoring.calculos.CXCPagosFactCalcular import CXCPagosFactCalcular
from utils.adelantafactoring.calculos.CXCDevFactCalcular import CXCDevFactCalcular
from utils.adelantafactoring.calculos.CXCAcumuladoDIMCalcular import (
    CXCAcumuladoDIMCalcular,
)


@celery_app.task(
    bind=True,
    name="toolbox.tablas_cxc",
    queue="cronjobs",
    max_retries=0,  # Sin reintentos, una sola ejecución
    default_retry_delay=60,
)
def tablas_cxc_task(self) -> Dict[str, Any]:
    """
    🎯 Task Celery: Actualizar Tablas CXC con ETL Power BI completo
    Equivalente a ActualizarTablasCXCCronjob
    """
    try:
        logger.info("🚀 Iniciando task: Tablas CXC")

        # Ejecutar lógica async en event loop
        result = asyncio.run(_actualizar_tablas_cxc_logic())

        logger.info("✅ Task completada: Tablas CXC")
        return {
            "status": "success",
            "message": "Tablas CXC actualizadas exitosamente",
            "records": result.get("records", {}),
            "timestamp": result.get("timestamp"),
        }

    except Exception as e:
        error_msg = f"❌ Error en task Tablas CXC: {str(e)}"
        error_type = type(e).__name__
        logger.error(error_msg)

        # Crear error serializable para Celery
        serializable_error = {
            "error_type": error_type,
            "error_message": str(e),
            "timestamp": datetime.now().isoformat(),
        }

        # Intentar retry con error serializable
        if self.request.retries < self.max_retries:
            logger.info(
                f"🔄 Reintentando task (intento {self.request.retries + 1}/{self.max_retries})"
            )
            raise self.retry(countdown=60)
        else:
            # Si ya no hay más retries, devolver el error serializable
            logger.error("❌ Máximo de reintentos alcanzado")
            return {
                "status": "failed",
                "error": serializable_error,
                "message": "Error al actualizar tablas CXC",
            }


async def _actualizar_tablas_cxc_logic() -> Dict[str, Any]:
    """
    Lógica principal para actualizar Tablas CXC
    Manejo robusto de conexiones DB para evitar event loop issues
    """
    # Crear factory fresco para esta task
    repo_factory = create_repository_factory()

    try:
        logger.info("🔄 Creando repositories...")

        # Crear repositories frescos
        tipo_cambio_repo = await repo_factory.create_tipo_cambio_repository()
        cxc_acumulado_dim_repo = (
            await repo_factory.create_cxc_acumulado_dim_repository()
        )
        cxc_pagos_fact_repo = await repo_factory.create_cxc_pagos_fact_repository()
        cxc_dev_fact_repo = await repo_factory.create_cxc_dev_fact_repository()

        # Inicializar calculadores
        pagos_calculador = CXCPagosFactCalcular()
        dev_calculador = CXCDevFactCalcular()
        acumulado_calculador = CXCAcumuladoDIMCalcular()

        # === 1. OBTENER TIPO DE CAMBIO ===
        logger.info("💱 Obteniendo tipos de cambio...")

        try:
            tipo_cambio_records = await tipo_cambio_repo.get_all_dicts(exclude_pk=True)
            tipo_cambio_df = pd.DataFrame(tipo_cambio_records)
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
                await cxc_pagos_fact_repo.delete_and_bulk_insert_chunked(
                    pagos_data, chunk_size=2000
                )
                logger.info("✅ Pagos actualizados correctamente")
            else:
                logger.warning("⚠️ No se obtuvieron datos de pagos")

        except Exception as e:
            logger.error(f"❌ Error actualizando CXCPagosFact: {e}")

        # === 3. ACTUALIZAR CXC DEV FACT ===
        dev_data = []
        try:
            logger.info("📊 Procesando datos de devoluciones...")
            dev_data = await dev_calculador.calcular()

            if dev_data:
                logger.info(
                    f"💾 Insertando {len(dev_data)} registros de devoluciones..."
                )
                await cxc_dev_fact_repo.delete_and_bulk_insert_chunked(
                    dev_data, chunk_size=2000
                )
                logger.info("✅ Devoluciones actualizadas correctamente")

        except Exception as e:
            logger.error(f"❌ Error actualizando devoluciones: {e}")

        # === 4. ACTUALIZAR CXC ACUMULADO DIM CON ETL COMPLETO ===
        acumulado_data = []
        try:
            logger.info("📊 Procesando ETL Acumulado DIM (replica Power BI)...")

            pagos_df = pd.DataFrame(pagos_data) if pagos_data else pd.DataFrame()
            if pagos_df.empty:
                logger.warning("⚠️ No hay datos de pagos para procesar")
            else:
                acumulado_data = await acumulado_calculador.calcular(
                    cxc_pagos_fact_df=pagos_df, tipo_cambio_df=tipo_cambio_df
                )

                if acumulado_data:
                    logger.info(
                        f"💾 Insertando {len(acumulado_data)} registros acumulados..."
                    )
                    await cxc_acumulado_dim_repo.delete_and_bulk_insert_chunked(
                        acumulado_data, chunk_size=2000
                    )
                    logger.info("✅ ETL Acumulado DIM completado correctamente")

        except Exception as e:
            logger.error(f"❌ Error en ETL Acumulado DIM: {e}")
            raise

        # === 5. RESUMEN FINAL ===
        total_pagos = len(pagos_data) if pagos_data else 0
        total_dev = len(dev_data) if dev_data else 0
        total_acum = len(acumulado_data) if acumulado_data else 0

        now = datetime.now(BaseCronjob.peru_tz)
        now_str = now.isoformat()

        logger.info("🎉 ETL CXC completo finalizado exitosamente")
        logger.info(
            f"📈 Resumen: {total_pagos} pagos, {total_dev} devoluciones, {total_acum} acumulados procesados"
        )

        return {
            "status": "success",
            "records": {
                "pagos": total_pagos,
                "devoluciones": total_dev,
                "acumulados": total_acum,
            },
            "timestamp": now_str,
        }

    except Exception as e:
        logger.error(f"❌ Error en lógica Tablas CXC: {str(e)}")
        raise e


