"""
🚀 Celery Tasks para Cronjobs
Migración de cronjobs a tareas de Celery
"""

import asyncio
import pandas as pd
import gc
from datetime import datetime
from typing import Dict, Any

from config.celery_config import celery_app
from config.repository_factory import create_repository_factory
from config.logger import logger
from cronjobs.BaseCronjob import BaseCronjob
from toolbox.api.kpi_api import get_kpi


@celery_app.task(
    bind=True,
    name="toolbox.kpi_acumulado",
    queue="cronjobs",
    max_retries=0,  # Sin reintentos, una sola ejecución
    default_retry_delay=60,
)
def actualizar_kpi_acumulado_task(self) -> Dict[str, Any]:
    """
    🎯 Task Celery: Actualizar KPI Acumulado
    Equivalente a ActualizarTablaKPIAcumuladoCronjob
    """
    try:
        logger.info("🚀 Iniciando task: Actualizar KPI Acumulado")

        # Ejecutar lógica async en event loop
        result = asyncio.run(_actualizar_kpi_acumulado_logic())

        logger.info("✅ Task completada: Actualizar KPI Acumulado")
        return {
            "status": "success",
            "message": "KPI Acumulado actualizado exitosamente",
            "records_processed": result.get("records", 0),
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        error_msg = f"❌ Error en task KPI Acumulado: {str(e)}"
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
                "timestamp": datetime.now().isoformat(),
            }


async def _actualizar_kpi_acumulado_logic() -> Dict[str, Any]:
    """
    Lógica principal para actualizar KPI Acumulado
    Manejo robusto de conexiones DB para evitar event loop issues
    """
    # Crear factory fresco para esta task
    repo_factory = create_repository_factory()

    try:
        logger.info("🔄 Creando repositories...")

        # Crear repositories frescos
        tipo_cambio_repo = await repo_factory.create_tipo_cambio_repository()
        kpi_acumulado_repo = await repo_factory.create_kpi_acumulado_repository()

        logger.info("📊 Obteniendo datos de TipoCambio...")

        # TipoCambio
        tipo_cambio_records = await tipo_cambio_repo.get_all_dicts(exclude_pk=True)
        tipo_cambio_df = pd.DataFrame(tipo_cambio_records)
        tipo_cambio_df["TipoCambioFecha"] = pd.to_datetime(
            tipo_cambio_df["TipoCambioFecha"]
        )

        logger.info("🧮 Calculando KPI Acumulado...")

        # KPI Acumulado
        kpi_acumulado_calcular = await get_kpi(
            tipo_cambio_df=tipo_cambio_df,
            start_date=BaseCronjob.obtener_datetime_fecha_inicio(),
            end_date=BaseCronjob.obtener_datetime_fecha_fin(),
            fecha_corte=BaseCronjob.obtener_datetime_fecha_fin(),
            tipo_reporte=0,
            as_df=False,
        )

        logger.info(f"💾 Insertando {len(kpi_acumulado_calcular)} registros...")

        # Usar bulk insert optimizado
        await kpi_acumulado_repo.delete_and_bulk_insert_chunked(
            kpi_acumulado_calcular, chunk_size=5000
        )

        logger.info("✅ KPI Acumulado completado exitosamente")
        return {"records": len(kpi_acumulado_calcular)}

    except Exception as e:
        logger.error(f"❌ Error en lógica KPI Acumulado: {str(e)}")
        raise e

    finally:
        # Limpiar recursos del factory de forma robusta
        try:
            logger.info("🧹 Limpiando recursos...")
            await repo_factory.cleanup()
            gc.collect()
            logger.info("✅ Recursos limpiados")
        except Exception as cleanup_error:
            logger.error(f"⚠️ Error limpiando recursos: {cleanup_error}")
