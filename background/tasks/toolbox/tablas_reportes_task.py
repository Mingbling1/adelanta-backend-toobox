# background/tasks/toolbox/tablas_reportes_task.py
"""� Celery Task para Tablas Reportes - Business Logic Completa"""

import pandas as pd
from datetime import datetime
from typing import Dict, Any
from config.celery_config import celery_app
from config.repository_factory import create_repository_factory
from config.logger import logger
from cronjobs.BaseCronjob import BaseCronjob
from utils.adelantafactoring.calculos import NuevosClientesNuevosPagadoresCalcular
from utils.adelantafactoring.calculos import SaldosCalcular
import orjson
from config.redis import redis_client_manager
from toolbox.api.kpi_api import get_kpi
from asgiref.sync import async_to_sync
from repositories.datamart.TipoCambioRepository import TipoCambioRepository
from repositories.datamart.KPIRepository import KPIRepository
from repositories.datamart.NuevosClientesNuevosPagadoresRepository import (
    NuevosClientesNuevosPagadoresRepository,
)
from repositories.datamart.SaldosRepository import SaldosRepository
from repositories.datamart.ActualizacionReportesRepository import (
    ActualizacionReportesRepository,
)


@celery_app.task(
    bind=True,
    name="toolbox.tablas_reportes",
    queue="cronjobs",
    max_retries=0,  # Sin reintentos, una sola ejecución
    default_retry_delay=60,
)
def tablas_reportes_task(self) -> Dict[str, Any]:
    """
    🎯 Task Celery: Actualizar Tablas Reportes (KPI, NuevosClientes, Saldos)
    Equivalente a ActualizarTablasReportesCronjob
    """
    try:
        logger.info("🚀 Iniciando task: Tablas Reportes")

        # Ejecutar lógica async sin conflictos de event loop
        result = async_to_sync(_actualizar_tablas_reportes_logic)()

        logger.info("✅ Task completada: Tablas Reportes")
        return {
            "status": "success",
            "message": "Tablas de reportes actualizadas exitosamente",
            "records": result.get("records", {}),
            "timestamp": result.get("timestamp"),
        }

    except Exception as e:
        error_msg = f"❌ Error en task Tablas Reportes: {str(e)}"
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
                "message": "Error al actualizar tablas de reportes",
            }


async def _actualizar_tablas_reportes_logic() -> Dict[str, Any]:
    """
    Lógica principal para actualizar Tablas Reportes
    Manejo robusto de conexiones DB para evitar event loop issues
    """
    # Crear factory fresco para esta task
    repo_factory = create_repository_factory()
    status_key = "ActualizarTablasReportesCronjob_status"

    try:
        logger.info("🔄 Creando sesión y repositories...")

        # Usar UNA sola sesión para toda la tarea y cerrarla al final
        async with repo_factory.session_manager.session() as db_session:
            tipo_cambio_repo = TipoCambioRepository(db_session)
            kpi_repo = KPIRepository(db_session)
            nuevos_clientes_repo = NuevosClientesNuevosPagadoresRepository(db_session)
            saldos_repo = SaldosRepository(db_session)
            actualizacion_reportes_repo = ActualizacionReportesRepository(db_session)

            logger.info("📊 Obteniendo datos de TipoCambio...")

            # TipoCambio
            tipo_cambio_records = await tipo_cambio_repo.get_all_dicts(exclude_pk=True)
            tipo_cambio_df = pd.DataFrame(tipo_cambio_records)
            tipo_cambio_df["TipoCambioFecha"] = pd.to_datetime(
                tipo_cambio_df["TipoCambioFecha"]
            )

            logger.info("🧮 Calculando KPI...")

            # KPI
            kpi_calcular = await get_kpi(
                tipo_cambio_df=tipo_cambio_df,
                start_date=BaseCronjob.obtener_datetime_fecha_inicio(),
                end_date=BaseCronjob.obtener_datetime_fecha_fin(),
                fecha_corte=BaseCronjob.obtener_datetime_fecha_fin(),
                tipo_reporte=2,
                as_df=False,
            )

            logger.info(f"💾 Insertando {len(kpi_calcular)} registros KPI...")

            await kpi_repo.delete_and_bulk_insert_chunked(kpi_calcular, chunk_size=5000)

            logger.info("🧮 Calculando NuevosClientesNuevosPagadores...")

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

            logger.info(
                f"💾 Insertando {len(nuevos_clientes_nuevos_pagadores_calcular)} registros NuevosClientes..."
            )

            await nuevos_clientes_repo.delete_and_bulk_insert_chunked(
                nuevos_clientes_nuevos_pagadores_calcular, chunk_size=5000
            )

            logger.info("🧮 Calculando Saldos...")

            # Saldos
            saldos_calcular = SaldosCalcular().calcular()

            logger.info(f"💾 Insertando {len(saldos_calcular)} registros Saldos...")

            await saldos_repo.delete_and_bulk_insert_chunked(
                saldos_calcular, chunk_size=5000
            )

            # Obtenemos el timestamp
            now = datetime.now(BaseCronjob.peru_tz)

            # Actualizamos en la base de datos mediante el repositorio utilizando el método create
            await actualizacion_reportes_repo.create(
                {"ultima_actualizacion": now, "estado": "Success", "detalle": None}
            )

            # Si todo se ejecuta sin errores, se guarda en Redis el status Active con la hora actual
            now_str = now.isoformat()
            status_value = orjson.dumps(
                {"status": "Active", "timestamp": now_str, "error": None}
            ).decode("utf-8")
            client = redis_client_manager.get_client()
            await client.set(status_key, status_value)

            logger.info("✅ Tablas Reportes completado exitosamente")

            # Retornamos resultado similar al cronjob
            return {
                "status": "success",
                "records": {
                    "kpi": len(kpi_calcular) if kpi_calcular else 0,
                    "nuevos_clientes": (
                        len(nuevos_clientes_nuevos_pagadores_calcular)
                        if nuevos_clientes_nuevos_pagadores_calcular
                        else 0
                    ),
                    "saldos": len(saldos_calcular) if saldos_calcular else 0,
                },
                "timestamp": now_str,
            }

    except Exception as e:
        # Intentar registrar el error en la tabla de actualizaciones si es posible
        try:
            async with repo_factory.session_manager.session() as db_session:
                actualizacion_reportes_repo = ActualizacionReportesRepository(
                    db_session
                )
                now = datetime.now(BaseCronjob.peru_tz)
                await actualizacion_reportes_repo.create(
                    {
                        "ultima_actualizacion": now,
                        "estado": "Error",
                        "detalle": str(e),
                    }
                )
                now_str = now.isoformat()
        except Exception:
            # Si incluso esto falla, al menos produce timestamp para Redis
            now_str = datetime.now(BaseCronjob.peru_tz).isoformat()

        # Guardar estado de error en Redis
        status_value = orjson.dumps(
            {"status": "Error", "timestamp": now_str, "error": str(e)}
        ).decode("utf-8")
        client = redis_client_manager.get_client()
        await client.set(status_key, status_value)
        logger.error(f"❌ Error en lógica Tablas Reportes: {str(e)}")
        raise e
    finally:
        # Asegurar cierre del engine creado para esta tarea
        await repo_factory.cleanup()
