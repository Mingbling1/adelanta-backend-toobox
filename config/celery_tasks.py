"""
🚀 Celery Tasks para Cronjobs
Migración de cronjobs a tareas de Celery
"""

import asyncio
import pandas as pd
import orjson
from datetime import datetime
from typing import Dict, Any

from config.celery_config import celery_app
from config.repository_factory import repository_factory
from config.logger import logger
from config.redis import redis_client_manager
from cronjobs.BaseCronjob import BaseCronjob
from utils.adelantafactoring.calculos import KPICalcular


@celery_app.task(
    bind=True,
    name="actualizar_kpi_acumulado_task",
    queue="cronjobs",
    max_retries=1,
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
        logger.error(f"❌ Error en task KPI Acumulado: {str(e)}")
        # Retry automático de Celery
        raise self.retry(exc=e)


async def _actualizar_kpi_acumulado_logic() -> Dict[str, Any]:
    """
    Lógica principal para actualizar KPI Acumulado
    """

    # Usar context managers para repositories
    async with repository_factory.create_tipo_cambio_repository() as tipo_cambio_repo:
        # TipoCambio
        tipo_cambio_records = await tipo_cambio_repo.get_all_dicts(exclude_pk=True)
        tipo_cambio_df = pd.DataFrame(tipo_cambio_records)
        tipo_cambio_df["TipoCambioFecha"] = pd.to_datetime(
            tipo_cambio_df["TipoCambioFecha"]
        )

        # KPI Acumulado calculation
        kpi_acumulado_calcular = await KPICalcular(tipo_cambio_df).calcular(
            start_date=BaseCronjob.obtener_datetime_fecha_inicio(),
            end_date=BaseCronjob.obtener_datetime_fecha_fin(),
            fecha_corte=BaseCronjob.obtener_datetime_fecha_fin(),
            tipo_reporte=0,
        )

    # Usar context manager separado para operación de escritura
    async with repository_factory.create_kpi_acumulado_repository() as kpi_acumulado_repo:
        # Usar bulk insert optimizado
        await kpi_acumulado_repo.delete_and_bulk_insert_chunked(
            kpi_acumulado_calcular, chunk_size=5000
        )

    return {"records": len(kpi_acumulado_calcular)}


@celery_app.task(
    bind=True,
    name="actualizar_tablas_reportes_task",
    queue="cronjobs",
    max_retries=1,
    default_retry_delay=60,
)
def actualizar_tablas_reportes_task(self) -> Dict[str, Any]:
    """
    🎯 Task Celery: Actualizar Tablas Reportes
    Equivalente a ActualizarTablasReportesCronjob
    """
    try:
        logger.info("🚀 Iniciando task: Actualizar Tablas Reportes")

        result = asyncio.run(_actualizar_tablas_reportes_logic())

        logger.info("✅ Task completada: Actualizar Tablas Reportes")
        return {
            "status": "success",
            "message": "Tablas de reportes actualizadas exitosamente",
            "kpi_records": result.get("kpi_records", 0),
            "saldos_records": result.get("saldos_records", 0),
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"❌ Error en task Tablas Reportes: {str(e)}")
        raise self.retry(exc=e)


async def _actualizar_tablas_reportes_logic() -> Dict[str, Any]:
    """
    Lógica principal para actualizar tablas de reportes
    """
    status_key = "ActualizarTablasReportesCronjob_status"

    # TipoCambio usando context manager
    async with repository_factory.create_tipo_cambio_repository() as tipo_cambio_repo:
        tipo_cambio_records = await tipo_cambio_repo.get_all_dicts(exclude_pk=True)
        tipo_cambio_df = pd.DataFrame(tipo_cambio_records)
        tipo_cambio_df["TipoCambioFecha"] = pd.to_datetime(
            tipo_cambio_df["TipoCambioFecha"]
        )

        # KPI usando toolbox
        from toolbox.api.kpi_api import get_kpi

        kpi_calcular = await get_kpi(
            tipo_cambio_df=tipo_cambio_df,
            start_date=BaseCronjob.obtener_datetime_fecha_inicio(),
            end_date=BaseCronjob.obtener_datetime_fecha_fin(),
            fecha_corte=BaseCronjob.obtener_datetime_fecha_fin(),
            tipo_reporte=2,
            as_df=False,
        )

    # KPI repository
    async with repository_factory.create_kpi_repository() as kpi_repo:
        await kpi_repo.delete_and_bulk_insert_chunked(kpi_calcular, chunk_size=2000)

    # NuevosClientesNuevosPagadores
    from utils.adelantafactoring.calculos import (
        NuevosClientesNuevosPagadoresCalcular,
    )

    nuevos_clientes_calcular = NuevosClientesNuevosPagadoresCalcular(
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

    async with repository_factory.create_nuevos_clientes_nuevos_pagadores_repository() as nuevos_clientes_repo:
        await nuevos_clientes_repo.delete_and_bulk_insert_chunked(
            nuevos_clientes_calcular, chunk_size=2000
        )

    # Saldos
    from utils.adelantafactoring.calculos import SaldosCalcular

    saldos_calcular = SaldosCalcular().calcular()

    async with repository_factory.create_saldos_repository() as saldos_repo:
        await saldos_repo.delete_and_bulk_insert_chunked(
            saldos_calcular, chunk_size=2000
        )

    # Actualizar status
    now = datetime.now(BaseCronjob.peru_tz)

    async with repository_factory.create_actualizacion_reportes_repository() as actualizacion_repo:
        await actualizacion_repo.create(
            {"ultima_actualizacion": now, "estado": "Success", "detalle": None}
        )

    # Redis status
    now_str = now.isoformat()
    status_value = orjson.dumps(
        {"status": "Active", "timestamp": now_str, "error": None}
    ).decode("utf-8")

    client = redis_client_manager.get_client()
    await client.set(status_key, status_value)

    return {
        "kpi_records": len(kpi_calcular),
        "saldos_records": len(saldos_calcular),
    }
