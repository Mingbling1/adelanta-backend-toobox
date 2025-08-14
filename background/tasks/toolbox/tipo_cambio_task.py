"""
📋 Task: Actualización Tipo de Cambio SUNAT
🎯 Descripción: Obtiene y actualiza tipos de cambio desde API SUNAT usando Celery
🔧 Patrón: Router → Celery Task (+ Repository Factory) → Database
"""

from typing import Dict, Any
from config.celery_config import celery_app
from config.repository_factory import create_repository_factory
from config.logger import logger
from schemas.datamart.TipoCambioSchema import TipoCambioPostRequestSchema
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential
from fastapi import HTTPException
import httpx
import asyncio
from typing import List, Set
from cronjobs.BaseCronjob import BaseCronjob


@celery_app.task(name="toolbox.tipo_cambio", bind=True, max_retries=0)
def tipo_cambio_task(self, batch_size: int = 1):
    """
    🎯 Actualizar Tipo de Cambio SUNAT usando Celery

    Args:
        batch_size: Tamaño del lote para procesamiento (default: 1)

    Returns:
        dict: Resultado de la ejecución con estadísticas
    """

    try:
        # 📊 Logging inicial
        logger.info(
            f"🚀 Iniciando actualización tipo de cambio - Task ID: {self.request.id}"
        )

        # 🎯 Business Logic: Ejecutar actualización
        result = asyncio.run(_execute_tipo_cambio_update(batch_size=batch_size))

        logger.info(f"✅ Task completada exitosamente: {result}")
        return result

    except Exception as e:
        error_msg = f"❌ Error en tipo_cambio_task: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


async def _execute_tipo_cambio_update(
    batch_size: int = 1,
) -> Dict[str, Any]:
    """
    🔄 Lógica principal de actualización de tipo de cambio
    """

    # 📅 Configurar fechas usando BaseCronjob
    repository_factory = create_repository_factory()
    start_dt = BaseCronjob.obtener_datetime_fecha_inicio()
    end_dt = BaseCronjob.obtener_datetime_fecha_fin()

    logger.info(
        f"📅 Rango de fechas: {start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')}"
    )

    try:
        logger.info("🔄 Creando repositories...")

        # 🔧 Crear repository usando repository factory (patrón correcto)
        tipo_cambio_repo = await repository_factory.create_tipo_cambio_repository()

        # 📊 Obtener tipos de cambio existentes
        existing_records = await tipo_cambio_repo.get_all(limit=10000)
        existing_dates = {record.TipoCambioFecha for record in existing_records}

        # 📅 Generar todas las fechas del rango
        all_dates = {
            (start_dt + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((end_dt - start_dt).days + 1)
        }

        # 🔍 Identificar fechas faltantes
        missing_dates = list(all_dates - existing_dates)
        logger.info(f"📋 Fechas faltantes encontradas: {len(missing_dates)}")

        if not missing_dates:
            return {
                "status": "success",
                "message": "No hay fechas faltantes para actualizar",
                "dates_processed": 0,
                "dates_failed": 0,
            }

        # 🔄 Procesar en lotes
        final_results = []
        failed_dates = set()

        for i in range(0, len(missing_dates), batch_size):
            batch = missing_dates[i : i + batch_size]
            logger.info(f"🔄 Procesando lote {i//batch_size + 1}: {batch}")

            # 🌐 Obtener tipos de cambio del lote
            batch_results = await _process_batch(batch, failed_dates)
            final_results.extend(batch_results)

            # ⏱️ Pausa entre lotes para evitar rate limiting
            if i + batch_size < len(missing_dates):
                await asyncio.sleep(2)

        # 💾 Guardar resultados en base de datos
        if final_results:
            try:
                created_records = await tipo_cambio_repo.create_many(
                    [
                        {
                            "TipoCambioFecha": record.TipoCambioFecha,
                            "TipoCambioCompra": record.TipoCambioCompra,
                            "TipoCambioVenta": record.TipoCambioVenta,
                        }
                        for record in final_results
                    ]
                )
                # 🔧 Verificar si create_many retorna None o lista
                records_count = (
                    len(created_records)
                    if created_records is not None
                    else len(final_results)
                )
                logger.info(f"💾 Guardados {records_count} registros en BD")
            except Exception as e:
                logger.error(f"❌ Error guardando en BD: {str(e)}")
                raise

        # 📊 Resultado final
        return {
            "status": "success",
            "message": "Actualización completada",
            "dates_processed": len(final_results),
            "dates_failed": len(failed_dates),
            "failed_dates": list(failed_dates),
            "date_range": f"{start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')}",
        }

    except Exception as e:
        logger.error(f"❌ Error en _execute_tipo_cambio_update: {str(e)}")
        raise



async def _process_batch(
    batch_dates: List[str], failed_dates: Set[str]
) -> List[TipoCambioPostRequestSchema]:
    """
    🔄 Procesar un lote de fechas para obtener tipos de cambio
    """
    tasks = [_get_exchange_rate(date) for date in batch_dates]

    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logger.error(f"❌ Error en gather: {str(e)}")
        return []

    successful_results = []

    for idx, result in enumerate(results):
        current_date = batch_dates[idx]

        if isinstance(result, Exception):
            logger.warning(f"⚠️ Error para {current_date}: {str(result)}")
            failed_dates.add(current_date)
        elif isinstance(result, TipoCambioPostRequestSchema):
            successful_results.append(result)
            logger.info(f"✅ Tipo cambio obtenido para {current_date}")
        else:
            logger.error(f"❌ Respuesta inesperada para {current_date}: {type(result)}")
            failed_dates.add(current_date)

    return successful_results


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def _get_exchange_rate(date_str: str) -> TipoCambioPostRequestSchema:
    """
    🌐 Obtener tipo de cambio de SUNAT para una fecha específica
    """
    url = f"https://api.apis.net.pe/v1/tipo-cambio-sunat?fecha={date_str}"

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=30.0)

        if response.status_code != 200:
            error_msg = f"API request failed with status {response.status_code} for date {date_str}"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)

        if not response.text:
            error_msg = f"API response is empty for date {date_str}"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)

        json_data = response.json()
        logger.debug(f"📊 Datos obtenidos para {date_str}: {json_data}")

        # 🔄 Mapear respuesta a schema
        data = {
            "TipoCambioFecha": json_data["fecha"],
            "TipoCambioCompra": float(json_data["compra"]),
            "TipoCambioVenta": float(json_data["venta"]),
        }

        return TipoCambioPostRequestSchema(**data)

    except Exception as e:
        logger.error(f"❌ Exception para {date_str}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
