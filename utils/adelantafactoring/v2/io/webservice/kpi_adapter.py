"""
📡 KPI Webservice Adapter V2 - Comunicación externa optimizada
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Any
import aiohttp
from config.logger import logger
from ...config.settings import settings


class KPIWebserviceAdapter:
    """
    Adaptador para comunicación con webservice de KPI
    Mantiene compatibilidad con la interfaz original
    """

    def __init__(self):
        self.base_url = settings.WEBSERVICE_BASE_URL
        self.token_url = settings.KPI_TOKEN_URL
        self.data_url = settings.KPI_DATA_URL
        self.credentials = settings.KPI_CREDENTIALS
        self.timeout = settings.REQUEST_TIMEOUT
        self.max_retries = settings.MAX_RETRIES

    async def obtener_colocaciones(
        self,
        start_date: datetime,
        end_date: datetime,
        fecha_corte: datetime,
        tipo_reporte: int = 2,
    ) -> List[Dict[str, Any]]:
        """
        Obtiene colocaciones del webservice manteniendo compatibilidad con KPIObtener

        Args:
            start_date: Fecha inicio del período
            end_date: Fecha fin del período
            fecha_corte: Fecha de corte para el reporte
            tipo_reporte: Tipo de reporte (2=normal, 0=acumulado)

        Returns:
            Lista de diccionarios con datos de colocaciones
        """
        logger.info(
            f"📊 Obteniendo colocaciones KPI - Período: {start_date} a {end_date}"
        )

        try:
            # 1. Obtener token de autenticación
            token = await self._get_auth_token()

            # 2. Realizar llamada a API de colocaciones
            data = await self._fetch_colocaciones_data(
                token, start_date, end_date, fecha_corte, tipo_reporte
            )

            logger.info(
                f"✅ Colocaciones obtenidas exitosamente: {len(data)} registros"
            )
            return data

        except Exception as e:
            logger.error(f"❌ Error obteniendo colocaciones: {e}")
            raise

    async def _get_auth_token(self) -> str:
        """Obtiene token de autenticación del webservice"""
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout)
        ) as session:
            try:
                async with session.post(
                    self.token_url,
                    json=self.credentials,
                    headers={"Content-Type": "application/json"},
                ) as response:
                    response.raise_for_status()
                    result = await response.json()

                    if "access_token" not in result:
                        raise ValueError("Token no encontrado en respuesta")

                    return result["access_token"]

            except Exception as e:
                logger.error(f"Error obteniendo token de autenticación: {e}")
                raise

    async def _fetch_colocaciones_data(
        self,
        token: str,
        start_date: datetime,
        end_date: datetime,
        fecha_corte: datetime,
        tipo_reporte: int,
    ) -> List[Dict[str, Any]]:
        """Realiza llamada a API de colocaciones con reintentos"""
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        payload = {
            "fecha_inicio": start_date.strftime(settings.DATE_FORMATS["webservice"]),
            "fecha_fin": end_date.strftime(settings.DATE_FORMATS["webservice"]),
            "fecha_corte": fecha_corte.strftime(settings.DATE_FORMATS["webservice"]),
            "tipo_reporte": tipo_reporte,
        }

        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as session:
                    async with session.post(
                        self.data_url, json=payload, headers=headers
                    ) as response:
                        response.raise_for_status()
                        data = await response.json()

                        if not isinstance(data, list):
                            raise ValueError(
                                f"Respuesta inesperada del webservice: {type(data)}"
                            )

                        return data

            except Exception as e:
                logger.warning(f"Intento {attempt + 1}/{self.max_retries} falló: {e}")
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(2**attempt)  # Exponential backoff

        raise RuntimeError("Máximo de reintentos alcanzado")
