"""
📡 Cliente webservice para CXC Pagos Fact - Comunicación con endpoints externos
"""

try:
    from utils.adelantafactoring.v2.config.settings import settings
except ImportError:
    # Fallback para desarrollo aislado
    class _FallbackSettings:
        WEBSERVICE_BASE_URL = "https://webservice.adelantafactoring.com"

    settings = _FallbackSettings()

from typing import List, Dict, Optional
import aiohttp
import asyncio
from config.logger import logger


class CXCPagosFactWebserviceClient:
    """Cliente para obtener datos de pagos de facturas desde webservice"""

    def __init__(self):
        self.base_url = settings.WEBSERVICE_BASE_URL
        self.endpoint = "/webservice/liquidacionPago/subquery"
        self.full_url = f"{self.base_url}{self.endpoint}"

    async def fetch_pagos_facturas(self, timeout: int = 30) -> List[Dict]:
        """
        Obtiene datos de pagos de facturas del webservice

        Args:
            timeout: Timeout en segundos para la petición

        Returns:
            Lista de diccionarios con datos de pagos
        """
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as session:
                async with session.get(self.full_url) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Asegurar que devolvemos una lista
                        if isinstance(data, list):
                            logger.debug(
                                f"Pagos de facturas obtenidos exitosamente: {len(data)} registros"
                            )
                            return data
                        else:
                            logger.warning(
                                "La respuesta no es una lista, devolviendo lista vacía"
                            )
                            return []
                    else:
                        logger.error(
                            f"Error HTTP {response.status} obteniendo pagos de facturas"
                        )
                        return []

        except asyncio.TimeoutError:
            logger.error(f"Timeout obteniendo pagos de facturas desde {self.full_url}")
            return []
        except Exception as e:
            logger.error(f"Error obteniendo pagos de facturas: {e}")
            return []

    async def health_check(self) -> bool:
        """
        Verifica la conectividad con el webservice

        Returns:
            True si el webservice responde, False en caso contrario
        """
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5)
            ) as session:
                async with session.get(self.full_url) as response:
                    return response.status == 200
        except Exception:
            return False
