"""
📡 CXC Acumulado DIM Client V2 - Comunicación asíncrona con webservice
Obtiene datos acumulados dimensionales CXC desde webservice externo
"""

import aiohttp
import asyncio
from typing import List, Dict, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

# Imports con fallback para desarrollo aislado
try:
    from utils.adelantafactoring.v2.config.settings import settings
    from config.logger import logger
except ImportError:
    # Fallback para desarrollo aislado
    class _FallbackSettings:
        WEBSERVICE_BASE_URL = "https://webservice.adelantafactoring.com"
        REQUEST_TIMEOUT = 30

    class _FallbackLogger:
        def debug(self, msg):
            print(f"DEBUG: {msg}")

        def info(self, msg):
            print(f"INFO: {msg}")

        def warning(self, msg):
            print(f"WARNING: {msg}")

        def error(self, msg):
            print(f"ERROR: {msg}")

    settings = _FallbackSettings()
    logger = _FallbackLogger()


class CXCAcumuladoDIMWebserviceClient:
    """Cliente especializado para obtener datos acumulados DIM CXC"""

    def __init__(self):
        self.base_url = settings.WEBSERVICE_BASE_URL
        self.timeout = getattr(settings, "REQUEST_TIMEOUT", 30)
        self.endpoint = "/webservice/liquidacionCab/subquery-cab"

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def fetch_acumulado_dim_data(
        self, params: Optional[Dict] = None
    ) -> List[Dict]:
        """
        Obtiene datos acumulados DIM desde el webservice.

        Args:
            params: Parámetros opcionales para la consulta

        Returns:
            Lista de datos acumulados DIM
        """
        url = f"{self.base_url}{self.endpoint}"

        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params=params) as response:

                    if response.status != 200:
                        logger.error(
                            f"HTTP {response.status} al obtener acumulado DIM desde {url}"
                        )
                        response.raise_for_status()

                    data = await response.json()

                    # Asegurar que devolvemos una lista
                    if isinstance(data, list):
                        logger.debug(
                            f"Acumulado DIM CXC obtenido exitosamente: {len(data)} registros"
                        )
                        return data
                    else:
                        logger.warning(
                            "La respuesta del webservice no es una lista, devolviendo lista vacía"
                        )
                        return []

        except aiohttp.ClientError as e:
            logger.error(f"Error de cliente HTTP obteniendo acumulado DIM: {e}")
            raise
        except asyncio.TimeoutError as e:
            logger.error(f"Timeout obteniendo acumulado DIM desde {url}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado obteniendo acumulado DIM: {e}")
            raise

    async def health_check(self) -> bool:
        """
        Verifica si el webservice está disponible.

        Returns:
            True si el webservice responde correctamente
        """
        try:
            # Hacer una consulta simple sin parámetros
            await self.fetch_acumulado_dim_data()
            logger.info("Health check CXC acumulado DIM: OK")
            return True
        except Exception as e:
            logger.error(f"Health check CXC acumulado DIM falló: {e}")
            return False
