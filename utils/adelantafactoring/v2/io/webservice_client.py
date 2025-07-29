"""
🌐 Base Webservice Client V2

Cliente base para comunicación con webservices de Adelanta Factoring.
Proporciona funcionalidad común de HTTP requests con retry y logging.
"""

import aiohttp
import asyncio
from typing import List, Dict, Any
from tenacity import retry, stop_after_attempt, wait_fixed

from config import logger


class BaseWebserviceClient:
    """
    🔌 Cliente base para webservices con funcionalidad async y retry automático.
    """

    def __init__(self, timeout: int = 30):
        self.timeout = aiohttp.ClientTimeout(total=timeout)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_data_async(self, url: str) -> List[Dict[str, Any]]:
        """
        📡 Obtiene datos desde URL de forma asíncrona con retry automático.

        Args:
            url: URL del webservice

        Returns:
            Lista de diccionarios con los datos
        """
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"✅ Datos obtenidos exitosamente desde {url}")
                        return data if isinstance(data, list) else []
                    else:
                        logger.error(f"❌ Error HTTP {response.status} desde {url}")
                        return []

        except asyncio.TimeoutError:
            logger.error(f"⏰ Timeout al conectar con {url}")
            raise
        except Exception as e:
            logger.error(f"❌ Error obteniendo datos desde {url}: {str(e)}")
            raise

    def get_data_sync(self, url: str) -> List[Dict[str, Any]]:
        """
        📡 Wrapper síncrono para compatibilidad legacy.

        Args:
            url: URL del webservice

        Returns:
            Lista de diccionarios con los datos
        """
        try:
            return asyncio.run(self.get_data_async(url))
        except Exception as e:
            logger.error(f"❌ Error en get_data_sync: {str(e)}")
            return []
