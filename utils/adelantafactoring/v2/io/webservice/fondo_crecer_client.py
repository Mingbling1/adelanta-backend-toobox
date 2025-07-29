"""
📡 Webservice FondoCrecer V2 - Cliente especializado

Cliente optimizado para obtener datos de Fondo Crecer con garantías
"""

from typing import List, Dict, Any
from config.logger import logger


# Import con fallback para compatibilidad
try:
    from utils.adelantafactoring.v2.engines.data_engine import DataEngine
    from utils.adelantafactoring.v2.config.settings import settings
except ImportError:
    # Fallback simple para desarrollo
    import requests
    from tenacity import retry, stop_after_attempt, wait_exponential

    # Instancia fallback con configuración básica
    class _FallbackSettings:
        GOOGLE_SHEETS_URLS = {
            "fondo_crecer": "https://script.google.com/macros/s/AKfycbyFKvZcqZNBm2XktdOR4lrv5Wwd_PwovO85INFieEqzQexXgwXD5XuF-nPWPME1sjGFlQ/exec"
        }

    settings = _FallbackSettings()

    class DataEngine:
        def __init__(self):
            self._cache = {}

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=10),
        )
        def fetch_webservice_data(
            self, url: str, timeout: int = 60
        ) -> List[Dict[str, Any]]:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            return data if isinstance(data, list) else []

        def get_cached_data(self, key: str):
            return self._cache.get(key)

        def set_cached_data(self, key: str, data: Any):
            self._cache[key] = data


class FondoCrecerWebservice:
    """Cliente especializado para FondoCrecer"""

    def __init__(self):
        self.data_engine = DataEngine()
        self.url = settings.GOOGLE_SHEETS_URLS["fondo_crecer"]

    def fetch_fondo_crecer_data(self) -> List[Dict[str, Any]]:
        """
        Obtiene datos de FondoCrecer del webservice

        Returns:
            Lista de diccionarios con datos de fondo crecer
        """
        try:
            logger.info("Iniciando obtención de datos FondoCrecer")

            # Verificar cache primero
            cache_key = "fondo_crecer_data"
            cached_data = self.data_engine.get_cached_data(cache_key)

            if cached_data is not None:
                logger.info("Datos FondoCrecer obtenidos desde cache")
                return cached_data

            # Obtener datos frescos
            data = self.data_engine.fetch_webservice_data(self.url)

            # Validación básica de estructura
            if not data:
                logger.warning("No se obtuvieron datos del webservice FondoCrecer")
                return []

            # Verificar estructura esperada
            if not isinstance(data, list):
                logger.error(f"Se esperaba lista, recibido: {type(data)}")
                return []

            # Validación específica para FondoCrecer (debe tener LIQUIDACION y GARANTIA)
            valid_data = []
            for item in data:
                if (
                    isinstance(item, dict)
                    and "LIQUIDACION" in item
                    and "GARANTIA" in item
                ):
                    valid_data.append(item)
                else:
                    logger.warning(f"Registro inválido ignorado: {item}")

            # Cache de datos para futuras consultas
            self.data_engine.set_cached_data(cache_key, valid_data)

            logger.info(
                f"Datos FondoCrecer obtenidos exitosamente: {len(valid_data)} registros válidos de {len(data)} totales"
            )
            return valid_data

        except Exception as e:
            logger.error(f"Error obteniendo datos FondoCrecer: {e}")
            raise

    async def fetch_fondo_crecer_data_async(self) -> List[Dict[str, Any]]:
        """
        Versión asíncrona para obtener datos de FondoCrecer

        Returns:
            Lista de diccionarios con datos de fondo crecer
        """
        try:
            logger.info("Iniciando obtención async de datos FondoCrecer")

            # Si DataEngine tiene método async, usarlo
            if hasattr(self.data_engine, "fetch_webservice_data_async"):
                data = await self.data_engine.fetch_webservice_data_async(self.url)
            else:
                # Fallback a versión síncrona
                import asyncio

                data = await asyncio.get_event_loop().run_in_executor(
                    None, self.fetch_fondo_crecer_data
                )
                return data

            if not data:
                logger.warning(
                    "No se obtuvieron datos del webservice async FondoCrecer"
                )
                return []

            # Misma validación que versión síncrona
            valid_data = []
            for item in data:
                if (
                    isinstance(item, dict)
                    and "LIQUIDACION" in item
                    and "GARANTIA" in item
                ):
                    valid_data.append(item)

            logger.info(
                f"Datos FondoCrecer async obtenidos: {len(valid_data)} registros válidos"
            )
            return valid_data

        except Exception as e:
            logger.error(f"Error obteniendo datos FondoCrecer async: {e}")
            raise
