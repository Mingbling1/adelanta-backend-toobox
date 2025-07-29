"""
📡 Webservice FondoPromocional V2 - Cliente especializado

Cliente optimizado para obtener datos de Fondo Promocional
"""

from typing import List, Dict, Any

# Importaciones absolutas con fallback
try:
    from utils.adelantafactoring.v2.engines.data_engine import DataEngine
    from utils.adelantafactoring.v2.config.settings import settings
except ImportError:
    # Fallback simple para desarrollo
    class DataEngine:
        def __init__(self):
            pass

        def get_data(self, **kwargs):
            return []

    # Instancia fallback con configuración básica
    class _FallbackSettings:
        GOOGLE_SHEETS_URLS = {
            "fondo_promocional": "https://script.google.com/macros/s/AKfycbzpX9RKtvJwN1QgFMU15hi1DXHtRhFlIC6jW8_QYTB-sQQIntsDO7fG6jWgKJb95V6X/exec"
        }

    settings = _FallbackSettings()


from config.logger import logger


class FondoPromocionalWebservice:
    """Cliente especializado para FondoPromocional"""

    def __init__(self):
        self.data_engine = DataEngine()
        self.url = settings.GOOGLE_SHEETS_URLS["fondo_promocional"]

    def fetch_fondo_promocional_data(self) -> List[Dict[str, Any]]:
        """
        Obtiene datos de FondoPromocional del webservice

        Returns:
            Lista de diccionarios con datos de fondo promocional
        """
        try:
            logger.info("Iniciando obtención de datos FondoPromocional")

            # Verificar cache primero
            cache_key = "fondo_promocional_data"
            cached_data = self.data_engine.get_cached_data(cache_key)

            if cached_data is not None:
                logger.info("Datos obtenidos desde cache")
                return cached_data

            # Obtener datos frescos
            data = self.data_engine.fetch_webservice_data(self.url)

            # Validación básica de estructura
            if not data:
                logger.warning("No se obtuvieron datos del webservice")
                return []

            # Verificar estructura esperada
            if not isinstance(data, list):
                logger.error(f"Se esperaba lista, recibido: {type(data)}")
                return []

            # Cache de datos para futuras consultas
            self.data_engine.set_cached_data(cache_key, data)

            logger.info(
                f"Datos FondoPromocional obtenidos exitosamente: {len(data)} registros"
            )
            return data

        except Exception as e:
            logger.error(f"Error obteniendo datos FondoPromocional: {e}")
            raise

    async def fetch_fondo_promocional_data_async(self) -> List[Dict[str, Any]]:
        """
        Versión asíncrona para obtener datos de FondoPromocional

        Returns:
            Lista de diccionarios con datos de fondo promocional
        """
        try:
            logger.info("Iniciando obtención async de datos FondoPromocional")

            data = await self.data_engine.fetch_webservice_data_async(self.url)

            if not data:
                logger.warning("No se obtuvieron datos del webservice async")
                return []

            logger.info(
                f"Datos FondoPromocional async obtenidos: {len(data)} registros"
            )
            return data

        except Exception as e:
            logger.error(f"Error obteniendo datos FondoPromocional async: {e}")
            raise
