"""
Fuente de datos para saldos
"""

import aiohttp
from typing import Dict

try:
    from utils.adelantafactoring.v2.config.settings import settings
except ImportError:
    # Fallback básico
    class _FallbackSettings:
        GOOGLE_SHEETS_URLS = {
            "saldos": "https://script.google.com/macros/s/AKfycbzSFKR3DyDo9Ezxsq_75DDJ1vze76Lj_kC4iXiFMvAE_t6Xbi9rHrejT9v8CnWqWV9UKw/exec"
        }

    settings = _FallbackSettings()


class SaldosDataSource:
    """Fuente de datos para obtener información de saldos"""

    SALDOS_URL = settings.GOOGLE_SHEETS_URLS["saldos"]

    def __init__(self):
        self._cache = None
        self._cache_time = None

    async def obtener_saldos(self, force_refresh: bool = False) -> Dict:
        """
        Obtiene datos de saldos desde Google Scripts

        Args:
            force_refresh: Forzar obtención de datos (ignorar cache)

        Returns:
            Dict con los datos de saldos
        """
        # Cache simple (opcional, siguiendo patrón original)
        if not force_refresh and self._cache is not None:
            return self._cache

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.SALDOS_URL) as response:
                    if response.status == 200:
                        data = await response.json()
                        self._cache = data
                        return data
                    else:
                        raise Exception(f"Error obteniendo datos: {response.status}")
        except Exception as e:
            # Fallback para compatibilidad con el patrón original
            print(f"Error en obtención async: {e}")
            return self._obtener_sincrono()

    def _obtener_sincrono(self) -> Dict:
        """Fallback síncrono compatible con BaseObtener original"""
        import requests

        try:
            response = requests.get(self.SALDOS_URL)
            if response.status_code == 200:
                data = response.json()
                self._cache = data
                return data
            else:
                raise Exception(f"Error obteniendo datos: {response.status_code}")
        except Exception as e:
            print(f"Error en obtención síncrona: {e}")
            return {"error": str(e)}

    def clear_cache(self):
        """Limpia el cache de datos"""
        self._cache = None
        self._cache_time = None
