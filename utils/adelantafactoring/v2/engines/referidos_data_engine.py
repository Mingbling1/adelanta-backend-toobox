"""
🔌 Referidos Data Engine V2 - Adelanta Factoring Financial ETL

Motor de datos optimizado para obtener información de referidos desde fuentes externas.
Implementa patrón async/sync con reintentos automáticos y manejo robusto de errores.
"""

import httpx
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import List, Dict
from config.logger import logger
from ..config.settings import ReferidosConfig


class ReferidosDataEngine:
    """
    🚀 Motor de datos para referidos con extracción optimizada.

    Características:
    - ⚡ Soporte async/sync
    - 🔄 Reintentos automáticos con backoff exponencial
    - 🛡️ Manejo robusto de errores
    - 📊 Compatible 100% con ReferidosObtener legacy
    """

    def __init__(self):
        self.config = ReferidosConfig()
        self._timeout_sync = 60
        self._timeout_async = 120

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def extract_referidos_data(self) -> List[Dict]:
        """
        📥 Extrae datos de referidos de forma síncrona.

        Mantiene compatibilidad completa con ReferidosObtener.referidos_obtener()

        Returns:
            List[Dict]: Lista de diccionarios con datos de referidos

        Raises:
            requests.exceptions.RequestException: Para errores de red/HTTP
            ValueError: Para errores de validación
        """
        try:
            logger.info("🔄 Iniciando extracción de datos de referidos...")

            response = requests.get(
                self.config.referidos_url,
                timeout=self._timeout_sync,
                headers={"User-Agent": "Adelanta-Financial-ETL/2.0"},
            )

            response.raise_for_status()
            data = response.json()

            if not isinstance(data, list):
                raise ValueError("❌ Los datos recibidos no tienen formato de lista")

            logger.info(
                f"✅ Datos de referidos extraídos exitosamente: {len(data)} registros"
            )
            return data

        except requests.exceptions.Timeout:
            logger.error("⏰ Timeout al obtener datos de referidos")
            raise
        except requests.exceptions.ConnectionError:
            logger.error("🔌 Error de conexión al obtener datos de referidos")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"🚨 Error HTTP al obtener datos de referidos: {e}")
            raise
        except Exception as e:
            logger.error(f"💥 Error inesperado al obtener datos de referidos: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    async def extract_referidos_data_async(self) -> List[Dict]:
        """
        📥 Extrae datos de referidos de forma asíncrona.

        Returns:
            List[Dict]: Lista de diccionarios con datos de referidos

        Raises:
            httpx.HTTPError: Para errores de red/HTTP
            ValueError: Para errores de validación
        """
        try:
            logger.info("🔄 Iniciando extracción asíncrona de datos de referidos...")

            async with httpx.AsyncClient(timeout=self._timeout_async) as client:
                response = await client.get(
                    self.config.referidos_url,
                    headers={"User-Agent": "Adelanta-Financial-ETL/2.0"},
                )

                response.raise_for_status()
                data = response.json()

                if not isinstance(data, list):
                    raise ValueError(
                        "❌ Los datos recibidos no tienen formato de lista"
                    )

                logger.info(
                    f"✅ Datos de referidos extraídos exitosamente (async): {len(data)} registros"
                )
                return data

        except httpx.TimeoutException:
            logger.error("⏰ Timeout al obtener datos de referidos (async)")
            raise
        except httpx.ConnectError:
            logger.error("🔌 Error de conexión al obtener datos de referidos (async)")
            raise
        except httpx.HTTPStatusError as e:
            logger.error(f"🚨 Error HTTP al obtener datos de referidos (async): {e}")
            raise
        except Exception as e:
            logger.error(
                f"💥 Error inesperado al obtener datos de referidos (async): {e}"
            )
            raise

    def validate_raw_data(self, data: List[Dict]) -> List[Dict]:
        """
        🔍 Valida estructura básica de datos RAW.

        Args:
            data: Lista de diccionarios con datos RAW

        Returns:
            List[Dict]: Datos validados

        Raises:
            ValueError: Si los datos no tienen la estructura esperada
        """
        if not data:
            logger.warning("⚠️ No se recibieron datos de referidos")
            return []

        required_fields = {"REFERENCIA", "LIQUIDACIÓN", "EJECUTIVO", "MES"}

        for i, record in enumerate(data):
            if not isinstance(record, dict):
                raise ValueError(f"❌ Registro {i} no es un diccionario válido")

            # Normalizar nombres de campos (case-insensitive)
            normalized_keys = {k.upper().strip() for k in record.keys()}
            missing_fields = required_fields - normalized_keys

            if missing_fields:
                logger.warning(
                    f"⚠️ Registro {i} tiene campos faltantes: {missing_fields}"
                )

        logger.info(f"✅ Validación de datos RAW completada: {len(data)} registros")
        return data
