"""
🎯 Base Calculator V2 - Arquitectura Hexagonal

Calculador base para la nueva arquitectura sin dependencias legacy.
Implementa patrones modernos con async/await y logging optimizado.
"""

import time
import asyncio
from functools import wraps
from datetime import datetime
from typing import Any, Callable, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
from config import logger


class BaseCalcularV2:
    """
    🏗️ Calculador base V2 con arquitectura hexagonal.

    Características:
    - Sin dependencias legacy
    - Async/await nativo
    - Logging optimizado
    - Decoradores de performance
    - Patrones modernos
    """

    def __init__(self):
        self._execution_stats = {}
        self._last_execution = None

    @staticmethod
    def timeit(func: Callable) -> Callable:
        """
        ⏱️ Decorador para medir tiempo de ejecución.
        Compatible con funciones sync y async.
        """

        @wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            start_time = time.time()
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(self, *args, **kwargs)
                else:
                    result = func(self, *args, **kwargs)

                execution_time = time.time() - start_time

                # Guardar estadísticas
                method_name = func.__name__
                self._execution_stats[method_name] = {
                    "execution_time": execution_time,
                    "timestamp": datetime.now(),
                    "success": True,
                }

                logger.info(f"⏱️ {method_name} ejecutado en {execution_time:.2f}s")

                return result

            except Exception as e:
                execution_time = time.time() - start_time
                method_name = func.__name__

                self._execution_stats[method_name] = {
                    "execution_time": execution_time,
                    "timestamp": datetime.now(),
                    "success": False,
                    "error": str(e),
                }

                logger.error(
                    f"❌ {method_name} falló después de {execution_time:.2f}s: {str(e)}"
                )
                raise

        @wraps(func)
        def sync_wrapper(self, *args, **kwargs):
            start_time = time.time()
            try:
                result = func(self, *args, **kwargs)
                execution_time = time.time() - start_time

                method_name = func.__name__
                self._execution_stats[method_name] = {
                    "execution_time": execution_time,
                    "timestamp": datetime.now(),
                    "success": True,
                }

                logger.info(f"⏱️ {method_name} ejecutado en {execution_time:.2f}s")

                return result

            except Exception as e:
                execution_time = time.time() - start_time
                method_name = func.__name__

                self._execution_stats[method_name] = {
                    "execution_time": execution_time,
                    "timestamp": datetime.now(),
                    "success": False,
                    "error": str(e),
                }

                logger.error(
                    f"❌ {method_name} falló después de {execution_time:.2f}s: {str(e)}"
                )
                raise

        # Retornar wrapper apropiado según el tipo de función
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    def get_execution_stats(self) -> Dict[str, Any]:
        """
        📊 Obtiene estadísticas de ejecución del calculador.

        Returns:
            Diccionario con estadísticas de rendimiento
        """
        return {
            "execution_stats": self._execution_stats,
            "last_execution": self._last_execution,
            "total_methods_executed": len(self._execution_stats),
            "successful_executions": sum(
                1
                for stat in self._execution_stats.values()
                if stat.get("success", False)
            ),
            "failed_executions": sum(
                1
                for stat in self._execution_stats.values()
                if not stat.get("success", True)
            ),
        }

    def reset_stats(self) -> None:
        """
        🔄 Reinicia las estadísticas de ejecución.
        """
        self._execution_stats = {}
        self._last_execution = None
        logger.info("📊 Estadísticas de ejecución reiniciadas")

    async def health_check(self) -> Dict[str, Any]:
        """
        🏥 Verifica el estado de salud del calculador.

        Returns:
            Diccionario con estado de salud
        """
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "calculator_type": self.__class__.__name__,
            "execution_stats": self.get_execution_stats(),
        }


class BaseObtenerV2:
    """
    📡 Obtener base V2 para extracción de datos.

    Implementa patrones modernos sin dependencias legacy.
    """

    def __init__(self):
        self._last_fetch = None
        self._fetch_stats = {}

    async def obtener_json(self) -> List[Dict[str, Any]]:
        """
        📥 Método base para obtener datos en formato JSON.
        Debe ser implementado por subclases.

        Returns:
            Lista de diccionarios con datos
        """
        raise NotImplementedError("Subclases deben implementar obtener_json")

    def get_fetch_stats(self) -> Dict[str, Any]:
        """
        📊 Obtiene estadísticas de extracción de datos.

        Returns:
            Diccionario con estadísticas
        """
        return {"last_fetch": self._last_fetch, "fetch_stats": self._fetch_stats}


# Utility functions para compatibilidad
def normalize_text(text: str) -> str:
    """
    🔤 Normaliza texto removiendo acentos y convirtiendo a minúsculas.

    Args:
        text: Texto a normalizar

    Returns:
        Texto normalizado
    """
    import unicodedata

    if not isinstance(text, str):
        text = str(text)

    # Normalizar y remover acentos
    normalized = unicodedata.normalize("NFD", text)
    without_accents = "".join(c for c in normalized if unicodedata.category(c) != "Mn")

    return without_accents.lower().strip()


def validate_dataframe_columns(df: "pd.DataFrame", required_columns: List[str]) -> None:
    """
    ✅ Valida que un DataFrame tenga las columnas requeridas.

    Args:
        df: DataFrame a validar
        required_columns: Lista de columnas requeridas

    Raises:
        ValueError: Si faltan columnas requeridas
    """
    import pandas as pd

    if not isinstance(df, pd.DataFrame):
        raise ValueError("El parámetro debe ser un DataFrame de pandas")

    present_columns = set(df.columns)
    missing_columns = [col for col in required_columns if col not in present_columns]

    if missing_columns:
        raise ValueError(f"Faltan columnas requeridas: {missing_columns}")


def rename_dataframe_columns(
    df: "pd.DataFrame", column_mapping: Dict[str, str]
) -> "pd.DataFrame":
    """
    🔄 Renombra columnas de DataFrame usando mapeo.

    Args:
        df: DataFrame original
        column_mapping: Diccionario con mapeo {original: nuevo}

    Returns:
        DataFrame con columnas renombradas
    """
    import pandas as pd

    if not isinstance(df, pd.DataFrame):
        return df

    # Solo renombrar columnas que existen
    existing_mapping = {
        old: new for old, new in column_mapping.items() if old in df.columns
    }

    if existing_mapping:
        logger.debug(f"🔄 Renombrando columnas: {existing_mapping}")
        return df.rename(columns=existing_mapping)

    return df
