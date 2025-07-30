"""
🌐 API V2 - Diferido

Interfaz pública compatible con V1 - MANTIENE CONSTRUCTORES EXACTOS
"""

import pandas as pd
from io import BytesIO
from typing import Union

# Fallback para desarrollo
try:
    from ..engines.diferido_calculation_engine import DiferidoCalculationEngine
    from ..config.settings import settings
except ImportError:

    class _FallbackSettings:
        @staticmethod
        def logger(message: str) -> None:
            print(f"[DiferidoAPI] {message}")

    settings = _FallbackSettings()

    # Mock para desarrollo aislado
    class DiferidoCalculationEngine:
        def calcular_diferido(self, file_buffer, df_interno, hasta):
            return pd.DataFrame({"test": [1, 2, 3]})

        def calcular_diferido_externo(self, file_buffer, hasta):
            return pd.DataFrame({"test": [1, 2, 3]})

        def calcular_diferido_interno(self, df, hasta):
            return pd.DataFrame({"test": [1, 2, 3]})

        async def calcular_diferido_async(self, file_buffer, df_interno, hasta):
            return pd.DataFrame({"test": [1, 2, 3]})

        async def reorder_date_columns_async(self, date_cols):
            return date_cols

        async def comparar_diferidos_async(self, df_externo, df_calculado):
            return pd.DataFrame({"test": [1, 2, 3]})


# ===== CLASE PRINCIPAL COMPATIBLE CON V1 =====


class DiferidoCalcularV2:
    """
    API V2 para cálculos de diferido
    Mantiene EXACTAMENTE la misma interfaz que DiferidoCalcular V1:

    Constructor V1: DiferidoCalcular(file_path_externo: str, df_interno: pd.DataFrame)
    Método V1: calcular_diferido(hasta: str) -> pd.DataFrame
    Método V1: calcular_diferido_async(hasta: str) -> pd.DataFrame
    """

    def __init__(self, file_buffer: Union[BytesIO, str], df_interno: pd.DataFrame):
        """
        Constructor compatible con V1

        Args:
            file_buffer: Archivo Excel o path (era file_path_externo en V1)
            df_interno: DataFrame interno con datos KPI
        """
        self.file_buffer = file_buffer
        self.df_interno = df_interno
        self.engine = DiferidoCalculationEngine()

        # Log de inicialización para compatibilidad
        settings.logger("DiferidoCalcularV2 inicializado - compatibilidad total V1")

    def calcular_diferido(self, hasta: str) -> pd.DataFrame:
        """
        Calcula diferido completo - INTERFAZ EXACTA V1

        Args:
            hasta (str): Formato "YYYY-MM"

        Returns:
            pd.DataFrame: Resultado comparado (exacto como V1)
        """
        return self.engine.calcular_diferido(
            file_buffer=self.file_buffer, df_interno=self.df_interno, hasta=hasta
        )

    async def calcular_diferido_async(self, hasta: str) -> pd.DataFrame:
        """
        Calcula diferido completo asíncrono - INTERFAZ EXACTA V1

        Args:
            hasta (str): Formato "YYYY-MM"

        Returns:
            pd.DataFrame: Resultado comparado (exacto como V1)
        """
        return await self.engine.calcular_diferido_async(
            file_buffer=self.file_buffer, df_interno=self.df_interno, hasta=hasta
        )

    # ==================== MÉTODOS ADICIONALES PARA COMPATIBILIDAD ====================

    async def reorder_date_columns_async(self, date_cols: list[str]) -> list[str]:
        """Método exacto de V1 para compatibilidad"""
        return await self.engine.reorder_date_columns_async(date_cols)

    async def comparar_diferidos_async(self, df_externo, df_calculado):
        """Método exacto de V1 para compatibilidad"""
        return await self.engine.comparar_diferidos_async(df_externo, df_calculado)


# ===== FUNCIONES PÚBLICAS ADICIONALES =====


def calcular_diferido_completo(
    file_buffer: Union[BytesIO, str], df_interno: pd.DataFrame, hasta: str
) -> pd.DataFrame:
    """
    Replica DiferidoCalcular.calcular_diferido() de V1

    Comparación completa: externo vs interno

    Args:
        file_buffer: Archivo Excel con datos externos
        df_interno: DataFrame con datos internos
        hasta: Período límite (formato YYYY-MM)

    Returns:
        DataFrame con comparación completa
    """
    engine = DiferidoCalculationEngine()
    return engine.calcular_diferido(file_buffer, df_interno, hasta)


def calcular_diferido_externo(
    file_buffer: Union[BytesIO, str], hasta: str
) -> pd.DataFrame:
    """
    Replica DiferidoExternoCalcular.calcular_diferido_externo() de V1

    Solo cálculos externos

    Args:
        file_buffer: Archivo Excel con datos externos
        hasta: Período límite (formato YYYY-MM)

    Returns:
        DataFrame con cálculos externos
    """
    engine = DiferidoCalculationEngine()
    return engine.calcular_diferido_externo(file_buffer, hasta)


def calcular_diferido_interno(df_interno: pd.DataFrame, hasta: str) -> pd.DataFrame:
    """
    Replica DiferidoInternoCalcular.calcular_diferido_interno() de V1

    Solo cálculos internos

    Args:
        df_interno: DataFrame con datos internos
        hasta: Período límite (formato YYYY-MM)

    Returns:
        DataFrame con cálculos internos
    """
    engine = DiferidoCalculationEngine()
    return engine.calcular_diferido_interno(df_interno, hasta)


# ===== FUNCIONES ASÍNCRONAS =====


async def calcular_diferido_completo_async(
    file_buffer: Union[BytesIO, str], df_interno: pd.DataFrame, hasta: str
) -> pd.DataFrame:
    """
    Versión asíncrona de calcular_diferido_completo

    Args:
        file_buffer: Archivo Excel con datos externos
        df_interno: DataFrame con datos internos
        hasta: Período límite (formato YYYY-MM)

    Returns:
        DataFrame con comparación completa
    """
    engine = DiferidoCalculationEngine()
    return await engine.calcular_diferido_async(file_buffer, df_interno, hasta)
