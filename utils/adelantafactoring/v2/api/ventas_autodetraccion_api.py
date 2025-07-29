"""
API/Fachada para cálculos de autodetracción de ventas
Mantiene compatibilidad con la interfaz original
"""

import pandas as pd
from io import BytesIO

from ..engines.ventas_autodetraccion_engine import VentasAutodetraccionesEngine
from ..schemas.ventas_autodetraccion_schema import VentasAutodetraccionesRequest


class VentasAutodetraccionesAPI:
    """
    Fachada que mantiene compatibilidad con la interfaz original de VentasAutodetraccionesCalcular
    Delega el procesamiento al motor de cálculo hexagonal
    """

    def __init__(self, tipo_cambio_df: pd.DataFrame, comprobantes_df: pd.DataFrame):
        """
        Constructor compatible con la versión original

        Args:
            tipo_cambio_df: DataFrame con la información de tipo de cambio
            comprobantes_df: DataFrame con los comprobantes de ventas
        """
        self.tipo_cambio_df = tipo_cambio_df
        self.comprobantes_df = comprobantes_df
        self._engine = VentasAutodetraccionesEngine()

    async def generar_excel_autodetraccion(self, hasta: str) -> BytesIO:
        """
        Genera el Excel con la información filtrada por el mes indicado en 'hasta'.
        Mantiene la interfaz exacta de la versión original.

        Args:
            hasta: Valor en formato 'YYYY-MM'

        Returns:
            BytesIO: Buffer con el archivo Excel generado
        """
        # Crear request para el motor
        request = VentasAutodetraccionesRequest(
            hasta=hasta,
            comprobantes_df=self.comprobantes_df,
            tipo_cambio_df=self.tipo_cambio_df,
        )

        # Delegar al motor
        result = await self._engine.calculate(request)

        # Retornar solo el buffer para mantener compatibilidad
        return result.excel_buffer

    async def generar_excel_autodetraccion_con_estadisticas(self, hasta: str) -> dict:
        """
        Versión extendida que retorna estadísticas adicionales
        (Nueva funcionalidad disponible en v2)

        Args:
            hasta: Valor en formato 'YYYY-MM'

        Returns:
            dict: Diccionario con el buffer y estadísticas
        """
        # Crear request para el motor
        request = VentasAutodetraccionesRequest(
            hasta=hasta,
            comprobantes_df=self.comprobantes_df,
            tipo_cambio_df=self.tipo_cambio_df,
        )

        # Delegar al motor
        result = await self._engine.calculate(request)

        return {
            "excel_buffer": result.excel_buffer,
            "registro_ventas_count": result.registro_ventas_count,
            "autodetraccion_count": result.autodetraccion_count,
            "total_autodetraccion": result.total_autodetraccion,
            "hasta": result.hasta,
        }

    # Métodos de compatibilidad adicionales si se necesitan
    def set_estados_invalidos(self, estados: list) -> None:
        """Permite configurar estados de documentos inválidos"""
        self._engine.estado_invalido = estados

    def get_engine(self) -> VentasAutodetraccionesEngine:
        """Acceso al motor para funcionalidades avanzadas"""
        return self._engine
