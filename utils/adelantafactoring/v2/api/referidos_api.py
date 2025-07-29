"""
🌐 Referidos API V2 - Adelanta Factoring Financial ETL

Interfaz pública simple y elegante para el módulo de referidos.
Mantiene compatibilidad 100% con el sistema legacy.
"""

import pandas as pd
from typing import Union, List, Dict, Tuple
from config.logger import logger
from ..processing.transformers.referidos_transformer import ReferidosTransformer
from ..schemas.referidos_schema import ReferidosProcessedSchema


class ReferidosAPI:
    """
    🌟 API principal para operaciones con referidos.

    Características:
    - 🔄 Interfaz simple: af.referidos.calcular()
    - ⚡ Optimizado con arquitectura hexagonal
    - 🛡️ Manejo robusto de errores
    - 📊 Compatible 100% con ReferidosCalcular legacy
    """

    def __init__(self):
        self._transformer = ReferidosTransformer()

    def calcular(self, as_df: bool = False) -> Union[List[Dict], pd.DataFrame]:
        """
        🎯 Método principal para calcular datos de referidos.

        Replica exactamente ReferidosCalcular.calcular() con optimizaciones.

        Args:
            as_df: Si True devuelve DataFrame, si False devuelve lista de dicts

        Returns:
            Union[List[Dict], pd.DataFrame]: Datos de referidos procesados

        Example:
            ```python
            import utils.adelantafactoring.v2 as af

            # Obtener como lista de diccionarios (compatible legacy)
            referidos = af.referidos.calcular()

            # Obtener como DataFrame para análisis
            referidos_df = af.referidos.calcular(as_df=True)
            ```
        """
        try:
            logger.info("🎯 Iniciando cálculo de referidos via API v2...")
            result = self._transformer.transform_referidos(as_df=as_df)
            logger.info("✅ Cálculo de referidos completado exitosamente")
            return result

        except Exception as e:
            logger.error(f"💥 Error en API de referidos: {e}")
            raise

    async def calcular_async(
        self, as_df: bool = False
    ) -> Union[List[Dict], pd.DataFrame]:
        """
        🚀 Método asíncrono para calcular datos de referidos.

        Args:
            as_df: Si True devuelve DataFrame, si False devuelve lista de dicts

        Returns:
            Union[List[Dict], pd.DataFrame]: Datos de referidos procesados
        """
        try:
            logger.info("🚀 Iniciando cálculo asíncrono de referidos...")
            result = await self._transformer.transform_referidos_async(as_df=as_df)
            logger.info("✅ Cálculo asíncrono completado exitosamente")
            return result

        except Exception as e:
            logger.error(f"💥 Error en API asíncrona de referidos: {e}")
            raise

    def calcular_con_metadatos(
        self, as_df: bool = False
    ) -> Tuple[Union[List[Dict], pd.DataFrame], ReferidosProcessedSchema]:
        """
        🔍 Calcula referidos con metadatos detallados del procesamiento.

        Args:
            as_df: Si True devuelve DataFrame, si False devuelve lista de dicts

        Returns:
            Tuple: (datos_procesados, metadatos_procesamiento)
        """
        try:
            logger.info("🔍 Iniciando cálculo con metadatos...")
            result = self._transformer.get_detailed_processing_result(as_df=as_df)
            logger.info("✅ Cálculo con metadatos completado")
            return result

        except Exception as e:
            logger.error(f"💥 Error en cálculo con metadatos: {e}")
            raise

    def obtener_estadisticas(self) -> Dict:
        """
        📊 Obtiene estadísticas del último procesamiento.

        Returns:
            Dict: Estadísticas de procesamiento
        """
        return self._transformer.get_processing_stats()

    def obtener_reporte_validacion(self) -> Dict:
        """
        📋 Obtiene reporte detallado de errores de validación.

        Returns:
            Dict: Reporte con errores y estadísticas de validación
        """
        return self._transformer.get_validation_report()

    def verificar_conectividad(self) -> bool:
        """
        🔍 Verifica conectividad con el webservice de referidos.

        Returns:
            bool: True si la conectividad es exitosa
        """
        try:
            logger.info("🔍 Verificando conectividad con webservice...")
            raw_data = self._transformer.data_engine.extract_referidos_data()

            if raw_data:
                logger.info("✅ Conectividad verificada exitosamente")
                return True
            else:
                logger.warning("⚠️ Conectividad OK pero sin datos")
                return False

        except Exception as e:
            logger.error(f"❌ Error de conectividad: {e}")
            return False


# 🌟 Instancia global para uso directo
referidos = ReferidosAPI()


# 🔄 Funciones de compatibilidad legacy
def calcular_referidos(as_df: bool = False) -> Union[List[Dict], pd.DataFrame]:
    """
    🔄 Función de compatibilidad legacy.

    Esta función mantiene compatibilidad con imports antiguos:
    from utils.adelantafactoring.v2.api.referidos import calcular_referidos
    """
    return referidos.calcular(as_df=as_df)


async def calcular_referidos_async(
    as_df: bool = False,
) -> Union[List[Dict], pd.DataFrame]:
    """🔄 Función asíncrona de compatibilidad legacy"""
    return await referidos.calcular_async(as_df=as_df)
