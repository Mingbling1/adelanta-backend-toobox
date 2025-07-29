"""
🔄 Referidos Transformer V2 - Adelanta Factoring Financial ETL

Transformador especializado para el procesamiento completo de datos de referidos.
Implementa el patrón Calculator → Obtainer → Schema de forma optimizada.
"""

import pandas as pd
from typing import Union, List, Dict, Tuple
from config.logger import logger
from ...engines.referidos_data_engine import ReferidosDataEngine
from ...engines.referidos_validation_engine import ReferidosValidationEngine
from ...schemas.referidos import ReferidosProcessedSchema


class ReferidosTransformer:
    """
    🚀 Transformador principal para datos de referidos.

    Características:
    - 🔄 Pipeline ETL completo: Extract → Transform → Load
    - ⚡ Optimizado con Pydantic RUST y list comprehensions
    - 🛡️ Manejo robusto de errores en cada etapa
    - 📊 Compatible 100% con ReferidosCalcular legacy
    """

    def __init__(self):
        self.data_engine = ReferidosDataEngine()
        self.validation_engine = ReferidosValidationEngine()
        self._processing_stats = {}

    def transform_referidos(
        self, as_df: bool = False
    ) -> Union[List[Dict], pd.DataFrame]:
        """
        🔄 Flujo principal de transformación de referidos.

        Replica exactamente la funcionalidad de ReferidosCalcular.calcular()
        con optimizaciones de rendimiento y mejor manejo de errores.

        Args:
            as_df: Si True devuelve DataFrame, si False devuelve lista de dicts

        Returns:
            Union[List[Dict], pd.DataFrame]: Datos transformados según as_df
        """
        try:
            logger.info("🚀 Iniciando transformación completa de referidos...")

            # 1️⃣ EXTRACT: Obtener datos RAW
            raw_data = self._extract_raw_data()

            # 2️⃣ TRANSFORM: Validar y procesar
            processed_data = self._transform_and_validate(raw_data)

            # 3️⃣ LOAD: Formatear salida
            result = self._format_output(processed_data, as_df)

            # 📊 Guardar estadísticas para auditoría
            self._save_processing_stats(raw_data, processed_data)

            logger.info("✅ Transformación de referidos completada exitosamente")
            return result

        except Exception as e:
            logger.error(f"💥 Error crítico en transformación de referidos: {e}")
            raise

    async def transform_referidos_async(
        self, as_df: bool = False
    ) -> Union[List[Dict], pd.DataFrame]:
        """
        🔄 Flujo asíncrono de transformación de referidos.

        Args:
            as_df: Si True devuelve DataFrame, si False devuelve lista de dicts

        Returns:
            Union[List[Dict], pd.DataFrame]: Datos transformados según as_df
        """
        try:
            logger.info("🚀 Iniciando transformación asíncrona de referidos...")

            # 1️⃣ EXTRACT: Obtener datos RAW de forma asíncrona
            raw_data = await self.data_engine.extract_referidos_data_async()

            # 2️⃣ TRANSFORM: Validar y procesar (síncronos)
            processed_data = self._transform_and_validate(raw_data)

            # 3️⃣ LOAD: Formatear salida
            result = self._format_output(processed_data, as_df)

            # 📊 Guardar estadísticas para auditoría
            self._save_processing_stats(raw_data, processed_data)

            logger.info("✅ Transformación asíncrona de referidos completada")
            return result

        except Exception as e:
            logger.error(f"💥 Error crítico en transformación asíncrona: {e}")
            raise

    def _extract_raw_data(self) -> List[Dict]:
        """📥 Extrae datos RAW del webservice"""
        try:
            logger.info("📥 Extrayendo datos RAW de referidos...")
            raw_data = self.data_engine.extract_referidos_data()

            # Validación básica de estructura
            validated_raw = self.data_engine.validate_raw_data(raw_data)

            logger.info(f"✅ Extracción completada: {len(validated_raw)} registros")
            return validated_raw

        except Exception as e:
            logger.error(f"💥 Error en extracción de datos RAW: {e}")
            raise

    def _transform_and_validate(self, raw_data: List[Dict]) -> List[Dict]:
        """🔄 Transforma y valida datos RAW"""
        try:
            logger.info("🔄 Transformando y validando datos...")

            # 1. Validar estructura y convertir a schema estandarizado
            validated_data = self.validation_engine.validate_raw_to_schema(raw_data)

            # 2. Eliminar duplicados por CodigoLiquidacion
            deduplicated_data, duplicates_count = (
                self.validation_engine.validate_and_deduplicate(validated_data)
            )

            logger.info(
                f"✅ Transformación completada: {len(deduplicated_data)} registros únicos"
            )
            return deduplicated_data

        except Exception as e:
            logger.error(f"💥 Error en transformación y validación: {e}")
            raise

    def _format_output(
        self, processed_data: List[Dict], as_df: bool
    ) -> Union[List[Dict], pd.DataFrame]:
        """📤 Formatea la salida según el parámetro as_df"""
        try:
            if as_df:
                logger.info("📊 Convirtiendo a DataFrame...")
                df = pd.DataFrame(processed_data)
                logger.info(
                    f"✅ DataFrame creado: {len(df)} filas, {len(df.columns)} columnas"
                )
                return df
            else:
                logger.info("📋 Retornando lista de diccionarios...")
                return processed_data

        except Exception as e:
            logger.error(f"💥 Error en formateo de salida: {e}")
            raise

    def _save_processing_stats(self, raw_data: List[Dict], processed_data: List[Dict]):
        """📊 Guarda estadísticas del procesamiento para auditoría"""
        self._processing_stats = {
            "raw_records": len(raw_data),
            "processed_records": len(processed_data),
            "validation_errors": len(self.validation_engine.validation_errors),
            "processing_efficiency": (
                len(processed_data) / len(raw_data) if raw_data else 0
            ),
        }

        logger.info(f"📊 Estadísticas de procesamiento: {self._processing_stats}")

    def get_processing_stats(self) -> Dict:
        """📊 Obtiene estadísticas del último procesamiento"""
        return self._processing_stats.copy()

    def get_validation_report(self) -> Dict:
        """📋 Obtiene reporte detallado de validación"""
        return self.validation_engine.get_validation_report()

    def get_detailed_processing_result(
        self, as_df: bool = False
    ) -> Tuple[Union[List[Dict], pd.DataFrame], ReferidosProcessedSchema]:
        """
        🔍 Obtiene resultado detallado con metadatos completos.

        Args:
            as_df: Si True devuelve DataFrame, si False devuelve lista de dicts

        Returns:
            Tuple: (datos_procesados, schema_con_metadatos)
        """
        try:
            logger.info("🔍 Generando resultado detallado con metadatos...")

            # Extraer y procesar datos
            raw_data = self._extract_raw_data()
            processed_data = self._transform_and_validate(raw_data)

            # Crear schema con metadatos
            duplicates_removed = len(raw_data) - len(processed_data)
            processed_schema = self.validation_engine.validate_final_output(
                processed_data, duplicates_removed
            )

            # Formatear salida principal
            formatted_output = self._format_output(processed_data, as_df)

            logger.info("✅ Resultado detallado generado exitosamente")
            return formatted_output, processed_schema

        except Exception as e:
            logger.error(f"💥 Error generando resultado detallado: {e}")
            raise
