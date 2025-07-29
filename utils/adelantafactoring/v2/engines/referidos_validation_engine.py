"""
✅ Referidos Validation Engine V2 - Adelanta Factoring Financial ETL

Motor de validación con Pydantic RUST-powered para máximo rendimiento.
Utiliza list comprehensions optimizadas para validación masiva de datos.
"""

import pandas as pd
from typing import List, Dict, Tuple
from config.logger import logger
from ..schemas.referidos_schema import ReferidosCalcularSchema, ReferidosProcessedSchema


class ReferidosValidationEngine:
    """
    🔍 Motor de validación ultra-eficiente para datos de referidos.

    Características:
    - ⚡ Validación RUST-powered con Pydantic 2.0+
    - 🚀 List comprehensions optimizadas
    - 🛡️ Manejo robusto de errores por registro
    - 📊 Compatible 100% con el flujo legacy
    """

    def __init__(self):
        self.validation_errors: List[Dict] = []

    def validate_raw_to_schema(self, raw_data: List[Dict]) -> List[Dict]:
        """
        🔄 Valida datos RAW y los convierte al schema estandarizado.

        Optimización: Usa set intersection para filtrar columnas antes de la validación.

        Args:
            raw_data: Lista de diccionarios con datos RAW del webservice

        Returns:
            List[Dict]: Datos validados con ReferidosCalcularSchema
        """
        if not raw_data:
            logger.warning("⚠️ No hay datos RAW para validar")
            return []

        try:
            logger.info(f"🔄 Iniciando validación de {len(raw_data)} registros RAW...")

            # ⚡ Optimización: Filtrar columnas necesarias usando set intersection
            df = pd.DataFrame(raw_data)
            schema_fields = set(ReferidosCalcularSchema.model_fields.keys())
            available_cols = set(df.columns)

            logger.debug(f"📊 Campos disponibles: {available_cols}")
            logger.debug(f"📋 Campos requeridos: {schema_fields}")

            # Mapeo de normalización de columnas (manteniendo compatibilidad)
            column_mapping = {
                "REFERENCIA": "Referencia",
                "Liquidación": "CodigoLiquidacion",  # Columna real del webservice
                "LIQUIDACIÓN": "CodigoLiquidacion",  # Fallback mayúsculas
                "LIQUIDACION": "CodigoLiquidacion",  # Fallback sin tilde
                "EJECUTIVO": "Ejecutivo",
                "MES": "Mes",
            }

            # Renombrar columnas
            df_renamed = df.rename(columns=column_mapping)

            # ⚡ List comprehension ultra-eficiente con Pydantic RUST
            validated_records = []
            validation_errors = []

            for i, record in enumerate(df_renamed.to_dict(orient="records")):
                try:
                    validated = ReferidosCalcularSchema(**record).model_dump()
                    validated_records.append(validated)
                except Exception as e:
                    error_detail = {
                        "record_index": i,
                        "record_data": record,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    }
                    validation_errors.append(error_detail)
                    logger.warning(f"⚠️ Error validando registro {i}: {e}")

            # Guardar errores para auditoría
            self.validation_errors = validation_errors

            if validation_errors:
                logger.warning(
                    f"⚠️ Se encontraron {len(validation_errors)} errores de validación"
                )

            logger.info(
                f"✅ Validación completada: {len(validated_records)}/{len(raw_data)} registros válidos"
            )
            return validated_records

        except Exception as e:
            logger.error(f"💥 Error crítico en validación RAW: {e}")
            raise

    def validate_and_deduplicate(self, data: List[Dict]) -> Tuple[List[Dict], int]:
        """
        🔄 Valida datos y elimina duplicados por CodigoLiquidacion.

        Args:
            data: Lista de diccionarios validados

        Returns:
            Tuple[List[Dict], int]: (datos_sin_duplicados, cantidad_duplicados_eliminados)
        """
        if not data:
            return [], 0

        try:
            logger.info(f"🔄 Eliminando duplicados de {len(data)} registros...")

            df = pd.DataFrame(data)
            original_count = len(df)

            # Eliminar duplicados por CodigoLiquidacion (manteniendo el último)
            df_deduplicated = df.drop_duplicates(
                subset="CodigoLiquidacion", keep="last"
            )
            final_count = len(df_deduplicated)
            duplicates_removed = original_count - final_count

            if duplicates_removed > 0:
                logger.info(f"🗑️ Eliminados {duplicates_removed} registros duplicados")

            result = df_deduplicated.to_dict(orient="records")

            logger.info(f"✅ Deduplicación completada: {final_count} registros únicos")
            return result, duplicates_removed

        except Exception as e:
            logger.error(f"💥 Error en deduplicación: {e}")
            raise

    def validate_final_output(
        self, data: List[Dict], duplicates_removed: int = 0
    ) -> ReferidosProcessedSchema:
        """
        ✅ Validación final y creación del schema de salida procesado.

        Args:
            data: Lista de diccionarios procesados y deduplicados
            duplicates_removed: Cantidad de duplicados eliminados

        Returns:
            ReferidosProcessedSchema: Schema completo con metadatos
        """
        try:
            logger.info(f"🔄 Validación final de {len(data)} registros...")

            # ⚡ Validación final con Pydantic ultra-eficiente
            validated_schemas = [ReferidosCalcularSchema(**record) for record in data]

            # Crear schema de salida con metadatos
            processed_result = ReferidosProcessedSchema(
                data=validated_schemas,
                total_records=len(validated_schemas),
                duplicates_removed=duplicates_removed,
            )

            logger.info("✅ Validación final completada exitosamente")
            return processed_result

        except Exception as e:
            logger.error(f"💥 Error en validación final: {e}")
            raise

    def get_validation_report(self) -> Dict:
        """
        📊 Obtiene reporte detallado de errores de validación.

        Returns:
            Dict: Reporte con estadísticas y errores detallados
        """
        return {
            "total_errors": len(self.validation_errors),
            "errors": self.validation_errors,
            "error_types": list(
                set(err["error_type"] for err in self.validation_errors)
            ),
        }
