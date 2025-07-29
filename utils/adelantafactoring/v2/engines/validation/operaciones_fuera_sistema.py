"""
🔍 Validation Engine V2 - Operaciones Fuera Sistema

Engine especializado para validación y transformación de datos de operaciones fuera del sistema
con validadores financieros optimizados y manejo robusto de errores.
"""

import pandas as pd
from typing import List, Dict, Any, Tuple
from datetime import datetime

from config import logger
from ...schemas.operaciones_fuera_sistema_schema import (
    OperacionesFueraSistemaCalcularSchema,
    OperacionesFueraSistemaProcessedSchema,
)


class OperacionesFueraSistemaValidationEngine:
    """
    ✅ Engine de validación para operaciones fuera del sistema.

    Aplica validaciones Pydantic con optimizaciones para grandes volúmenes de datos.
    """

    def __init__(self):
        self.schema_fields = set(
            OperacionesFueraSistemaCalcularSchema.model_fields.keys()
        )

    def validate_and_transform_batch(
        self, df: pd.DataFrame
    ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        """
        🔄 Valida y transforma lote de registros con Pydantic optimizado.

        Args:
            df: DataFrame con datos RAW

        Returns:
            Tuple (validated_records, stats) con datos validados y estadísticas
        """
        try:
            logger.info(f"🔍 Iniciando validación de {len(df)} registros")

            if df.empty:
                logger.warning("⚠️ DataFrame vacío para validación")
                return [], {"total": 0, "valid": 0, "invalid": 0, "pen": 0, "usd": 0}

            # Filtrar solo columnas del schema
            available_columns = df.columns.intersection(self.schema_fields)
            df_filtered = df[available_columns].copy()

            logger.info(
                f"📊 Columnas disponibles para validación: {len(available_columns)}"
            )

            # Convertir a diccionarios para validación
            records = df_filtered.to_dict(orient="records")

            validated_records = []
            invalid_count = 0
            pen_count = 0
            usd_count = 0

            for i, record in enumerate(records):
                try:
                    # Validar con Pydantic
                    validated_record = OperacionesFueraSistemaCalcularSchema(**record)
                    validated_dict = validated_record.model_dump()

                    validated_records.append(validated_dict)

                    # Contar por moneda
                    moneda = validated_dict.get("Moneda", "").upper()
                    if moneda == "PEN":
                        pen_count += 1
                    elif moneda == "USD":
                        usd_count += 1

                except Exception as validation_error:
                    invalid_count += 1
                    logger.debug(
                        f"❌ Error validación registro {i}: {str(validation_error)}"
                    )
                    continue

            # Estadísticas
            stats = {
                "total": len(records),
                "valid": len(validated_records),
                "invalid": invalid_count,
                "pen": pen_count,
                "usd": usd_count,
            }

            logger.info(
                f"✅ Validación completada: {stats['valid']}/{stats['total']} válidos"
            )
            logger.info(f"📊 Distribución: PEN={pen_count}, USD={usd_count}")

            return validated_records, stats

        except Exception as e:
            logger.error(f"❌ Error en validación batch: {str(e)}")
            return [], {"total": 0, "valid": 0, "invalid": 0, "pen": 0, "usd": 0}

    def apply_business_rules(
        self, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        📋 Aplica reglas de negocio específicas para operaciones fuera del sistema.

        Args:
            records: Lista de registros validados

        Returns:
            Lista de registros con reglas de negocio aplicadas
        """
        try:
            logger.info(f"📋 Aplicando reglas de negocio a {len(records)} registros")

            processed_records = []

            for record in records:
                # Regla 1: Normalizar códigos de liquidación
                if record.get("CodigoLiquidacion"):
                    record["CodigoLiquidacion"] = (
                        str(record["CodigoLiquidacion"]).strip().upper()
                    )

                # Regla 2: Validar coherencia de montos
                self._validate_financial_consistency(record)

                # Regla 3: Normalizar ejecutivos
                if record.get("Ejecutivo"):
                    record["Ejecutivo"] = record["Ejecutivo"].strip().title()

                # Regla 4: Validar días efectivos
                if record.get("DiasEfectivo", 0) < 0:
                    record["DiasEfectivo"] = 0

                processed_records.append(record)

            logger.info(
                f"✅ Reglas de negocio aplicadas a {len(processed_records)} registros"
            )

            return processed_records

        except Exception as e:
            logger.error(f"❌ Error aplicando reglas de negocio: {str(e)}")
            return records

    def _validate_financial_consistency(self, record: Dict[str, Any]) -> None:
        """
        💰 Valida consistencia entre campos financieros.

        Args:
            record: Registro a validar (se modifica in-place)
        """
        try:
            # Validar que MontoCobrar no sea negativo
            if record.get("MontoCobrar", 0) < 0:
                logger.warning(
                    f"⚠️ MontoCobrar negativo corregido: {record.get('CodigoLiquidacion')}"
                )
                record["MontoCobrar"] = 0

            # Validar coherencia IGV
            comision_sin_igv = record.get("MontoComisionEstructuracion", 0)
            igv_comision = record.get("ComisionEstructuracionIGV", 0)
            comision_con_igv = record.get("ComisionEstructuracionConIGV", 0)

            if comision_sin_igv > 0 and igv_comision > 0:
                expected_total = comision_sin_igv + igv_comision
                if (
                    abs(comision_con_igv - expected_total) > 0.01
                ):  # Tolerancia de 1 centavo
                    logger.debug(
                        f"⚠️ Inconsistencia IGV detectada: {record.get('CodigoLiquidacion')}"
                    )

        except Exception as e:
            logger.debug(f"❌ Error validando consistencia financiera: {str(e)}")

    def create_processed_response(
        self, records: List[Dict[str, Any]], stats: Dict[str, int]
    ) -> OperacionesFueraSistemaProcessedSchema:
        """
        📦 Crea respuesta estructurada con datos procesados y metadatos.

        Args:
            records: Lista de registros validados
            stats: Estadísticas del procesamiento

        Returns:
            Schema con datos procesados y metadatos
        """
        try:
            # Convertir records a schemas
            validated_schemas = []
            for record in records:
                try:
                    schema = OperacionesFueraSistemaCalcularSchema(**record)
                    validated_schemas.append(schema)
                except Exception as e:
                    logger.debug(f"❌ Error creando schema final: {str(e)}")
                    continue

            # Crear respuesta procesada
            processed_response = OperacionesFueraSistemaProcessedSchema(
                data=validated_schemas,
                total_records=stats.get("total", 0),
                pen_records=stats.get("pen", 0),
                usd_records=stats.get("usd", 0),
                filtered_records=stats.get("invalid", 0),
                processing_timestamp=datetime.now(),
            )

            logger.info(
                f"📦 Respuesta procesada creada: {len(validated_schemas)} registros"
            )

            return processed_response

        except Exception as e:
            logger.error(f"❌ Error creando respuesta procesada: {str(e)}")
            # Respuesta vacía en caso de error
            return OperacionesFueraSistemaProcessedSchema(
                data=[],
                total_records=0,
                pen_records=0,
                usd_records=0,
                filtered_records=0,
                processing_timestamp=datetime.now(),
            )

    def process_dataframe(
        self, df: pd.DataFrame
    ) -> OperacionesFueraSistemaProcessedSchema:
        """
        🎯 Método principal para procesar DataFrame completo.

        Args:
            df: DataFrame con datos RAW

        Returns:
            Schema con datos completamente procesados
        """
        try:
            logger.info(f"🎯 Procesando DataFrame con {len(df)} registros")

            # Paso 1: Validación y transformación
            validated_records, stats = self.validate_and_transform_batch(df)

            # Paso 2: Aplicar reglas de negocio
            business_records = self.apply_business_rules(validated_records)

            # Paso 3: Crear respuesta estructurada
            response = self.create_processed_response(business_records, stats)

            logger.info(
                f"✅ Procesamiento completado: {len(response.data)} registros finales"
            )

            return response

        except Exception as e:
            logger.error(f"❌ Error en process_dataframe: {str(e)}")
            return OperacionesFueraSistemaProcessedSchema(
                data=[],
                total_records=0,
                pen_records=0,
                usd_records=0,
                filtered_records=0,
                processing_timestamp=datetime.now(),
            )
