"""
🚀 API Layer V2 - Operaciones Fuera Sistema

API unificada para operaciones fuera del sistema con arquitectura hexagonal pura.
Sin dependencias legacy - 100% aislada de v1.
"""

import asyncio
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime

from config import logger
from ..engines.data.operaciones_fuera_sistema import OperacionesFueraSistemaDataEngine
from ..engines.validation.operaciones_fuera_sistema import (
    OperacionesFueraSistemaValidationEngine,
)
from ..processing.transformers.operaciones_fuera_sistema_transformer import (
    OperacionesFueraSistemaTransformer,
)
from ..schemas.operaciones_fuera_sistema_schema import OperacionesFueraSistemaProcessedSchema
from ..core.base import BaseCalcularV2


class OperacionesFueraSistemaAPI(BaseCalcularV2):
    """
    🎯 API Principal V2 para Operaciones Fuera del Sistema.

    Implementación hexagonal pura sin dependencias legacy.
    Arquitectura moderna con async/await y validación optimizada.
    """

    def __init__(self):
        super().__init__()

        # Engines hexagonales
        self.data_engine = OperacionesFueraSistemaDataEngine()
        self.validation_engine = OperacionesFueraSistemaValidationEngine()
        self.transformer = OperacionesFueraSistemaTransformer()

        # Metadatos
        self._processing_stats = {}
        self._last_execution = None

    @BaseCalcularV2.timeit
    async def calcular(self) -> List[Dict[str, Any]]:
        """
        🎯 Método principal compatible con sistema legacy.
        Mantiene la misma interfaz que el OperacionesFueraSistemaCalcular original.

        Returns:
            Lista de diccionarios con datos de operaciones fuera del sistema
        """
        try:
            logger.info("🚀 Iniciando cálculo de operaciones fuera del sistema V2")

            # Obtener datos procesados
            processed_data = await self.get_processed_data()

            # Convertir a formato legacy para compatibilidad
            legacy_data = self.transformer.to_legacy_format(processed_data)

            # Actualizar estadísticas
            self._processing_stats = {
                "total_records": len(legacy_data),
                "pen_records": processed_data.pen_records,
                "usd_records": processed_data.usd_records,
                "processing_time": datetime.now(),
                "success": True,
            }

            self._last_execution = datetime.now()

            logger.info(f"✅ Cálculo completado: {len(legacy_data)} registros")

            return legacy_data

        except Exception as e:
            logger.error(f"❌ Error en calcular: {str(e)}")
            self._processing_stats = {
                "error": str(e),
                "processing_time": datetime.now(),
                "success": False,
            }
            return []

    async def get_processed_data(self) -> OperacionesFueraSistemaProcessedSchema:
        """
        📊 Obtiene datos completamente procesados con validación y transformación.

        Returns:
            Schema con datos procesados y metadatos
        """
        try:
            logger.info(
                "📊 Obteniendo datos procesados de operaciones fuera del sistema"
            )

            # Paso 1: Extraer datos desde webservices
            raw_dataframe = await self.data_engine.get_processed_data()

            if raw_dataframe.empty:
                logger.warning("⚠️ No se obtuvieron datos desde webservices")
                return OperacionesFueraSistemaProcessedSchema(
                    data=[],
                    total_records=0,
                    pen_records=0,
                    usd_records=0,
                    filtered_records=0,
                    processing_timestamp=datetime.now(),
                )

            # Paso 2: Validar y transformar datos
            processed_data = self.validation_engine.process_dataframe(raw_dataframe)

            logger.info(
                f"✅ Datos procesados obtenidos: {len(processed_data.data)} registros"
            )

            return processed_data

        except Exception as e:
            logger.error(f"❌ Error obteniendo datos procesados: {str(e)}")
            return OperacionesFueraSistemaProcessedSchema(
                data=[],
                total_records=0,
                pen_records=0,
                usd_records=0,
                filtered_records=0,
                processing_timestamp=datetime.now(),
            )

    async def get_data_by_currency(self, currency: str) -> List[Dict[str, Any]]:
        """
        💱 Obtiene datos filtrados por moneda específica.

        Args:
            currency: "PEN" o "USD"

        Returns:
            Lista de registros filtrados por moneda
        """
        try:
            logger.info(f"💱 Obteniendo datos para moneda: {currency}")

            # Obtener todos los datos procesados
            processed_data = await self.get_processed_data()

            # Filtrar por moneda
            filtered_data = self.transformer.filter_by_currency(
                processed_data, currency
            )

            # Convertir a formato legacy
            legacy_data = self.transformer.to_legacy_format(filtered_data)

            logger.info(
                f"✅ Datos obtenidos para {currency}: {len(legacy_data)} registros"
            )

            return legacy_data

        except Exception as e:
            logger.error(f"❌ Error obteniendo datos por moneda {currency}: {str(e)}")
            return []

    def get_dataframe(self) -> pd.DataFrame:
        """
        📊 Obtiene datos como DataFrame para análisis (método síncrono).

        Returns:
            DataFrame con datos de operaciones fuera del sistema
        """
        try:
            logger.info("📊 Obteniendo DataFrame de operaciones fuera del sistema")

            # Ejecutar método async de forma síncrona
            processed_data = asyncio.run(self.get_processed_data())

            # Convertir a DataFrame
            df = self.transformer.to_dataframe(processed_data)

            logger.info(f"✅ DataFrame obtenido: {len(df)} filas")

            return df

        except Exception as e:
            logger.error(f"❌ Error obteniendo DataFrame: {str(e)}")
            return pd.DataFrame()

    def get_excel_dataframe(self) -> pd.DataFrame:
        """
        📋 Obtiene DataFrame formateado para exportación a Excel.

        Returns:
            DataFrame con formato amigable para Excel
        """
        try:
            logger.info("📋 Obteniendo DataFrame para Excel")

            # Obtener datos procesados
            processed_data = asyncio.run(self.get_processed_data())

            # Convertir a formato Excel
            df_excel = self.transformer.to_excel_format(processed_data)

            logger.info(f"✅ DataFrame Excel obtenido: {len(df_excel)} filas")

            return df_excel

        except Exception as e:
            logger.error(f"❌ Error obteniendo DataFrame Excel: {str(e)}")
            return pd.DataFrame()

    def get_summary_statistics(self) -> Dict[str, Any]:
        """
        📈 Obtiene estadísticas resumidas de los datos.

        Returns:
            Diccionario con estadísticas y metadatos
        """
        try:
            logger.info("📈 Generando estadísticas resumidas")

            # Obtener datos procesados
            processed_data = asyncio.run(self.get_processed_data())

            # Generar estadísticas
            stats = self.transformer.get_summary_stats(processed_data)

            # Agregar metadatos de procesamiento
            stats.update(
                {
                    "last_execution": (
                        self._last_execution.isoformat()
                        if self._last_execution
                        else None
                    ),
                    "processing_stats": self._processing_stats,
                    "version": "v2.0",
                    "engine": "hexagonal",
                }
            )

            logger.info("✅ Estadísticas generadas")

            return stats

        except Exception as e:
            logger.error(f"❌ Error generando estadísticas: {str(e)}")
            return {"error": str(e), "timestamp": datetime.now().isoformat()}

    # Métodos de compatibilidad legacy
    def obtener_datos_json(self) -> List[Dict[str, Any]]:
        """
        🔄 Método legacy síncrono para compatibilidad.

        Returns:
            Lista de diccionarios con datos
        """
        return asyncio.run(self.calcular())

    def obtener_dataframe(self) -> pd.DataFrame:
        """
        🔄 Método legacy para obtener DataFrame.

        Returns:
            DataFrame con datos
        """
        return self.get_dataframe()


# Instancia global para compatibilidad legacy
operaciones_fuera_sistema_calcular = OperacionesFueraSistemaAPI()
