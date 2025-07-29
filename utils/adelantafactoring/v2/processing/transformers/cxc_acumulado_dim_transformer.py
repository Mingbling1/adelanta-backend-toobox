"""
🔄 CXC Acumulado DIM Transformer V2 - Transformaciones de datos complejas
Convierte entre formatos raw, schema y DataFrame para datos acumulados dimensionales
Optimiza performance para grandes volúmenes de datos financieros
"""

import pandas as pd
from typing import List, Dict, Any

# Imports con fallback para desarrollo aislado
try:
    from utils.adelantafactoring.v2.schemas.cxc_acumulado_dim_schema import (
        CXCAcumuladoDIMRawSchema,
        CXCAcumuladoDIMCalcularSchema,
    )
    from config.logger import logger
except ImportError:
    # Fallback para desarrollo aislado
    class _FallbackLogger:
        def debug(self, msg):
            print(f"DEBUG: {msg}")

        def info(self, msg):
            print(f"INFO: {msg}")

        def warning(self, msg):
            print(f"WARNING: {msg}")

        def error(self, msg):
            print(f"ERROR: {msg}")

    logger = _FallbackLogger()

    # Schemas dummy para desarrollo
    class CXCAcumuladoDIMRawSchema:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def model_dump(self):
            return self.__dict__.copy()

    class CXCAcumuladoDIMCalcularSchema:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def model_dump(self):
            return self.__dict__.copy()


class CXCAcumuladoDIMTransformer:
    """Transformador especializado para datos acumulados DIM CXC"""

    def __init__(self):
        self.transformation_count = 0
        self.optimization_stats = {}

    def raw_to_schema_list(
        self, raw_data: List[Dict]
    ) -> List[CXCAcumuladoDIMRawSchema]:
        """
        Convierte datos raw a lista de schemas Pydantic validados.

        Args:
            raw_data: Lista de diccionarios desde webservice

        Returns:
            Lista de schemas Pydantic validados
        """
        if not raw_data:
            logger.warning("Sin datos raw para transformar a schemas acumulado DIM")
            return []

        try:
            validated_schemas = []
            errors = []

            # Procesar en lotes para mejor performance
            batch_size = 1000
            for i in range(0, len(raw_data), batch_size):
                batch = raw_data[i : i + batch_size]

                for j, record in enumerate(batch):
                    try:
                        schema_obj = CXCAcumuladoDIMRawSchema(**record)
                        validated_schemas.append(schema_obj)
                    except Exception as e:
                        errors.append(f"Registro {i+j}: {str(e)}")
                        logger.warning(f"Error validando registro {i+j}: {e}")

                # Log progreso para lotes grandes
                if len(raw_data) > 1000:
                    logger.debug(
                        f"Procesado lote {i//batch_size + 1}: {len(validated_schemas)} válidos"
                    )

            logger.debug(
                f"Schemas acumulado DIM validados: {len(validated_schemas)}/{len(raw_data)}"
            )
            if errors:
                logger.warning(
                    f"Errores de validación: {len(errors)} registros fallidos"
                )

            self.transformation_count += len(validated_schemas)
            return validated_schemas

        except Exception as e:
            logger.error(f"Error en transformación raw_to_schema acumulado DIM: {e}")
            return []

    def raw_to_dataframe_optimized(self, raw_data: List[Dict]) -> pd.DataFrame:
        """
        Convierte datos raw a DataFrame optimizado para grandes volúmenes.

        Args:
            raw_data: Lista de diccionarios desde webservice

        Returns:
            DataFrame optimizado
        """
        if not raw_data:
            logger.warning("Sin datos raw para DataFrame acumulado DIM")
            return pd.DataFrame()

        try:
            # Crear DataFrame base
            df = pd.DataFrame(raw_data)
            logger.debug(
                f"DataFrame base acumulado DIM: {len(df)} filas, {len(df.columns)} columnas"
            )

            # Optimizar tipos de datos específicos para acumulado DIM
            df = self._optimize_acumulado_dim_dtypes(df)

            # Limpiar y normalizar datos financieros
            df = self._clean_financial_fields(df)

            # Procesar fechas
            df = self._process_date_fields(df)

            # Validar integridad básica
            df = self._validate_basic_integrity(df)

            logger.info(f"DataFrame acumulado DIM optimizado: {len(df)} registros")
            self.transformation_count += len(df)

            return df

        except Exception as e:
            logger.error(f"Error creando DataFrame acumulado DIM desde raw data: {e}")
            return pd.DataFrame()

    def dataframe_to_final_schema_list(
        self, df: pd.DataFrame
    ) -> List[CXCAcumuladoDIMCalcularSchema]:
        """
        Convierte DataFrame procesado a lista de schemas finales.

        Args:
            df: DataFrame procesado con ETL aplicado

        Returns:
            Lista de schemas finales validados
        """
        if df.empty:
            return []

        try:
            # Convertir a diccionarios primero
            dict_list = self.dataframe_to_dict_list(df)

            # Validar con schema final
            final_schemas = []
            errors = []

            for i, record in enumerate(dict_list):
                try:
                    schema_obj = CXCAcumuladoDIMCalcularSchema(**record)
                    final_schemas.append(schema_obj)
                except Exception as e:
                    errors.append(f"Registro {i}: {str(e)}")
                    logger.warning(f"Error validando registro final {i}: {e}")

            logger.debug(f"Schemas finales acumulado DIM: {len(final_schemas)}")
            if errors:
                logger.warning(f"Errores en validación final: {len(errors)} registros")

            return final_schemas

        except Exception as e:
            logger.error(f"Error convirtiendo a schemas finales: {e}")
            return []

    def dataframe_to_dict_list(self, df: pd.DataFrame) -> List[Dict]:
        """
        Convierte DataFrame a lista de diccionarios optimizada para acumulado DIM.

        Args:
            df: DataFrame a convertir

        Returns:
            Lista de diccionarios optimizada
        """
        if df.empty:
            return []

        try:
            # Preparar DataFrame para serialización
            df_clean = df.copy()

            # Convertir tipos especiales a formatos serializables
            for col in df_clean.columns:
                if df_clean[col].dtype == "object":
                    # Reemplazar NaN con None para campos string
                    df_clean[col] = df_clean[col].where(pd.notna(df_clean[col]), None)
                elif pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                    # Convertir fechas a string ISO
                    df_clean[col] = (
                        df_clean[col]
                        .dt.strftime("%Y-%m-%d")
                        .where(pd.notna(df_clean[col]), None)
                    )
                elif pd.api.types.is_numeric_dtype(df_clean[col]):
                    # Reemplazar NaN con None para campos numéricos
                    df_clean[col] = df_clean[col].where(pd.notna(df_clean[col]), None)

            # Convertir a diccionarios
            dict_list = df_clean.to_dict(orient="records")
            logger.debug(
                f"DataFrame acumulado DIM convertido a {len(dict_list)} diccionarios"
            )

            return dict_list

        except Exception as e:
            logger.error(
                f"Error convirtiendo DataFrame acumulado DIM a diccionarios: {e}"
            )
            return []

    def _optimize_acumulado_dim_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimiza tipos de datos específicos para acumulado DIM.

        Args:
            df: DataFrame a optimizar

        Returns:
            DataFrame con tipos optimizados
        """
        try:
            df_optimized = df.copy()

            # Optimizar columnas enteras
            integer_columns = ["IdLiquidacionCab", "IdLiquidacionDet", "DiasEfectivo"]
            for col in integer_columns:
                if col in df_optimized.columns:
                    df_optimized[col] = pd.to_numeric(
                        df_optimized[col], errors="coerce", downcast="integer"
                    )

            # Optimizar columnas flotantes (financieras) - mantener precisión
            float_columns = [
                "DeudaAnterior",
                "TasaNominalMensualPorc",
                "FinanciamientoPorc",
                "NetoConfirmado",
                "FondoResguardo",
                "ComisionEstructuracionPorc",
            ]
            for col in float_columns:
                if col in df_optimized.columns:
                    df_optimized[col] = pd.to_numeric(
                        df_optimized[col], errors="coerce"
                    )

            # Optimizar strings categóricos
            categorical_columns = [
                "Moneda",
                "Estado",
                "TipoOperacion",
                "TipoOperacionDetalle",
            ]
            for col in categorical_columns:
                if col in df_optimized.columns and df_optimized[col].dtype == "object":
                    unique_ratio = df_optimized[col].nunique() / len(df_optimized)
                    if unique_ratio < 0.1:  # Menos del 10% de valores únicos
                        df_optimized[col] = df_optimized[col].astype("category")

            # Log estadísticas de optimización
            memory_before = df.memory_usage(deep=True).sum()
            memory_after = df_optimized.memory_usage(deep=True).sum()
            memory_saved = memory_before - memory_after

            self.optimization_stats = {
                "memory_before_mb": memory_before / 1024 / 1024,
                "memory_after_mb": memory_after / 1024 / 1024,
                "memory_saved_mb": memory_saved / 1024 / 1024,
                "optimization_ratio": (
                    memory_saved / memory_before if memory_before > 0 else 0
                ),
            }

            logger.debug(
                f"Tipos optimizados - Memoria ahorrada: {memory_saved/1024/1024:.2f} MB"
            )
            return df_optimized

        except Exception as e:
            logger.warning(
                f"Error optimizando tipos de datos: {e}, usando DataFrame original"
            )
            return df

    def _clean_financial_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia y normaliza campos financieros específicos"""
        try:
            # Campos financieros que requieren limpieza especial
            financial_fields = [
                "DeudaAnterior",
                "NetoConfirmado",
                "FondoResguardo",
                "TasaNominalMensualPorc",
                "FinanciamientoPorc",
                "ComisionEstructuracionPorc",
            ]

            for field in financial_fields:
                if field in df.columns:
                    # Convertir a numérico y rellenar NaN con 0
                    df[field] = pd.to_numeric(df[field], errors="coerce").fillna(0)

            return df

        except Exception as e:
            logger.warning(f"Error limpiando campos financieros: {e}")
            return df

    def _process_date_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Procesa campos de fecha específicos"""
        try:
            date_fields = [
                "FechaConfirmado",
                "FechaOperacion",
                "FechaInteresConfirming",
            ]

            for field in date_fields:
                if field in df.columns:
                    df[field] = pd.to_datetime(df[field], errors="coerce")

            return df

        except Exception as e:
            logger.warning(f"Error procesando campos de fecha: {e}")
            return df

    def _validate_basic_integrity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida integridad básica de datos críticos"""
        try:
            # Verificar que campos críticos no estén completamente vacíos
            critical_fields = ["CodigoLiquidacion", "RUCCliente", "RUCPagador"]

            for field in critical_fields:
                if field in df.columns:
                    empty_count = df[field].isna().sum()
                    if empty_count > 0:
                        logger.warning(
                            f"Campo crítico '{field}' tiene {empty_count} valores vacíos"
                        )

            return df

        except Exception as e:
            logger.warning(f"Error validando integridad básica: {e}")
            return df

    def get_transformation_stats(self) -> Dict[str, Any]:
        """
        Retorna estadísticas de transformaciones realizadas.

        Returns:
            Diccionario con estadísticas completas
        """
        return {
            "total_transformations": self.transformation_count,
            "transformer_type": "CXCAcumuladoDIMTransformer",
            "optimization_stats": self.optimization_stats,
            "supported_formats": ["raw_dict", "pydantic_schema", "pandas_dataframe"],
            "specializations": ["financial_data", "dimensional_data", "etl_processing"],
        }
