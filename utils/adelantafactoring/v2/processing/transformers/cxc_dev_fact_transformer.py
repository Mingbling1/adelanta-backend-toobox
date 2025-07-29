"""
🔄 CXC Dev Fact Transformer V2 - Transformaciones de datos
Convierte entre formatos raw, schema y DataFrame optimizando memoria y performance
"""

import pandas as pd
from typing import List, Dict, Any

# Imports con fallback para desarrollo aislado
try:
    from utils.adelantafactoring.v2.schemas.cxc_dev_fact_schema import (
        CXCDevFactBaseSchema,
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

    # Schema dummy para desarrollo
    class CXCDevFactBaseSchema:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def model_dump(self):
            return self.__dict__.copy()


class CXCDevFactTransformer:
    """Transformador especializado para datos de devoluciones de facturas CXC"""

    def __init__(self):
        self.transformation_count = 0

    def raw_to_schema_list(self, raw_data: List[Dict]) -> List[CXCDevFactBaseSchema]:
        """
        Convierte datos raw a lista de schemas Pydantic validados.

        Args:
            raw_data: Lista de diccionarios desde webservice

        Returns:
            Lista de schemas Pydantic validados
        """
        if not raw_data:
            logger.warning("Sin datos raw para transformar a schemas")
            return []

        try:
            validated_schemas = []
            errors = []

            for i, record in enumerate(raw_data):
                try:
                    schema_obj = CXCDevFactBaseSchema(**record)
                    validated_schemas.append(schema_obj)
                except Exception as e:
                    errors.append(f"Registro {i}: {str(e)}")
                    logger.warning(f"Error validando registro {i}: {e}")

            logger.debug(f"Schemas validados: {len(validated_schemas)}/{len(raw_data)}")
            if errors:
                logger.warning(
                    f"Errores de validación: {len(errors)} registros fallidos"
                )

            self.transformation_count += len(validated_schemas)
            return validated_schemas

        except Exception as e:
            logger.error(f"Error en transformación raw_to_schema: {e}")
            return []

    def schema_list_to_dict_list(
        self, schema_list: List[CXCDevFactBaseSchema]
    ) -> List[Dict]:
        """
        Convierte lista de schemas a lista de diccionarios.

        Args:
            schema_list: Lista de schemas Pydantic

        Returns:
            Lista de diccionarios
        """
        if not schema_list:
            return []

        try:
            dict_list = [schema.model_dump() for schema in schema_list]
            logger.debug(f"Schemas convertidos a diccionarios: {len(dict_list)}")
            return dict_list

        except Exception as e:
            logger.error(f"Error convirtiendo schemas a diccionarios: {e}")
            return []

    def raw_to_dataframe_optimized(self, raw_data: List[Dict]) -> pd.DataFrame:
        """
        Convierte datos raw directamente a DataFrame optimizado (alias para compatibilidad).

        Args:
            raw_data: Lista de diccionarios desde webservice

        Returns:
            DataFrame optimizado
        """
        return self.raw_to_dataframe(raw_data)

    def raw_to_dataframe(self, raw_data: List[Dict]) -> pd.DataFrame:
        """
        Convierte datos raw directamente a DataFrame optimizado.

        Args:
            raw_data: Lista de diccionarios desde webservice

        Returns:
            DataFrame optimizado
        """
        if not raw_data:
            logger.warning("Sin datos raw para DataFrame")
            return pd.DataFrame()

        try:
            # Crear DataFrame base
            df = pd.DataFrame(raw_data)
            logger.debug(
                f"DataFrame base creado: {len(df)} filas, {len(df.columns)} columnas"
            )

            # Optimizar tipos de datos
            df = self._optimize_dataframe_dtypes(df)

            # Limpiar datos NaN específicamente para campos financieros
            if "MontoDevolucion" in df.columns:
                df["MontoDevolucion"] = pd.to_numeric(
                    df["MontoDevolucion"], errors="coerce"
                ).fillna(0)

            if "DescuentoDevolucion" in df.columns:
                df["DescuentoDevolucion"] = pd.to_numeric(
                    df["DescuentoDevolucion"], errors="coerce"
                ).fillna(0)

            # Manejar fechas
            if "FechaDesembolso" in df.columns:
                df["FechaDesembolso"] = pd.to_datetime(
                    df["FechaDesembolso"], errors="coerce"
                )

            logger.info(f"DataFrame optimizado creado: {len(df)} registros")
            self.transformation_count += len(df)

            return df

        except Exception as e:
            logger.error(f"Error creando DataFrame desde raw data: {e}")
            return pd.DataFrame()

    def schema_list_to_dataframe(
        self, schema_list: List[CXCDevFactBaseSchema]
    ) -> pd.DataFrame:
        """
        Convierte lista de schemas a DataFrame optimizado.

        Args:
            schema_list: Lista de schemas Pydantic validados

        Returns:
            DataFrame optimizado
        """
        if not schema_list:
            return pd.DataFrame()

        try:
            # Convertir a lista de diccionarios primero
            dict_list = self.schema_list_to_dict_list(schema_list)

            # Crear DataFrame optimizado
            df = pd.DataFrame(dict_list)
            df = self._optimize_dataframe_dtypes(df)

            logger.debug(f"DataFrame desde schemas: {len(df)} registros")
            return df

        except Exception as e:
            logger.error(f"Error creando DataFrame desde schemas: {e}")
            return pd.DataFrame()

    def dataframe_to_dict_list(self, df: pd.DataFrame) -> List[Dict]:
        """
        Convierte DataFrame a lista de diccionarios optimizada.

        Args:
            df: DataFrame a convertir

        Returns:
            Lista de diccionarios
        """
        if df.empty:
            return []

        try:
            # Convertir NaN y NaT a None para JSON serialization
            df_clean = df.copy()

            # Reemplazar NaN con None
            df_clean = df_clean.where(pd.notna(df_clean), None)

            # Convertir a diccionarios
            dict_list = df_clean.to_dict(orient="records")
            logger.debug(f"DataFrame convertido a {len(dict_list)} diccionarios")

            return dict_list

        except Exception as e:
            logger.error(f"Error convirtiendo DataFrame a diccionarios: {e}")
            return []

    def _optimize_dataframe_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimiza tipos de datos del DataFrame para mejor performance y memoria.

        Args:
            df: DataFrame a optimizar

        Returns:
            DataFrame con tipos optimizados
        """
        try:
            df_optimized = df.copy()

            # Optimizar columnas enteras
            integer_columns = [
                "IdLiquidacionDevolucion",
                "IdLiquidacionDet",
                "EstadoDevolucion",
            ]
            for col in integer_columns:
                if col in df_optimized.columns:
                    df_optimized[col] = pd.to_numeric(
                        df_optimized[col], errors="coerce", downcast="integer"
                    )

            # Optimizar columnas flotantes (financieras)
            float_columns = ["MontoDevolucion", "DescuentoDevolucion"]
            for col in float_columns:
                if col in df_optimized.columns:
                    df_optimized[col] = pd.to_numeric(
                        df_optimized[col], errors="coerce"
                    )

            # Optimizar strings (category para campos con valores repetidos)
            string_columns = ["ObservacionDevolucion"]
            for col in string_columns:
                if col in df_optimized.columns and df_optimized[col].dtype == "object":
                    # Solo convertir a category si hay muchos duplicados
                    unique_ratio = df_optimized[col].nunique() / len(df_optimized)
                    if unique_ratio < 0.5:  # Menos del 50% de valores únicos
                        df_optimized[col] = df_optimized[col].astype("category")

            logger.debug("Tipos de datos optimizados en DataFrame")
            return df_optimized

        except Exception as e:
            logger.warning(
                f"Error optimizando tipos de datos: {e}, usando DataFrame original"
            )
            return df

    def get_transformation_stats(self) -> Dict[str, Any]:
        """
        Retorna estadísticas de transformaciones realizadas.

        Returns:
            Diccionario con estadísticas
        """
        return {
            "total_transformations": self.transformation_count,
            "transformer_type": "CXCDevFactTransformer",
            "supported_formats": ["raw_dict", "pydantic_schema", "pandas_dataframe"],
        }
