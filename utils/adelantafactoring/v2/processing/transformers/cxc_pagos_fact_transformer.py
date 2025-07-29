"""
🔄 Transformador para CXC Pagos Fact - Conversiones entre formatos de datos
"""

try:
    from utils.adelantafactoring.v2.schemas.cxc_pagos_fact_schema import (
        CXCPagosFactSchema,
    )
    from utils.adelantafactoring.v2.config.settings import settings
except ImportError:
    # Fallback para desarrollo aislado
    class _FallbackSettings:
        WEBSERVICE_BASE_URL = "https://webservice.adelantafactoring.com"

    settings = _FallbackSettings()

    # Fallback para el schema
    try:
        from utils.adelantafactoring.schemas.CXCPagosFactCalcularSchema import (
            CXCPagosFactCalcularSchema as CXCPagosFactSchema,
        )
    except ImportError:
        CXCPagosFactSchema = None

import pandas as pd
from typing import List, Dict, Any
from config.logger import logger


class CXCPagosFactTransformer:
    """Transformador para datos de pagos de facturas CXC"""

    def __init__(self):
        self.logger = logger
        self.schema = CXCPagosFactSchema

    def transform_raw_to_schema(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Transforma datos raw a formato de schema validado

        Args:
            raw_data: Lista de diccionarios con datos raw

        Returns:
            Lista de diccionarios validados con schema
        """
        if not raw_data:
            self.logger.warning("No hay datos raw para transformar")
            return []

        if not self.schema:
            self.logger.warning("Schema no disponible, devolviendo datos raw")
            return raw_data

        try:
            # Usar list comprehension optimizada con Pydantic
            validated_data = [self.schema(**record).model_dump() for record in raw_data]

            self.logger.debug(
                f"Datos transformados exitosamente: {len(validated_data)} registros"
            )
            return validated_data

        except Exception as e:
            self.logger.error(f"Error transformando datos raw a schema: {e}")
            # Fallback: devolver datos raw si la validación falla
            return raw_data

    def transform_schema_to_dataframe(self, schema_data: List[Dict]) -> pd.DataFrame:
        """
        Convierte datos validados por schema a DataFrame

        Args:
            schema_data: Lista de diccionarios validados

        Returns:
            DataFrame con datos estructurados
        """
        if not schema_data:
            self.logger.warning("No hay datos de schema para convertir a DataFrame")
            return pd.DataFrame()

        try:
            df = pd.DataFrame(schema_data)

            # Optimizaciones específicas para pagos de facturas
            df = self._optimize_dataframe_types(df)

            self.logger.debug(f"DataFrame creado exitosamente: {len(df)} filas")
            return df

        except Exception as e:
            self.logger.error(f"Error convirtiendo schema a DataFrame: {e}")
            return pd.DataFrame()

    def raw_to_dataframe_optimized(self, raw_data: List[Dict]) -> pd.DataFrame:
        """
        Convierte datos raw directamente a DataFrame optimizado

        Args:
            raw_data: Lista de diccionarios con datos raw

        Returns:
            DataFrame optimizado
        """
        if not raw_data:
            self.logger.warning("No hay datos raw para convertir a DataFrame")
            return pd.DataFrame()

        try:
            # Crear DataFrame directamente
            df = pd.DataFrame(raw_data)

            # Optimizar tipos de datos
            df = self._optimize_dataframe_types(df)

            self.logger.debug(f"DataFrame optimizado creado: {len(df)} filas")
            return df

        except Exception as e:
            self.logger.error(f"Error convirtiendo raw data a DataFrame: {e}")
            return pd.DataFrame()

    def dataframe_to_dict_list(self, df: pd.DataFrame) -> List[Dict]:
        """
        Convierte DataFrame a lista de diccionarios (alias para compatibilidad)

        Args:
            df: DataFrame con datos procesados

        Returns:
            Lista de diccionarios para API/export
        """
        return self.transform_dataframe_to_list_dict(df)

    def transform_dataframe_to_list_dict(self, df: pd.DataFrame) -> List[Dict]:
        """
        Convierte DataFrame a lista de diccionarios

        Args:
            df: DataFrame con datos procesados

        Returns:
            Lista de diccionarios para API/export
        """
        if df.empty:
            return []

        try:
            # Convertir usando orient='records' para mejor performance
            result = df.to_dict("records")

            # Limpiar valores NaN y normalizar tipos
            result = self._clean_dict_data(result)

            self.logger.debug(f"DataFrame convertido a lista: {len(result)} registros")
            return result

        except Exception as e:
            self.logger.error(f"Error convirtiendo DataFrame a lista: {e}")
            return []

    def _optimize_dataframe_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimiza tipos de datos del DataFrame para mejor performance

        Args:
            df: DataFrame original

        Returns:
            DataFrame con tipos optimizados
        """
        try:
            # Campos enteros
            integer_fields = ["IdLiquidacionPago", "IdLiquidacionDet", "DiasMora"]
            for field in integer_fields:
                if field in df.columns:
                    df[field] = (
                        pd.to_numeric(df[field], errors="coerce")
                        .fillna(0)
                        .astype("int32")
                    )

            # Campos float/decimal
            float_fields = [
                "MontoCobrarPago",
                "MontoPago",
                "InteresPago",
                "GastosPago",
                "SaldoDeuda",
                "ExcesoPago",
            ]
            for field in float_fields:
                if field in df.columns:
                    df[field] = (
                        pd.to_numeric(df[field], errors="coerce")
                        .fillna(0.0)
                        .astype("float64")
                    )

            # Campos de texto como categorías para optimizar memoria
            string_fields = ["TipoPago", "ObservacionPago"]
            for field in string_fields:
                if field in df.columns:
                    df[field] = df[field].astype(str).astype("category")

            # Campos de fecha
            date_fields = ["FechaPago", "FechaPagoCreacion", "FechaPagoModificacion"]
            for field in date_fields:
                if field in df.columns:
                    df[field] = pd.to_datetime(df[field], errors="coerce")

            return df

        except Exception as e:
            self.logger.error(f"Error optimizando tipos de DataFrame: {e}")
            return df

    def _clean_dict_data(self, data: List[Dict]) -> List[Dict]:
        """
        Limpia datos de diccionarios eliminando NaN y normalizando tipos

        Args:
            data: Lista de diccionarios con posibles valores NaN

        Returns:
            Lista de diccionarios limpios
        """
        try:
            cleaned_data = []

            for record in data:
                cleaned_record = {}

                for key, value in record.items():
                    # Manejar valores NaN de pandas
                    if pd.isna(value):
                        if key in ["ObservacionPago"]:
                            cleaned_record[key] = ""
                        elif key in [
                            "MontoCobrarPago",
                            "MontoPago",
                            "InteresPago",
                            "GastosPago",
                            "SaldoDeuda",
                            "ExcesoPago",
                        ]:
                            cleaned_record[key] = 0.0
                        elif key in [
                            "IdLiquidacionPago",
                            "IdLiquidacionDet",
                            "DiasMora",
                        ]:
                            cleaned_record[key] = 0
                        else:
                            cleaned_record[key] = None
                    else:
                        # Convertir tipos específicos
                        if key in ["IdLiquidacionPago", "IdLiquidacionDet", "DiasMora"]:
                            cleaned_record[key] = int(value) if value is not None else 0
                        elif key in [
                            "MontoCobrarPago",
                            "MontoPago",
                            "InteresPago",
                            "GastosPago",
                            "SaldoDeuda",
                            "ExcesoPago",
                        ]:
                            cleaned_record[key] = (
                                float(value) if value is not None else 0.0
                            )
                        elif key in [
                            "FechaPago",
                            "FechaPagoCreacion",
                            "FechaPagoModificacion",
                        ]:
                            # Convertir timestamps a string ISO si es necesario
                            if pd.notna(value):
                                if hasattr(value, "isoformat"):
                                    cleaned_record[key] = value.isoformat()
                                else:
                                    cleaned_record[key] = str(value)
                            else:
                                cleaned_record[key] = None
                        else:
                            cleaned_record[key] = value

                cleaned_data.append(cleaned_record)

            return cleaned_data

        except Exception as e:
            self.logger.error(f"Error limpiando datos de diccionarios: {e}")
            return data

    def aggregate_by_liquidacion(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrega datos de pagos por liquidación

        Args:
            df: DataFrame con datos de pagos individuales

        Returns:
            DataFrame agregado por liquidación
        """
        if df.empty or "IdLiquidacionDet" not in df.columns:
            return df

        try:
            # Campos para sumar
            sum_fields = [
                "MontoPago",
                "InteresPago",
                "GastosPago",
                "SaldoDeuda",
                "ExcesoPago",
            ]

            # Campos para promediar
            avg_fields = ["DiasMora"]

            # Campos para tomar el último valor
            last_fields = ["TipoPago", "ObservacionPago", "FechaPago"]

            # Crear agregaciones
            agg_dict = {}

            for field in sum_fields:
                if field in df.columns:
                    agg_dict[field] = "sum"

            for field in avg_fields:
                if field in df.columns:
                    agg_dict[field] = "mean"

            for field in last_fields:
                if field in df.columns:
                    agg_dict[field] = "last"

            # Realizar agregación
            df_agg = df.groupby("IdLiquidacionDet").agg(agg_dict).reset_index()

            self.logger.debug(
                f"Agregación por liquidación completada: {len(df_agg)} registros"
            )
            return df_agg

        except Exception as e:
            self.logger.error(f"Error agregando datos por liquidación: {e}")
            return df
