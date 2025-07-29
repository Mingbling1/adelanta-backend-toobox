"""
🔄 CXC ETL Processor Transformer V2 - Transformaciones especializadas para ETL completo
Maneja transformaciones y optimizaciones de datos para el pipeline completo de CXC
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any
import time
from datetime import datetime

try:
    from utils.adelantafactoring.v2.processing.transformers.cxc_acumulado_dim_transformer import (
        CXCAcumuladoDIMTransformer,
    )
    from utils.adelantafactoring.v2.processing.transformers.cxc_pagos_fact_transformer import (
        CXCPagosFactTransformer,
    )
    from utils.adelantafactoring.v2.processing.transformers.cxc_dev_fact_transformer import (
        CXCDevFactTransformer,
    )
except ImportError:
    # Fallback para desarrollo aislado
    class CXCAcumuladoDIMTransformer:
        def raw_to_dataframe_optimized(self, data):
            return pd.DataFrame(data) if data else pd.DataFrame()

        def dataframe_to_dict_list(self, df):
            return df.to_dict("records") if not df.empty else []

        def get_transformation_stats(self):
            return {"transformer_type": "CXCAcumuladoDIMTransformer"}

    class CXCPagosFactTransformer:
        def raw_to_dataframe_optimized(self, data):
            return pd.DataFrame(data) if data else pd.DataFrame()

        def dataframe_to_dict_list(self, df):
            return df.to_dict("records") if not df.empty else []

        def get_transformation_stats(self):
            return {"transformer_type": "CXCPagosFactTransformer"}

    class CXCDevFactTransformer:
        def raw_to_dataframe_optimized(self, data):
            return pd.DataFrame(data) if data else pd.DataFrame()

        def dataframe_to_dict_list(self, df):
            return df.to_dict("records") if not df.empty else []

        def get_transformation_stats(self):
            return {"transformer_type": "CXCDevFactTransformer"}


class CXCETLProcessorTransformer:
    """Transformador especializado para ETL completo de CXC"""

    def __init__(self):
        # Inicializar transformadores especializados
        self.acumulado_transformer = CXCAcumuladoDIMTransformer()
        self.pagos_transformer = CXCPagosFactTransformer()
        self.dev_transformer = CXCDevFactTransformer()

        # Configuración de transformación
        self.memory_optimization = True
        self.batch_size = 10000
        self.preserve_precision = True

        # Estadísticas de transformación
        self.transformation_stats = {
            "transformations_count": 0,
            "total_records_transformed": 0,
            "total_time_seconds": 0.0,
            "memory_peak_mb": 0.0,
            "errors_count": 0,
        }

    def transform_raw_data_to_dataframes(
        self, acumulado_raw: List[Dict], pagos_raw: List[Dict], dev_raw: List[Dict]
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Transforma datos raw a DataFrames optimizados usando transformadores especializados

        Returns:
            Tuple[df_acumulado, df_pagos, df_dev]
        """
        try:
            start_time = time.time()
            self.transformation_stats["transformations_count"] += 1

            # Transformar cada conjunto de datos usando su transformador especializado
            df_acumulado = self.acumulado_transformer.raw_to_dataframe_optimized(
                acumulado_raw
            )
            df_pagos = self.pagos_transformer.raw_to_dataframe_optimized(pagos_raw)
            df_dev = self.dev_transformer.raw_to_dataframe_optimized(dev_raw)

            # Aplicar optimizaciones comunes
            if self.memory_optimization:
                df_acumulado = self._apply_memory_optimization(df_acumulado)
                df_pagos = self._apply_memory_optimization(df_pagos)
                df_dev = self._apply_memory_optimization(df_dev)

            # Estadísticas
            total_records = len(df_acumulado) + len(df_pagos) + len(df_dev)
            self.transformation_stats["total_records_transformed"] += total_records

            end_time = time.time()
            transformation_time = end_time - start_time
            self.transformation_stats["total_time_seconds"] += transformation_time

            print(
                f"✅ Datos transformados a DataFrames: {total_records} registros en {transformation_time:.2f}s"
            )

            return df_acumulado, df_pagos, df_dev

        except Exception as e:
            self.transformation_stats["errors_count"] += 1
            print(f"❌ Error transformando datos raw: {e}")
            raise

    def transform_dataframes_to_output(
        self, df_acumulado: pd.DataFrame, df_pagos: pd.DataFrame, df_dev: pd.DataFrame
    ) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """
        Transforma DataFrames procesados a formato de salida usando transformadores especializados

        Returns:
            Tuple[acumulado_output, pagos_output, dev_output]
        """
        try:
            start_time = time.time()

            # Transformar usando transformadores especializados
            acumulado_output = self.acumulado_transformer.dataframe_to_dict_list(
                df_acumulado
            )
            pagos_output = self.pagos_transformer.dataframe_to_dict_list(df_pagos)
            dev_output = self.dev_transformer.dataframe_to_dict_list(df_dev)

            # Aplicar limpieza final
            acumulado_output = self._clean_output_data(acumulado_output)
            pagos_output = self._clean_output_data(pagos_output)
            dev_output = self._clean_output_data(dev_output)

            end_time = time.time()
            transformation_time = end_time - start_time
            total_records = len(acumulado_output) + len(pagos_output) + len(dev_output)

            print(
                f"✅ DataFrames transformados a salida: {total_records} registros en {transformation_time:.2f}s"
            )

            return acumulado_output, pagos_output, dev_output

        except Exception as e:
            self.transformation_stats["errors_count"] += 1
            print(f"❌ Error transformando DataFrames a salida: {e}")
            raise

    def _apply_memory_optimization(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica optimizaciones de memoria al DataFrame"""
        if df.empty:
            return df

        try:
            # Convertir strings categóricas
            string_cols = df.select_dtypes(include=["object"]).columns
            for col in string_cols:
                if col in df.columns:
                    unique_ratio = df[col].nunique() / len(df)
                    if (
                        unique_ratio < 0.5
                    ):  # Si menos del 50% son únicos, convertir a categórico
                        df[col] = df[col].astype("category")

            # Optimizar tipos numéricos
            numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
            for col in numeric_cols:
                if col in df.columns:
                    if df[col].dtype == "int64":
                        # Verificar si puede ser int32
                        if (
                            df[col].min() >= np.iinfo(np.int32).min
                            and df[col].max() <= np.iinfo(np.int32).max
                        ):
                            df[col] = df[col].astype("int32")
                    elif df[col].dtype == "float64":
                        # Verificar si puede ser float32 (con cuidado en campos financieros)
                        if (
                            not self.preserve_precision
                            or col not in self._get_precision_sensitive_columns()
                        ):
                            df[col] = df[col].astype("float32")

            return df

        except Exception as e:
            print(f"⚠️ Warning en optimización de memoria: {e}")
            return df

    def _get_precision_sensitive_columns(self) -> List[str]:
        """Retorna columnas que requieren precisión decimal (no optimizar)"""
        return [
            "DeudaAnterior",
            "NetoConfirmado",
            "FondoResguardo",
            "MontoCobrar",
            "MontoDesembolso",
            "Interes",
            "MontoPago",
            "MontoDevolucion",
            "InteresPago",
            "GastosPago",
            "SaldoDeuda",
            "SaldoTotal",
            "SaldoTotalPen",
        ]

    def _clean_output_data(self, data: List[Dict]) -> List[Dict]:
        """Limpia datos de salida para consistencia"""
        if not data:
            return data

        try:
            cleaned_data = []

            for record in data:
                cleaned_record = {}

                for key, value in record.items():
                    # Convertir NaN a None
                    if pd.isna(value):
                        cleaned_record[key] = None
                    # Convertir numpy types a types nativos de Python
                    elif isinstance(value, (np.integer, np.int32, np.int64)):
                        cleaned_record[key] = int(value)
                    elif isinstance(value, (np.floating, np.float32, np.float64)):
                        cleaned_record[key] = float(value)
                    elif isinstance(value, np.bool_):
                        cleaned_record[key] = bool(value)
                    elif isinstance(value, pd.Timestamp):
                        cleaned_record[key] = value.isoformat()
                    else:
                        cleaned_record[key] = value

                cleaned_data.append(cleaned_record)

            return cleaned_data

        except Exception as e:
            print(f"⚠️ Warning limpiando datos de salida: {e}")
            return data

    def optimize_large_dataset(
        self, df: pd.DataFrame, chunk_size: int = None
    ) -> pd.DataFrame:
        """Optimiza datasets grandes usando procesamiento por chunks"""
        if df.empty:
            return df

        try:
            if chunk_size is None:
                chunk_size = self.batch_size

            if len(df) <= chunk_size:
                return self._apply_memory_optimization(df)

            # Procesar por chunks
            chunks = []
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i : i + chunk_size].copy()
                chunk_optimized = self._apply_memory_optimization(chunk)
                chunks.append(chunk_optimized)

            # Recombinar chunks
            df_optimized = pd.concat(chunks, ignore_index=True)

            print(
                f"✅ Dataset optimizado por chunks: {len(df)} registros en {len(chunks)} chunks"
            )

            return df_optimized

        except Exception as e:
            print(f"⚠️ Warning en optimización por chunks: {e}")
            return df

    def validate_data_integrity(
        self, original_data: Dict[str, Any], transformed_data: Dict[str, Any]
    ) -> Dict[str, bool]:
        """Valida integridad de datos después de transformación"""
        try:
            integrity_check = {
                "record_count_match": True,
                "essential_fields_present": True,
                "data_types_consistent": True,
                "no_data_loss": True,
            }

            # Verificar conteo de registros
            original_count = (
                len(original_data.get("acumulado", []))
                + len(original_data.get("pagos", []))
                + len(original_data.get("dev", []))
            )

            transformed_count = (
                len(transformed_data.get("acumulado", []))
                + len(transformed_data.get("pagos", []))
                + len(transformed_data.get("dev", []))
            )

            if original_count != transformed_count:
                integrity_check["record_count_match"] = False
                print(
                    f"⚠️ Conteo de registros no coincide: {original_count} → {transformed_count}"
                )

            # Verificar campos esenciales
            essential_fields = ["IdLiquidacionDet", "IdLiquidacionCab"]
            for table_name, records in transformed_data.items():
                if records and isinstance(records, list) and records[0]:
                    for field in essential_fields:
                        if field not in records[0]:
                            integrity_check["essential_fields_present"] = False
                            print(f"⚠️ Campo esencial faltante en {table_name}: {field}")
                            break

            return integrity_check

        except Exception as e:
            print(f"❌ Error validando integridad de datos: {e}")
            return {
                "record_count_match": False,
                "essential_fields_present": False,
                "data_types_consistent": False,
                "no_data_loss": False,
            }

    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas comprehensivas del transformador"""
        try:
            # Estadísticas principales
            main_stats = self.transformation_stats.copy()

            # Estadísticas de transformadores especializados
            specialized_stats = {
                "acumulado_stats": self.acumulado_transformer.get_transformation_stats(),
                "pagos_stats": self.pagos_transformer.get_transformation_stats(),
                "dev_stats": self.dev_transformer.get_transformation_stats(),
            }

            # Calcular métricas derivadas
            derived_metrics = {}
            if main_stats["transformations_count"] > 0:
                derived_metrics["avg_records_per_transformation"] = (
                    main_stats["total_records_transformed"]
                    / main_stats["transformations_count"]
                )

                if main_stats["total_time_seconds"] > 0:
                    derived_metrics["avg_records_per_second"] = (
                        main_stats["total_records_transformed"]
                        / main_stats["total_time_seconds"]
                    )
                    derived_metrics["avg_time_per_transformation"] = (
                        main_stats["total_time_seconds"]
                        / main_stats["transformations_count"]
                    )

            # Configuración actual
            config = {
                "memory_optimization": self.memory_optimization,
                "batch_size": self.batch_size,
                "preserve_precision": self.preserve_precision,
            }

            return {
                "main_stats": main_stats,
                "specialized_stats": specialized_stats,
                "derived_metrics": derived_metrics,
                "config": config,
                "timestamp": datetime.now().isoformat(),
                "transformer_type": "CXCETLProcessorTransformer",
            }

        except Exception as e:
            print(f"❌ Error obteniendo estadísticas: {e}")
            return {"error": str(e), "transformer_type": "CXCETLProcessorTransformer"}

    def reset_stats(self):
        """Reinicia estadísticas del transformador"""
        self.transformation_stats = {
            "transformations_count": 0,
            "total_records_transformed": 0,
            "total_time_seconds": 0.0,
            "memory_peak_mb": 0.0,
            "errors_count": 0,
        }
        print("✅ Estadísticas del transformador reiniciadas")
