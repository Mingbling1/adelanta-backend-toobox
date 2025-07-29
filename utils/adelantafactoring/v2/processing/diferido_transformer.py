"""
🔄 Processing V2 - Diferido Transformer

Transformaciones específicas para datos de diferidos
"""

import pandas as pd
from typing import Dict, List, Any
from datetime import datetime

# Fallback para desarrollo
try:
    from ..schemas.diferido_schema import (
        DiferidoExternoSchema,
        DiferidoInternoSchema,
        DiferidoComparacionSchema,
    )
    from ..config.settings import settings
except ImportError:

    class _FallbackSettings:
        logger = print

    settings = _FallbackSettings()

    # Fallback schemas simples
    class DiferidoExternoSchema:
        def __init__(self, **kwargs):
            pass

    class DiferidoInternoSchema:
        def __init__(self, **kwargs):
            pass

    class DiferidoComparacionSchema:
        def __init__(self, **kwargs):
            pass


class DiferidoTransformer:
    """Transformaciones específicas para datos de diferidos"""

    def __init__(self):
        self.logger = settings.logger

    def transform_raw_to_externo_schema(
        self, raw_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Transforma datos raw a formato DiferidoExternoSchema

        Args:
            raw_data: Datos raw desde Excel

        Returns:
            Lista de registros validados con schema externo
        """
        try:
            validated_records = []

            for record in raw_data:
                try:
                    # Separar campos base de columnas dinámicas de fechas
                    base_fields = {
                        "CodigoLiquidacion": record.get("CodigoLiquidacion", ""),
                        "NroDocumento": record.get("NroDocumento", ""),
                        "FechaOperacion": record.get("FechaOperacion"),
                        "FechaConfirmado": record.get("FechaConfirmado"),
                        "Moneda": record.get("Moneda", "PEN"),
                        "Interes": record.get("Interes", 0),
                        "DiasEfectivo": record.get("DiasEfectivo", 0),
                    }

                    # Extraer columnas de fechas dinámicas
                    date_columns = {}
                    for key, value in record.items():
                        if self._is_date_column(key):
                            date_columns[key] = value

                    base_fields["date_columns"] = date_columns

                    # Validar con schema
                    try:
                        validated_record = DiferidoExternoSchema(**base_fields)
                        validated_records.append(validated_record.model_dump())
                    except Exception:
                        # Fallback simple si schema falla
                        validated_records.append(base_fields)

                except Exception as e:
                    self.logger(f"Error transformando registro: {str(e)}")
                    continue

            self.logger(f"✅ Transformados {len(validated_records)} registros externos")
            return validated_records

        except Exception as e:
            self.logger(f"❌ Error en transform_raw_to_externo_schema: {str(e)}")
            raise

    def transform_calculated_to_interno_schema(
        self, calculated_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Transforma datos calculados a formato DiferidoInternoSchema

        Args:
            calculated_data: Datos calculados internamente

        Returns:
            Lista de registros validados con schema interno
        """
        try:
            validated_records = []

            for record in calculated_data:
                try:
                    # Campos base para schema interno
                    base_fields = {
                        "CodigoLiquidacion": record.get("CodigoLiquidacion", ""),
                        "NroDocumento": record.get("NroDocumento", ""),
                        "FechaOperacion": record.get("FechaOperacion"),
                        "FechaConfirmado": record.get("FechaConfirmado"),
                        "Moneda": record.get("Moneda", "PEN"),
                        "Interes": record.get("Interes", 0),
                        "DiasEfectivo": record.get("DiasEfectivo", 0),
                        "monto_diferido_calculado": record.get(
                            "monto_diferido_calculado", 0
                        ),
                        "fecha_calculo": record.get("fecha_calculo", datetime.now()),
                    }

                    # Validar con schema
                    try:
                        validated_record = DiferidoInternoSchema(**base_fields)
                        validated_records.append(validated_record.model_dump())
                    except Exception:
                        # Fallback simple si schema falla
                        validated_records.append(base_fields)

                except Exception as e:
                    self.logger(f"Error transformando registro calculado: {str(e)}")
                    continue

            self.logger(f"✅ Transformados {len(validated_records)} registros internos")
            return validated_records

        except Exception as e:
            self.logger(f"❌ Error en transform_calculated_to_interno_schema: {str(e)}")
            raise

    def transform_dataframe_to_list_dict(
        self, df: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """
        Convierte DataFrame a lista de diccionarios

        Args:
            df: DataFrame a convertir

        Returns:
            Lista de diccionarios
        """
        try:
            if df.empty:
                return []

            # Convertir a dict preservando tipos
            records = df.to_dict(orient="records")

            # Limpiar valores NaN y convertir tipos
            cleaned_records = []
            for record in records:
                cleaned_record = {}
                for key, value in record.items():
                    if pd.isna(value):
                        cleaned_record[key] = (
                            0 if isinstance(value, (int, float)) else ""
                        )
                    else:
                        cleaned_record[key] = value
                cleaned_records.append(cleaned_record)

            self.logger(f"✅ Convertidos {len(cleaned_records)} registros de DataFrame")
            return cleaned_records

        except Exception as e:
            self.logger(f"❌ Error en transform_dataframe_to_list_dict: {str(e)}")
            raise

    def transform_comparison_results(
        self, comparison_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Transforma resultados de comparación a formato estándar

        Args:
            comparison_data: Datos de comparación raw

        Returns:
            Resultados de comparación formateados
        """
        try:
            # Estructura estándar para respuesta de comparación
            transformed = {
                "total_registros": comparison_data.get("total_registros_externos", 0)
                + comparison_data.get("total_registros_internos", 0),
                "registros_con_diferencias": comparison_data.get(
                    "diferencias_encontradas", {}
                ).get("registros_con_diferencias", 0),
                "diferencias_significativas": comparison_data.get(
                    "diferencias_encontradas", {}
                ).get("diferencias_significativas", 0),
                "periodo_analizado": comparison_data.get("periodo_analizado", "N/A"),
                "fecha_procesamiento": comparison_data.get(
                    "timestamp", datetime.now().isoformat()
                ),
                "columnas_analizadas": comparison_data.get(
                    "columnas_fecha_analizadas", []
                ),
                "detalle_diferencias": comparison_data.get(
                    "diferencias_encontradas", {}
                ).get("detalle_diferencias", []),
            }

            self.logger("✅ Resultados de comparación transformados")
            return transformed

        except Exception as e:
            self.logger(f"❌ Error transformando resultados de comparación: {str(e)}")
            raise

    def _is_date_column(self, column_name: str) -> bool:
        """
        Determina si una columna es de tipo fecha (formato mes-YYYY)

        Args:
            column_name: Nombre de la columna

        Returns:
            True si es columna de fecha
        """
        import re

        # Patrón para detectar columnas como "enero-2024", "febrero-2024", etc.
        pattern = r"^(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)-\d{4}$"
        return bool(re.match(pattern, column_name.lower().strip()))

    def normalize_date_columns_order(
        self, data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Normaliza el orden de las columnas de fechas en los datos

        Args:
            data: Lista de registros con columnas de fechas

        Returns:
            Lista con columnas de fechas ordenadas cronológicamente
        """
        try:
            if not data:
                return data

            # Identificar todas las columnas de fechas únicas
            date_columns = set()
            for record in data:
                for key in record.keys():
                    if self._is_date_column(key):
                        date_columns.add(key)

            # Ordenar columnas cronológicamente
            ordered_date_columns = self._order_date_columns_chronologically(
                list(date_columns)
            )

            # Reordenar cada registro
            ordered_data = []
            for record in data:
                ordered_record = {}

                # Primero agregar campos no-fecha
                for key, value in record.items():
                    if not self._is_date_column(key):
                        ordered_record[key] = value

                # Luego agregar columnas de fechas en orden cronológico
                for date_col in ordered_date_columns:
                    if date_col in record:
                        ordered_record[date_col] = record[date_col]

                ordered_data.append(ordered_record)

            self.logger(
                f"✅ Normalizadas {len(ordered_data)} registros con orden de fechas"
            )
            return ordered_data

        except Exception as e:
            self.logger(f"❌ Error normalizando orden de columnas: {str(e)}")
            return data  # Retornar original si falla

    def _order_date_columns_chronologically(self, date_columns: List[str]) -> List[str]:
        """Ordena columnas de fechas cronológicamente"""

        def sort_key(col: str):
            try:
                month_str, year_str = col.split("-")
                year = int(year_str)
                months_order = [
                    "enero",
                    "febrero",
                    "marzo",
                    "abril",
                    "mayo",
                    "junio",
                    "julio",
                    "agosto",
                    "septiembre",
                    "octubre",
                    "noviembre",
                    "diciembre",
                ]
                month_idx = (
                    months_order.index(month_str) if month_str in months_order else 99
                )
                return (year, month_idx)
            except Exception:
                return (9999, 99)

        return sorted(date_columns, key=sort_key)
