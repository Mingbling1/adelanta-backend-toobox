"""
⚙️ Engine V2 - Diferido Calculation Engine

Motor especializado para cálculos de diferidos internos y comparaciones
"""

import pandas as pd
import asyncio
from typing import Dict, List, Any, Tuple
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import re
import locale

# Fallback para desarrollo
try:
    from ..config.settings import settings
except ImportError:

    class _FallbackSettings:
        logger = print  # Fallback simple

    settings = _FallbackSettings()


class DiferidoCalculationEngine:
    """Motor especializado para cálculos financieros de diferidos"""

    # Constantes de columnas (del V1 original)
    LISTA_COLUMNAS = [
        "CodigoLiquidacion",
        "NroDocumento",
        "FechaOperacion",
        "FechaConfirmado",
        "Moneda",
        "Interes",
        "DiasEfectivo",
    ]

    def __init__(self):
        self.logger = settings.logger
        # Configurar locale para manejo de fechas en español
        try:
            locale.setlocale(locale.LC_TIME, "es_ES.UTF-8")
        except locale.Error:
            pass  # Fallback si no está disponible

    async def calculate_diferido_interno_async(
        self, df: pd.DataFrame, hasta: str
    ) -> pd.DataFrame:
        """
        Calcula diferidos internos de manera asíncrona

        Args:
            df: DataFrame con datos base
            hasta: Período hasta el cual calcular (YYYY-MM)

        Returns:
            DataFrame con cálculos de diferidos internos
        """
        try:
            # Ejecutar cálculo en hilo separado para no bloquear
            result = await asyncio.to_thread(self.calculate_diferido_interno, df, hasta)

            self.logger(f"✅ Cálculo diferido interno completado para período: {hasta}")
            return result

        except Exception as e:
            self.logger(f"❌ Error en cálculo diferido interno: {str(e)}")
            raise

    def calculate_diferido_interno(self, df: pd.DataFrame, hasta: str) -> pd.DataFrame:
        """
        Realiza el cálculo de diferidos internos (lógica del V1)

        Args:
            df: DataFrame con datos base
            hasta: Período hasta el cual calcular (formato YYYY-MM)

        Returns:
            DataFrame con los cálculos de diferidos
        """
        try:
            if df.empty:
                return pd.DataFrame()

            # Crear copia para no modificar original
            df_work = df.copy()

            # Validar columnas requeridas
            missing_cols = [
                col for col in self.LISTA_COLUMNAS if col not in df_work.columns
            ]
            if missing_cols:
                raise ValueError(f"Faltan columnas requeridas: {missing_cols}")

            # Convertir fechas
            df_work["FechaOperacion"] = pd.to_datetime(df_work["FechaOperacion"])
            df_work["FechaConfirmado"] = pd.to_datetime(df_work["FechaConfirmado"])

            # Procesar cada fila para calcular diferidos por mes
            results = []
            for index, row in df_work.iterrows():
                row_results = self._calculate_row_diferidos(row, hasta)
                results.append(row_results)

            # Convertir resultados a DataFrame
            if results:
                result_df = pd.DataFrame(results)
                self.logger(f"✅ Procesadas {len(results)} filas para diferido interno")
                return result_df
            else:
                return pd.DataFrame()

        except Exception as e:
            self.logger(f"❌ Error en calculate_diferido_interno: {str(e)}")
            raise

    def _calculate_row_diferidos(self, row: pd.Series, hasta: str) -> Dict[str, Any]:
        """
        Calcula diferidos para una fila específica

        Args:
            row: Fila de datos
            hasta: Período límite

        Returns:
            Diccionario con cálculos por mes
        """
        try:
            # Extraer datos básicos de la fila
            result = {
                "CodigoLiquidacion": row["CodigoLiquidacion"],
                "NroDocumento": row["NroDocumento"],
                "FechaOperacion": row["FechaOperacion"],
                "FechaConfirmado": row["FechaConfirmado"],
                "Moneda": row["Moneda"],
                "Interes": float(row["Interes"]),
                "DiasEfectivo": int(row["DiasEfectivo"]),
            }

            # Calcular diferidos por mes hasta el período límite
            year_limite, month_limite = map(int, hasta.split("-"))
            fecha_limite = datetime(year_limite, month_limite, 1)

            # Generar meses desde fecha de operación hasta límite
            fecha_operacion = pd.to_datetime(row["FechaOperacion"])
            fecha_actual = fecha_operacion.replace(day=1)  # Primer día del mes

            while fecha_actual <= fecha_limite:
                mes_str = fecha_actual.strftime("%B-%Y").lower()  # formato: enero-2024

                # Calcular monto diferido para este mes
                monto = self._calcular_monto_por_mes(row, fecha_actual)
                result[mes_str] = float(monto) if monto else 0.0

                # Avanzar al siguiente mes
                fecha_actual += relativedelta(months=1)

            return result

        except Exception as e:
            self.logger(f"Error calculando fila: {str(e)}")
            return {}

    def _calcular_monto_por_mes(self, row: pd.Series, mes_fecha: datetime) -> float:
        """
        Calcula el monto diferido para un mes específico (lógica del V1)

        Args:
            row: Datos de la operación
            mes_fecha: Fecha del mes a calcular

        Returns:
            Monto diferido para ese mes
        """
        try:
            fecha_operacion = pd.to_datetime(row["FechaOperacion"])
            fecha_confirmado = pd.to_datetime(row["FechaConfirmado"])
            interes = float(row["Interes"])
            dias_efectivo = int(row["DiasEfectivo"])

            # Inicio y fin del mes
            mes_inicio = mes_fecha.replace(day=1)
            mes_fin = (mes_inicio + relativedelta(months=1)) - timedelta(days=1)

            # Verificar si el mes está en el rango de la operación
            if mes_fin < fecha_operacion or mes_inicio > fecha_confirmado:
                return 0.0

            # Calcular días efectivos en este mes
            inicio_efectivo = max(mes_inicio, fecha_operacion)
            fin_efectivo = min(mes_fin, fecha_confirmado)

            if inicio_efectivo > fin_efectivo:
                return 0.0

            dias_en_mes = (fin_efectivo - inicio_efectivo).days + 1

            # Calcular proporción del interés para este mes
            if dias_efectivo > 0:
                proporcion = dias_en_mes / dias_efectivo
                monto_mes = interes * proporcion
                return round(monto_mes, 2)

            return 0.0

        except Exception as e:
            self.logger(f"Error calculando monto por mes: {str(e)}")
            return 0.0


class DiferidoComparisonEngine:
    """Motor especializado para comparar diferidos externos vs internos"""

    def __init__(self):
        self.logger = settings.logger

    async def compare_diferidos_async(
        self, df_externo: pd.DataFrame, df_interno: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Compara diferidos externos vs internos de manera asíncrona

        Args:
            df_externo: DataFrame con datos externos
            df_interno: DataFrame con datos calculados internamente

        Returns:
            Diccionario con resultados de comparación
        """
        try:
            result = await asyncio.to_thread(
                self.compare_diferidos, df_externo, df_interno
            )

            self.logger("✅ Comparación de diferidos completada")
            return result

        except Exception as e:
            self.logger(f"❌ Error en comparación de diferidos: {str(e)}")
            raise

    def compare_diferidos(
        self, df_externo: pd.DataFrame, df_interno: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Realiza la comparación entre diferidos externos e internos (lógica del V1)

        Args:
            df_externo: DataFrame con datos externos
            df_interno: DataFrame con datos calculados

        Returns:
            Diccionario con análisis de diferencias
        """
        try:
            # Identificar columnas de fechas dinámicas
            date_cols_externo = self._get_date_columns(df_externo)
            date_cols_interno = self._get_date_columns(df_interno)

            # Unión de columnas de fechas y ordenamiento cronológico
            union_date_cols = list(set(date_cols_externo + date_cols_interno))
            date_columns_ordered = self._reorder_date_columns(union_date_cols)

            # Columnas fijas para comparación
            fixed_cols = [
                "CodigoLiquidacion",
                "NroDocumento",
                "Interes",
                "FechaOperacion",
                "FechaConfirmado",
                "Moneda",
                "DiasEfectivo",
            ]

            # Reindexar ambos DataFrames con mismo orden de columnas
            all_cols = fixed_cols + date_columns_ordered
            df_ext_reindexed = df_externo.reindex(columns=all_cols, fill_value=0)
            df_int_reindexed = df_interno.reindex(columns=all_cols, fill_value=0)

            # Agrupar por clave de liquidación y moneda
            key_cols = ["CodigoLiquidacion", "Moneda"]
            numeric_cols = [
                col
                for col in all_cols
                if col not in key_cols + ["FechaOperacion", "FechaConfirmado"]
            ]

            # Agrupar y sumar valores numéricos
            df_ext_grouped = df_ext_reindexed.groupby(key_cols)[numeric_cols].sum(
                numeric_only=True
            )
            df_int_grouped = df_int_reindexed.groupby(key_cols)[numeric_cols].sum(
                numeric_only=True
            )

            # Calcular diferencias
            diferencias = self._calculate_differences(
                df_ext_grouped, df_int_grouped, date_columns_ordered
            )

            # Generar reporte de comparación
            comparison_report = {
                "total_registros_externos": len(df_externo),
                "total_registros_internos": len(df_interno),
                "columnas_fecha_analizadas": date_columns_ordered,
                "diferencias_encontradas": diferencias,
                "timestamp": datetime.now().isoformat(),
            }

            return comparison_report

        except Exception as e:
            self.logger(f"❌ Error en compare_diferidos: {str(e)}")
            raise

    def _get_date_columns(self, df: pd.DataFrame) -> List[str]:
        """Identifica columnas de fechas en formato mes-YYYY"""
        date_cols = []
        for col in df.columns:
            if re.match(r"^[A-Za-z]+-\d{4}$", col):
                date_cols.append(col)
        return date_cols

    def _reorder_date_columns(self, date_cols: List[str]) -> List[str]:
        """
        Ordena columnas de fechas cronológicamente (lógica del V1)
        """

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

        return sorted(date_cols, key=sort_key)

    def _calculate_differences(
        self,
        df_externo: pd.DataFrame,
        df_interno: pd.DataFrame,
        date_columns: List[str],
    ) -> Dict[str, Any]:
        """Calcula diferencias entre DataFrames agrupados"""
        try:
            diferencias = {
                "registros_con_diferencias": 0,
                "diferencias_significativas": 0,
                "detalle_diferencias": [],
            }

            # Obtener todas las claves únicas
            all_keys = set(df_externo.index.tolist() + df_interno.index.tolist())

            for key in all_keys:
                key_differences = {}
                tiene_diferencias = False

                # Comparar valores para esta clave
                for col in date_columns:
                    ext_val = df_externo.loc[key, col] if key in df_externo.index else 0
                    int_val = df_interno.loc[key, col] if key in df_interno.index else 0

                    diferencia = abs(float(ext_val) - float(int_val))

                    if diferencia > 0.01:  # Umbral de diferencia significativa
                        key_differences[col] = {
                            "externo": float(ext_val),
                            "interno": float(int_val),
                            "diferencia": diferencia,
                        }
                        tiene_diferencias = True

                if tiene_diferencias:
                    diferencias["registros_con_diferencias"] += 1
                    if any(d["diferencia"] > 100 for d in key_differences.values()):
                        diferencias["diferencias_significativas"] += 1

                    diferencias["detalle_diferencias"].append(
                        {"clave": key, "diferencias": key_differences}
                    )

            return diferencias

        except Exception as e:
            self.logger(f"Error calculando diferencias: {str(e)}")
            return {"error": str(e)}
