"""
⚙️ CXC Dev Fact Calculation Engine V2 - Motor de cálculo financiero
Procesa y calcula métricas específicas para devoluciones de facturas CXC
"""

import pandas as pd
from typing import List, Dict

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
        pass


class CXCDevFactCalculationEngine:
    """Motor especializado para cálculos de devoluciones de facturas CXC"""

    def __init__(self):
        self.calculations_performed = []

    async def calculate_devolucion_metrics(self, data: pd.DataFrame) -> Dict:
        """
        Calcula métricas clave para devoluciones de facturas.

        Args:
            data: DataFrame con datos de devoluciones

        Returns:
            Diccionario con métricas calculadas
        """
        if data.empty:
            logger.warning("DataFrame vacío para cálculos de devoluciones")
            return self._empty_metrics()

        try:
            metrics = {
                "total_devoluciones": len(data),
                "monto_total_devoluciones": float(data["MontoDevolucion"].sum()),
                "descuento_total_devoluciones": float(
                    data["DescuentoDevolucion"].sum()
                ),
                "promedio_devolucion": float(data["MontoDevolucion"].mean()),
                "devolucion_maxima": float(data["MontoDevolucion"].max()),
                "devolucion_minima": float(data["MontoDevolucion"].min()),
                "estados_devolucion": data["EstadoDevolucion"].value_counts().to_dict(),
                "devoluciones_con_fecha": data["FechaDesembolso"].notna().sum(),
                "devoluciones_sin_fecha": data["FechaDesembolso"].isna().sum(),
            }

            # Agregar métricas por estado
            if "EstadoDevolucion" in data.columns:
                for estado in data["EstadoDevolucion"].unique():
                    subset = data[data["EstadoDevolucion"] == estado]
                    metrics[f"monto_estado_{estado}"] = float(
                        subset["MontoDevolucion"].sum()
                    )

            logger.debug(f"Métricas calculadas para {len(data)} devoluciones")
            self.calculations_performed.append("devolucion_metrics")

            return metrics

        except Exception as e:
            logger.error(f"Error calculando métricas de devoluciones: {e}")
            return self._empty_metrics()

    async def calculate_devolucion_summary(self, data: pd.DataFrame) -> Dict:
        """
        Genera resumen ejecutivo de devoluciones.

        Args:
            data: DataFrame con datos de devoluciones

        Returns:
            Resumen ejecutivo de devoluciones
        """
        if data.empty:
            return {"resumen": "Sin devoluciones para procesar"}

        try:
            total_registros = len(data)
            monto_total = data["MontoDevolucion"].sum()
            descuento_total = data["DescuentoDevolucion"].sum()

            # Categorizar por rangos de monto
            data["rango_monto"] = pd.cut(
                data["MontoDevolucion"],
                bins=[0, 1000, 5000, 10000, float("inf")],
                labels=["Pequeña", "Mediana", "Grande", "Muy Grande"],
            )

            resumen = {
                "total_devoluciones": total_registros,
                "monto_total_soles": float(monto_total),
                "descuento_total_soles": float(descuento_total),
                "monto_neto": float(monto_total - descuento_total),
                "devolucion_promedio": (
                    float(monto_total / total_registros) if total_registros > 0 else 0
                ),
                "distribucion_por_rango": data["rango_monto"].value_counts().to_dict(),
                "estados_resumen": data["EstadoDevolucion"].value_counts().to_dict(),
            }

            # Calcular tendencias si hay fechas
            if (
                "FechaDesembolso" in data.columns
                and data["FechaDesembolso"].notna().any()
            ):
                fechas_validas = data.dropna(subset=["FechaDesembolso"])
                if not fechas_validas.empty:
                    resumen["fecha_primera"] = (
                        fechas_validas["FechaDesembolso"].min().isoformat()
                    )
                    resumen["fecha_ultima"] = (
                        fechas_validas["FechaDesembolso"].max().isoformat()
                    )
                    resumen["periodo_dias"] = (
                        fechas_validas["FechaDesembolso"].max()
                        - fechas_validas["FechaDesembolso"].min()
                    ).days

            logger.info(f"Resumen generado para {total_registros} devoluciones")
            self.calculations_performed.append("devolucion_summary")

            return resumen

        except Exception as e:
            logger.error(f"Error generando resumen de devoluciones: {e}")
            return {"error": f"Error en resumen: {str(e)}"}

    async def process_financial_data(self, raw_data: List[Dict]) -> pd.DataFrame:
        """
        Procesa datos financieros aplicando lógica específica de negocio.

        Args:
            raw_data: Datos crudos desde webservice

        Returns:
            DataFrame procesado con cálculos aplicados
        """
        if not raw_data:
            logger.warning("Sin datos para procesar en engine")
            return pd.DataFrame()

        try:
            # Convertir a DataFrame
            df = pd.DataFrame(raw_data)
            logger.debug(f"DataFrame inicial creado con {len(df)} registros")

            # Aplicar cálculos específicos de negocio
            if "MontoDevolucion" in df.columns and "DescuentoDevolucion" in df.columns:
                # Calcular monto neto
                df["MontoNeto"] = df["MontoDevolucion"] - df["DescuentoDevolucion"]

                # Categorizar montos
                df["CategoriaMonto"] = pd.cut(
                    df["MontoDevolucion"],
                    bins=[0, 1000, 5000, 10000, float("inf")],
                    labels=["Pequeña", "Mediana", "Grande", "Muy_Grande"],
                )

                # Calcular porcentaje de descuento
                df["PorcentajeDescuento"] = (
                    df["DescuentoDevolucion"] / df["MontoDevolucion"]
                ) * 100
                df["PorcentajeDescuento"] = df["PorcentajeDescuento"].fillna(0)

            # Procesar fechas
            if "FechaDesembolso" in df.columns:
                df["FechaDesembolso"] = pd.to_datetime(
                    df["FechaDesembolso"], errors="coerce"
                )
                df["TieneFecha"] = df["FechaDesembolso"].notna()

            logger.info(f"Datos financieros procesados: {len(df)} registros")
            self.calculations_performed.append("financial_processing")

            return df

        except Exception as e:
            logger.error(f"Error procesando datos financieros: {e}")
            return pd.DataFrame()

    def _empty_metrics(self) -> Dict:
        """Retorna métricas vacías por defecto"""
        return {
            "total_devoluciones": 0,
            "monto_total_devoluciones": 0.0,
            "descuento_total_devoluciones": 0.0,
            "promedio_devolucion": 0.0,
            "devolucion_maxima": 0.0,
            "devolucion_minima": 0.0,
            "estados_devolucion": {},
            "devoluciones_con_fecha": 0,
            "devoluciones_sin_fecha": 0,
        }

    def get_calculation_history(self) -> List[str]:
        """Retorna historial de cálculos realizados"""
        return self.calculations_performed.copy()
