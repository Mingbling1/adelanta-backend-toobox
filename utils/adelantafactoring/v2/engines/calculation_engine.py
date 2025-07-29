"""
🧮 Calculation Engine V2 - Motor de cálculos financieros especializados
"""

import pandas as pd
import numpy as np
import math
import logging

# Logger local para V2 (aislado)
logger = logging.getLogger(__name__)

try:
    from ..config.settings import settings
except ImportError:
    # Fallback para tests aislados
    class MockSettings:
        INTERESES_PEN = 0.14
        INTERESES_USD = 0.12

    settings = MockSettings()


class CalculationEngine:
    """
    Motor especializado para cálculos financieros y KPIs
    Centraliza toda la lógica matemática compleja
    """

    def __init__(self):
        self.intereses_pen = settings.INTERESES_PEN
        self.intereses_usd = settings.INTERESES_USD

    def calculate_income_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula métricas de ingresos financieros

        Args:
            df: DataFrame con datos financieros base

        Returns:
            DataFrame con métricas de ingresos calculadas
        """
        df = df.copy()

        logger.debug("🧮 Calculando métricas de ingresos...")

        # Factor de conversión para soles
        factor = np.where(df["Moneda"] == "USD", df.get("TipoCambioVenta", 1), 1)

        # Ingresos base (sin IGV)
        df["Ingresos"] = (
            df.get("ComisionEstructuracionConIGV", 0) / 1.18
            + df.get("Interes", 0)
            + df.get("GastosDiversosConIGV", 0) / 1.18
        )
        df["IngresosSoles"] = df["Ingresos"] * factor

        # Totales con IGV
        df["TotalIngresos"] = df.get("ComisionEstructuracionConIGV", 0) / 1.18 + df.get(
            "Interes", 0
        )
        df["TotalIngresosSoles"] = df["TotalIngresos"] * factor

        return df

    def calculate_funding_costs(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula costos de fondeo según moneda

        Args:
            df: DataFrame con datos de operaciones

        Returns:
            DataFrame con costos de fondeo calculados
        """
        df = df.copy()

        logger.debug("💰 Calculando costos de fondeo...")

        # Tasa de costo según moneda
        cost_rate = np.where(
            df["Moneda"] == "PEN", self.intereses_pen, self.intereses_usd
        )

        # Cálculo de costo compuesto
        df["CostosFondo"] = (
            (1 + cost_rate) ** (df.get("DiasEfectivo", 0) / 365) - 1
        ) * df.get("MontoDesembolso", 0)

        # Conversión a soles
        factor = np.where(df["Moneda"] == "USD", df.get("TipoCambioVenta", 1), 1)
        df["CostosFondoSoles"] = df["CostosFondo"] * factor

        return df

    def calculate_profitability(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula utilidad y rentabilidad

        Args:
            df: DataFrame con ingresos y costos calculados

        Returns:
            DataFrame con métricas de rentabilidad
        """
        df = df.copy()

        logger.debug("📈 Calculando rentabilidad...")

        # Utilidad = Ingresos - Costos de Fondeo
        df["Utilidad"] = df.get("TotalIngresosSoles", 0) - df.get("CostosFondoSoles", 0)

        return df

    def calculate_week_of_month(self, date_series: pd.Series) -> pd.Series:
        """
        Calcula número de semana dentro del mes para una serie de fechas

        Args:
            date_series: Serie de pandas con fechas

        Returns:
            Serie con números de semana
        """

        def week_of_month(dt) -> int:
            """Calcula número de semana dentro del mes. Maneja valores NaT/None."""
            # Manejar valores nulos o NaT
            if pd.isna(dt) or dt is None:
                return 1  # Default a semana 1 si la fecha es nula

            # Convertir a datetime si es necesario
            if isinstance(dt, str):
                try:
                    dt = pd.to_datetime(dt)
                except Exception:
                    return 1

            try:
                first = dt.replace(day=1)
                dom = dt.day + first.weekday()
                return math.ceil(dom / 7)
            except (AttributeError, ValueError, TypeError):
                return 1  # Default a semana 1 si hay cualquier error

        return date_series.apply(week_of_month)

    def create_time_dimensions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Crea dimensiones temporales (semana del mes, etc.)

        Args:
            df: DataFrame con fechas

        Returns:
            DataFrame con dimensiones temporales añadidas
        """
        df = df.copy()

        # Semana del mes - Solo si hay fechas válidas
        if "FechaOperacion" in df.columns and not df["FechaOperacion"].isna().all():
            week_numbers = self.calculate_week_of_month(df["FechaOperacion"])
            df["MesSemana"] = week_numbers.apply(lambda w: f"Semana {w}")
        else:
            # Si no hay fechas válidas, asignar semana por defecto
            df["MesSemana"] = "Semana 1"

        return df

    def normalize_rates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza tasas porcentuales (convierte de >1 a decimal)

        Args:
            df: DataFrame con columnas de tasas

        Returns:
            DataFrame con tasas normalizadas
        """
        df = df.copy()

        # Normalizar TasaNominalMensualPorc
        if "TasaNominalMensualPorc" in df.columns:
            df["TasaNominalMensualPorc"] = df["TasaNominalMensualPorc"].apply(
                lambda x: x / 100 if x >= 1 else x
            )

        return df

    def calculate_all_kpi_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ejecuta todos los cálculos KPI en secuencia optimizada

        Args:
            df: DataFrame base con datos limpios

        Returns:
            DataFrame con todos los KPIs calculados
        """
        logger.info("🎯 Iniciando cálculo completo de KPIs...")

        # Secuencia de cálculos
        df = self.normalize_rates(df)
        df = self.calculate_income_metrics(df)
        df = self.calculate_funding_costs(df)
        df = self.calculate_profitability(df)
        df = self.create_time_dimensions(df)

        logger.info("✅ KPIs calculados exitosamente")
        return df
