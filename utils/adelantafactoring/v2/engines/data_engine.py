"""
⚙️ Data Engine V2 - Motor de extracción y transformación de datos
"""

import pandas as pd
import numpy as np
from typing import List, Optional
import logging

# Logger local para V2 (aislado)
logger = logging.getLogger(__name__)

try:
    from ..config.settings import settings
except ImportError:
    # Fallback para tests aislados
    class MockSettings:
        PROCESSING_OPTIONS = {
            "apply_legacy_date_formatting": True,
            "validate_financial_precision": True,
            "enable_fuzzy_matching": True,
            "preserve_source_data": True,
        }

    settings = MockSettings()


class DataEngine:
    """
    Motor especializado para extracción y transformación de datos financieros
    Centraliza toda la lógica de manipulación de DataFrames
    """

    def __init__(self):
        self.date_formats = settings.DATE_FORMATS
        self.processing_options = settings.PROCESSING_OPTIONS

    def validate_required_columns(
        self, df: pd.DataFrame, required_columns: List[str]
    ) -> pd.DataFrame:
        """
        Valida que existan las columnas mínimas requeridas

        Args:
            df: DataFrame a validar
            required_columns: Lista de columnas obligatorias

        Returns:
            DataFrame validado (copia)

        Raises:
            ValueError: Si faltan columnas obligatorias
        """
        missing = set(required_columns) - set(df.columns)
        if missing:
            msg = f"Faltan columnas obligatorias: {missing}"
            logger.error(msg)
            raise ValueError(msg)

        logger.info(
            f"✅ Validación de columnas exitosa: {len(required_columns)} columnas verificadas"
        )
        return df.copy()

    def convert_date_columns(
        self,
        df: pd.DataFrame,
        date_columns: List[str],
        source_format: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Convierte columnas de fecha manteniendo integridad financiera

        Args:
            df: DataFrame a procesar
            date_columns: Lista de columnas de fecha a convertir
            source_format: Formato de fecha fuente (None para auto-detección)

        Returns:
            DataFrame con fechas convertidas
        """
        df = df.copy()

        for col in date_columns:
            if col in df.columns:
                logger.debug(f"Convirtiendo columna de fecha: {col}")

                # Convertir a datetime
                df[col] = pd.to_datetime(df[col], format=source_format, errors="coerce")

                # Eliminar timezone y normalizar a medianoche (preserva precisión financiera)
                if df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_localize(None)
                df[col] = df[col].dt.normalize()

        return df

    def clean_numeric_columns(
        self, df: pd.DataFrame, numeric_columns: List[str]
    ) -> pd.DataFrame:
        """
        Limpia y convierte columnas numéricas preservando precisión financiera

        Args:
            df: DataFrame a procesar
            numeric_columns: Lista de columnas numéricas a limpiar

        Returns:
            DataFrame con columnas numéricas limpias
        """
        df = df.copy()

        for col in numeric_columns:
            if col in df.columns:
                logger.debug(f"Limpiando columna numérica: {col}")
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df

    def clean_string_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia columnas de texto comunes (RUC, códigos, etc.)

        Args:
            df: DataFrame a procesar

        Returns:
            DataFrame con strings limpios
        """
        df = df.copy()

        # RUC Cliente - limpiar espacios
        if "RUCCliente" in df.columns:
            df["RUCCliente"] = df["RUCCliente"].astype(str).str.strip()

        # RUC Pagador - limpiar caracteres especiales
        if "RUCPagador" in df.columns:
            df["RUCPagador"] = (
                df["RUCPagador"]
                .astype(str)
                .str.replace("[: ]", "", regex=True)
                .str.strip()
            )

        # Código de Liquidación - formato especial
        if "CodigoLiquidacion" in df.columns:
            df["CodigoLiquidacion"] = (
                df["CodigoLiquidacion"]
                .astype(str)
                .str.strip()
                .str.split("-")
                .str[:2]
                .str.join("-")
            )

        # Número de Documento - remover espacios
        if "NroDocumento" in df.columns:
            df["NroDocumento"] = (
                df["NroDocumento"].astype(str).str.replace(r"\s+", "", regex=True)
            )

        return df

    def create_calculated_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Crea columnas calculadas estándar (Mes, Año, etc.)

        Args:
            df: DataFrame a procesar

        Returns:
            DataFrame con columnas calculadas añadidas
        """
        df = df.copy()

        # Solo procesar si hay fechas válidas en FechaOperacion
        if "FechaOperacion" in df.columns and not df["FechaOperacion"].isna().all():
            df["Mes"] = df["FechaOperacion"].dt.strftime("%Y-%m")
            df["Año"] = df["FechaOperacion"].dt.year.astype(str)
            df["MesAño"] = df["FechaOperacion"].dt.strftime("%B-%Y")
        else:
            # Valores por defecto si no hay fechas válidas
            df["Mes"] = "Unknown"
            df["Año"] = "Unknown"
            df["MesAño"] = "Unknown"

        return df

    def merge_exchange_rates(
        self, df: pd.DataFrame, exchange_rate_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Fusiona datos con tipos de cambio

        Args:
            df: DataFrame principal
            exchange_rate_df: DataFrame con tipos de cambio

        Returns:
            DataFrame fusionado con tipos de cambio
        """
        df = df.copy()

        # Preparar DataFrame de tipo de cambio
        tc_df = exchange_rate_df.copy()

        if "TipoCambioFecha" in tc_df.columns:
            tc_df["TipoCambioFecha"] = pd.to_datetime(
                tc_df["TipoCambioFecha"], errors="coerce"
            )

        # Convertir columnas numéricas
        numeric_cols = ["TipoCambioVenta", "TipoCambioCompra"]
        for col in numeric_cols:
            if col in tc_df.columns:
                tc_df[col] = pd.to_numeric(tc_df[col], errors="coerce").fillna(1)

        # Realizar merge
        merged = df.merge(
            tc_df, left_on="FechaOperacion", right_on="TipoCambioFecha", how="left"
        )

        return merged

    def create_financial_calculations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Crea cálculos financieros básicos (conversiones de moneda)

        Args:
            df: DataFrame con datos financieros

        Returns:
            DataFrame con cálculos financieros añadidos
        """
        df = df.copy()

        # Factor de conversión USD -> PEN
        factor = np.where(df["Moneda"] == "USD", df.get("TipoCambioVenta", 1), 1)

        # Conversiones a soles
        if "NetoConfirmado" in df.columns:
            df["ColocacionSoles"] = df["NetoConfirmado"] * factor

        if "MontoDesembolso" in df.columns:
            df["MontoDesembolsoSoles"] = df["MontoDesembolso"] * factor

        if "MontoPago" in df.columns:
            df["MontoPagoSoles"] = df["MontoPago"] * factor

        return df

    def remove_duplicates_with_priority(
        self,
        df_main: pd.DataFrame,
        df_secondary: pd.DataFrame,
        key_column: str = "CodigoLiquidacion",
    ) -> pd.DataFrame:
        """
        Combina DataFrames eliminando duplicados con prioridad al secundario

        Args:
            df_main: DataFrame principal (menor prioridad)
            df_secondary: DataFrame secundario (mayor prioridad)
            key_column: Columna clave para detectar duplicados

        Returns:
            DataFrame combinado sin duplicados
        """
        # Detectar duplicados
        keys_main = set(df_main[key_column].dropna().astype(str))
        keys_secondary = set(df_secondary[key_column].dropna().astype(str))
        duplicated_keys = keys_main.intersection(keys_secondary)

        logger.info(f"🔍 Duplicados detectados: {len(duplicated_keys)}")

        if duplicated_keys:
            # Remover duplicados del DataFrame principal
            df_main_clean = df_main[
                ~df_main[key_column].astype(str).isin(duplicated_keys)
            ]
            logger.info(
                f"🧹 Registros removidos del principal: {len(df_main) - len(df_main_clean)}"
            )
        else:
            df_main_clean = df_main.copy()

        # Combinar DataFrames
        result = pd.concat([df_main_clean, df_secondary], ignore_index=True, sort=False)

        return result
