"""
🔄 FondoPromocional Transformer V2 - Procesamiento especializado

Transformer dedicado para lógica de negocio de FondoPromocional
"""

import pandas as pd
from typing import List, Dict, Any
from config.logger import logger


class FondoPromocionalTransformer:
    """Transformer especializado para FondoPromocional"""

    def __init__(self):
        self.expected_columns = ["LIQUIDACION"]
        self.column_mapping = {"liquidacion": "CodigoLiquidacion"}

    def transform_raw_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Transforma datos crudos en DataFrame procesado

        Args:
            raw_data: Datos crudos del webservice

        Returns:
            DataFrame procesado y limpio
        """
        try:
            if not raw_data:
                logger.warning("Datos crudos vacíos para transformación")
                return pd.DataFrame()

            # Crear DataFrame desde datos crudos
            df = pd.DataFrame(raw_data)

            if df.empty:
                logger.warning("DataFrame vacío después de creación")
                return df

            logger.info(f"Iniciando transformación de {len(df)} registros")

            # Validar columnas esperadas
            self._validate_columns(df)

            # Renombrar columnas
            df = self._rename_columns(df)

            # Limpiar datos
            df = self._clean_data(df)

            # Remover duplicados
            df = self._remove_duplicates(df)

            logger.info(f"Transformación completada: {len(df)} registros finales")
            return df

        except Exception as e:
            logger.error(f"Error durante transformación: {e}")
            raise

    def transform_to_dict_list(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Convierte DataFrame procesado a lista de diccionarios

        Args:
            df: DataFrame procesado

        Returns:
            Lista de diccionarios
        """
        if df.empty:
            return []

        return df.to_dict(orient="records")

    def _validate_columns(self, df: pd.DataFrame) -> None:
        """Valida que existan las columnas esperadas"""
        df_columns_normalized = [col.lower().strip() for col in df.columns]
        expected_normalized = [col.lower() for col in self.expected_columns]

        missing = [
            col for col in expected_normalized if col not in df_columns_normalized
        ]

        if missing:
            raise ValueError(f"Faltan columnas requeridas: {missing}")

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renombra columnas según mapeo"""
        df = df.copy()

        # Crear mapeo de renombrado case-insensitive
        rename_map = {}
        for col in df.columns:
            normalized_col = col.lower().strip()
            if normalized_col in self.column_mapping:
                rename_map[col] = self.column_mapping[normalized_col]

        if rename_map:
            df = df.rename(columns=rename_map)
            logger.info(f"Columnas renombradas: {rename_map}")

        return df

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia y normaliza los datos"""
        df = df.copy()

        # Limpiar código de liquidación
        if "CodigoLiquidacion" in df.columns:
            df["CodigoLiquidacion"] = (
                df["CodigoLiquidacion"].astype(str).str.strip().str.upper()
            )

            # Remover valores nulos o vacíos
            df = df[df["CodigoLiquidacion"].notna()]
            df = df[df["CodigoLiquidacion"] != ""]
            df = df[df["CodigoLiquidacion"] != "NAN"]

        return df

    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remueve duplicados manteniendo el último registro"""
        if df.empty:
            return df

        initial_count = len(df)

        if "CodigoLiquidacion" in df.columns:
            df = df.drop_duplicates(subset="CodigoLiquidacion", keep="last")

        final_count = len(df)

        if initial_count != final_count:
            logger.info(
                f"Duplicados removidos: {initial_count - final_count} registros"
            )

        return df
