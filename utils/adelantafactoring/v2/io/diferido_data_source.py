"""
📁 IO V2 - Diferido Excel Reader

Lectura y validación de archivos Excel para diferidos externos
"""

import pandas as pd
import asyncio
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import openpyxl
from decimal import Decimal

# Fallback para desarrollo
try:
    from ..config.settings import settings
except ImportError:

    class _FallbackSettings:
        logger = print  # Fallback simple

    settings = _FallbackSettings()


# Mapeo de meses en español a número (del V1 original)
MONTH_MAP = {
    "ENE": "enero",
    "FEB": "febrero",
    "MAR": "marzo",
    "ABR": "abril",
    "MAY": "mayo",
    "JUN": "junio",
    "JUL": "julio",
    "AGO": "agosto",
    "SET": "septiembre",
    "OCT": "octubre",
    "NOV": "noviembre",
    "DIC": "diciembre",
}


class DiferidoExcelReader:
    """Lector especializado para archivos Excel de diferidos externos"""

    def __init__(self):
        self.logger = settings.logger

    async def read_diferido_excel(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Lee archivo Excel de diferidos externos

        Args:
            file_path: Ruta al archivo Excel

        Returns:
            Lista de registros validados
        """
        try:
            # Verificar que el archivo existe
            if not Path(file_path).exists():
                raise FileNotFoundError(f"No se encontró el archivo: {file_path}")

            # Leer Excel en un hilo separado para evitar bloqueo
            df = await asyncio.to_thread(self._read_excel_sync, file_path)

            # Procesar columnas de fechas dinámicas
            df_processed = await self._process_date_columns(df)

            # Convertir a formato estándar
            records = df_processed.to_dict(orient="records")

            self.logger(f"✅ Leídos {len(records)} registros del Excel: {file_path}")
            return records

        except Exception as e:
            self.logger(f"❌ Error leyendo Excel {file_path}: {str(e)}")
            raise

    def _read_excel_sync(self, file_path: str) -> pd.DataFrame:
        """Lee el archivo Excel de manera síncrona"""
        try:
            # Intentar leer con diferentes parámetros
            df = pd.read_excel(file_path, engine="openpyxl", header=0)

            if df.empty:
                raise ValueError("El archivo Excel está vacío")

            return df

        except Exception as e:
            self.logger(f"Error en lectura síncrona: {str(e)}")
            raise

    async def _process_date_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Procesa las columnas de fechas dinámicas (formato mes-YYYY)

        Args:
            df: DataFrame original

        Returns:
            DataFrame procesado con columnas normalizadas
        """
        try:
            df_copy = df.copy()

            # Normalizar nombres de columnas básicas
            column_mapping = {
                "Codigo Liquidacion": "CodigoLiquidacion",
                "Nro Documento": "NroDocumento",
                "Fecha Operacion": "FechaOperacion",
                "Fecha Confirmado": "FechaConfirmado",
                "Moneda": "Moneda",
                "Interes": "Interes",
                "Dias Efectivo": "DiasEfectivo",
            }

            # Aplicar mapeo de columnas básicas
            for old_name, new_name in column_mapping.items():
                if old_name in df_copy.columns:
                    df_copy.rename(columns={old_name: new_name}, inplace=True)

            # Procesar columnas de fechas dinámicas
            date_columns = {}
            for col in df_copy.columns:
                if self._is_date_column(col):
                    # Normalizar valores monetarios
                    df_copy[col] = pd.to_numeric(df_copy[col], errors="coerce").fillna(
                        0
                    )
                    date_columns[col] = col

            # Agregar metadatos de columnas de fechas
            if date_columns:
                df_copy.attrs["date_columns"] = date_columns

            return df_copy

        except Exception as e:
            self.logger(f"Error procesando columnas de fechas: {str(e)}")
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

        # Patrón para detectar columnas como "ene-2024", "feb-2024", etc.
        pattern = r"^(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)-\d{4}$"
        return bool(re.match(pattern, column_name.lower().strip()))

    def convert_date_column_format(self, col: str) -> str:
        """
        Convierte formato de columna de fecha del Excel (ej: '2021AGO')
        al formato estándar (ej: 'agosto-2021')
        """
        import re

        # Buscar patrón YYYY + abreviatura de mes
        match = re.match(r"^(\d{4})([A-Z]{3})$", col.upper())
        if match:
            year, month_abbr = match.groups()
            month_name = MONTH_MAP.get(month_abbr, month_abbr.lower())
            return f"{month_name}-{year}"

        return col  # Retornar original si no coincide


class DiferidoDataSource:
    """Gestor centralizado para fuentes de datos de diferidos"""

    def __init__(self):
        self.excel_reader = DiferidoExcelReader()
        self.logger = settings.logger

    async def get_external_data(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Obtiene datos externos desde archivo Excel

        Args:
            file_path: Ruta al archivo Excel

        Returns:
            Lista de registros de diferidos externos
        """
        try:
            # Leer datos
            records = await self.excel_reader.read_diferido_excel(file_path)

            self.logger(f"✅ Obtenidos {len(records)} registros externos")
            return records

        except Exception as e:
            self.logger(f"❌ Error obteniendo datos externos: {str(e)}")
            raise

    def get_internal_data_config(self, hasta: str) -> Dict[str, Any]:
        """
        Prepara configuración para obtener datos internos

        Args:
            hasta: Período hasta el cual calcular (YYYY-MM)

        Returns:
            Configuración para cálculo interno
        """
        try:
            # Preparar parámetros para cálculo interno
            config = {
                "hasta": hasta,
                "include_detailed_calculations": True,
                "preserve_precision": True,
                "generate_comparison_keys": True,
            }

            self.logger(f"✅ Configuración para datos internos preparada: {hasta}")
            return config

        except Exception as e:
            self.logger(f"❌ Error preparando configuración interna: {str(e)}")
            raise
