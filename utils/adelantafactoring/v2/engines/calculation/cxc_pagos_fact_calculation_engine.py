"""
⚙️ Motor de cálculo para CXC Pagos Fact - Lógica financiera especializada
"""

try:
    from utils.adelantafactoring.v2.config.settings import settings
except ImportError:
    # Fallback para desarrollo aislado
    class _FallbackSettings:
        WEBSERVICE_BASE_URL = "https://webservice.adelantafactoring.com"

    settings = _FallbackSettings()

import pandas as pd
import asyncio
from typing import List, Dict, Optional
from decimal import Decimal
from config.logger import logger


class CXCPagosFactCalculationEngine:
    """Motor especializado para cálculos de pagos de facturas CXC"""

    def __init__(self):
        self.logger = logger

    async def process_pagos_async(self, raw_data: List[Dict]) -> pd.DataFrame:
        """
        Procesa datos de pagos de forma asíncrona

        Args:
            raw_data: Lista de diccionarios con datos raw de pagos

        Returns:
            DataFrame procesado con cálculos aplicados
        """
        return await asyncio.to_thread(self.process_pagos, raw_data)

    def process_pagos(self, raw_data: List[Dict]) -> pd.DataFrame:
        """
        Procesa datos de pagos aplicando lógica financiera

        Args:
            raw_data: Lista de diccionarios con datos raw de pagos

        Returns:
            DataFrame procesado con cálculos aplicados
        """
        if not raw_data:
            self.logger.warning("No hay datos de pagos para procesar")
            return pd.DataFrame()

        try:
            # Convertir a DataFrame
            df = pd.DataFrame(raw_data)
            self.logger.debug(f"DataFrame de pagos creado con {len(df)} filas")

            # Aplicar transformaciones financieras
            df = self._apply_financial_transformations(df)

            # Aplicar cálculos específicos de pagos
            df = self._calculate_payment_metrics(df)

            # Normalizar campos de fecha
            df = self._normalize_date_fields(df)

            self.logger.info(f"Procesamiento de pagos completado: {len(df)} registros")
            return df

        except Exception as e:
            self.logger.error(f"Error procesando datos de pagos: {e}")
            raise e

    def _apply_financial_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformaciones financieras específicas

        Args:
            df: DataFrame con datos de pagos

        Returns:
            DataFrame con transformaciones aplicadas
        """
        try:
            # Convertir campos numéricos preservando precisión
            numeric_fields = [
                "MontoCobrarPago",
                "MontoPago",
                "InteresPago",
                "GastosPago",
                "SaldoDeuda",
                "ExcesoPago",
            ]

            for field in numeric_fields:
                if field in df.columns:
                    df[field] = pd.to_numeric(df[field], errors="coerce").fillna(0)

            # Convertir campos enteros
            integer_fields = ["IdLiquidacionPago", "IdLiquidacionDet", "DiasMora"]
            for field in integer_fields:
                if field in df.columns:
                    df[field] = (
                        pd.to_numeric(df[field], errors="coerce").fillna(0).astype(int)
                    )

            # Limpiar campos de texto
            string_fields = ["ObservacionPago", "TipoPago"]
            for field in string_fields:
                if field in df.columns:
                    df[field] = df[field].fillna("").astype(str)

            return df

        except Exception as e:
            self.logger.error(f"Error aplicando transformaciones financieras: {e}")
            raise e

    def _calculate_payment_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula métricas específicas de pagos

        Args:
            df: DataFrame con datos de pagos

        Returns:
            DataFrame con métricas calculadas
        """
        try:
            if df.empty:
                return df

            # Calcular porcentaje de pago
            if "MontoPago" in df.columns and "MontoCobrarPago" in df.columns:
                df["PorcentajePago"] = (
                    df["MontoPago"] / df["MontoCobrarPago"].replace(0, 1) * 100
                ).round(2)

            # Calcular monto total (principal + intereses + gastos)
            required_cols = ["MontoPago", "InteresPago", "GastosPago"]
            if all(col in df.columns for col in required_cols):
                df["MontoTotalPago"] = (
                    df["MontoPago"] + df["InteresPago"] + df["GastosPago"]
                )

            # Clasificar tipo de pago
            if "TipoPago" in df.columns:
                df["ClasificacionPago"] = df["TipoPago"].apply(
                    self._classify_payment_type
                )

            # Calcular días desde pago (si existe fecha de pago)
            if "FechaPago" in df.columns:
                df["FechaPago"] = pd.to_datetime(df["FechaPago"], errors="coerce")
                current_date = pd.Timestamp.now()
                df["DiasDesdesPago"] = (current_date - df["FechaPago"]).dt.days

            return df

        except Exception as e:
            self.logger.error(f"Error calculando métricas de pagos: {e}")
            raise e

    def _classify_payment_type(self, tipo_pago: str) -> str:
        """
        Clasifica el tipo de pago en categorías estándar

        Args:
            tipo_pago: Tipo de pago original

        Returns:
            Clasificación estándar del tipo de pago
        """
        if not isinstance(tipo_pago, str):
            return "NO_CLASIFICADO"

        tipo_upper = tipo_pago.upper().strip()

        if "COMPLETO" in tipo_upper or "TOTAL" in tipo_upper:
            return "PAGO_COMPLETO"
        elif "PARCIAL" in tipo_upper:
            return "PAGO_PARCIAL"
        elif "ADELANTO" in tipo_upper or "ANTICIPO" in tipo_upper:
            return "ADELANTO"
        elif "EXCESO" in tipo_upper:
            return "EXCESO"
        elif tipo_upper == "" or tipo_upper == "NULL":
            return "SIN_ESPECIFICAR"
        else:
            return "OTRO"

    def _normalize_date_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza campos de fecha a formato estándar

        Args:
            df: DataFrame con campos de fecha

        Returns:
            DataFrame con fechas normalizadas
        """
        try:
            date_fields = ["FechaPago", "FechaPagoCreacion", "FechaPagoModificacion"]

            for field in date_fields:
                if field in df.columns:
                    df[field] = pd.to_datetime(df[field], errors="coerce")
                    # Normalizar a medianoche UTC para consistencia
                    df[field] = df[field].dt.normalize()

            return df

        except Exception as e:
            self.logger.error(f"Error normalizando campos de fecha: {e}")
            return df

    def calculate_payment_summary(self, df: pd.DataFrame) -> Dict:
        """
        Calcula resumen estadístico de pagos

        Args:
            df: DataFrame con datos de pagos procesados

        Returns:
            Diccionario con estadísticas de pagos
        """
        if df.empty:
            return {
                "total_registros": 0,
                "monto_total_pagos": 0,
                "promedio_pago": 0,
                "tipos_pago": {},
            }

        try:
            summary = {
                "total_registros": len(df),
                "monto_total_pagos": (
                    float(df["MontoPago"].sum()) if "MontoPago" in df.columns else 0
                ),
                "promedio_pago": (
                    float(df["MontoPago"].mean()) if "MontoPago" in df.columns else 0
                ),
                "tipos_pago": (
                    df["TipoPago"].value_counts().to_dict()
                    if "TipoPago" in df.columns
                    else {}
                ),
                "dias_mora_promedio": (
                    float(df["DiasMora"].mean()) if "DiasMora" in df.columns else 0
                ),
                "saldo_deuda_total": (
                    float(df["SaldoDeuda"].sum()) if "SaldoDeuda" in df.columns else 0
                ),
            }

            return summary

        except Exception as e:
            self.logger.error(f"Error calculando resumen de pagos: {e}")
            return {"error": str(e)}
