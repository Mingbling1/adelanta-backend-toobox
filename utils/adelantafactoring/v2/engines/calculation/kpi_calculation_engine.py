"""
⚙️ KPI Calculation Engine V2 - Motor de cálculos KPI especializados
Maneja toda la lógica de cálculos de KPI y métricas financieras
"""

import pandas as pd
import numpy as np
from typing import Dict, Any
from datetime import datetime

try:
    from utils.adelantafactoring.v2.config.settings import settings
except ImportError:
    # Fallback para desarrollo aislado
    class _FallbackSettings:
        WEBSERVICE_BASE_URL = "https://webservice.adelantafactoring.com"
        KPI_DEFAULT_TIPO_CAMBIO = 3.8
    
    settings = _FallbackSettings()


class KPICalculationEngine:
    """Motor especializado para cálculos KPI y métricas financieras"""
    
    def __init__(self, tipo_cambio_df: pd.DataFrame):
        self.tipo_cambio_df = tipo_cambio_df if not tipo_cambio_df.empty else pd.DataFrame()
        
        # Configuración de cálculos
        self.precision_decimal = 2
        self.default_tipo_cambio = getattr(settings, 'KPI_DEFAULT_TIPO_CAMBIO', 3.8)
        
        # Estadísticas de operación
        self.calculations_performed = 0
        self.last_calculation_time = None
    
    async def apply_kpi_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica lógica completa de KPI al DataFrame
        
        Args:
            df: DataFrame con datos financieros
            
        Returns:
            DataFrame con cálculos KPI aplicados
        """
        try:
            if df.empty:
                return df
            
            self.last_calculation_time = datetime.now()
            self.calculations_performed += 1
            
            # Obtener tipo de cambio actual
            tipo_cambio = self.get_tipo_cambio_actual()
            
            # Aplicar cálculos KPI básicos
            df_processed = self._apply_basic_kpi_calculations(df, tipo_cambio)
            
            # Aplicar cálculos de conversión de moneda
            df_processed = self._apply_currency_conversion(df_processed, tipo_cambio)
            
            # Aplicar cálculos de rentabilidad
            df_processed = self._apply_profitability_calculations(df_processed)
            
            # Aplicar normalización de campos
            df_processed = self._apply_field_normalization(df_processed)
            
            return df_processed
            
        except Exception as e:
            print(f"❌ Error en apply_kpi_logic: {e}")
            return df
    
    def get_tipo_cambio_actual(self) -> float:
        """
        Obtiene el tipo de cambio más reciente
        
        Returns:
            Tipo de cambio de venta actual
        """
        try:
            if self.tipo_cambio_df.empty:
                print(f"⚠️ No hay datos de tipo de cambio, usando {self.default_tipo_cambio} por defecto")
                return self.default_tipo_cambio
            
            # Convertir fecha y ordenar
            tc_df = self.tipo_cambio_df.copy()
            tc_df["TipoCambioFecha"] = pd.to_datetime(tc_df["TipoCambioFecha"])
            ultimo_tc = tc_df.sort_values("TipoCambioFecha", ascending=False).iloc[0]
            
            tipo_cambio = float(ultimo_tc["TipoCambioVenta"])
            
            # Validar que el tipo de cambio sea razonable
            if tipo_cambio <= 0 or tipo_cambio > 10:
                print(f"⚠️ Tipo de cambio inválido ({tipo_cambio}), usando {self.default_tipo_cambio}")
                return self.default_tipo_cambio
            
            return tipo_cambio
            
        except Exception as e:
            print(f"❌ Error obteniendo tipo de cambio: {e}")
            return self.default_tipo_cambio
    
    def _apply_basic_kpi_calculations(self, df: pd.DataFrame, tipo_cambio: float) -> pd.DataFrame:
        """Aplica cálculos KPI básicos"""
        try:
            df = df.copy()
            
            # Validar columnas mínimas requeridas
            df = self._validate_minimum_columns(df)
            
            # Calcular campos básicos si no existen
            if "NetoConfirmado" in df.columns and "SaldoTotal" not in df.columns:
                df["SaldoTotal"] = df["NetoConfirmado"].fillna(0.0)
            
            # Calcular montos en soles si no existen
            if "Moneda" in df.columns and "NetoConfirmado" in df.columns:
                if "NetoConfirmadoSoles" not in df.columns:
                    df["NetoConfirmadoSoles"] = np.where(
                        df["Moneda"] == "PEN",
                        df["NetoConfirmado"],
                        df["NetoConfirmado"] * tipo_cambio
                    )
            
            return df
            
        except Exception as e:
            print(f"❌ Error en cálculos KPI básicos: {e}")
            return df
    
    def _apply_currency_conversion(self, df: pd.DataFrame, tipo_cambio: float) -> pd.DataFrame:
        """Aplica conversiones de moneda a soles"""
        try:
            if "Moneda" not in df.columns:
                return df
            
            # Campos monetarios para convertir
            monetary_fields = [
                "DeudaAnterior", "NetoConfirmado", "FondoResguardo", 
                "MontoCobrar", "MontoDesembolso", "SaldoTotal"
            ]
            
            for field in monetary_fields:
                if field in df.columns:
                    soles_field = f"{field}Soles"
                    if soles_field not in df.columns:
                        df[soles_field] = np.where(
                            df["Moneda"] == "PEN",
                            df[field].fillna(0.0),
                            df[field].fillna(0.0) * tipo_cambio
                        )
            
            return df
            
        except Exception as e:
            print(f"❌ Error en conversión de moneda: {e}")
            return df
    
    def _apply_profitability_calculations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica cálculos de rentabilidad"""
        try:
            # Calcular rentabilidad básica
            if "NetoConfirmado" in df.columns and "DeudaAnterior" in df.columns:
                df["RentabilidadPorc"] = np.where(
                    df["DeudaAnterior"] > 0,
                    ((df["NetoConfirmado"] - df["DeudaAnterior"]) / df["DeudaAnterior"]) * 100,
                    0.0
                )
            
            # Calcular margen de ganancia
            if "NetoConfirmado" in df.columns and "MontoCobrar" in df.columns:
                df["MargenGanancia"] = df["NetoConfirmado"] - df["MontoCobrar"].fillna(0.0)
            
            return df
            
        except Exception as e:
            print(f"❌ Error en cálculos de rentabilidad: {e}")
            return df
    
    def _apply_field_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica normalización de campos"""
        try:
            # Normalizar campos de texto
            text_fields = ["Estado", "TipoOperacion", "Moneda"]
            for field in text_fields:
                if field in df.columns:
                    df[field] = df[field].fillna("").astype(str).str.upper()
            
            # Normalizar campos numéricos
            numeric_fields = [
                "DeudaAnterior", "NetoConfirmado", "FondoResguardo",
                "MontoCobrar", "MontoDesembolso", "SaldoTotal"
            ]
            for field in numeric_fields:
                if field in df.columns:
                    df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0.0)
                    df[field] = df[field].round(self.precision_decimal)
            
            # Normalizar fechas
            date_fields = ["FechaOperacion", "FechaConfirmado"]
            for field in date_fields:
                if field in df.columns:
                    df[field] = pd.to_datetime(df[field], errors='coerce')
                    # Normalizar a medianoche UTC
                    df[field] = df[field].dt.normalize()
            
            return df
            
        except Exception as e:
            print(f"❌ Error en normalización de campos: {e}")
            return df
    
    def _validate_minimum_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida que existan las columnas mínimas necesarias"""
        try:
            required_columns = ["IdLiquidacionCab", "IdLiquidacionDet"]
            
            for col in required_columns:
                if col not in df.columns:
                    print(f"⚠️ Columna requerida faltante: {col}")
                    df[col] = range(len(df))  # Generar IDs temporales
            
            return df
            
        except Exception as e:
            print(f"❌ Error validando columnas mínimas: {e}")
            return df
    
    def calculate_kpi_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calcula métricas KPI del DataFrame
        
        Returns:
            Diccionario con métricas calculadas
        """
        try:
            if df.empty:
                return {"error": "DataFrame vacío"}
            
            metrics = {
                "total_registros": len(df),
                "total_operaciones": df["IdLiquidacionCab"].nunique() if "IdLiquidacionCab" in df.columns else 0,
                "calculation_timestamp": datetime.now().isoformat()
            }
            
            # Métricas monetarias
            if "NetoConfirmadoSoles" in df.columns:
                metrics["total_neto_confirmado_soles"] = float(df["NetoConfirmadoSoles"].sum())
                metrics["promedio_neto_confirmado_soles"] = float(df["NetoConfirmadoSoles"].mean())
            
            # Métricas por moneda
            if "Moneda" in df.columns:
                monedas = df["Moneda"].value_counts().to_dict()
                metrics["distribución_monedas"] = {str(k): int(v) for k, v in monedas.items()}
            
            # Métricas de estado
            if "Estado" in df.columns:
                estados = df["Estado"].value_counts().to_dict()
                metrics["distribución_estados"] = {str(k): int(v) for k, v in estados.items()}
            
            # Estadísticas del motor
            metrics["engine_stats"] = {
                "calculations_performed": self.calculations_performed,
                "last_calculation_time": self.last_calculation_time.isoformat() if self.last_calculation_time else None,
                "tipo_cambio_actual": self.get_tipo_cambio_actual()
            }
            
            return metrics
            
        except Exception as e:
            return {"error": f"Error calculando métricas: {str(e)}"}
    
    def get_engine_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del motor KPI"""
        return {
            "engine_type": "KPICalculationEngine",
            "calculations_performed": self.calculations_performed,
            "last_calculation_time": self.last_calculation_time.isoformat() if self.last_calculation_time else None,
            "tipo_cambio_records": len(self.tipo_cambio_df),
            "default_tipo_cambio": self.default_tipo_cambio,
            "precision_decimal": self.precision_decimal
        }
    
    def reset_stats(self):
        """Reinicia estadísticas del motor"""
        self.calculations_performed = 0
        self.last_calculation_time = None
        print("✅ Estadísticas del motor KPI reiniciadas")
