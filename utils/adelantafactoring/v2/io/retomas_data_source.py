"""Retomas data source for v2 architecture."""

import pandas as pd
from datetime import datetime
from typing import Any, Dict, List
from utils.adelantafactoring.v2.io.base_data_source import BaseDataSource


class RetomasDataSource(BaseDataSource):
    """Data source for retomas financial processing."""
    
    # Field constants from original RetomasCalcular
    RUC_PAGADOR = "RUCPagador"
    RAZON_SOCIAL_PAGADOR = "RazonSocialPagador"
    RUC_CLIENTE = "RUCCliente"
    RAZON_SOCIAL_CLIENTE = "RazonSocialCliente"
    MONTO_DESEMBOLSO_SOLES = "MontoDesembolsoSoles"
    MONTO_PAGO_SOLES = "MontoPagoSoles"
    FECHA_OPERACION = "FechaOperacion"
    FECHA_PAGO = "FechaPago"
    
    def __init__(self, dataframe: pd.DataFrame):
        """Initialize with pandas DataFrame."""
        self.df = self._normalize_dataframe(dataframe)
    
    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize DataFrame with financial data patterns."""
        df_copy = df.copy()
        
        # Normalize datetime columns to tz-naive as per original logic
        date_columns = [self.FECHA_OPERACION, self.FECHA_PAGO]
        for col in date_columns:
            if col in df_copy.columns:
                df_copy[col] = pd.to_datetime(df_copy[col], errors="coerce")
                # Convert to tz-naive as per original RetomasCalcular
                df_copy[col] = df_copy[col].dt.tz_localize(None) if df_copy[col].dt.tz is not None else df_copy[col]
        
        return df_copy
    
    def calcular_desembolsos(self, fecha_corte: datetime) -> pd.DataFrame:
        """Calculate desembolsos filtered by fecha_corte."""
        # Ensure fecha_corte is tz-naive
        fecha_corte = fecha_corte.replace(tzinfo=None)
        
        return (
            self.df.loc[
                self.df[self.FECHA_OPERACION] >= fecha_corte,
                [
                    self.RUC_PAGADOR,
                    self.RAZON_SOCIAL_PAGADOR,
                    self.MONTO_DESEMBOLSO_SOLES,
                ],
            ]
            .groupby(by=[self.RUC_PAGADOR, self.RAZON_SOCIAL_PAGADOR])
            .agg({self.MONTO_DESEMBOLSO_SOLES: "sum"})
        )
    
    def calcular_cobranzas(self, fecha_corte: datetime) -> pd.DataFrame:
        """Calculate cobranzas filtered by fecha_corte."""
        # Ensure fecha_corte is tz-naive
        fecha_corte = fecha_corte.replace(tzinfo=None)
        
        return (
            self.df.loc[
                self.df[self.FECHA_PAGO] >= fecha_corte,
                [
                    self.RUC_PAGADOR,
                    self.RAZON_SOCIAL_PAGADOR,
                    self.MONTO_PAGO_SOLES,
                ],
            ]
            .groupby(by=[self.RUC_PAGADOR, self.RAZON_SOCIAL_PAGADOR])
            .agg({self.MONTO_PAGO_SOLES: "sum"})
        )
    
    def get_data(self, **filters) -> List[Dict[str, Any]]:
        """Get data as list of dictionaries."""
        return self.df.to_dict(orient="records")
    
    def get_dataframe(self) -> pd.DataFrame:
        """Get underlying dataframe."""
        return self.df
