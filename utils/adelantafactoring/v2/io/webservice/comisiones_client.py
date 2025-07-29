"""
📡 WebserviceClient V2 - Comisiones

Cliente para obtener datos de comisiones desde fuentes externas
"""

import pandas as pd
from typing import Dict, Optional
import asyncio

try:
    from utils.adelantafactoring.v2.config.settings import settings
except ImportError:
    # Fallback básico
    class _FallbackSettings:
        WEBSERVICE_BASE_URL = "https://webservice.adelantafactoring.com"

    settings = _FallbackSettings()


class ComisionesWebserviceClient:
    """Cliente para obtener datos de comisiones"""

    def __init__(self):
        # Configuración básica para V2
        self.base_url = settings.WEBSERVICE_BASE_URL

    def get_kpi_data(self, fecha_corte: Optional[str] = None) -> pd.DataFrame:
        """
        Obtiene datos KPI base para cálculos de comisiones

        Args:
            fecha_corte: Fecha de corte para filtrar datos

        Returns:
            DataFrame con datos KPI
        """
        try:
            # Simulación de datos KPI para V2
            # En V1 esto viene del parámetro kpi_df del constructor
            sample_data = {
                "CodigoLiquidacion": ["LIQ001", "LIQ002", "LIQ003"],
                "RUCCliente": ["12345678901", "98765432109", "11111111111"],
                "RUCPagador": ["20123456789", "20987654321", "20111111111"],
                "MontoDesembolso": [10000.0, 15000.0, 20000.0],
                "TasaNominalMensualPorc": [0.025, 0.03, 0.028],
                "DiasEfectivo": [30, 45, 60],
                "FechaOperacion": pd.to_datetime(
                    ["2024-01-15", "2024-02-20", "2024-03-10"]
                ),
                "TipoOperacion": ["Factoring", "Confirming", "Capital de Trabajo"],
                "Ejecutivo": ["LEO", "CRISTIAN", "GUADALUPE"],
            }

            df = pd.DataFrame(sample_data)

            if fecha_corte:
                fecha_corte_dt = pd.to_datetime(fecha_corte)
                df = df[df["FechaOperacion"] <= fecha_corte_dt]

            return df

        except Exception:
            # Fallback a datos vacíos en caso de error
            return pd.DataFrame()

    async def get_kpi_data_async(
        self, fecha_corte: Optional[str] = None
    ) -> pd.DataFrame:
        """Versión async del método get_kpi_data"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.get_kpi_data, fecha_corte)

    def get_referidos_data(self) -> pd.DataFrame:
        """
        Obtiene datos de referidos necesarios para comisiones

        Returns:
            DataFrame con datos de referidos
        """
        try:
            # Datos simulados para referidos
            # En producción esto vendría de ReferidosCalcular
            sample_data = {
                "RUCCliente": ["12345678901", "98765432109"],
                "Referencia": ["REF001", "REF002"],
                "EjecutivoReferido": ["LEO", "CRISTIAN"],
            }

            return pd.DataFrame(sample_data)

        except Exception:
            return pd.DataFrame()

    def get_fondos_data(self) -> Dict[str, pd.DataFrame]:
        """
        Obtiene datos de fondos especiales (Crecer, Promocional)

        Returns:
            Dict con DataFrames de fondos
        """
        try:
            # Datos de Fondo Crecer
            fondo_crecer = pd.DataFrame(
                {
                    "CodigoLiquidacion": ["LIQ001", "LIQ002"],
                    "Garantia": [0.75, 0.80],  # Ya convertidos a decimal
                }
            )

            # Datos de Fondo Promocional
            fondo_promocional = pd.DataFrame(
                {"CodigoLiquidacion": ["LIQ003"], "TipoFondo": ["Promocional"]}
            )

            return {
                "fondo_crecer": fondo_crecer,
                "fondo_promocional": fondo_promocional,
            }

        except Exception:
            return {"fondo_crecer": pd.DataFrame(), "fondo_promocional": pd.DataFrame()}

    async def get_fondos_data_async(self) -> Dict[str, pd.DataFrame]:
        """Versión async del método get_fondos_data"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.get_fondos_data)
