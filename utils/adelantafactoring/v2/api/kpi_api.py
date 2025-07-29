"""
🌐 KPI API V2 - Interfaz pública simplificada para KPI
"""

import pandas as pd
from datetime import datetime
from typing import Union, List, Dict, Any
from config.logger import logger
from ..processing.transformers.kpi_transformer import KPITransformer


class KPIV2:
    """
    Interfaz pública simplificada para cálculos KPI v2
    Mantiene compatibilidad con la API original
    """

    def __init__(self, tipo_cambio_df: pd.DataFrame):
        """
        Inicializa la API KPI v2

        Args:
            tipo_cambio_df: DataFrame con tipos de cambio históricos
        """
        self.transformer = KPITransformer(tipo_cambio_df)
        logger.info("🚀 KPI API V2 inicializada exitosamente")

    async def calculate_monthly(
        self,
        start_date: datetime,
        end_date: datetime,
        fecha_corte: datetime,
        tipo_reporte: int = 2,
        as_df: bool = False,
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Calcula KPIs mensuales con pipeline optimizado

        Args:
            start_date: Fecha inicio del período
            end_date: Fecha fin del período
            fecha_corte: Fecha de corte para el reporte
            tipo_reporte: Tipo de reporte (2=normal, 0=acumulado)
            as_df: Si True, retorna DataFrame; si False, lista de dicts

        Returns:
            Datos KPI calculados y validados
        """
        logger.info(
            f"📊 Calculando KPIs mensuales - {start_date.strftime('%Y-%m')} a {end_date.strftime('%Y-%m')}"
        )

        result = await self.transformer.transform_kpi_data(
            start_date=start_date,
            end_date=end_date,
            fecha_corte=fecha_corte,
            tipo_reporte=tipo_reporte,
            as_df=as_df,
        )

        record_count = len(result) if isinstance(result, list) else len(result)
        logger.info(f"✅ KPIs mensuales calculados: {record_count} registros")

        return result

    async def calculate_accumulated(
        self,
        start_date: datetime,
        end_date: datetime,
        fecha_corte: datetime,
        as_df: bool = False,
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Calcula KPIs acumulados (tipo_reporte = 0)

        Args:
            start_date: Fecha inicio del período
            end_date: Fecha fin del período
            fecha_corte: Fecha de corte para el reporte
            as_df: Si True, retorna DataFrame; si False, lista de dicts

        Returns:
            Datos KPI acumulados calculados y validados
        """
        return await self.calculate_monthly(
            start_date=start_date,
            end_date=end_date,
            fecha_corte=fecha_corte,
            tipo_reporte=0,  # Acumulado
            as_df=as_df,
        )

    def get_compatibility_report(
        self, sample_data: pd.DataFrame, tipo_reporte: int = 2
    ) -> Dict[str, Any]:
        """
        Genera reporte de compatibilidad para datos de entrada

        Args:
            sample_data: Datos de muestra para analizar
            tipo_reporte: Tipo de reporte para verificar esquema

        Returns:
            Reporte detallado de compatibilidad
        """
        return self.transformer.get_compatibility_report(sample_data, tipo_reporte)


# Función de conveniencia para compatibilidad con v1
async def calculate_kpi_v2(
    tipo_cambio_df: pd.DataFrame,
    start_date: datetime,
    end_date: datetime,
    fecha_corte: datetime,
    tipo_reporte: int = 2,
    as_df: bool = False,
) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Función de conveniencia para cálculo directo de KPIs
    Mantiene compatibilidad con la interfaz original

    Args:
        tipo_cambio_df: DataFrame con tipos de cambio
        start_date: Fecha inicio del período
        end_date: Fecha fin del período
        fecha_corte: Fecha de corte para el reporte
        tipo_reporte: Tipo de reporte (2=normal, 0=acumulado)
        as_df: Si True, retorna DataFrame; si False, lista de dicts

    Returns:
        Datos KPI calculados y validados
    """
    kpi_api = KPIV2(tipo_cambio_df)
    return await kpi_api.calculate_monthly(
        start_date, end_date, fecha_corte, tipo_reporte, as_df
    )
