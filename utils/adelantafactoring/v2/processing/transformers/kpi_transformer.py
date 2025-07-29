"""
🔄 KPI Transformer V2 - Transformador especializado para datos KPI
"""

import pandas as pd
import logging
from datetime import datetime
from typing import Union, List, Dict, Any

# Logger local para V2 (aislado)
logger = logging.getLogger(__name__)

from ...engines.data_engine import DataEngine
from ...engines.calculation_engine import CalculationEngine
from ...engines.validation_engine import ValidationEngine
from ...io.webservice.kpi_adapter import KPIWebserviceAdapter


class KPITransformer:
    """
    Transformador especializado para procesar datos KPI
    Implementa el pipeline completo de transformación
    """

    def __init__(self, tipo_cambio_df: pd.DataFrame):
        """
        Inicializa el transformador con DataFrame de tipo de cambio

        Args:
            tipo_cambio_df: DataFrame con tipos de cambio históricos
        """
        self.tipo_cambio_df = tipo_cambio_df

        # Inicializar engines
        self.data_engine = DataEngine()
        self.calculation_engine = CalculationEngine()
        self.validation_engine = ValidationEngine()
        self.webservice_adapter = KPIWebserviceAdapter()

        # Columnas requeridas mínimas
        self.required_columns = [
            "Ejecutivo",
            "FechaOperacion",
            "NetoConfirmado",
            "Moneda",
            "MontoDesembolso",
            "DiasEfectivo",
            "ComisionEstructuracionConIGV",
            "Interes",
            "GastosDiversosConIGV",
            "RUCCliente",
            "RUCPagador",
            "RazonSocialPagador",
            "CodigoLiquidacion",
            "NroDocumento",
        ]

    async def transform_kpi_data(
        self,
        start_date: datetime,
        end_date: datetime,
        fecha_corte: datetime,
        tipo_reporte: int = 2,
        as_df: bool = False,
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Pipeline completo de transformación KPI

        Args:
            start_date: Fecha inicio del período
            end_date: Fecha fin del período
            fecha_corte: Fecha de corte para el reporte
            tipo_reporte: Tipo de reporte (2=normal, 0=acumulado)
            as_df: Si True, retorna DataFrame; si False, lista de dicts

        Returns:
            Datos KPI transformados y validados
        """
        logger.info(
            f"🔄 Iniciando transformación KPI - Período: {start_date} a {end_date}"
        )

        try:
            # 1. Obtener datos raw del webservice
            raw_data = await self.webservice_adapter.obtener_colocaciones(
                start_date, end_date, fecha_corte, tipo_reporte
            )
            df = pd.DataFrame(raw_data)

            # 2. Validaciones básicas
            df = self._validate_and_clean_raw_data(df)

            # 3. Enriquecimiento con datos externos
            df = await self._enrich_with_external_data(df)

            # 4. Transformaciones de datos
            df = self._apply_data_transformations(df)

            # 5. Cálculos financieros
            df = self._calculate_financial_metrics(df)

            # 6. Validación final con Pydantic
            result = self._validate_final_output(df, tipo_reporte, as_df)

            logger.info("✅ Transformación KPI completada exitosamente")
            return result

        except Exception as e:
            logger.error(f"❌ Error en transformación KPI: {e}")
            raise

    def _validate_and_clean_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida y limpia datos raw del webservice"""
        logger.debug("🧹 Validando y limpiando datos raw...")

        # Validar columnas mínimas
        df = self.data_engine.validate_required_columns(df, self.required_columns)

        # Limpiar tipos de datos
        df = self.validation_engine.validate_data_types(df)

        return df

    async def _enrich_with_external_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enriquece con datos externos (operaciones fuera de sistema, etc.)"""
        logger.debug("🔗 Enriqueciendo con datos externos...")

        # TODO: Implementar integración con otros módulos v2
        # Por ahora, solo añadir columna FueraSistema por defecto
        df["FueraSistema"] = "no"

        return df

    def _apply_data_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica transformaciones estándar de datos"""
        logger.debug("🔧 Aplicando transformaciones de datos...")

        # Convertir fechas
        date_columns = ["FechaOperacion", "FechaConfirmado", "FechaDesembolso"]
        df = self.data_engine.convert_date_columns(df, date_columns)

        # Limpiar strings y códigos
        df = self.data_engine.clean_string_columns(df)

        # Crear columnas calculadas básicas
        df = self.data_engine.create_calculated_columns(df)

        # Fusionar con tipos de cambio
        df = self.data_engine.merge_exchange_rates(df, self.tipo_cambio_df)

        return df

    def _calculate_financial_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula métricas financieras completas"""
        logger.debug("📊 Calculando métricas financieras...")

        # Limpiar columnas numéricas financieras
        numeric_columns = [
            "NetoConfirmado",
            "MontoDesembolso",
            "MontoPago",
            "ComisionEstructuracionConIGV",
            "Interes",
            "GastosDiversosConIGV",
            "DiasEfectivo",
            "TipoCambioVenta",
        ]
        df = self.data_engine.clean_numeric_columns(df, numeric_columns)

        # Crear conversiones básicas de moneda
        df = self.data_engine.create_financial_calculations(df)

        # Ejecutar todos los cálculos KPI
        df = self.calculation_engine.calculate_all_kpi_metrics(df)

        # TODO: Integrar cálculo de referidos cuando esté en v2
        # df = self._calculate_referidos(df)

        # TODO: Integrar merge con sector pagadores cuando esté en v2
        # df = self._merge_sector_pagadores(df)

        return df

    def _validate_final_output(
        self, df: pd.DataFrame, tipo_reporte: int, as_df: bool
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """Validación final con esquemas Pydantic"""
        logger.debug("✅ Validando salida final...")

        # Asegurar precisión financiera
        df = self.validation_engine.ensure_financial_precision(df)

        # Validar con Pydantic
        result = self.validation_engine.validate_kpi_data(
            df, tipo_reporte, return_as_df=as_df
        )

        return result

    def get_compatibility_report(
        self, df: pd.DataFrame, tipo_reporte: int = 2
    ) -> Dict[str, Any]:
        """
        Genera reporte de compatibilidad del DataFrame con esquemas

        Args:
            df: DataFrame a analizar
            tipo_reporte: Tipo de reporte para seleccionar esquema

        Returns:
            Diccionario con reporte de compatibilidad
        """
        from ...schemas.kpi_schema import KPICalcularSchema, KPIAcumuladoCalcularSchema

        schema_class = (
            KPIAcumuladoCalcularSchema if tipo_reporte == 0 else KPICalcularSchema
        )

        return self.validation_engine.check_schema_compatibility(df, schema_class)
