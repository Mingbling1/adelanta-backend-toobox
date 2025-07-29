"""
✅ Validation Engine V2 - Motor de validación con Pydantic optimizado
"""

import pandas as pd
import logging
from typing import List, Dict, Any, Union
from pydantic import ValidationError

# Logger local para V2 (aislado)
logger = logging.getLogger(__name__)

from ..schemas.kpi_schema import KPICalcularSchema, KPIAcumuladoCalcularSchema


class ValidationEngine:
    """
    Motor especializado para validación de datos con Pydantic
    Optimizado para list comprehensions y alto rendimiento
    """

    def __init__(self):
        self.kpi_schema = KPICalcularSchema
        self.kpi_acumulado_schema = KPIAcumuladoCalcularSchema

    def validate_kpi_data(
        self, df: pd.DataFrame, tipo_reporte: int = 2, return_as_df: bool = False
    ) -> Union[List[Dict[str, Any]], pd.DataFrame]:
        """
        Valida datos KPI usando esquemas Pydantic optimizados

        Args:
            df: DataFrame con datos a validar
            tipo_reporte: Tipo de reporte (2=normal, 0=acumulado)
            return_as_df: Si True, retorna DataFrame; si False, lista de dicts

        Returns:
            Datos validados como lista de dicts o DataFrame

        Raises:
            ValidationError: Si hay errores de validación
        """
        logger.info(f"🔍 Validando datos KPI - Tipo reporte: {tipo_reporte}")

        try:
            # Seleccionar esquema según tipo de reporte
            schema_class = (
                self.kpi_acumulado_schema if tipo_reporte == 0 else self.kpi_schema
            )

            # Filtrar columnas que existen en el esquema (optimización)
            schema_fields = set(schema_class.model_fields.keys())
            available_columns = df.columns.intersection(schema_fields)
            df_filtered = df[available_columns]

            logger.debug(
                f"📊 Columnas filtradas: {len(available_columns)}/{len(schema_fields)}"
            )

            # Validación con list comprehension optimizada
            records = df_filtered.to_dict(orient="records")
            validated_models = [schema_class(**record) for record in records]

            # Serialización
            validated_data = [model.model_dump() for model in validated_models]

            logger.info(f"✅ Validación exitosa: {len(validated_data)} registros")

            if return_as_df:
                return pd.DataFrame(validated_data)
            return validated_data

        except ValidationError as err:
            logger.error(
                f"❌ Error de validación KPI (tipo_reporte={tipo_reporte}): {err}"
            )
            raise
        except Exception as e:
            logger.error(f"❌ Error inesperado en validación: {e}")
            raise

    def validate_required_fields(
        self, data: Dict[str, Any], required_fields: List[str]
    ) -> Dict[str, Any]:
        """
        Valida campos requeridos en un diccionario individual

        Args:
            data: Diccionario con datos a validar
            required_fields: Lista de campos obligatorios

        Returns:
            Diccionario validado

        Raises:
            ValueError: Si faltan campos requeridos
        """
        missing_fields = [
            field
            for field in required_fields
            if field not in data or data[field] is None
        ]

        if missing_fields:
            raise ValueError(f"Campos requeridos faltantes: {missing_fields}")

        return data

    def ensure_financial_precision(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Asegura precisión financiera en columnas monetarias

        Args:
            df: DataFrame con datos financieros

        Returns:
            DataFrame con precisión financiera garantizada
        """
        df = df.copy()

        # Columnas monetarias que requieren precisión decimal
        monetary_columns = [
            "NetoConfirmado",
            "MontoDesembolso",
            "MontoPago",
            "MontoDevolucion",
            "ComisionEstructuracionConIGV",
            "Interes",
            "GastosDiversosConIGV",
            "ColocacionSoles",
            "MontoDesembolsoSoles",
            "MontoPagoSoles",
            "IngresosSoles",
            "CostosFondoSoles",
            "TotalIngresosSoles",
            "Utilidad",
        ]

        for col in monetary_columns:
            if col in df.columns:
                # Redondear a 2 decimales para precisión financiera
                df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

        return df

    def check_schema_compatibility(
        self, df: pd.DataFrame, schema_class: type
    ) -> Dict[str, Any]:
        """
        Verifica compatibilidad entre DataFrame y esquema Pydantic

        Args:
            df: DataFrame a verificar
            schema_class: Clase de esquema Pydantic

        Returns:
            Diccionario con resultados de compatibilidad
        """
        schema_fields = set(schema_class.model_fields.keys())
        df_columns = set(df.columns)

        missing_in_df = schema_fields - df_columns
        extra_in_df = df_columns - schema_fields
        common_fields = schema_fields.intersection(df_columns)

        compatibility_report = {
            "schema_fields_count": len(schema_fields),
            "df_columns_count": len(df_columns),
            "common_fields_count": len(common_fields),
            "missing_in_df": list(missing_in_df),
            "extra_in_df": list(extra_in_df),
            "compatibility_percentage": len(common_fields) / len(schema_fields) * 100,
        }

        logger.info(
            f"📋 Compatibilidad de esquema: {compatibility_report['compatibility_percentage']:.1f}%"
        )

        return compatibility_report

    def validate_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Valida y corrige tipos de datos comunes

        Args:
            df: DataFrame a validar

        Returns:
            DataFrame con tipos corregidos
        """
        df = df.copy()

        # Asegurar que códigos sean strings
        string_columns = [
            "CodigoLiquidacion",
            "RUCCliente",
            "RUCPagador",
            "NroDocumento",
        ]
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # Asegurar que cantidades sean float
        numeric_columns = ["NetoConfirmado", "MontoDesembolso", "DiasEfectivo"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df
