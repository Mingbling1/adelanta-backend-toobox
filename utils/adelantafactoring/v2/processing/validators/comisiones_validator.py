"""
✅ Validator V2 - Comisiones

Validaciones especializadas para datos de comisiones usando Pydantic
"""

import pandas as pd
from typing import List, Dict, Any

# Importaciones absolutas y seguras
try:
    from utils.adelantafactoring.v2.schemas.comisiones_schema import (
        ComisionesSchema,
        RegistroComisionSchema,
        PromocionSchema,
    )
except ImportError:
    # Fallback simple para desarrollo
    class ComisionesSchema:
        model_fields = {
            "RUCCliente": None,
            "RUCPagador": None,
            "Tipo": None,
            "Detalle": None,
        }

        def __init__(self, **kwargs):
            pass

        def model_dump(self):
            return {}

    class RegistroComisionSchema:
        model_fields = {"RUCCliente": None, "RUCPagador": None, "Tipo": None}

        def __init__(self, **kwargs):
            pass

        def model_dump(self):
            return {}

    class PromocionSchema:
        model_fields = {"Ejecutivo": None, "TipoOperacion": None}

        def __init__(self, **kwargs):
            pass

        def model_dump(self):
            return {}


class ComisionesValidator:
    """Validador para datos de comisiones con Pydantic"""

    def __init__(self):
        """Inicializa el validador"""
        pass

    def validate_comisiones_bulk(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Valida múltiples registros de comisiones usando Pydantic

        Args:
            df: DataFrame con datos de comisiones

        Returns:
            Lista de diccionarios validados
        """
        if df.empty:
            return []

        # Obtener campos del schema
        schema_fields = set(ComisionesSchema.model_fields.keys())
        df_filtered = df[df.columns.intersection(schema_fields)]

        # Validar usando list comprehension optimizada
        validated_records = []
        for registro in df_filtered.to_dict(orient="records"):
            try:
                validated = ComisionesSchema(**registro)
                validated_records.append(validated.model_dump())
            except Exception as e:
                # Log error pero continúa procesando
                print(f"Error validando registro: {e}")
                continue

        return validated_records

    def validate_registro_comision_bulk(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Valida registros individuales de comisión (compatibilidad v1)

        Args:
            df: DataFrame con registros de comisión

        Returns:
            Lista de diccionarios validados
        """
        if df.empty:
            return []

        schema_fields = set(RegistroComisionSchema.model_fields.keys())
        df_filtered = df[df.columns.intersection(schema_fields)]

        validated_records = []
        for registro in df_filtered.to_dict(orient="records"):
            try:
                validated = RegistroComisionSchema(**registro)
                validated_records.append(validated.model_dump())
            except Exception as e:
                print(f"Error validando registro comisión: {e}")
                continue

        return validated_records

    def validate_promociones_bulk(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Valida datos de promociones

        Args:
            df: DataFrame con datos de promociones

        Returns:
            Lista de diccionarios validados
        """
        if df.empty:
            return []

        schema_fields = set(PromocionSchema.model_fields.keys())
        df_filtered = df[df.columns.intersection(schema_fields)]

        validated_records = []
        for registro in df_filtered.to_dict(orient="records"):
            try:
                validated = PromocionSchema(**registro)
                validated_records.append(validated.model_dump())
            except Exception as e:
                print(f"Error validando promoción: {e}")
                continue

        return validated_records

    def validate_required_columns(
        self, df: pd.DataFrame, required_columns: List[str]
    ) -> bool:
        """
        Valida que el DataFrame tenga las columnas requeridas

        Args:
            df: DataFrame a validar
            required_columns: Lista de columnas requeridas

        Returns:
            True si todas las columnas están presentes
        """
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            print(f"Columnas faltantes: {missing_columns}")
            return False
        return True

    def validate_ejecutivo_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Valida y normaliza nombres de ejecutivos

        Args:
            df: DataFrame con columna 'Ejecutivo'

        Returns:
            DataFrame con nombres validados
        """
        if "Ejecutivo" not in df.columns:
            return df

        df = df.copy()

        # Validar que no hay valores nulos
        null_ejecutivos = df["Ejecutivo"].isnull().sum()
        if null_ejecutivos > 0:
            print(f"Advertencia: {null_ejecutivos} registros con ejecutivo nulo")
            df["Ejecutivo"] = df["Ejecutivo"].fillna("SIN_ASIGNAR")

        # Normalizar espacios y mayúsculas
        df["Ejecutivo"] = df["Ejecutivo"].str.strip().str.upper()

        return df

    def validate_montos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Valida campos de montos financieros

        Args:
            df: DataFrame con campos de montos

        Returns:
            DataFrame con montos validados
        """
        df = df.copy()

        # Columnas de montos a validar
        monto_columns = [col for col in df.columns if "Monto" in col or "Costo" in col]

        for col in monto_columns:
            if col in df.columns:
                # Convertir a numérico, valores inválidos a 0
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

                # Validar que no hay valores negativos (excepto si es permitido)
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    print(f"Advertencia: {negative_count} valores negativos en {col}")

        return df

    def validate_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Valida campos de fechas

        Args:
            df: DataFrame con campos de fechas

        Returns:
            DataFrame con fechas validadas
        """
        df = df.copy()

        # Columnas de fechas a validar
        date_columns = [col for col in df.columns if "Fecha" in col]

        for col in date_columns:
            if col in df.columns:
                # Convertir a datetime, valores inválidos a NaT
                df[col] = pd.to_datetime(df[col], errors="coerce")

                # Contar valores nulos después de conversión
                null_dates = df[col].isnull().sum()
                if null_dates > 0:
                    print(f"Advertencia: {null_dates} fechas inválidas en {col}")

        return df

    def get_validation_summary(
        self, original_count: int, validated_count: int
    ) -> Dict[str, Any]:
        """
        Genera resumen de validación

        Args:
            original_count: Número de registros originales
            validated_count: Número de registros validados exitosamente

        Returns:
            Diccionario con resumen de validación
        """
        success_rate = (
            (validated_count / original_count * 100) if original_count > 0 else 0
        )

        return {
            "registros_originales": original_count,
            "registros_validados": validated_count,
            "registros_rechazados": original_count - validated_count,
            "tasa_exito": round(success_rate, 2),
            "estado": (
                "OK"
                if success_rate >= 95
                else "ADVERTENCIA" if success_rate >= 80 else "ERROR"
            ),
        }
