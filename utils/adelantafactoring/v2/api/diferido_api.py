"""
🌐 API V2 - Diferido

Interfaz pública simple para cálculos y comparaciones de diferidos
"""

import pandas as pd
from typing import Dict, List, Any
from datetime import datetime

# Fallback para desarrollo
try:
    from ..engines.diferido_calculation_engine import (
        DiferidoCalculationEngine,
        DiferidoComparisonEngine,
    )
    from ..io.diferido_data_source import DiferidoDataSource
    from ..processing.diferido_transformer import DiferidoTransformer
    from ..schemas.diferido_schema import DiferidoRequestSchema, DiferidoResponseSchema
    from ..config.settings import settings
except ImportError:

    class _FallbackSettings:
        logger = print

    settings = _FallbackSettings()

    # Fallback classes simples
    class DiferidoCalculationEngine:
        def __init__(self):
            pass

        async def calculate_diferido_interno_async(self, df, hasta):
            return pd.DataFrame()

    class DiferidoComparisonEngine:
        def __init__(self):
            pass

        async def compare_diferidos_async(self, df1, df2):
            return {}

    class DiferidoDataSource:
        def __init__(self):
            pass

        async def get_external_data(self, path):
            return []

        def get_internal_data_config(self, hasta):
            return {}

    class DiferidoTransformer:
        def __init__(self):
            pass

        def transform_dataframe_to_list_dict(self, df):
            return []

        def transform_comparison_results(self, data):
            return {}


async def calculate_diferido_comparison(
    file_path_externo: str, df_interno: pd.DataFrame, hasta: str
) -> Dict[str, Any]:
    """
    Función pública para calcular y comparar diferidos externos vs internos

    Args:
        file_path_externo: Ruta al archivo Excel con datos externos
        df_interno: DataFrame con datos internos para cálculo
        hasta: Período límite para cálculos (formato YYYY-MM)

    Returns:
        Diccionario con resultados de comparación completa
    """
    try:
        settings.logger(f"🚀 Iniciando cálculo de diferidos hasta: {hasta}")

        # Inicializar componentes
        data_source = DiferidoDataSource()
        calculation_engine = DiferidoCalculationEngine()
        comparison_engine = DiferidoComparisonEngine()
        transformer = DiferidoTransformer()

        # 1. Obtener datos externos desde Excel
        settings.logger("📁 Obteniendo datos externos...")
        external_data = await data_source.get_external_data(file_path_externo)
        df_externo = pd.DataFrame(external_data)

        # 2. Calcular diferidos internos
        settings.logger("⚙️ Calculando diferidos internos...")
        df_interno_calculated = (
            await calculation_engine.calculate_diferido_interno_async(df_interno, hasta)
        )

        # 3. Comparar ambos datasets
        settings.logger("🔍 Comparando diferidos externos vs internos...")
        comparison_results = await comparison_engine.compare_diferidos_async(
            df_externo, df_interno_calculated
        )

        # 4. Transformar resultados a formato estándar
        settings.logger("🔄 Transformando resultados...")
        comparison_results["periodo_analizado"] = hasta
        transformed_results = transformer.transform_comparison_results(
            comparison_results
        )

        # 5. Agregar datos adicionales
        final_results = {
            **transformed_results,
            "archivo_externo": file_path_externo,
            "registros_externos": len(external_data),
            "registros_internos_calculados": len(df_interno_calculated),
            "timestamp_procesamiento": datetime.now().isoformat(),
        }

        settings.logger(
            f"✅ Cálculo completado. {final_results['registros_con_diferencias']} registros con diferencias"
        )
        return final_results

    except Exception as e:
        settings.logger(f"❌ Error en calculate_diferido_comparison: {str(e)}")
        raise


async def calculate_diferido_interno_only(
    df_interno: pd.DataFrame, hasta: str
) -> List[Dict[str, Any]]:
    """
    Función pública para calcular solo diferidos internos

    Args:
        df_interno: DataFrame con datos internos
        hasta: Período límite (formato YYYY-MM)

    Returns:
        Lista de registros con diferidos calculados
    """
    try:
        settings.logger(f"🚀 Calculando diferidos internos hasta: {hasta}")

        # Inicializar componentes
        calculation_engine = DiferidoCalculationEngine()
        transformer = DiferidoTransformer()

        # Calcular diferidos internos
        df_calculated = await calculation_engine.calculate_diferido_interno_async(
            df_interno, hasta
        )

        # Transformar a lista de diccionarios
        results = transformer.transform_dataframe_to_list_dict(df_calculated)

        settings.logger(f"✅ Calculados {len(results)} registros de diferidos internos")
        return results

    except Exception as e:
        settings.logger(f"❌ Error en calculate_diferido_interno_only: {str(e)}")
        raise


async def get_diferido_external_data(file_path_externo: str) -> List[Dict[str, Any]]:
    """
    Función pública para obtener solo datos externos desde Excel

    Args:
        file_path_externo: Ruta al archivo Excel

    Returns:
        Lista de registros externos validados
    """
    try:
        settings.logger(f"📁 Obteniendo datos externos de: {file_path_externo}")

        # Inicializar componentes
        data_source = DiferidoDataSource()
        transformer = DiferidoTransformer()

        # Obtener datos raw
        external_data = await data_source.get_external_data(file_path_externo)

        # Transformar a schema estándar
        validated_data = transformer.transform_raw_to_externo_schema(external_data)

        settings.logger(f"✅ Obtenidos {len(validated_data)} registros externos")
        return validated_data

    except Exception as e:
        settings.logger(f"❌ Error en get_diferido_external_data: {str(e)}")
        raise


def validate_diferido_request(
    hasta: str, file_path_externo: str = None
) -> Dict[str, Any]:
    """
    Función pública para validar parámetros de request de diferido

    Args:
        hasta: Período límite (formato YYYY-MM)
        file_path_externo: Ruta al archivo Excel (opcional)

    Returns:
        Diccionario con resultado de validación
    """
    try:
        # Validar formato de fecha
        import re

        if not re.match(r"^\d{4}-\d{2}$", hasta):
            return {
                "valid": False,
                "error": "El parámetro 'hasta' debe tener el formato 'YYYY-MM'",
            }

        # Validar mes en rango válido
        year, month = hasta.split("-")
        month_int = int(month)
        if month_int < 1 or month_int > 12:
            return {"valid": False, "error": "El mes debe estar entre 01 y 12"}

        # Validar archivo si se proporciona
        if file_path_externo:
            from pathlib import Path

            if not Path(file_path_externo).exists():
                return {
                    "valid": False,
                    "error": f"No se encontró el archivo: {file_path_externo}",
                }

        return {
            "valid": True,
            "hasta_parsed": {"year": int(year), "month": month_int, "period": hasta},
            "file_path_validated": file_path_externo,
        }

    except Exception as e:
        return {"valid": False, "error": f"Error validando request: {str(e)}"}


# Funciones de conveniencia para compatibilidad con V1
async def comparar_diferidos_async(
    file_path_externo: str, df_interno: pd.DataFrame, hasta: str
) -> Dict[str, Any]:
    """
    Función de compatibilidad con V1 - wrapper para calculate_diferido_comparison

    Args:
        file_path_externo: Ruta al archivo Excel
        df_interno: DataFrame con datos internos
        hasta: Período límite

    Returns:
        Resultados de comparación
    """
    return await calculate_diferido_comparison(file_path_externo, df_interno, hasta)


async def calcular_diferido_interno_async(
    df_interno: pd.DataFrame, hasta: str
) -> List[Dict[str, Any]]:
    """
    Función de compatibilidad con V1 - wrapper para calculate_diferido_interno_only

    Args:
        df_interno: DataFrame con datos internos
        hasta: Período límite

    Returns:
        Lista de diferidos calculados
    """
    return await calculate_diferido_interno_only(df_interno, hasta)
