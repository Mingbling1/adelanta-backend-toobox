"""
🌐 API pública para CXC Pagos Fact - Interfaz simple para operaciones de pagos
"""

try:
    from utils.adelantafactoring.v2.io.webservice.cxc_pagos_fact_client import (
        CXCPagosFactWebserviceClient,
    )
    from utils.adelantafactoring.v2.engines.calculation.cxc_pagos_fact_calculation_engine import (
        CXCPagosFactCalculationEngine,
    )
    from utils.adelantafactoring.v2.processing.transformers.cxc_pagos_fact_transformer import (
        CXCPagosFactTransformer,
    )
    from utils.adelantafactoring.v2.config.settings import settings
except ImportError:
    # Fallback para desarrollo aislado
    class _FallbackSettings:
        WEBSERVICE_BASE_URL = "https://webservice.adelantafactoring.com"

    settings = _FallbackSettings()

    # Fallback para componentes V2
    CXCPagosFactWebserviceClient = None
    CXCPagosFactCalculationEngine = None
    CXCPagosFactTransformer = None

import pandas as pd
from typing import List, Dict, Optional
import asyncio
from config.logger import logger


async def get_cxc_pagos_fact_data() -> List[Dict]:
    """
    Función pública principal para obtener datos de pagos de facturas CXC

    Returns:
        Lista de diccionarios con datos de pagos procesados
    """
    try:
        # Verificar disponibilidad de componentes V2
        if not all(
            [
                CXCPagosFactWebserviceClient,
                CXCPagosFactCalculationEngine,
                CXCPagosFactTransformer,
            ]
        ):
            logger.warning("Componentes V2 no disponibles, usando fallback V1")
            return await _fallback_v1_pagos_fact()

        # Inicializar componentes V2
        client = CXCPagosFactWebserviceClient()
        engine = CXCPagosFactCalculationEngine()
        transformer = CXCPagosFactTransformer()

        logger.info("🚀 Iniciando obtención de datos CXC Pagos Fact V2")

        # 1. Obtener datos del webservice
        raw_data = await client.fetch_pagos_facturas()

        if not raw_data:
            logger.warning("No se obtuvieron datos de pagos")
            return []

        # 2. Transformar datos raw a schema validado
        validated_data = transformer.transform_raw_to_schema(raw_data)

        # 3. Procesar datos con motor de cálculo
        df_processed = await engine.process_pagos_async(validated_data)

        # 4. Convertir resultado a lista de diccionarios
        result = transformer.transform_dataframe_to_list_dict(df_processed)

        logger.info(f"✅ Proceso CXC Pagos Fact V2 completado: {len(result)} registros")
        return result

    except Exception as e:
        logger.error(f"❌ Error en proceso CXC Pagos Fact V2: {e}")
        # Fallback a V1 en caso de error
        return await _fallback_v1_pagos_fact()


async def get_cxc_pagos_fact_dataframe() -> pd.DataFrame:
    """
    Obtiene datos de pagos como DataFrame para análisis avanzado

    Returns:
        DataFrame con datos de pagos procesados
    """
    try:
        data = await get_cxc_pagos_fact_data()

        if not data:
            return pd.DataFrame()

        # Crear DataFrame optimizado
        if CXCPagosFactTransformer:
            transformer = CXCPagosFactTransformer()
            df = transformer.transform_schema_to_dataframe(data)
        else:
            df = pd.DataFrame(data)

        logger.debug(f"DataFrame CXC Pagos Fact creado: {len(df)} filas")
        return df

    except Exception as e:
        logger.error(f"Error creando DataFrame CXC Pagos Fact: {e}")
        return pd.DataFrame()


async def get_payment_summary() -> Dict:
    """
    Obtiene resumen estadístico de pagos de facturas

    Returns:
        Diccionario con estadísticas de pagos
    """
    try:
        df = await get_cxc_pagos_fact_dataframe()

        if df.empty:
            return {"error": "No hay datos de pagos disponibles"}

        # Calcular resumen con motor de cálculo
        if CXCPagosFactCalculationEngine:
            engine = CXCPagosFactCalculationEngine()
            summary = engine.calculate_payment_summary(df)
        else:
            # Fallback básico
            summary = {
                "total_registros": len(df),
                "monto_total_pagos": (
                    float(df["MontoPago"].sum()) if "MontoPago" in df.columns else 0
                ),
            }

        logger.info(
            f"Resumen de pagos generado: {summary.get('total_registros', 0)} registros"
        )
        return summary

    except Exception as e:
        logger.error(f"Error generando resumen de pagos: {e}")
        return {"error": str(e)}


async def validate_pagos_data(data: List[Dict]) -> List[Dict]:
    """
    Valida datos de pagos usando schemas V2

    Args:
        data: Lista de diccionarios con datos de pagos

    Returns:
        Lista de diccionarios validados
    """
    try:
        if not data:
            return []

        if CXCPagosFactTransformer:
            transformer = CXCPagosFactTransformer()
            validated_data = transformer.transform_raw_to_schema(data)
        else:
            # Fallback: usar validación V1
            validated_data = await _fallback_v1_validation(data)

        logger.debug(f"Validación de pagos completada: {len(validated_data)} registros")
        return validated_data

    except Exception as e:
        logger.error(f"Error validando datos de pagos: {e}")
        return data  # Devolver datos originales si falla validación


async def health_check() -> Dict:
    """
    Verifica el estado de salud del módulo CXC Pagos Fact

    Returns:
        Diccionario con estado de componentes
    """
    try:
        health_status = {
            "module": "cxc_pagos_fact",
            "version": "v2",
            "status": "healthy",
            "components": {
                "webservice_client": False,
                "calculation_engine": False,
                "transformer": False,
                "webservice_connection": False,
            },
        }

        # Verificar componentes V2
        if CXCPagosFactWebserviceClient:
            health_status["components"]["webservice_client"] = True

            # Verificar conectividad con webservice
            try:
                client = CXCPagosFactWebserviceClient()
                connection_ok = await client.health_check()
                health_status["components"]["webservice_connection"] = connection_ok
            except Exception:
                health_status["components"]["webservice_connection"] = False

        if CXCPagosFactCalculationEngine:
            health_status["components"]["calculation_engine"] = True

        if CXCPagosFactTransformer:
            health_status["components"]["transformer"] = True

        # Determinar estado general
        all_components = all(health_status["components"].values())
        health_status["status"] = "healthy" if all_components else "degraded"

        return health_status

    except Exception as e:
        return {
            "module": "cxc_pagos_fact",
            "version": "v2",
            "status": "error",
            "error": str(e),
        }


# === FUNCIONES DE FALLBACK V1 ===


async def _fallback_v1_pagos_fact() -> List[Dict]:
    """Fallback a implementación V1 si V2 no está disponible"""
    try:
        logger.warning("🔄 Usando fallback V1 para CXC Pagos Fact")

        # Intentar importar y usar V1
        from utils.adelantafactoring.calculos.CXCPagosFactCalcular import (
            CXCPagosFactCalcular,
        )

        calculator = CXCPagosFactCalcular()
        result = await calculator.calcular()

        logger.info(f"✅ Fallback V1 completado: {len(result)} registros")
        return result

    except ImportError:
        logger.error("❌ No se puede importar CXCPagosFactCalcular V1")
        return []
    except Exception as e:
        logger.error(f"❌ Error en fallback V1: {e}")
        return []


async def _fallback_v1_validation(data: List[Dict]) -> List[Dict]:
    """Fallback a validación V1"""
    try:
        from utils.adelantafactoring.schemas.CXCPagosFactCalcularSchema import (
            CXCPagosFactCalcularSchema,
        )

        validated_data = [
            CXCPagosFactCalcularSchema(**record).model_dump() for record in data
        ]
        return validated_data

    except ImportError:
        logger.warning("Schema V1 no disponible, devolviendo datos sin validar")
        return data
    except Exception as e:
        logger.error(f"Error en validación V1 fallback: {e}")
        return data


# === FUNCIONES DE COMPATIBILIDAD ===


def calculate_cxc_pagos_fact() -> List[Dict]:
    """
    Función síncrona para compatibilidad con código existente

    Returns:
        Lista de diccionarios con datos de pagos
    """
    try:
        return asyncio.run(get_cxc_pagos_fact_data())
    except Exception as e:
        logger.error(f"Error en función síncrona CXC Pagos Fact: {e}")
        return []
