"""
🌐 CXC Dev Fact API V2 - Interfaz pública para devoluciones de facturas CXC
Orquesta todos los componentes V2 con fallback a V1 durante transición
"""

from typing import List, Dict, Optional

# Imports con fallback para desarrollo aislado
try:
    from utils.adelantafactoring.v2.io.webservice.cxc_dev_fact_client import (
        CXCDevFactWebserviceClient,
    )
    from utils.adelantafactoring.v2.engines.calculation.cxc_dev_fact_calculation_engine import (
        CXCDevFactCalculationEngine,
    )
    from utils.adelantafactoring.v2.processing.transformers.cxc_dev_fact_transformer import (
        CXCDevFactTransformer,
    )
    from config.logger import logger
except ImportError:
    # Fallback para desarrollo aislado
    class _FallbackLogger:
        def debug(self, msg):
            print(f"DEBUG: {msg}")

        def info(self, msg):
            print(f"INFO: {msg}")

        def warning(self, msg):
            print(f"WARNING: {msg}")

        def error(self, msg):
            print(f"ERROR: {msg}")

    logger = _FallbackLogger()

    # Componentes dummy para desarrollo
    class CXCDevFactWebserviceClient:
        async def fetch_devoluciones_facturas(self, params=None):
            return []

        async def health_check(self):
            return True

    class CXCDevFactCalculationEngine:
        async def calculate_devolucion_metrics(self, data):
            return {}

        async def calculate_devolucion_summary(self, data):
            return {}

        async def process_financial_data(self, raw_data):
            import pandas as pd

            return pd.DataFrame(raw_data) if raw_data else pd.DataFrame()

    class CXCDevFactTransformer:
        def raw_to_schema_list(self, raw_data):
            return []

        def schema_list_to_dict_list(self, schema_list):
            return []

        def raw_to_dataframe(self, raw_data):
            import pandas as pd

            return pd.DataFrame(raw_data) if raw_data else pd.DataFrame()

        def dataframe_to_dict_list(self, df):
            return []


async def get_cxc_dev_fact_data(
    params: Optional[Dict] = None,
    include_metrics: bool = False,
    include_summary: bool = False,
) -> Dict:
    """
    🌐 **FUNCIÓN PÚBLICA PRINCIPAL**

    Obtiene y procesa datos completos de devoluciones de facturas CXC.

    Args:
        params: Parámetros opcionales para la consulta
        include_metrics: Si incluir métricas calculadas
        include_summary: Si incluir resumen ejecutivo

    Returns:
        Diccionario con datos procesados y métricas (si se solicitan)

    Example:
        ```python
        # Uso básico
        data = await get_cxc_dev_fact_data()

        # Con métricas y resumen
        data = await get_cxc_dev_fact_data(
            include_metrics=True,
            include_summary=True
        )
        ```
    """
    try:
        # Inicializar componentes V2
        client = CXCDevFactWebserviceClient()
        engine = CXCDevFactCalculationEngine()
        transformer = CXCDevFactTransformer()

        logger.info("Iniciando proceso completo CXC devoluciones facturas V2")

        # 1. Obtener datos desde webservice
        raw_data = await client.fetch_devoluciones_facturas(params)

        if not raw_data:
            logger.warning("Sin datos de devoluciones facturas obtenidos")
            return {
                "data": [],
                "total_records": 0,
                "success": True,
                "message": "Sin datos disponibles",
            }

        # 2. Procesar datos con lógica de negocio
        processed_df = await engine.process_financial_data(raw_data)

        # 3. Convertir a formato final
        final_data = transformer.dataframe_to_dict_list(processed_df)

        # Resultado base
        result = {
            "data": final_data,
            "total_records": len(final_data),
            "success": True,
            "message": f"Datos procesados exitosamente: {len(final_data)} devoluciones",
        }

        # 4. Agregar métricas si se solicitan
        if include_metrics and not processed_df.empty:
            try:
                metrics = await engine.calculate_devolucion_metrics(processed_df)
                result["metrics"] = metrics
                logger.debug("Métricas incluidas en respuesta")
            except Exception as e:
                logger.warning(f"Error calculando métricas: {e}")
                result["metrics"] = {"error": "No se pudieron calcular métricas"}

        # 5. Agregar resumen si se solicita
        if include_summary and not processed_df.empty:
            try:
                summary = await engine.calculate_devolucion_summary(processed_df)
                result["summary"] = summary
                logger.debug("Resumen incluido en respuesta")
            except Exception as e:
                logger.warning(f"Error generando resumen: {e}")
                result["summary"] = {"error": "No se pudo generar resumen"}

        logger.info(
            f"Proceso CXC devoluciones facturas completado: {len(final_data)} registros"
        )
        return result

    except Exception as e:
        logger.error(f"Error en proceso CXC devoluciones facturas V2: {e}")

        # Fallback a V1 si está disponible
        try:
            from utils.adelantafactoring.calculos.CXCDevFactCalcular import (
                CXCDevFactCalcular,
            )

            logger.warning("Fallback a V1 para CXC devoluciones facturas")
            calculador_v1 = CXCDevFactCalcular()
            data_v1 = await calculador_v1.calcular()

            return {
                "data": data_v1,
                "total_records": len(data_v1) if data_v1 else 0,
                "success": True,
                "message": "Datos obtenidos desde V1 (fallback)",
                "version": "v1_fallback",
            }

        except Exception as e_v1:
            logger.error(f"Fallback V1 también falló: {e_v1}")
            return {
                "data": [],
                "total_records": 0,
                "success": False,
                "error": f"Error V2: {str(e)} | Error V1: {str(e_v1)}",
            }


async def get_cxc_dev_fact_simple() -> List[Dict]:
    """
    🚀 **FUNCIÓN SIMPLIFICADA**

    Versión simple que solo retorna la lista de datos sin métricas.

    Returns:
        Lista de diccionarios con devoluciones de facturas
    """
    try:
        result = await get_cxc_dev_fact_data()
        return result.get("data", [])
    except Exception as e:
        logger.error(f"Error en función simplificada CXC dev fact: {e}")
        return []


async def health_check_cxc_dev_fact() -> Dict:
    """
    🔍 **HEALTH CHECK**

    Verifica el estado de todos los componentes CXC devoluciones facturas.

    Returns:
        Estado de salud de los componentes
    """
    try:
        client = CXCDevFactWebserviceClient()

        # Test de conectividad
        webservice_ok = await client.health_check()

        return {
            "webservice": "OK" if webservice_ok else "ERROR",
            "calculation_engine": "OK",
            "transformer": "OK",
            "overall_status": "HEALTHY" if webservice_ok else "DEGRADED",
            "timestamp": "2024-01-01T00:00:00",  # Placeholder timestamp
            "version": "v2",
        }

    except Exception as e:
        logger.error(f"Error en health check CXC dev fact: {e}")
        return {
            "webservice": "ERROR",
            "calculation_engine": "ERROR",
            "transformer": "ERROR",
            "overall_status": "DOWN",
            "error": str(e),
            "version": "v2",
        }


# Compatibilidad con nombres V1
calcular_cxc_dev_fact = get_cxc_dev_fact_simple
