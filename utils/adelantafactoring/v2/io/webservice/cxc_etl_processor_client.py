"""
📡 CXC ETL Processor Client V2 - Orquestador de múltiples fuentes de datos
Coordina la comunicación con todos los webservices necesarios para el ETL completo
"""

import asyncio
import pandas as pd
from typing import Tuple, Dict, List, Optional
from datetime import datetime

try:
    from utils.adelantafactoring.v2.config.settings import settings
    from utils.adelantafactoring.v2.io.webservice.cxc_acumulado_dim_client import (
        CXCAcumuladoDIMWebserviceClient,
    )
    from utils.adelantafactoring.v2.io.webservice.cxc_pagos_fact_client import (
        CXCPagosFactWebserviceClient,
    )
    from utils.adelantafactoring.v2.io.webservice.cxc_dev_fact_client import (
        CXCDevFactWebserviceClient,
    )
    from utils.adelantafactoring.v2.io.webservice.kpi_client import KPIWebserviceClient
except ImportError:
    # Fallback para desarrollo aislado
    class _FallbackSettings:
        WEBSERVICE_BASE_URL = "https://webservice.adelantafactoring.com"
        WEBSERVICE_TIMEOUT = 300
        MAX_RETRIES = 3

    settings = _FallbackSettings()

    # Clients simulados para desarrollo
    class CXCAcumuladoDIMWebserviceClient:
        async def fetch_acumulado_dim_data(self, fecha_corte=None):
            return []

    class CXCPagosFactWebserviceClient:
        async def fetch_pagos_fact_data(self, fecha_corte=None):
            return []

    class CXCDevFactWebserviceClient:
        async def fetch_dev_fact_data(self, fecha_corte=None):
            return []

    class KPIWebserviceClient:
        async def fetch_tipo_cambio_data(self):
            return [{"TipoCambioFecha": "2024-01-01", "TipoCambioVenta": 3.8}]


class CXCETLProcessorClient:
    """Cliente orquestador para ETL completo de CXC"""

    def __init__(self):
        self.base_url = settings.WEBSERVICE_BASE_URL
        self.timeout = getattr(settings, "WEBSERVICE_TIMEOUT", 300)

        # Inicializar clientes especializados
        self.acumulado_client = CXCAcumuladoDIMWebserviceClient()
        self.pagos_client = CXCPagosFactWebserviceClient()
        self.dev_client = CXCDevFactWebserviceClient()
        self.kpi_client = KPIWebserviceClient()

        # Estadísticas de operación
        self.last_operation_time = None
        self.total_operations = 0
        self.failed_operations = 0

    async def fetch_all_cxc_data(
        self, fecha_corte: Optional[str] = None
    ) -> Tuple[List[Dict], List[Dict], List[Dict], pd.DataFrame]:
        """
        Obtiene todos los datos necesarios para el ETL CXC de forma concurrente

        Returns:
            Tuple[acumulado_data, pagos_data, dev_data, tipo_cambio_df]
        """
        try:
            self.last_operation_time = datetime.now()

            # Ejecutar todas las consultas de forma concurrente para optimizar tiempo
            tasks = [
                self.acumulado_client.fetch_acumulado_dim_data(fecha_corte),
                self.pagos_client.fetch_pagos_fact_data(fecha_corte),
                self.dev_client.fetch_dev_fact_data(fecha_corte),
                self.kpi_client.fetch_tipo_cambio_data(),
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Procesar resultados y manejar excepciones
            acumulado_data = results[0] if not isinstance(results[0], Exception) else []
            pagos_data = results[1] if not isinstance(results[1], Exception) else []
            dev_data = results[2] if not isinstance(results[2], Exception) else []
            tipo_cambio_raw = (
                results[3] if not isinstance(results[3], Exception) else []
            )

            # Convertir tipo cambio a DataFrame
            tipo_cambio_df = (
                pd.DataFrame(tipo_cambio_raw) if tipo_cambio_raw else pd.DataFrame()
            )

            # Estadísticas
            self.total_operations += 1

            # Log de resultados
            if any(isinstance(r, Exception) for r in results):
                self.failed_operations += 1
                failed_clients = []
                if isinstance(results[0], Exception):
                    failed_clients.append(f"acumulado: {results[0]}")
                if isinstance(results[1], Exception):
                    failed_clients.append(f"pagos: {results[1]}")
                if isinstance(results[2], Exception):
                    failed_clients.append(f"dev: {results[2]}")
                if isinstance(results[3], Exception):
                    failed_clients.append(f"tipo_cambio: {results[3]}")

                print(f"⚠️ Algunos clientes fallaron: {', '.join(failed_clients)}")

            return acumulado_data, pagos_data, dev_data, tipo_cambio_df

        except Exception as e:
            self.failed_operations += 1
            print(f"❌ Error en fetch_all_cxc_data: {e}")
            raise

    async def fetch_acumulado_only(
        self, fecha_corte: Optional[str] = None
    ) -> List[Dict]:
        """Obtiene solo datos de acumulado DIM"""
        try:
            return await self.acumulado_client.fetch_acumulado_dim_data(fecha_corte)
        except Exception as e:
            print(f"❌ Error obteniendo datos acumulado: {e}")
            return []

    async def fetch_pagos_only(self, fecha_corte: Optional[str] = None) -> List[Dict]:
        """Obtiene solo datos de pagos"""
        try:
            return await self.pagos_client.fetch_pagos_fact_data(fecha_corte)
        except Exception as e:
            print(f"❌ Error obteniendo datos pagos: {e}")
            return []

    async def fetch_dev_only(self, fecha_corte: Optional[str] = None) -> List[Dict]:
        """Obtiene solo datos de devoluciones"""
        try:
            return await self.dev_client.fetch_dev_fact_data(fecha_corte)
        except Exception as e:
            print(f"❌ Error obteniendo datos devoluciones: {e}")
            return []

    async def fetch_tipo_cambio_only(self) -> pd.DataFrame:
        """Obtiene solo datos de tipo de cambio"""
        try:
            data = await self.kpi_client.fetch_tipo_cambio_data()
            return pd.DataFrame(data) if data else pd.DataFrame()
        except Exception as e:
            print(f"❌ Error obteniendo tipo cambio: {e}")
            return pd.DataFrame()

    async def health_check_all_clients(self) -> Dict[str, str]:
        """Verifica el estado de todos los clientes"""
        try:
            # Test rápido de cada cliente
            tasks = [
                self._test_client(self.acumulado_client, "acumulado"),
                self._test_client(self.pagos_client, "pagos"),
                self._test_client(self.dev_client, "dev"),
                self._test_client(self.kpi_client, "kpi"),
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            return {
                "acumulado": (
                    "healthy" if not isinstance(results[0], Exception) else "unhealthy"
                ),
                "pagos": (
                    "healthy" if not isinstance(results[1], Exception) else "unhealthy"
                ),
                "dev": (
                    "healthy" if not isinstance(results[2], Exception) else "unhealthy"
                ),
                "kpi": (
                    "healthy" if not isinstance(results[3], Exception) else "unhealthy"
                ),
                "overall": (
                    "healthy"
                    if all(not isinstance(r, Exception) for r in results)
                    else "degraded"
                ),
            }

        except Exception as e:
            print(f"❌ Error en health check: {e}")
            return {
                "acumulado": "unknown",
                "pagos": "unknown",
                "dev": "unknown",
                "kpi": "unknown",
                "overall": "unhealthy",
            }

    async def _test_client(self, client, client_name: str):
        """Test rápido de un cliente específico"""
        try:
            if hasattr(client, "health_check"):
                return await client.health_check()
            else:
                # Test básico llamando al método principal con límite pequeño
                if client_name == "acumulado":
                    await client.fetch_acumulado_dim_data()
                elif client_name == "pagos":
                    await client.fetch_pagos_fact_data()
                elif client_name == "dev":
                    await client.fetch_dev_fact_data()
                elif client_name == "kpi":
                    await client.fetch_tipo_cambio_data()
                return "healthy"
        except Exception as e:
            raise e

    def get_operation_stats(self) -> Dict:
        """Obtiene estadísticas de operaciones del cliente"""
        success_rate = (
            (self.total_operations - self.failed_operations)
            / max(self.total_operations, 1)
        ) * 100

        return {
            "total_operations": self.total_operations,
            "failed_operations": self.failed_operations,
            "success_rate": round(success_rate, 2),
            "last_operation": (
                self.last_operation_time.isoformat()
                if self.last_operation_time
                else None
            ),
            "client_type": "CXCETLProcessorClient",
        }

    async def validate_data_consistency(
        self, acumulado_data: List[Dict], pagos_data: List[Dict], dev_data: List[Dict]
    ) -> Dict[str, any]:
        """Valida la consistencia entre las diferentes fuentes de datos"""
        try:
            # Convertir a DataFrames para análisis
            df_acumulado = (
                pd.DataFrame(acumulado_data) if acumulado_data else pd.DataFrame()
            )
            df_pagos = pd.DataFrame(pagos_data) if pagos_data else pd.DataFrame()
            df_dev = pd.DataFrame(dev_data) if dev_data else pd.DataFrame()

            consistency_report = {"data_integrity": True, "warnings": [], "errors": []}

            # Verificar claves de unión
            if not df_acumulado.empty and not df_pagos.empty:
                if (
                    "IdLiquidacionDet" in df_acumulado.columns
                    and "IdLiquidacionDet" in df_pagos.columns
                ):
                    acumulado_ids = set(df_acumulado["IdLiquidacionDet"].dropna())
                    pagos_ids = set(df_pagos["IdLiquidacionDet"].dropna())

                    orphan_pagos = pagos_ids - acumulado_ids
                    if orphan_pagos:
                        consistency_report["warnings"].append(
                            f"Pagos huérfanos sin acumulado: {len(orphan_pagos)} registros"
                        )

            # Verificar claves dev
            if not df_acumulado.empty and not df_dev.empty:
                if (
                    "IdLiquidacionDet" in df_acumulado.columns
                    and "IdLiquidacionDet" in df_dev.columns
                ):
                    acumulado_ids = set(df_acumulado["IdLiquidacionDet"].dropna())
                    dev_ids = set(df_dev["IdLiquidacionDet"].dropna())

                    orphan_dev = dev_ids - acumulado_ids
                    if orphan_dev:
                        consistency_report["warnings"].append(
                            f"Devoluciones huérfanas sin acumulado: {len(orphan_dev)} registros"
                        )

            # Verificar datos vacíos
            if df_acumulado.empty:
                consistency_report["errors"].append("Datos acumulado vacíos")
                consistency_report["data_integrity"] = False

            return consistency_report

        except Exception as e:
            return {
                "data_integrity": False,
                "warnings": [],
                "errors": [f"Error validando consistencia: {str(e)}"],
            }
