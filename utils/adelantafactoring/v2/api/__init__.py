"""
🌐 API V2 - Interfaces públicas simplificadas
"""

from .kpi_api import KPIAPI, calculate_kpi_v2
from .ventas_autodetraccion_api import VentasAutodetraccionesAPI
from .sector_pagadores_api import SectorPagadoresAPI
from .nuevos_clientes_pagadores_api import NuevosClientesNuevosPagadoresAPI
from ..schemas.ventas_autodetraccion_schema import (
    VentasAutodetraccionesRequestSchema,
    VentasAutodetraccionesResponseSchema,
)

__all__ = [
    "KPIAPI",
    "calculate_kpi_v2",
    "VentasAutodetraccionesAPI",
    "SectorPagadoresAPI",
    "NuevosClientesNuevosPagadoresAPI",
    "VentasAutodetraccionesRequestSchema",
    "VentasAutodetraccionesResponseSchema",
]
