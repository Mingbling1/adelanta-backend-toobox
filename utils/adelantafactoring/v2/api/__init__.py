"""
🌐 API V2 - Interfaces públicas simplificadas
"""

from .kpi_api import KPIV2, calculate_kpi_v2
from .ventas_autodetraccion_api import VentasAutodetraccionesCalcular
from .sector_pagadores_api import SectorPagadoresCalcularV2
from .nuevos_clientes_pagadores_api import NuevosClientesNuevosPagadoresCalcularV2
from ..schemas.ventas_autodetraccion import (
    VentasAutodetraccionesRequestSchema,
    VentasAutodetraccionesResponseSchema,
)

__all__ = [
    "KPIV2",
    "calculate_kpi_v2",
    "VentasAutodetraccionesCalcular",
    "SectorPagadoresCalcularV2",
    "NuevosClientesNuevosPagadoresCalcularV2",
    "VentasAutodetraccionesRequestSchema",
    "VentasAutodetraccionesResponseSchema",
]
