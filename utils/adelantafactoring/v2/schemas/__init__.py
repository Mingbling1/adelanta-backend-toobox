"""
Schemas de dominio para Adelanta Factoring v2
"""

from .kpi_schema import (
    KPICalcularSchema,
    KPIAcumuladoCalcularSchema,
)
from .ventas_autodetraccion import (
    VentasAutodetraccionesRequest,
    VentasAutodetraccionesResult,
    RegistroVenta,
    AutodetraccionRecord,
)

__all__ = [
    "KPICalcularSchema",
    "KPIAcumuladoCalcularSchema",
    "VentasAutodetraccionesRequest",
    "VentasAutodetraccionesResult",
    "RegistroVenta",
    "AutodetraccionRecord",
]
