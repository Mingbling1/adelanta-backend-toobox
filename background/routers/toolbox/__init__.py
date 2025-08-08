# background/routers/toolbox/__init__.py
"""
🎮 Exportación centralizada de routers de background/toolbox
Facilita la importación en main.py
"""

from . import tablas_reportes_router
from . import tablas_cxc_router
from . import kpi_acumulado_router
from . import tipo_cambio_router  # 🆕 Nuevo router Tipo de Cambio

__all__ = [
    "tablas_reportes_router",
    "tablas_cxc_router",
    "kpi_acumulado_router",
    "tipo_cambio_router",  # 🆕 Exportar nuevo router
]
