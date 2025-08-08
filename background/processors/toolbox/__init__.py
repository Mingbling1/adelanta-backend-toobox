# background/processors/toolbox/__init__.py
"""
🔄 Exportación centralizada de processors de background/toolbox
Wrappers opcionales de compatibilidad para tasks Celery
"""

from .kpi_acumulado_processor import KpiAcumuladoProcessor
from .tablas_cxc_processor import TablasCxcProcessor
from .tablas_reportes_processor import TablasReportesProcessor
from .tipo_cambio_processor import TipoCambioProcessor  # 🆕 Nuevo processor

__all__ = [
    "KpiAcumuladoProcessor",
    "TablasCxcProcessor",
    "TablasReportesProcessor",
    "TipoCambioProcessor",  # 🆕 Exportar nuevo processor
]
