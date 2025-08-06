# background/tasks/toolbox/__init__.py
"""
🎯 Importación centralizada de todas las tasks de toolbox
Asegura que todas las tasks estén registradas en Celery
"""

# Importar todas las tasks para asegurar registro en Celery
from .kpi_acumulado_task import actualizar_kpi_acumulado_task
from .tablas_reportes_task import tablas_reportes_task
from .tablas_cxc_task import tablas_cxc_task

# Lista de todas las tasks exportadas
__all__ = [
    "actualizar_kpi_acumulado_task",
    "tablas_reportes_task",
    "tablas_cxc_task",
]
