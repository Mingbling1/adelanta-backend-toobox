# background/test/test_tablas_reportes.py
"""🧪 Test simple de compatibilidad para TablasReportesProcessor"""

from background.processors.toolbox.tablas_reportes_processor import (
    TablasReportesProcessor,
)


def test_tablas_reportes_processor_basic():
    """Test que TablasReportesProcessor tiene la misma interfaz que el cronjob original"""
    # 1. Crear processor instance
    processor = TablasReportesProcessor()

    # 2. Verificar constructor works
    assert processor is not None
    assert (
        processor.description
        == "Actualiza las tablas de reportes: KPI, NuevosClientesNuevosPagadores y Saldos usando Celery"
    )
    assert processor.status_key == "ActualizarTablasReportesCronjob_status"

    # 3. Verificar main methods exist and are callable
    assert hasattr(processor, "process")
    assert callable(getattr(processor, "process"))
    assert hasattr(processor, "process_sync")
    assert callable(getattr(processor, "process_sync"))
    assert hasattr(processor, "get_task_status")
    assert callable(getattr(processor, "get_task_status"))

    # Done - NO complex business logic testing según ARCHITECTURE_BACKGROUND.md
