# background/test/test_tablas_cxc.py
"""🧪 Test simple de compatibilidad para TablasCXCProcessor"""

from background.processors.toolbox.tablas_cxc_processor import TablasCXCProcessor


def test_tablas_cxc_processor_basic():
    """Test que TablasCXCProcessor tiene la misma interfaz que el cronjob original"""
    # 1. Crear processor instance
    processor = TablasCXCProcessor()

    # 2. Verificar constructor works
    assert processor is not None
    assert (
        processor.description
        == "Actualizar Tablas CXC con ETL Power BI completo usando Celery"
    )

    # 3. Verificar main methods exist and are callable
    assert hasattr(processor, "run")
    assert callable(getattr(processor, "run"))
    assert hasattr(processor, "run_sync")
    assert callable(getattr(processor, "run_sync"))
    assert hasattr(processor, "get_task_status")
    assert callable(getattr(processor, "get_task_status"))

    # Done - NO complex business logic testing según ARCHITECTURE_BACKGROUND.md
