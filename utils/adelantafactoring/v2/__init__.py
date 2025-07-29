"""
🏗️ Adelanta Factoring V2 - Arquitectura Hexagonal
Refactorización modular del sistema financiero ETL

📋 DOCUMENTACIÓN:
- ARCHITECTURE_GUIDE.md - Guía completa de arquitectura hexagonal
- QUICK_REFERENCE.md - Cheat sheet para desarrollo rápido

🚀 IMPORTACIONES SIMPLES:
- af.fondos.get_promocional()
- af.fondos.get_crecer()
- af.comisiones.calculate()
- af.comisiones.get_summary()
- af.kpi.calculate_monthly()
- af.cxc.process_etl()
- af.etl.run_financial_pipeline()

🏛️ ARQUITECTURA:
api/         → 🌐 Interfaz pública simple
engines/     → ⚙️ Motores especializados (cálculo, validación, datos)
io/          → 📡 Comunicación externa (webservices, archivos)
processing/  → 🔄 Pipelines (transformers, validators)
schemas/     → 📊 Contratos Pydantic
config/      → ⚙️ Configuración centralizada
"""

__version__ = "2.0.0"

# Importaciones absolutas con manejo de errores para desarrollo gradual
try:
    from utils.adelantafactoring.v2.api import fondos_api
    from utils.adelantafactoring.v2.api.fondos_api import (
        get_promocional,
        get_promocional_async,
        get_crecer,
        get_crecer_async,
    )
except ImportError as e:
    import logging

    logging.warning(f"Módulos fondos V2 no están disponibles: {e}")
    fondos_api = None

try:
    from utils.adelantafactoring.v2.api import comisiones_api
    from utils.adelantafactoring.v2.api.comisiones_api import (
        calculate,
        calculate_async,
        get_summary,
    )
except ImportError as e:
    import logging

    logging.warning(f"Módulos comisiones V2 no están disponibles: {e}")
    comisiones_api = None

# Aliases para compatibilidad y API simple
__all__ = [
    "fondos_api",
    "get_promocional",
    "get_promocional_async",
    "get_crecer",
    "get_crecer_async",
    "comisiones_api",
    "calculate",
    "calculate_async",
    "get_summary",
]
