"""
🚀 Configuración de Celery para Adelanta Backend Toolbox
"""

from celery import Celery
from config.settings import settings
from config.logger import logger

# Configuración de Celery
celery_app = Celery(
    "adelanta-toolbox",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=[
        "background.tasks.toolbox",  # 🆕 Importar tasks del directorio background
        "config.celery_tasks",  # Mantener importación legacy para compatibilidad
    ],
)

# Configuración avanzada de Celery - OPTIMIZADO PARA 2GB RAM / 2 CORES
celery_app.conf.update(
    # Configuración de tareas
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="America/Lima",
    enable_utc=True,
    # 🧠 CONFIGURACIÓN DE MEMORIA CRÍTICA - 2GB SYSTEM
    worker_max_tasks_per_child=1,  # UNA SOLA TAREA → limpia memoria completamente
    worker_prefetch_multiplier=1,  # Sin prefetch → menor RAM usage
    worker_max_memory_per_child=400000,  # 400MB máximo por worker (reducido)
    # 🚀 CONFIGURACIÓN DE PERFORMANCE
    task_acks_late=True,  # Confirmar después de completar → mayor confiabilidad
    task_reject_on_worker_lost=True,  # Rechazar tareas si worker falla
    worker_disable_rate_limits=True,  # Sin rate limits → mejor performance
    # 🔧 CONFIGURACIÓN DE THREADS (optimizado para 2 cores)
    worker_pool="threads",  # Threads en lugar de procesos → menor memoria
    worker_concurrency=2,  # 2 threads para 2 cores físicos
    # Configuración de resultados
    result_expires=3600,  # Resultados expiran en 1 hora
    task_track_started=True,  # Rastrear cuando las tareas inician
    # Configuración de routing
    task_routes={
        "config.celery_tasks.actualizar_kpi_acumulado_task": {"queue": "cronjobs"},
        "config.celery_tasks.actualizar_tablas_reportes_task": {"queue": "cronjobs"},
        "config.celery_tasks.actualizar_cxc_etl_task": {"queue": "cronjobs"},
    },
    # Configuración de colas
    task_default_queue="default",
    task_queues={
        "default": {
            "exchange": "default",
            "exchange_type": "direct",
            "routing_key": "default",
        },
        "cronjobs": {
            "exchange": "cronjobs",
            "exchange_type": "direct",
            "routing_key": "cronjobs",
        },
    },
)

logger.info("✅ Celery configurado correctamente")
