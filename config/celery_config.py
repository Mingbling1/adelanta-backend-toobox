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
        "config.celery_tasks",  # Importar módulo de tareas
    ],
)

# Configuración avanzada de Celery
celery_app.conf.update(
    # Configuración de tareas
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="America/Lima",
    enable_utc=True,
    # Configuración de workers - OPTIMIZADO PARA 2GB RAM + MEMORIA EXTRA
    worker_max_tasks_per_child=1,  # UNA SOLA TAREA POR WORKER (crítico para memoria)
    worker_prefetch_multiplier=1,  # Una tarea a la vez por worker
    worker_max_memory_per_child=750000,  # 750MB máximo por worker (aumentado de 512MB)
    task_acks_late=True,  # Confirmar tarea después de completar
    task_reject_on_worker_lost=True,  # Rechazar tareas si worker falla
    worker_disable_rate_limits=True,  # Deshabilitar rate limits para memoria
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
