"""
🚀 Configuración de Celery para Adelanta Backend Toolbox
"""

from celery import Celery
from celery.schedules import crontab
from config.settings import settings
from config.logger import logger

# Configuración de Celery
celery_app = Celery(
    "adelanta-toolbox",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=[
        "background.tasks.toolbox",  # 🆕 Importar tasks del directorio background
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
    # 🧠 CONFIGURACIÓN DE MEMORIA CRÍTICA - ANTI-OOM OPTIMIZADO
    worker_max_tasks_per_child=1,  # UNA SOLA TAREA → limpia memoria completamente
    worker_prefetch_multiplier=1,  # Sin prefetch → menor RAM usage
    worker_max_memory_per_child=600000,  # 600MB límite por worker (reducido de 750MB)
    # 🚀 CONFIGURACIÓN DE PERFORMANCE
    task_acks_late=True,  # Confirmar después de completar → mayor confiabilidad
    task_reject_on_worker_lost=True,  # Rechazar tareas si worker falla
    worker_disable_rate_limits=True,  # Sin rate limits → mejor performance
    # 🔧 CONFIGURACIÓN DE THREADS (optimizado para 2 cores)
    worker_pool="solo",  # Threads en lugar de procesos → menor memoria
    worker_concurrency=1,  # 2 threads para 2 cores físicos
    # Configuración de resultados
    result_expires=3600,  # Resultados expiran en 1 hora
    task_track_started=True,  # Rastrear cuando las tareas inician
    # Configuración de routing - ACTUALIZADA para nueva estructura
    task_routes={
        "toolbox.kpi_acumulado": {"queue": "cronjobs"},
        "toolbox.tablas_reportes": {"queue": "cronjobs"},
        "toolbox.tablas_cxc": {"queue": "cronjobs"},
        "toolbox.tipo_cambio": {"queue": "cronjobs"},  # 🆕 Nuevo task Tipo de Cambio
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
    # 🕐 CONFIGURACIÓN DE CELERY BEAT - CRÍTICA PARA FUNCIONAMIENTO
    beat_schedule={
        # 📊 Actualización automática de Tablas Reportes - 3 veces al día
        "actualizar-tablas-reportes-manana": {
            "task": "toolbox.tablas_reportes",
            "schedule": crontab(
                hour=7, minute=0
            ),  # Todos los días a las 7:00 AM (GMT-5 Lima)
            "options": {"queue": "cronjobs"},
        },
        "actualizar-tablas-reportes-mediodia": {
            "task": "toolbox.tablas_reportes",
            "schedule": crontab(
                hour=12, minute=0
            ),  # Todos los días a las 12:00 PM (GMT-5 Lima)
            "options": {"queue": "cronjobs"},
        },
        "actualizar-tablas-reportes-tarde": {
            "task": "toolbox.tablas_reportes",
            "schedule": crontab(
                hour=18, minute=0
            ),  # Todos los días a las 6:00 PM (GMT-5 Lima)
            "options": {"queue": "cronjobs"},
        },
        # 📈 Actualización automática de KPI Acumulado - 2 veces al día
        "actualizar-kpi-acumulado-manana": {
            "task": "toolbox.kpi_acumulado",
            "schedule": crontab(
                hour=7, minute=30
            ),  # Todos los días a las 7:30 AM (GMT-5 Lima)
            "options": {"queue": "cronjobs"},
        },
        "actualizar-kpi-acumulado-mediodia": {
            "task": "toolbox.kpi_acumulado",
            "schedule": crontab(
                hour=12, minute=30
            ),  # Todos los días a las 12:30 PM (GMT-5 Lima)
            "options": {"queue": "cronjobs"},
        },
        # � Actualización automática de Tablas CXC - 2 veces al día
        "actualizar-tablas-cxc-manana": {
            "task": "toolbox.tablas_cxc",
            "schedule": crontab(
                hour=8, minute=0
            ),  # Todos los días a las 8:00 AM (GMT-5 Lima)
            "options": {"queue": "cronjobs"},
        },
        "actualizar-tablas-cxc-tarde": {
            "task": "toolbox.tablas_cxc",
            "schedule": crontab(
                hour=13, minute=0
            ),  # Todos los días a las 1:00 PM (GMT-5 Lima)
            "options": {"queue": "cronjobs"},
        },
        # �💱 Actualización automática de Tipo de Cambio SUNAT - Diaria
        "actualizar-tipo-cambio-diario": {
            "task": "toolbox.tipo_cambio",
            "schedule": crontab(
                hour=6, minute=30
            ),  # Todos los días a las 6:30 AM (GMT-5 Lima)
            "options": {"queue": "cronjobs"},
            # 📅 Solo actualizar últimos 7 días por defecto
            "kwargs": {"batch_size": 1},
        },
    },
    beat_scheduler="celery.beat:PersistentScheduler",  # Scheduler por defecto
)

logger.info("✅ Celery configurado correctamente")
