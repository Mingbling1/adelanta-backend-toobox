# 🏗️ Background Tasks Architecture - Adelanta Backend Toolbox

**🔧 Stack**: Celery + FastAPI + Repository Factory pattern para tareas asíncronas distribuidas.

## 📁 Estructura Core

```bash
background/
├── tasks/toolbox/              # 📋 Celery Tasks + Business Logic
│   ├── kpi_acumulado_task.py   # @celery_app.task + lógica completa
│   ├── tablas_reportes_task.py # Automatizada: 7AM y 6PM daily
│   └── tablas_cxc_task.py
├── processors/                 # 🔄 OPCIONAL: Wrappers compatibilidad
│   ├── base_processor.py       # get_task_status() centralizado
│   └── toolbox/                # Heredan de BaseProcessor
├── routers/                    # 🎮 API Endpoints
│   ├── base_router.py          # Centralizados: /status, /available, /scheduled
│   └── toolbox/                # Específicos: /execute por módulo
└── schemas/task_schema.py      # 📋 Validación Pydantic
```

## 🔄 Flujo de Responsabilidades

**Task Pattern**: Router → Celery Task (+ Repository Factory) → Database

1. **Router**: Endpoints HTTP (`POST /execute`, `GET /status/{task_id}`)
2. **Task**: Business logic + Celery orchestration + repository_factory
3. **Repository Factory**: Sesiones aisladas, cleanup automático, pool optimizado
4. **BaseProcessor**: Formateo centralizado, status management (OPCIONAL)

## 📋 Patrones de Implementación

### Task Estándar

```python
# background/tasks/toolbox/ejemplo_task.py
@celery_app.task(name="toolbox.ejemplo", bind=True, max_retries=0)
def ejemplo_task(self):
    repository_factory = create_repository_factory()
    try:
        # Business logic aquí
        return {"status": "success", "records": count}
    finally:
        repository_factory.cleanup()
```

### Router Estándar

```python
# background/routers/toolbox/ejemplo_router.py
@router.post("/execute", response_class=ORJSONResponse)
async def execute_task():
    """🎯 Ejecutar [Descripción] usando Celery"""
    processor = EjemploProcessor()
    return await processor.run()

@router.get("/status/{task_id}")
async def get_status(task_id: str):
    return BaseProcessor.format_task_response(task_id)
```

### BaseProcessor Centralizado

```python
# background/processors/base_processor.py
class BaseProcessor:
    @staticmethod
    def format_task_response(task_id: str):
        """📊 Status unificado para cualquier task"""

    @staticmethod
    def get_available_tasks():
        """🤖 Auto-discovery desde celery_app.tasks"""

    @staticmethod
    def get_scheduled_tasks():
        """🕐 Beat schedule info para frontend"""
```

## 🎯 Principios de Arquitectura

### ✅ Reglas Core

-   **Tasks**: Contienen business logic + Celery orchestration
-   **Repository Factory**: NO usar `@inject`, sesiones aisladas por task
-   **Processors**: OPCIONALES, solo wrappers de compatibilidad
-   **BaseProcessor**: Centraliza status/formateo, elimina duplicación

### ✅ Configuración Celery

```python
# config/celery_config.py
include=["background.tasks.toolbox"]

beat_schedule = {
    "actualizar-tablas-reportes-manana": {
        "task": "toolbox.tablas_reportes",
        "schedule": crontab(hour=7, minute=0),  # 7:00 AM
    },
    "actualizar-tablas-reportes-tarde": {
        "task": "toolbox.tablas_reportes",
        "schedule": crontab(hour=18, minute=0), # 6:00 PM
    },
}
```

## 🚀 API Endpoints

### Centralizados (base_router.py)

```bash
GET  /tasks/status/{task_id}     # Status para cualquier task
GET  /tasks/available            # Auto-discovery de tasks disponibles
GET  /tasks/scheduled            # Beat schedule info para frontend
```

### Específicos (toolbox/)

```bash
POST /tasks/execute/kpi-acumulado     # Ejecutar task específica
POST /tasks/execute/tablas-reportes   # Con control remoto
POST /tasks/execute/tablas-cxc        # Logging + error handling
```

## 🔧 Comandos de Desarrollo

```bash
# Worker
uv run celery -A config.celery_config worker --loglevel=info --queues=cronjobs,default

# Scheduler (Beat)
uv run celery -A config.celery_config beat --loglevel=info

# Testing
pytest                           # Tests desarrollo (SQLite)
pytest -m production            # Tests producción (MySQL real)
```

## 📊 Estado Actual del Sistema

### ✅ Tasks Disponibles

-   `toolbox.kpi_acumulado` - Actualización KPI consolidados
-   `toolbox.tablas_reportes` - Reportes automáticos (7AM/6PM)
-   `toolbox.tablas_cxc` - Cuentas por cobrar
-   `toolbox.kpi` - KPI generales

### ✅ Automatización Configurada

-   **7:00 AM**: Ejecución automática tablas_reportes
-   **6:00 PM**: Ejecución automática tablas_reportes
-   **Scheduler**: PersistentScheduler con archivo de estado

### ✅ Endpoints Funcionales

-   Auto-discovery de tasks desde celery_app
-   Status centralizado con formateo unificado
-   Beat schedule info para dashboard frontend

## 🛠️ Troubleshooting

### ❌ Task no aparece en /tasks/available

1. Verificar import en `background/tasks/toolbox/__init__.py`
2. Verificar decorador: `@celery_app.task(name="toolbox.xxx")`
3. Verificar include en `config/celery_config.py`

### ❌ Celery Beat restart (exit code 137)

1. Verificar formato crontab (no strings): `crontab(hour=7, minute=0)`
2. Verificar memoria Docker: aumentar límites en docker-compose.yml
3. Verificar beat_schedule syntax en celery_config.py

### 🆕 Agregar Nueva Task

```bash
# 1. Crear task
touch background/tasks/toolbox/nueva_task.py

# 2. Implementar con patrón estándar
@celery_app.task(name="toolbox.nueva_task")
def nueva_task(): ...

# 3. Agregar import
# background/tasks/toolbox/__init__.py
from .nueva_task import nueva_task

# 4. ¡Automático! Aparece en /tasks/available
```

## 📈 Ventajas del Sistema Actual

-   **🔄 Auto-Discovery**: Nuevas tasks detectadas automáticamente
-   **🛡️ Robustez**: Manejo unificado de errores y fallbacks
-   **📊 Observabilidad**: Logging detallado y debug info
-   **🧹 Sin Duplicación**: ~150 líneas eliminadas por centralización
-   **⚡ Performance**: ORJSONResponse y pool optimizado
-   **🕐 Programación**: Beat schedule para tareas recurrentes

## 📋 Frontend Integration

### GET /tasks/scheduled Response

```json
{
    "success": true,
    "scheduled_tasks": {
        "actualizar-tablas-reportes-manana": {
            "task_name": "toolbox.tablas_reportes",
            "schedule_info": "Todos los días a las 07:00",
            "next_run": "2025-08-07T07:00:00",
            "enabled": true
        }
    },
    "total_scheduled": 2,
    "timezone": "America/Lima"
}
```

**Uso**: Dashboard de tareas programadas con horarios y próximas ejecuciones.

---

Esta arquitectura garantiza **escalabilidad**, **mantenibilidad** y **testabilidad** para el crecimiento futuro del proyecto.
