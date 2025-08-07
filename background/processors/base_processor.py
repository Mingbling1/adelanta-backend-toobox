# background/processors/base_processor.py
"""
🔄 Clase Base para Processors - Lógica Centralizada de Celery Task Status
Elimina duplicación de código entre todos los processors
"""

from typing import Dict, Any
from config.logger import logger
from background.schemas.task_schema import TaskStatusResponse
from config.celery_config import celery_app
from datetime import datetime, timedelta, time
import pytz


class BaseProcessor:
    """
    🔄 Clase base para todos los processors
    Centraliza la lógica de get_task_status() y formateo de respuestas
    """

    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        Obtener estado de una task de Celery con manejo robusto de errores
        ✅ LÓGICA CENTRALIZADA - Todos los processors heredan este método
        """
        try:

            result = celery_app.AsyncResult(task_id)

            # Manejo más cuidadoso del resultado
            task_result = None
            if result.ready():
                try:
                    task_result = result.result
                except Exception as result_error:
                    # Si no se puede obtener el resultado, crear uno sintético
                    logger.warning(
                        f"⚠️ No se pudo obtener resultado de task {task_id}: {result_error}"
                    )
                    task_result = {
                        "status": "failed",
                        "error": {
                            "error_type": "ResultRetrievalError",
                            "error_message": str(result_error),
                        },
                    }

            return {
                "task_id": task_id,
                "status": result.status,
                "result": task_result,
                "ready": result.ready(),
                "successful": result.successful() if result.ready() else None,
                "failed": result.failed() if result.ready() else None,
            }

        except Exception as e:
            logger.error(f"❌ Error obteniendo estado de task: {str(e)}")
            return {
                "task_id": task_id,
                "status": "ERROR",
                "result": None,
                "ready": False,
                "successful": None,
                "failed": None,
                "error": str(e),
            }

    @staticmethod
    def format_task_response(task_id: str) -> TaskStatusResponse:
        """
        🎯 Método estático para formatear respuestas Celery desde routers directos
        Unifica el formateo entre processors y routers que llaman directo a Celery
        """
        try:
            logger.info(f"🔍 API: Consultando estado de task: {task_id}")

            # Crear instancia temporal para usar get_task_status
            temp_processor = BaseProcessor()
            status_info = temp_processor.get_task_status(task_id)

            logger.info(f"📊 API: Estado de task {task_id}: {status_info['status']}")

            # Procesar resultado de forma más flexible
            task_result = status_info.get("result")
            error_info = None

            # Si hay un resultado y contiene información de error
            if isinstance(task_result, dict) and task_result.get("status") == "failed":
                error_info = task_result.get("error", {})
                if isinstance(error_info, dict):
                    error_msg = f"{error_info.get('error_type', 'Unknown')}: {error_info.get('error_message', 'Unknown error')}"
                else:
                    error_msg = str(error_info)
            else:
                error_msg = status_info.get("error")

            # Asegurar que todos los campos opcionales estén presentes
            response_data = {
                "task_id": status_info.get("task_id", task_id),
                "status": status_info.get("status", "UNKNOWN"),
                "result": task_result,
                "ready": status_info.get("ready", False),
                "successful": status_info.get("successful"),
                "failed": status_info.get("failed"),
                "error": error_msg,
            }

            return TaskStatusResponse(**response_data)

        except Exception as e:
            logger.error(f"❌ API: Error consultando estado: {str(e)}")

            # Devolver respuesta estructurada en caso de error
            error_response = {
                "task_id": task_id,
                "status": "API_ERROR",
                "result": None,
                "ready": False,
                "successful": None,
                "failed": None,
                "error": str(e),
            }

            return TaskStatusResponse(**error_response)

    def get_formatted_task_status(self, task_id: str) -> TaskStatusResponse:
        """
        🎯 Método de instancia que combina get_task_status + formateo
        Para processors que quieren usar el formateo completo
        """
        try:
            logger.info(f"🔍 Processor: Consultando estado de task: {task_id}")

            status_info = self.get_task_status(task_id)

            logger.info(
                f"📊 Processor: Estado de task {task_id}: {status_info['status']}"
            )

            # Procesar resultado de forma más flexible
            task_result = status_info.get("result")
            error_info = None

            # Si hay un resultado y contiene información de error
            if isinstance(task_result, dict) and task_result.get("status") == "failed":
                error_info = task_result.get("error", {})
                if isinstance(error_info, dict):
                    error_msg = f"{error_info.get('error_type', 'Unknown')}: {error_info.get('error_message', 'Unknown error')}"
                else:
                    error_msg = str(error_info)
            else:
                error_msg = status_info.get("error")

            # Asegurar que todos los campos opcionales estén presentes
            response_data = {
                "task_id": status_info.get("task_id", task_id),
                "status": status_info.get("status", "UNKNOWN"),
                "result": task_result,
                "ready": status_info.get("ready", False),
                "successful": status_info.get("successful"),
                "failed": status_info.get("failed"),
                "error": error_msg,
            }

            return TaskStatusResponse(**response_data)

        except Exception as e:
            logger.error(f"❌ Processor: Error consultando estado: {str(e)}")

            # Devolver respuesta estructurada en caso de error
            error_response = {
                "task_id": task_id,
                "status": "API_ERROR",
                "result": None,
                "ready": False,
                "successful": None,
                "failed": None,
                "error": str(e),
            }

            return TaskStatusResponse(**error_response)

    @staticmethod
    def get_available_tasks() -> Dict[str, Any]:
        """
        🎯 Método estático para obtener automáticamente todas las tasks disponibles
        Extrae información directamente del registro de Celery con máxima flexibilidad
        """
        try:
            logger.info("📋 Obteniendo lista automática de tasks disponibles...")

            available_tasks = {}

            # Obtener tasks registradas desde celery_app
            registered_tasks = celery_app.tasks

            # Filtros más flexibles para detectar tasks de background
            background_task_patterns = [
                "toolbox.",
                "background.",
                # Agregar más patrones si es necesario
            ]

            # Filtrar tasks de background con múltiples patrones
            background_tasks = {}
            for name, task in registered_tasks.items():
                # Verificar si coincide con algún patrón de background tasks
                is_background_task = any(
                    name.startswith(pattern) for pattern in background_task_patterns
                )

                # Excluir tasks internas de Celery
                is_celery_internal = name.startswith("celery.")

                if is_background_task and not is_celery_internal:
                    background_tasks[name] = task

            logger.info(
                f"📊 Encontradas {len(background_tasks)} background tasks registradas:"
            )

            # Log detallado de todas las tasks encontradas
            for task_name in background_tasks.keys():
                logger.info(f"  ✅ {task_name}")

            for task_name, task_obj in background_tasks.items():
                # Extraer información de la task con manejo robusto de errores
                try:
                    # Limpiar el nombre para el key con múltiples patrones
                    clean_name = task_name
                    for pattern in background_task_patterns:
                        clean_name = clean_name.replace(pattern, "")

                    # Manejar casos especiales de nombres
                    if not clean_name:
                        clean_name = task_name.split(".")[-1]

                    # Obtener descripción desde docstring con fallback más robusto
                    description = "Task de background"
                    try:
                        if hasattr(task_obj, "__doc__") and task_obj.__doc__:
                            # Limpiar docstring y extraer primera línea significativa
                            doc_lines = task_obj.__doc__.strip().split("\n")
                            for line in doc_lines:
                                line = line.strip()
                                if (
                                    line
                                    and not line.startswith('"""')
                                    and not line.startswith("'''")
                                ):
                                    description = line
                                    break

                        # Si no hay docstring, obtener del nombre de la función
                        if description == "Task de background" and hasattr(
                            task_obj, "__name__"
                        ):
                            description = f"Task: {task_obj.__name__}"

                    except Exception as doc_error:
                        logger.warning(
                            f"⚠️ Error obteniendo docstring para {task_name}: {doc_error}"
                        )

                    # Generar endpoint basado en el nombre limpio
                    endpoint_name = clean_name.replace("_", "-")
                    endpoint = f"/tasks/execute/{endpoint_name}"

                    # Obtener información adicional del módulo
                    module_name = "Unknown"
                    try:
                        module_name = getattr(task_obj, "__module__", "Unknown")
                    except Exception as module_error:
                        logger.warning(
                            f"⚠️ Error obteniendo módulo para {task_name}: {module_error}"
                        )

                    available_tasks[clean_name] = {
                        "name": task_name,
                        "description": description,
                        "endpoint": endpoint,
                        "method": "POST",
                        "module": module_name,
                        "clean_name": clean_name,
                        "original_name": task_name,
                    }

                    logger.info(f"  📋 Procesada: {clean_name} -> {endpoint}")

                except Exception as task_error:
                    logger.error(f"❌ Error procesando task {task_name}: {task_error}")
                    # Agregar task básica aun con error para no perder información
                    fallback_name = task_name.split(".")[-1]
                    available_tasks[fallback_name] = {
                        "name": task_name,
                        "description": f"Task con error de procesamiento: {str(task_error)}",
                        "endpoint": f"/tasks/execute/{fallback_name}",
                        "method": "POST",
                        "module": "Error",
                        "error": str(task_error),
                    }
                    continue

            logger.info(f"✅ Procesadas {len(available_tasks)} tasks exitosamente")

            # Información detallada para debugging
            debug_info = {
                "total_registered_tasks": len(registered_tasks),
                "background_tasks_found": len(background_tasks),
                "successfully_processed": len(available_tasks),
                "registered_task_names": list(registered_tasks.keys()),
                "background_task_names": list(background_tasks.keys()),
            }

            return {
                "success": True,
                "available_tasks": available_tasks,
                "total_tasks": len(available_tasks),
                "auto_generated": True,
                "debug_info": debug_info,
            }

        except Exception as e:
            logger.error(f"❌ Error obteniendo tasks disponibles: {str(e)}")

            # Fallback a lista manual en caso de error
            return BaseProcessor._get_manual_tasks_list()

    @staticmethod
    def _get_manual_tasks_list() -> Dict[str, Any]:
        """
        🔄 Fallback: Lista manual de tasks en caso de que falle la automatización
        ACTUALIZADA: Incluye todas las 4 tasks conocidas
        """
        logger.info("🔄 Usando lista manual de tasks como fallback...")

        manual_tasks = {
            "kpi_acumulado": {
                "name": "toolbox.kpi_acumulado",
                "description": "Actualiza la tabla de KPI acumulado",
                "endpoint": "/tasks/execute/kpi-acumulado",
                "method": "POST",
                "module": "background.tasks.toolbox.kpi_acumulado_task",
            },
            "kpi": {
                "name": "toolbox.kpi",
                "description": "Actualiza tablas de reportes (versión alternativa)",
                "endpoint": "/tasks/execute/kpi",
                "method": "POST",
                "module": "background.tasks.toolbox.kpi_task",
            },
            "tablas_reportes": {
                "name": "toolbox.tablas_reportes",
                "description": "Actualiza las tablas de reportes: KPI, NuevosClientes, Saldos",
                "endpoint": "/tasks/execute/tablas-reportes",
                "method": "POST",
                "module": "background.tasks.toolbox.tablas_reportes_task",
            },
            "tablas_cxc": {
                "name": "toolbox.tablas_cxc",
                "description": "Actualizar Tablas CXC con ETL Power BI completo",
                "endpoint": "/tasks/execute/tablas-cxc",
                "method": "POST",
                "module": "background.tasks.toolbox.tablas_cxc_task",
            },
        }

        logger.info(f"📋 Fallback manual incluye {len(manual_tasks)} tasks definidas")

        return {
            "success": True,
            "available_tasks": manual_tasks,
            "total_tasks": len(manual_tasks),
            "auto_generated": False,
            "fallback": True,
            "note": "Lista manual usada como fallback - verificar configuración de Celery",
        }

    @staticmethod
    def get_scheduled_tasks() -> Dict[str, Any]:
        """
        🕐 Obtener información detallada del beat_schedule configurado en Celery
        Para mostrar en frontend las tareas programadas automáticamente
        """
        try:
            logger.info("🕐 Obteniendo información del beat_schedule...")

            # Obtener configuración del beat_schedule desde celery_app
            beat_schedule = celery_app.conf.get("beat_schedule", {})

            if not beat_schedule:
                logger.warning(
                    "⚠️ No se encontraron tareas programadas en beat_schedule"
                )
                return {
                    "success": True,
                    "scheduled_tasks": {},
                    "total_scheduled": 0,
                    "message": "No hay tareas programadas automáticamente",
                }

            scheduled_tasks = {}

            logger.info(f"📊 Procesando {len(beat_schedule)} tareas programadas...")

            for schedule_name, schedule_config in beat_schedule.items():
                try:
                    # Extraer información básica de la configuración
                    task_name = schedule_config.get("task", "Unknown")
                    schedule_obj = schedule_config.get("schedule")
                    options = schedule_config.get("options", {})

                    # Formatear información del schedule
                    schedule_info = "Configuración no disponible"
                    next_run = None

                    if schedule_obj:
                        try:
                            # Si es un objeto crontab, extraer información legible de forma segura
                            if hasattr(schedule_obj, "hour") and hasattr(
                                schedule_obj, "minute"
                            ):
                                # Extraer valores sin usar format() problemático
                                hour_val = getattr(schedule_obj, "hour", None)
                                minute_val = getattr(schedule_obj, "minute", None)
                                day_of_week_val = getattr(
                                    schedule_obj, "day_of_week", None
                                )
                                day_of_month_val = getattr(
                                    schedule_obj, "day_of_month", None
                                )
                                month_of_year_val = getattr(
                                    schedule_obj, "month_of_year", None
                                )

                                # Convertir valores de set/frozenset a enteros si es posible
                                if (
                                    isinstance(hour_val, (set, frozenset))
                                    and len(hour_val) == 1
                                ):
                                    hour_val = next(iter(hour_val))
                                if (
                                    isinstance(minute_val, (set, frozenset))
                                    and len(minute_val) == 1
                                ):
                                    minute_val = next(iter(minute_val))

                                # Formatear de forma segura
                                if isinstance(hour_val, int) and isinstance(
                                    minute_val, int
                                ):
                                    schedule_info = f"Todos los días a las {hour_val:02d}:{minute_val:02d}"
                                else:
                                    # Fallback a representación string directa
                                    schedule_info = f"Cron: {minute_val} {hour_val} {day_of_month_val} {month_of_year_val} {day_of_week_val}"

                            # Para next_run, vamos a calcularlo manualmente ya que Celery no lo proporciona fácilmente
                            if isinstance(hour_val, int) and isinstance(
                                minute_val, int
                            ):
                                try:

                                    # Timezone Lima
                                    lima_tz = pytz.timezone("America/Lima")
                                    now_lima = datetime.now(lima_tz)

                                    # Crear próxima ejecución para hoy
                                    today_execution = now_lima.replace(
                                        hour=hour_val,
                                        minute=minute_val,
                                        second=0,
                                        microsecond=0,
                                    )

                                    # Si ya pasó hoy, programar para mañana
                                    if today_execution <= now_lima:
                                        next_execution = today_execution + timedelta(
                                            days=1
                                        )
                                    else:
                                        next_execution = today_execution

                                    next_run = next_execution.isoformat()

                                except Exception as next_run_error:
                                    logger.debug(
                                        f"No se pudo calcular next_run para {schedule_name}: {next_run_error}"
                                    )
                                    next_run = None
                            else:
                                next_run = None

                        except Exception as schedule_parse_error:
                            logger.warning(
                                f"⚠️ Error parseando schedule para {schedule_name}: {schedule_parse_error}"
                            )
                            # Fallback seguro sin usar format()
                            schedule_info = repr(schedule_obj)
                            next_run = None

                    # Determinar descripción de la task
                    task_description = "Tarea programada automáticamente"
                    if task_name == "toolbox.tablas_reportes":
                        task_description = "Actualización automática de Tablas Reportes (KPI, NuevosClientes, Saldos)"
                    elif task_name == "toolbox.kpi_acumulado":
                        task_description = "Actualización automática de KPI Acumulado"
                    elif task_name == "toolbox.tablas_cxc":
                        task_description = "Actualización automática de Tablas CXC"

                    scheduled_tasks[schedule_name] = {
                        "schedule_name": schedule_name,
                        "task_name": task_name,
                        "description": task_description,
                        "schedule_info": schedule_info,
                        "next_run": next_run,
                        "queue": options.get("queue", "default"),
                        "enabled": True,  # Por defecto habilitado si está en configuración
                        "options": options,
                    }

                    logger.info(
                        f"  ✅ Procesada: {schedule_name} -> {task_name} ({schedule_info})"
                    )

                except Exception as task_error:
                    logger.error(
                        f"❌ Error procesando tarea programada {schedule_name}: {task_error}"
                    )
                    # Agregar información básica aun con error
                    scheduled_tasks[schedule_name] = {
                        "schedule_name": schedule_name,
                        "task_name": schedule_config.get("task", "Unknown"),
                        "description": "Error obteniendo información",
                        "schedule_info": "Error en configuración",
                        "next_run": None,
                        "queue": "unknown",
                        "enabled": False,
                        "error": str(task_error),
                    }

            logger.info(
                f"✅ Procesadas {len(scheduled_tasks)} tareas programadas exitosamente"
            )

            return {
                "success": True,
                "scheduled_tasks": scheduled_tasks,
                "total_scheduled": len(scheduled_tasks),
                "timezone": (
                    str(celery_app.conf.timezone) if celery_app.conf.timezone else "UTC"
                ),
                "beat_scheduler": celery_app.conf.beat_scheduler,
                "message": f"Se encontraron {len(scheduled_tasks)} tareas programadas automáticamente",
            }

        except Exception as e:
            logger.error(f"❌ Error obteniendo beat_schedule: {str(e)}")
            return {
                "success": False,
                "scheduled_tasks": {},
                "total_scheduled": 0,
                "error": str(e),
                "message": "Error obteniendo información de tareas programadas",
            }
