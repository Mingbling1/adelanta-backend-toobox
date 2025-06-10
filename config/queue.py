from rq import Queue
from config.redis import redis_client_manager
from config.logger import logger
from typing import Callable
import asyncio
import functools


class QueueManager:
    """
    Gestor de colas de tareas usando Redis Queue (RQ).
    Permite ejecutar funciones en segundo plano.
    """

    def __init__(self, queue_name="default"):
        """
        Inicializa un gestor de cola.

        Args:
            queue_name: Nombre de la cola (útil para separar por prioridad o tipo)
        """
        # Obtener la conexión Redis síncrona (importante para RQ)
        redis_conn = redis_client_manager.get_sync_client()
        self.queue = Queue(queue_name, connection=redis_conn)

    def enqueue(self, func: Callable, *args, **kwargs) -> str:
        """
        Encola una función para ejecución en segundo plano.

        Args:
            func: Función a ejecutar
            *args, **kwargs: Argumentos para la función

        Returns:
            ID del trabajo en la cola
        """
        job = self.queue.enqueue(func, *args, **kwargs)
        logger.info(f"Tarea encolada con ID: {job.id}")
        return job.id

    async def enqueue_async(self, func: Callable, *args, **kwargs) -> str:
        """
        Versión asíncrona de enqueue para uso en contextos async.

        Args:
            func: Función a ejecutar
            *args, **kwargs: Argumentos para la función

        Returns:
            ID del trabajo en la cola
        """
        # RQ no es async, así que ejecutamos en un thread separado
        loop = asyncio.get_event_loop()
        job_id = await loop.run_in_executor(
            None, functools.partial(self.enqueue, func, *args, **kwargs)
        )
        return job_id


# Crear instancias para diferentes colas
email_queue = QueueManager("emails")
