from fastapi import WebSocket
import logging
import asyncio
from typing import Set
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from config.redis import redis_manager
import json


class JobStatus(Enum):
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"
    CANCELLED = "cancelled"


@dataclass
class JobInfo:
    job_id: str
    status: JobStatus
    start_time: datetime
    end_time: datetime = None
    error_message: str = None


class WebSocketManager(logging.Handler):
    def __init__(self):
        super().__init__()
        self.active_connections: dict[str, WebSocket] = {}
        self.active_jobs: Set[str] = set()
        self.job_statuses: dict[str, JobStatus] = {}  # Tracking local de estados

        self.logger = logging.getLogger("uvicorn.error")
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.setFormatter(formatter)
        self.setLevel(logging.DEBUG)

    async def connect(self, websocket: WebSocket, job_id: str):
        try:
            await websocket.accept()
            self.active_connections[job_id] = websocket
            self.active_jobs.add(job_id)
            self.job_statuses[job_id] = JobStatus.RUNNING

            # Establecer estado inicial en Redis
            await redis_manager.set_job_status(
                job_id=job_id,
                status=JobStatus.RUNNING.value,
                details={
                    "start_time": datetime.now().isoformat(),
                    "websocket_connected": True,
                },
            )

            # Enviar mensaje de conexión estructurado
            await self.send_structured_message(
                "connection", f"🔌 Conectado al job: {job_id}", job_id
            )

            # Enviar logs existentes desde Redis
            await self.send_existing_logs(job_id)

        except Exception as e:
            logging.error(f"Error accepting WebSocket connection for {job_id}: {e}")

    async def send_existing_logs(self, job_id: str):
        """Enviar logs existentes desde Redis al conectarse"""
        try:
            logs = await redis_manager.get_job_logs(job_id, limit=50)

            if logs:
                for log_entry in reversed(logs):
                    message = f"[HISTORIAL] {log_entry.get('message', '')}"
                    await self.send_structured_message("log", message, job_id)

        except Exception as e:
            logging.error(f"Error sending existing logs for {job_id}: {e}")

    def disconnect(self, job_id: str):
        self.active_connections.pop(job_id, None)
        self.active_jobs.discard(job_id)
        self.job_statuses.pop(job_id, None)

    async def send_structured_message(
        self, message_type: str, message: str, job_id: str, close_after: bool = False
    ):
        """
        Envía un mensaje estructurado al WebSocket.

        Args:
            message_type: Tipo de mensaje ('log', 'success', 'error', 'connection')
            message: El mensaje a enviar
            job_id: ID del job
            close_after: Si debe cerrar la conexión después de enviar
        """
        websocket = self.active_connections.get(job_id)
        if websocket:
            try:
                structured_msg = {
                    "type": message_type,
                    "message": message,
                    "job_id": job_id,
                    "timestamp": datetime.now().isoformat(),
                    "status": self.job_statuses.get(job_id, JobStatus.RUNNING).value,
                }

                await websocket.send_text(json.dumps(structured_msg))

                # Si debe cerrar después de enviar
                if close_after:
                    await asyncio.sleep(0.5)  # Dar tiempo para que el mensaje llegue
                    await self.close_connection(job_id)

            except Exception as e:
                logging.error(
                    f"Error sending structured message to WebSocket {job_id}: {e}"
                )
                self.disconnect(job_id)

    async def send_message(self, message: str, job_id: str):
        """Método de compatibilidad - envía como log"""
        await self.send_structured_message("log", message, job_id)

    async def notify_job_completion(
        self, job_id: str, success: bool = True, error_message: str = None
    ):
        """
        Notifica que un job ha terminado y cierra la conexión.

        Args:
            job_id: ID del job
            success: Si terminó exitosamente
            error_message: Mensaje de error si falló
        """
        try:
            if success:
                self.job_statuses[job_id] = JobStatus.SUCCESS
                await self.send_structured_message(
                    "success",
                    f"✅ Job {job_id} completado exitosamente",
                    job_id,
                    close_after=True,
                )
            else:
                self.job_statuses[job_id] = JobStatus.ERROR
                error_msg = (
                    f"❌ Job {job_id} falló: {error_message or 'Error desconocido'}"
                )
                await self.send_structured_message(
                    "error", error_msg, job_id, close_after=True
                )

            # Actualizar estado en Redis (manejo seguro de errores)
            try:
                await redis_manager.set_job_status(
                    job_id=job_id,
                    status=(
                        JobStatus.SUCCESS.value if success else JobStatus.ERROR.value
                    ),
                    details={
                        "end_time": datetime.now().isoformat(),
                        "error_message": error_message if not success else None,
                        "websocket_connected": False,
                    },
                )
            except AttributeError as attr_err:
                logging.error(f"RedisClientManager missing method: {attr_err}")
            except Exception as redis_err:
                logging.error(f"Error updating Redis job status: {redis_err}")

        except Exception as e:
            logging.error(f"Error notifying job completion for {job_id}: {e}")

    async def close_connection(self, job_id: str):
        """Cierra una conexión WebSocket específica"""
        websocket = self.active_connections.get(job_id)
        if websocket:
            try:
                await websocket.close()
                logging.info(f"WebSocket connection closed for job {job_id}")
            except Exception as e:
                logging.debug(f"Error closing WebSocket for {job_id}: {e}")
            finally:
                self.disconnect(job_id)

    async def safe_send_message(self, log_entry: str, job_id: str):
        try:
            await self.send_message(log_entry, job_id)
        except Exception as e:
            logging.error(f"Error in WebSocket task: {e}")
            self.disconnect(job_id)

    def emit(self, record):
        self.logger.handle(record)

        current_job_id = getattr(self, "job_id", None)
        if current_job_id and current_job_id in self.active_jobs:
            log_entry = self.format(record)
            websocket = self.active_connections.get(current_job_id)
            if websocket:
                asyncio.create_task(self.safe_send_message(log_entry, current_job_id))


websocket_manager = WebSocketManager()
