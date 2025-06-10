from fastapi import WebSocket
import logging
import asyncio


class WebSocketManager(logging.Handler):
    def __init__(self):
        super().__init__()
        self.active_connections: dict[str, WebSocket] = {}
        self.log_dict = {}
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
        except Exception as e:
            logging.error(f"Error accepting WebSocket connection for {job_id}: {e}")

    def disconnect(self, job_id: str):
        self.active_connections.pop(job_id, None)

    async def send_message(self, message: str, job_id: str):
        websocket = self.active_connections.get(job_id)
        if websocket:
            try:
                await websocket.send_text(message)

            except Exception as e:
                logging.error(f"Error sending message to WebSocket {job_id}: {e}")
                self.disconnect(job_id)

    async def safe_send_message(self, log_entry: str, job_id: str):
        try:
            await self.send_message(log_entry, job_id)
        except Exception as e:
            logging.error(f"Error in WebSocket task: {e}")
            self.disconnect(job_id)

    def emit(self, record):
        log_entry = self.format(record)
        self.log_dict[record.created] = log_entry
        if len(self.log_dict) > 1000:  # Limitar tamaño del log
            oldest_key = min(self.log_dict.keys())
            del self.log_dict[oldest_key]
        self.logger.handle(record)

        job_id = getattr(self, "job_id", None)
        if job_id:
            asyncio.create_task(self.safe_send_message(log_entry, job_id))


websocket_manager = WebSocketManager()
