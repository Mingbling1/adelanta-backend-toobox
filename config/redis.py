import redis.asyncio as aioredis
import redis
import logging
from redis.asyncio import ConnectionPool
import asyncio
from typing import Callable, Any
from io import BytesIO
import math
import uuid
import pytz
import datetime
import orjson
from config.logger import logger
from config.settings import settings
import traceback


class RedisClientManager:
    def __init__(self, url: str, pool: ConnectionPool | None = None):
        self.url = url
        if pool is None:
            pool = ConnectionPool.from_url(url, max_connections=10)
        self.pool = pool
        self.client = aioredis.Redis(connection_pool=self.pool, decode_responses=True)
        
        # Conexión síncrona para RQ
        self.sync_client = redis.Redis.from_url(url, decode_responses=True)

    async def connect(self):
        try:
            logging.debug("Connecting to Redis...")
            await self.client.ping()
            # Verificar también la conexión síncrona
            if not self.sync_client.ping():
                raise Exception("Sync Redis client failed to ping")
        except Exception as e:
            logging.error("Failed to connect to Redis: %s", e)
            raise

    async def close(self):
        if self.client:
            await self.client.close()
            logging.debug("Redis client closed")

    def get_client(self) -> aioredis.Redis:
        """Obtiene el cliente asíncrono para operaciones normales"""
        if not self.client:
            raise Exception("Redis async client is not initialized")
        return self.client
    
    def get_sync_client(self) -> redis.Redis:
        """Obtiene el cliente síncrono para RQ y otras operaciones síncronas"""
        if not self.sync_client:
            raise Exception("Redis sync client is not initialized")
        return self.sync_client

    async def get_all_jobs(
        self, search: str = "", page: int = 1, page_size: int = 10
    ) -> list[dict[str, Any]]:

        client = self.get_client()
        keys = await client.keys("job:*:status")
        jobs = []
        for key in keys:
            job_id = key.decode("utf-8").split(":")[1]
            status = await client.get(f"job:{job_id}:status")
            name = await client.get(f"job:{job_id}:name") or b""
            description = await client.get(f"job:{job_id}:description") or b""
            created_at = await client.get(f"job:{job_id}:created_at") or b""
            created_by = await client.get(f"job:{job_id}:created_by") or b""
            download_link = (
                f"/cronjob/download/{job_id}" if status == b"completed" else None
            )
            # Obtener los parámetros almacenados (si existen)
            params_bytes = await client.get(f"job:{job_id}:params")
            params = {}
            if params_bytes:
                try:
                    params = orjson.loads(params_bytes)
                except Exception:
                    params = {}
            job = {
                "job_id": job_id,
                "name": name.decode("utf-8"),
                "description": description.decode("utf-8"),
                "status": status.decode("utf-8"),
                "created_at": created_at.decode("utf-8"),
                "download_link": download_link,
                "params": params,  # Nuevo campo con los parámetros usados
                "created_by": (
                    created_by.decode("utf-8")
                    if isinstance(created_by, bytes)
                    else created_by
                ),
            }
            if (
                search.lower() in job["name"].lower()
                or search.lower() in job["description"].lower()
            ):
                jobs.append(job)

        # Implementar la paginación
        total_jobs = len(jobs)
        total_pages = math.ceil(total_jobs / page_size)
        start = (page - 1) * page_size
        end = start + page_size
        paginated_jobs = jobs[start:end]

        # Añadir información de paginación a cada trabajo
        for job in paginated_jobs:
            job["total"] = total_jobs
            job["page"] = page
            job["page_size"] = page_size
            job["total_pages"] = total_pages

        return paginated_jobs

    async def create_job(
        self,
        name: str,
        description: str,
        expire: int,
        fetch_func: Callable[[], Any],
        is_buffer: bool = False,
        save_as: str | None = None,
        params: Any = None,
        created_by: str | None = None,
    ) -> str:
        """
        Paso 1: Generar job_id y guardar metadata básica en Redis.
        Paso 2: Definir job_wrapper() que ejecuta fetch_func() y guarda resultados.
        Paso 3: Lanzar job_wrapper() como tarea de fondo y retornar job_id.
        """
        client = self.get_client()
        job_id = str(uuid.uuid4())

        # 1) metadata inicial
        await self._set_initial_metadata(
            client, job_id, name, description, created_by, params, expire
        )

        # 2) definir y lanzar el wrapper
        asyncio.create_task(
            self._job_wrapper(job_id, fetch_func, is_buffer, save_as, expire)
        )
        return job_id

    # —————————————————————————————————————————————————————————————
    # Métodos auxiliares públicos/privados para componer create_job
    # —————————————————————————————————————————————————————————————

    async def _set_initial_metadata(
        self,
        client: redis.Redis,
        job_id: str,
        name: str,
        description: str,
        created_by: str | None,
        params: Any,
        expire: int,
    ):
        """Guarda status, name, description, timestamps y created_by en Redis."""
        await client.set(f"job:{job_id}:status", "pending", ex=expire)
        await client.set(f"job:{job_id}:name", name, ex=expire)
        await client.set(f"job:{job_id}:description", description, ex=expire)

        now_utc = datetime.datetime.now(pytz.utc)
        lima_now = now_utc.astimezone(pytz.timezone("America/Lima"))
        await client.set(f"job:{job_id}:created_at", lima_now.isoformat(), ex=expire)
        await client.set(f"job:{job_id}:created_by", created_by or "", ex=expire)

        if params is not None:
            await client.set(f"job:{job_id}:params", orjson.dumps(params), ex=expire)

    async def _job_wrapper(
        self,
        job_id: str,
        fetch_func: Callable[[], Any],
        is_buffer: bool,
        save_as: str | None,
        expire: int,
    ):
        """
        Ejecuta la función fetch_func(), guarda el buffer u/orjson en Redis,
        actualiza status y refresca TTL de las claves relevantes.
        """
        client = self.get_client()
        logger.debug(f"[job_wrapper] Iniciando job {job_id}")
        try:
            data = await fetch_func()
            # Guarda el resultado (buffer o serializado)
            await self._save_result(client, job_id, data, is_buffer, save_as, expire)

        except Exception:
            err = traceback.format_exc()
            logger.exception(f"[job_wrapper] Falló job {job_id}")
            await client.set(f"job:{job_id}:error", err, ex=expire)
            await client.set(f"job:{job_id}:status", "failed", ex=expire)

        else:
            # éxito: limpiar error previo y marcar completed
            await client.set(f"job:{job_id}:error", "", ex=expire)
            await client.set(f"job:{job_id}:status", "completed", ex=expire)
            logger.debug(f"[job_wrapper] Job {job_id} completado")

            # refrescar TTL de todas las claves del job
            keys = self._gather_refresh_keys(job_id, is_buffer, save_as)
            await self._refresh_keys(client, keys, expire)

        finally:
            # si por algún motivo sigue pending, lo marcamos failed
            status = await client.get(f"job:{job_id}:status")
            if status == "pending":
                await client.set(f"job:{job_id}:status", "failed", ex=expire)

    async def _save_result(
        self,
        client: redis.Redis,
        job_id: str,
        data: Any,
        is_buffer: bool,
        save_as: str | None,
        expire: int,
    ):
        """
        Guarda en Redis:
          - si is_buffer y data es BytesIO, lo guarda bajo buffer_csv/_excel/_zip…
          - si orjson, lo guarda como buffer_orjson
          - si no buffer, serializa con orjson y guarda en job:{job_id}
        """
        if is_buffer and isinstance(data, BytesIO):
            file_bytes = data.getvalue()
            key = self._determine_buffer_key(job_id, save_as, file_bytes)
            await client.set(key, file_bytes, ex=expire)
        elif is_buffer and save_as == "orjson":
            # data ya es bytes JSON
            await client.set(f"job:{job_id}:buffer_orjson", data, ex=expire)
        else:
            payload = orjson.dumps(data)
            await client.set(f"job:{job_id}", payload, ex=expire)

    def _determine_buffer_key(
        self, job_id: str, save_as: str | None, file_bytes: bytes
    ) -> str:
        """
        Elige la clave Redis para el buffer:
         - si save_as es 'csv'|'excel'|'zip' usa ese
         - si no, intenta decodificar a utf8 para csv, si falla, excel
        """
        if save_as in ("zip", "excel", "csv"):
            suffix = "zip" if save_as == "zip" else save_as
        else:
            try:
                file_bytes.decode("utf-8")
                suffix = "csv"
            except Exception:
                suffix = "excel"
        pref = "buffer" if suffix != "zip" else ""
        return f"job:{job_id}:{pref + '_' if pref else ''}{suffix}"

    def _gather_refresh_keys(
        self, job_id: str, is_buffer: bool, save_as: str | None
    ) -> list[str]:
        """
        Lista de claves cuya TTL debe refrescarse cuando el job completa con éxito.
        """
        base = [
            f"job:{job_id}:status",
            f"job:{job_id}:name",
            f"job:{job_id}:description",
            f"job:{job_id}:created_at",
            f"job:{job_id}:created_by",
            f"job:{job_id}:params",
        ]
        if is_buffer:
            buf_key = (
                "buffer_orjson"
                if save_as == "orjson"
                else ("zip" if save_as == "zip" else f"buffer_{save_as or 'csv'}")
            )
            base.append(f"job:{job_id}:{buf_key}")
        return base

    async def _refresh_keys(self, client: redis.Redis, keys: list[str], ttl: int):
        """Vuelve a fijar expire=ttl en cada clave de la lista."""
        for key in keys:
            try:
                await client.expire(key, ttl)
            except Exception as e:
                logger.exception(f"Error refrescando TTL para {key}: {e}")


# redis_client_manager = RedisClientManager("redis://localhost:6379")
redis_client_manager = RedisClientManager(settings.REDIS_URL)
