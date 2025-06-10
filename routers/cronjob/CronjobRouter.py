from fastapi import APIRouter, HTTPException, Query
from apscheduler.jobstores.base import JobLookupError
from cronjobs.BaseCronjob import BaseCronjob
from config.logger import logger
from config.cronjob import CRONJOB
from cronjobs.schemas.CronjobSchema import CronjobSchema, CronjobNowSchema
from apscheduler.triggers.cron import CronTrigger
from config.wsmanager import websocket_manager
from fastapi import WebSocket, WebSocketDisconnect
import math
from fastapi.websockets import WebSocketState
from fastapi.responses import ORJSONResponse, StreamingResponse
import traceback
import aiohttp
import json
import asyncio
from config.redis import redis_client_manager  # Adjust the import path as necessary
import orjson

router = APIRouter()


@router.get("/jobs/registered")
async def list_registered_jobs():
    jobs = []
    for job_id, job_info in BaseCronjob._registry.items():
        job_details = {
            "job_id": job_id,
            "class_name": job_info["class_name"],
            "description": job_info["description"],
        }
        jobs.append(job_details)
    logger.debug(jobs)
    return jobs


@router.get("/jobs/active")
async def list_active_jobs(
    cronjob: CRONJOB,
    limit: int = 10,
    offset: int = 1,
    job_id: str = Query(None),
    name: str = Query(None),
):
    jobs = []
    for job in cronjob.get_jobs():
        job_info = {
            "job_id": job.id,
            "next_run_time": job.next_run_time,
            "name": job.name,
            "state": "active" if job.next_run_time else "inactive",
        }
        jobs.append(job_info)

    # Filtrar por job_id y name
    if job_id:
        jobs = [job for job in jobs if job_id in job["job_id"]]
    if name:
        jobs = [job for job in jobs if name.lower() in job["name"].lower()]

    # Paginación
    total_jobs = len(jobs)
    total_pages = math.ceil(total_jobs / limit)
    start = (offset - 1) * limit
    end = start + limit
    paginated_jobs = jobs[start:end]

    # Añadir total_pages a cada trabajo
    for job in paginated_jobs:
        job["total_pages"] = total_pages

    return paginated_jobs


@router.post("/jobs/schedule")
async def schedule_job(
    config: CronjobSchema,
    cronjob: CRONJOB,
):
    try:
        job_info = BaseCronjob._registry.get(config.cronjob_config.job_id)
        if not job_info:
            raise HTTPException(status_code=404, detail="Job not found")

        job_callable = job_info["callable"]
        cronjob_kwargs = config.cronjob_config.model_dump(exclude={"job_id"})
        logger.debug(cronjob_kwargs)
        for hour, minute in config.times:
            trigger = CronTrigger(
                day_of_week=config.day_of_week,
                hour=hour,
                minute=minute,
                timezone="America/Lima",
            )
            cronjob.add_job(
                job_callable,
                trigger,
                id=f"{config.cronjob_config.job_id}_{hour}_{minute}",
                replace_existing=True,
                kwargs=cronjob_kwargs,
            )

        return {
            "message": f"Job {config.cronjob_config.job_id} scheduled successfully at specified times"
        }
    except HTTPException as e:
        return ORJSONResponse(
            status_code=e.status_code,
            content={"detail": e.detail},
        )
    except Exception as e:
        error_trace = traceback.format_exc()
        print(error_trace)
        return ORJSONResponse(
            status_code=500,
            content={"detail": str(e), "traceback": error_trace},
        )


@router.post("/jobs/schedule/now")
async def schedule_job_now(config: CronjobNowSchema):
    try:
        job_info = BaseCronjob._registry.get(config.cronjob_config.job_id)
        if not job_info:
            raise HTTPException(status_code=404, detail="Job not found")

        job_callable = job_info["callable"]
        cronjob_kwargs = config.cronjob_config.model_dump(exclude={"job_id"})
        logger.debug(cronjob_kwargs)

        async def job_wrapper():
            async def send_output(message: str, job_id: str):
                await websocket_manager.send_message(message, job_id)

            log_dict = {}

            log_handler = websocket_manager
            log_handler.log_dict = log_dict
            log_handler.job_id = config.cronjob_config.job_id

            logger.addHandler(log_handler)

            try:
                await send_output(
                    f"Starting job {config.cronjob_config.job_id}",
                    config.cronjob_config.job_id,
                )
                await job_callable(**cronjob_kwargs)
                await send_output(
                    f"Job {config.cronjob_config.job_id} completed",
                    config.cronjob_config.job_id,
                )
            except Exception as e:
                await send_output(
                    f"Job {config.cronjob_config.job_id} failed: {str(e)}",
                    config.cronjob_config.job_id,
                )
                logger.error(f"Job {config.cronjob_config.job_id} failed: {str(e)}")
                raise e
            finally:
                logger.removeHandler(log_handler)

        await job_wrapper()

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/activate/{job_id}")
async def activate_job(job_id: str, cronjob: CRONJOB):
    try:
        cronjob.resume_job(job_id)
        return {"message": f"Job {job_id} activated successfully"}
    except JobLookupError:
        raise HTTPException(status_code=404, detail="Job not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/deactivate/{job_id}")
async def deactivate_job(job_id: str, cronjob: CRONJOB):
    try:
        cronjob.pause_job(job_id)
        return {"message": f"Job {job_id} deactivated successfully"}
    except JobLookupError:
        raise HTTPException(status_code=404, detail="Job not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.websocket("/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    await websocket_manager.connect(websocket, job_id)
    try:
        while True:
            data = await websocket.receive_text()
            await websocket_manager.send_message(f"Message text was: {data}", job_id)
    except WebSocketDisconnect:
        websocket_manager.disconnect(job_id)


@router.websocket("/jobs/relay")
async def relay_websocket(
    websocket: WebSocket, page: int = 1, page_size: int = 10, search: str = ""
):
    try:
        await websocket.accept()
    except Exception as e:
        logger.error("Error accepting websocket: %s", e)
        return

    remote_url = (
        f"wss://webservice.adelantafactoring.com/webservice/cronjob/jobs/websocket"
        f"?page={page}&page_size={page_size}&search={search}"
    )
    # Verificar si el websocket remoto está disponible
    # Bucle principal mientras la conexión del cliente esté activa
    while websocket.application_state == WebSocketState.CONNECTED:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(remote_url) as remote_ws:
                    # Recorrer mensajes provenientes del websocket remoto
                    async for msg in remote_ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                await websocket.send_json(data)
                            except Exception as e:
                                logger.error("Error parsing JSON: %s", e)
                                await websocket.send_json([])  # Devuelve []
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error("Remote websocket error")
                            break
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            logger.info("Remote websocket closed")
                            break
        except Exception as e:
            logger.error("Error connecting to remote websocket: %s", e)
            # Envía [] en caso de error y continúa reintentando
            try:
                await websocket.send_json([])
            except RuntimeError as r_err:
                logger.error(
                    "Cannot send message, connection already closed: %s", r_err
                )
        # Espera 3 segundos antes de reiniciar la conexión al websocket remoto
        await asyncio.sleep(1)


@router.post("/force-check")
async def force_check_scheduler(cronjob: CRONJOB):
    cronjob.wakeup()  # Forzar a revisar los jobs pendientes
    return {"message": "Scheduler triggered to check for jobs"}


@router.websocket("/jobs/status/actualizacion_reportes")
async def websocket_actualizacion_reportes_status(websocket: WebSocket):
    """
    Envía de forma persistente (por websocket) el estado actual del cronjob de Actualización de Reportes
    obtenido desde Redis. Se envían actualizaciones cada 3 segundos. Si no hay datos, se envía None.
    """
    await websocket.accept()
    try:
        while True:
            try:
                # Verificar si el websocket sigue conectado antes de cada operación
                if websocket.client_state == WebSocketState.DISCONNECTED:
                    logger.info("Cliente WebSocket desconectado, terminando bucle")
                    break

                status_key = "ActualizarTablasReportesCronjob_status"
                client = redis_client_manager.get_client()
                status_bytes = await client.get(status_key)
                status_value_str = (
                    status_bytes.decode("utf-8") if status_bytes is not None else None
                )

                if status_value_str is None or status_value_str == "":
                    # Enviar un objeto vacío en lugar de None
                    data = {"status": "Sin datos", "timestamp": "-"}
                else:
                    data = orjson.loads(status_value_str)

                # Verificar nuevamente antes de enviar
                if websocket.client_state == WebSocketState.DISCONNECTED:
                    logger.info("Cliente desconectado antes de enviar datos")
                    break

                await websocket.send_json(data)
            except WebSocketDisconnect:
                logger.info("WebSocket desconectado durante el envío")
                break
            except RuntimeError as runtime_err:
                if "Cannot call" in str(
                    runtime_err
                ) and "close message has been sent" in str(runtime_err):
                    logger.info("WebSocket ya cerrado: %s", runtime_err)
                    break
                else:
                    logger.error("Error de runtime: %s", runtime_err)
                    break
            except Exception as inner_e:
                logger.error("Error obteniendo status: %s", inner_e)
                try:
                    # Verificar antes de enviar mensaje de error
                    if websocket.client_state != WebSocketState.DISCONNECTED:
                        await websocket.send_json(None)
                except Exception:
                    # Si falla al enviar, probablemente el socket ya está cerrado
                    logger.info(
                        "No se pudo enviar mensaje de error, posiblemente el socket ya está cerrado"
                    )
                    break

            await asyncio.sleep(3)
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error("Error en websocket_actualizacion_reportes_status: %s", e)
    finally:
        # Intentar cerrar limpiamente, pero solo si aún no está cerrado
        if websocket.client_state != WebSocketState.DISCONNECTED:
            try:
                await websocket.close()
            except RuntimeError:
                pass  # Ignorar silenciosamente si ya está cerrado


@router.get("/download/{job_id}", response_class=StreamingResponse)
async def download_job_result(job_id: str):
    client = redis_client_manager.get_client()
    try:
        # ZIP
        if data := await client.get(f"job:{job_id}:zip"):
            return StreamingResponse(
                iter([data]),
                media_type="application/x-zip-compressed",
                headers={
                    "Content-Disposition": f"attachment; filename=job_{job_id}.zip"
                },
            )

        # EXCEL
        if data := await client.get(f"job:{job_id}:buffer_excel"):
            return StreamingResponse(
                iter([data]),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": f"attachment; filename=job_{job_id}.xlsx"
                },
            )

        # CSV
        if data := await client.get(f"job:{job_id}:buffer_csv"):
            return StreamingResponse(
                iter([data]),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename=job_{job_id}.csv"
                },
            )

        # ORJSON
        if data := await client.get(f"job:{job_id}:buffer_orjson"):
            return StreamingResponse(
                iter([data]),
                media_type="application/json",
                headers={
                    "Content-Disposition": f"attachment; filename=job_{job_id}.json"
                },
            )

        raise HTTPException(404, "Job result not found or not completed yet")

    except Exception:
        logger.error("Error in download_job_result:\n%s", traceback.format_exc())
        raise HTTPException(500, "Internal server error")


@router.websocket("/jobs/utilidades")
async def websocket_utilidades(
    websocket: WebSocket, page: int = 1, page_size: int = 10, search: str = ""
):
    await websocket.accept()
    try:
        while True:
            # Obtiene los trabajos desde Redis u otra fuente.
            jobs = await redis_client_manager.get_all_jobs(
                search=search, page=page, page_size=page_size
            )
            if not isinstance(jobs, list):
                logger.error("Jobs data is not a list: %s", jobs)
                await websocket.send_json([])  # Envia lista vacía en caso de error
                await asyncio.sleep(3)
                continue

            valid_jobs = []
            for job in jobs:
                if isinstance(job, dict) and "job_id" in job:
                    ttl = await redis_client_manager.get_client().ttl(
                        f"job:{job['job_id']}:status"
                    )
                    job["expires_in"] = ttl
                    valid_jobs.append(job)
                else:
                    logger.error("Invalid job format: %s", job)

            # Si no hay trabajos válidos, envia [] y continua
            if not valid_jobs:
                logger.info("No valid jobs to send")
                await websocket.send_json([])
                await asyncio.sleep(3)
                continue

            await websocket.send_json(valid_jobs)
            await asyncio.sleep(3)  # Envia actualizaciones cada 3 segundos

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error("Error in websocket_endpoint: %s", e)
        # Intenta enviar un mensaje vacio para no cerrar abruptamente (opcional)
        try:
            await websocket.send_json([])
        except Exception as send_err:
            logger.error("Error sending empty JSON: %s", send_err)
        await websocket.close()
