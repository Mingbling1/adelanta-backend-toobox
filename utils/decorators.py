from functools import wraps
from config.redis import redis_client_manager
from config.logger import logger
from typing import Callable, Any, Literal, Sequence, Union
from io import BytesIO

# alias de formatos permitidos
FormatType = Literal["zip", "excel", "csv", "orjson"]
SaveAsArg = Union[FormatType, Sequence[FormatType]]  # uno o varios


def create_job(
    name: str,
    description: str,
    expire: int,
    is_buffer: bool = False,
    save_as: SaveAsArg | None = None,
    capture_params: bool = False,
    created_by: str | None = None,
):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                # normalizar save_as a lista
                if isinstance(save_as, str):
                    valid_formats = [save_as]
                elif isinstance(save_as, (list, tuple)):
                    valid_formats = list(save_as)
                else:
                    valid_formats = []

                # determinar dinámicamente el formato a usar
                effective_save_as: str | None = None
                if is_buffer:
                    tipo = kwargs.get("tipo")
                    if not tipo and len(args) > 1 and isinstance(args[1], str):
                        tipo = args[1]
                    if tipo in valid_formats:
                        effective_save_as = tipo
                    elif valid_formats:
                        effective_save_as = valid_formats[0]

                if "created_by" in kwargs:
                    cb = kwargs.get("created_by")
                elif args and isinstance(args[-1], str):
                    cb = args[-1]
                else:
                    cb = created_by or ""
                if capture_params:
                    # Procesar argumentos posicionales, ignorando BytesIO
                    actual_args = []
                    for arg in (
                        args[1:] if args and hasattr(args[0], "__class__") else args
                    ):
                        if isinstance(arg, BytesIO):
                            continue
                        elif hasattr(arg, "filename"):
                            actual_args.append(arg.filename)
                        else:
                            actual_args.append(arg)
                    # Procesar kwargs, ignorando valores BytesIO
                    params = {"args": actual_args, "kwargs": {}}
                    for k, v in kwargs.items():
                        if isinstance(v, BytesIO):
                            continue
                        if hasattr(v, "filename"):
                            try:
                                params["kwargs"][k] = str(v.filename)
                            except Exception:
                                params["kwargs"][k] = "UnknownFilename"
                        else:
                            params["kwargs"][k] = v
                    logger.debug(f"Parámetros capturados: {params}")
                else:
                    params = None

                job_id = await redis_client_manager.create_job(
                    name,
                    description,
                    expire,
                    lambda: func(*args, **kwargs),
                    is_buffer,
                    effective_save_as,
                    params,
                    cb,
                )

                return {"job_id": job_id, "success": "Conforme a lo esperado"}
            except Exception as e:
                logger.exception("Error en create_job decorator:")
                raise e

        return wrapper

    return decorator
