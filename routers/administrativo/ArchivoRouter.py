from fastapi import APIRouter, Depends, HTTPException
from services.administrativo.ArchivoService import ArchivoService
from schemas.administrativo.ArchivoSchema import (
    ArchivoCreateSchema,
    ArchivoOutputSchema,
    ArchivoSearchOutputSchema,
    ArchivoUpdateSchema,
)
from fastapi.responses import ORJSONResponse
import traceback
from typing import List


router = APIRouter()


@router.get("/search", response_model=List[ArchivoSearchOutputSchema])
async def get_all_and_search(
    limit: int = 10,
    offset: int = 1,
    nombre_archivo: str = None,
    tipo_archivo: str = None,
    gasto_id: str | None = None,
    service: ArchivoService = Depends(),
):
    try:
        return await service.get_all_and_search(
            limit, offset, nombre_archivo, tipo_archivo, gasto_id
        )
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

@router.get("/{archivo_id}", response_model=ArchivoOutputSchema)
async def get_by_id(archivo_id: str, service: ArchivoService = Depends()):
    try:
        return await service.get_by_id(archivo_id)
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

@router.post("", response_model=ArchivoOutputSchema)
async def create(input: ArchivoCreateSchema, service: ArchivoService = Depends()):
    try:
        return await service.create(input)
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

@router.put("/{archivo_id}", response_model=ArchivoOutputSchema)
async def update_archivo(
    archivo_id: str,
    input: ArchivoUpdateSchema,
    service: ArchivoService = Depends(),
):
    try:
        return await service.update(archivo_id, input)
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
    



