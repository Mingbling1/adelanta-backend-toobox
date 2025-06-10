from fastapi import APIRouter, Depends, HTTPException
from services.master.TablaMaestraDetalleService import TablaMaestraDetalleService
from schemas.master.TablaMaestraDetalleSchema import (
    TablaMaestraDetalleCreateSchema,
    TablaMaestraDetalleOutputSchema,
    TablaMaestraDetalleSearchOutputSchema,
    TablaMaestraDetalleUpdateSchema,
)
from fastapi.responses import ORJSONResponse
import traceback

router = APIRouter()


@router.get("/search", response_model=list[TablaMaestraDetalleSearchOutputSchema])
async def get_all_and_search(
    limit: int = 10,
    offset: int = 1,
    codigo: int = None,
    valor: str = None,
    descripcion: str = None,
    tabla_maestra_id: str = None,
    tabla_nombre: str = None,
    tipo: str = None,
    service: TablaMaestraDetalleService = Depends(),
):
    try:
        return await service.get_all_and_search(
            limit,
            offset,
            codigo,
            valor,
            descripcion,
            tabla_maestra_id,
            tabla_nombre,
            tipo,
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

@router.get(
    "/{tabla_maestra_detalle_id}", response_model=TablaMaestraDetalleOutputSchema
)
async def get_by_id(
    tabla_maestra_detalle_id: str, service: TablaMaestraDetalleService = Depends()
):
    try:
        return await service.get_by_id(tabla_maestra_detalle_id)
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

@router.post("", response_model=TablaMaestraDetalleOutputSchema)
async def create(
    input: TablaMaestraDetalleCreateSchema,
    service: TablaMaestraDetalleService = Depends(),
):
    try:
        return await service.create(input)
    except Exception as e:
        return ORJSONResponse(
            status_code=500,
            content={"detail": str(e)},
        )


@router.put(
    "/{tabla_maestra_detalle_id}", response_model=TablaMaestraDetalleOutputSchema
)
async def update_tabla_maestra_detalle(
    tabla_maestra_detalle_id: str,
    input: TablaMaestraDetalleUpdateSchema,
    service: TablaMaestraDetalleService = Depends(),
):
    try:
        return await service.update(tabla_maestra_detalle_id, input)
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