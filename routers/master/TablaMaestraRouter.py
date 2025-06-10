from fastapi import APIRouter, Depends, HTTPException
from services.master.TablaMaestraService import TablaMaestraService
from schemas.master.TablaMaestraSchema import (
    TablaMaestraCreateSchema,
    TablaMaestraOutputSchema,
    TablaMaestraSearchOutputSchema,
    TablaMaestraUpdateSchema,
)
from fastapi.responses import ORJSONResponse
import traceback

router = APIRouter()


@router.get("/search", response_model=list[TablaMaestraSearchOutputSchema])
async def get_all_and_search(
    limit: int = 10,
    offset: int = 1,
    tabla_nombre: str = None,
    tipo: str = None,
    service: TablaMaestraService = Depends(),
):
    try:
        return await service.get_all_and_search(limit, offset, tabla_nombre, tipo)
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

@router.get("/{tabla_maestra_id}", response_model=TablaMaestraOutputSchema)
async def get_by_id(tabla_maestra_id: str, service: TablaMaestraService = Depends()):
    try:
        return await service.get_by_id(tabla_maestra_id)
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

@router.post("", response_model=TablaMaestraOutputSchema)
async def create(
    input: TablaMaestraCreateSchema, service: TablaMaestraService = Depends()
):
    try:
        return await service.create(input)
    except Exception as e:
        return ORJSONResponse(
            status_code=500,
            content={"detail": str(e)},
        )


@router.put("/{tabla_maestra_id}", response_model=TablaMaestraOutputSchema)
async def update_tabla_maestra(
    tabla_maestra_id: str,
    input: TablaMaestraUpdateSchema,
    service: TablaMaestraService = Depends(),
):
    try:
        return await service.update(tabla_maestra_id, input)
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