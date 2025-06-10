from fastapi import APIRouter, Depends, HTTPException
from services.administrativo.ProveedorService import ProveedorService
from schemas.administrativo.ProveedorSchema import (
    ProveedorCreateSchema,
    ProveedorOutputSchema,
    ProveedorSearchOutputSchema,
    ProveedorUpdateSchema,
)
from fastapi.responses import ORJSONResponse
import traceback

router = APIRouter()


@router.get(
    "/search",
    response_model=list[ProveedorSearchOutputSchema],
)
async def get_all_and_search(
    limit: int = 10,
    offset: int = 1,
    nombre_proveedor: str = None,
    tipo_proveedor: str = None,
    numero_documento: str = None,
    service: ProveedorService = Depends(),
):
    try:
        return await service.get_all_and_search(
            limit, offset, nombre_proveedor, tipo_proveedor, numero_documento
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


@router.get("/{proveedor_id}", response_model=ProveedorOutputSchema)
async def get_by_id(proveedor_id: str, service: ProveedorService = Depends()):
    try:
        return await service.get_by_id(proveedor_id)
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


@router.post("", response_model=ProveedorOutputSchema)
async def create(input: ProveedorCreateSchema, service: ProveedorService = Depends()):
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


@router.put("/{proveedor_id}", response_model=ProveedorOutputSchema)
async def update_proveedor(
    proveedor_id: str,
    input: ProveedorUpdateSchema,
    service: ProveedorService = Depends(),
):
    try:
        return await service.update(proveedor_id, input)
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
