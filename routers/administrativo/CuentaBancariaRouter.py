from fastapi import APIRouter, Depends
from services.administrativo.CuentaBancariaService import CuentaBancariaService
from schemas.administrativo.CuentaBancariaSchema import (
    CuentaBancariaCreateSchema,
    CuentaBancariaOutputSchema,
    CuentaBancariaSearchOutputSchema,
    CuentaBancariaUpdateSchema,
)
from fastapi.responses import ORJSONResponse
from fastapi import HTTPException
import traceback

router = APIRouter()


@router.get("/search", response_model=list[CuentaBancariaSearchOutputSchema])
async def get_all_and_search(
    limit: int = 10,
    offset: int = 1,
    nombre_cuenta_bancaria: str = None,
    banco: str = None,
    moneda: str = None,
    tipo_cuenta: str = None,
    proveedor_id: str = None,
    nombre_proveedor: str | None = None,
    numero_documento: str | None = None,
    service: CuentaBancariaService = Depends(),
):
    try:
        return await service.get_all_and_search(
            limit,
            offset,
            nombre_cuenta_bancaria,
            banco,
            moneda,
            tipo_cuenta,
            proveedor_id,
            nombre_proveedor,
            numero_documento,
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


@router.get("/{cuenta_bancaria_id}", response_model=CuentaBancariaOutputSchema)
async def get_by_id(
    cuenta_bancaria_id: str, service: CuentaBancariaService = Depends()
):
    try:
        return await service.get_by_id(cuenta_bancaria_id)
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


@router.post("", response_model=CuentaBancariaOutputSchema)
async def create(
    input: CuentaBancariaCreateSchema, service: CuentaBancariaService = Depends()
):
    try:
        return await service.create(input)
    except Exception as e:
        return ORJSONResponse(
            status_code=500,
            content={"detail": str(e)},
        )


@router.put("/{cuenta_bancaria_id}", response_model=CuentaBancariaOutputSchema)
async def update_cuenta_bancaria(
    cuenta_bancaria_id: str,
    input: CuentaBancariaUpdateSchema,
    service: CuentaBancariaService = Depends(),
):
    try:
        return await service.update(cuenta_bancaria_id, input)
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
