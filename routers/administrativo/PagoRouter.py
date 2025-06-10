from fastapi import APIRouter, Depends, HTTPException
from services.administrativo.PagoService import PagoService
from schemas.administrativo.PagoSchema import (
    PagoCreateSchema,
    PagoOutputSchema,
    PagoSearchOutputSchema,
    PagoUpdateSchema,
)
from fastapi.responses import ORJSONResponse
import traceback

router = APIRouter()


@router.get("/search", response_model=list[PagoSearchOutputSchema])
async def get_all_and_search(
    limit: int = 10,
    offset: int = 1,
    nombre_pago: str = None,
    pago_monto: float = None,
    pago_fecha: str = None,
    gasto_id: str | None = None,
    service: PagoService = Depends(),
):
    try:
        return await service.get_all_and_search(
            limit, offset, nombre_pago, pago_monto, pago_fecha, gasto_id
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


@router.get("/{pago_id}", response_model=PagoOutputSchema)
async def get_by_id(pago_id: str, service: PagoService = Depends()):
    try:
        return await service.get_by_id(pago_id)
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


@router.put("/{pago_id}", response_model=PagoOutputSchema)
async def update_pago(
    pago_id: str,
    input: PagoUpdateSchema,
    service: PagoService = Depends(),
):
    try:
        return await service.update(pago_id, input)
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


@router.post("/addPagoToGasto/{gasto_id}")
async def add_pago_to_gasto(
    gasto_id: str,
    input: PagoCreateSchema,
    service: PagoService = Depends(),
):
    try:
        await service.add_pago_to_gasto(gasto_id, input)
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


@router.post("/addPagoToGastoBulk")
async def add_pagos_to_gastos(
    gasto_ids: list[str],
    pagos: list[PagoCreateSchema],
    service: PagoService = Depends(),
):
    try:
        await service.add_pagos_to_gastos(gasto_ids, pagos)
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
