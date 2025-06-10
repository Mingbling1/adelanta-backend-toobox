from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Form,
    File,
    UploadFile,
    Path,
    Query,
)
from services.administrativo.GastoService import GastoService
from schemas.administrativo.GastoSchema import (
    GastoCreateSchema,
    GastoOutputSchema,
    GastoSearchOutputSchema,
    GastoUpdateSchema,
)
from fastapi.responses import ORJSONResponse
import traceback

router = APIRouter()


@router.get("/search", response_model=list[GastoSearchOutputSchema])
async def get_all_and_search(
    limit: int = 10,
    offset: int = 1,
    nombre_gasto: str = None,
    fecha_emision: str = None,
    gasto_id: str = None,
    service: GastoService = Depends(),
):
    try:
        return await service.get_all_and_search(
            limit, offset, nombre_gasto, fecha_emision, gasto_id
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


@router.get("/{gasto_id}", response_model=GastoOutputSchema)
async def get_by_id(gasto_id: str, service: GastoService = Depends()):
    try:
        return await service.get_by_id(gasto_id)
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


@router.post("", response_model=GastoOutputSchema)
async def create(input: GastoCreateSchema, service: GastoService = Depends()):
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


@router.post("/archivos")
async def create_gasto_con_archivos(
    gasto: GastoCreateSchema = Form(...),
    archivos: list[UploadFile] = File(...),
    service: GastoService = Depends(),
):
    try:
        await service.create_gasto_con_archivos(gasto, archivos)
    except HTTPException as e:
        print(e)
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


@router.delete("/{gasto_id}")
async def eliminar_gasto_con_archivos(
    gasto_id: str = Path(..., description="ID del gasto a eliminar"),
    updated_by: str = Query(..., description="ID del usuario que realiza la operación"),
    service: GastoService = Depends(),
):
    try:
        await service.eliminar_gasto_con_archivos(gasto_id, updated_by)
        return ORJSONResponse(
            status_code=200, content={"detail": "Gasto eliminado correctamente"}
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


@router.put("/{gasto_id}", response_model=GastoOutputSchema)
async def editar_gasto(
    gasto_id: str,
    gasto: GastoUpdateSchema = Form(...),
    service: GastoService = Depends(),
):
    try:
        return await service.editar_gasto(gasto_id, gasto.tipo_gasto)
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
