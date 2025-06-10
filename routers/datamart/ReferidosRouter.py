from fastapi import APIRouter, Depends, UploadFile, File, Query
from services.datamart.ReferidosService import ReferidosService
from schemas.datamart.ReferidosSchema import (
    ReferidosPostRequestSchema,
    ReferidosSchema,
)
from fastapi.responses import StreamingResponse, ORJSONResponse
from fastapi import HTTPException
from config.logger import logger
import traceback
from config.redis import redis_client_manager  # Ensure this is the correct import path

router = APIRouter()


@router.get("", response_model=list[ReferidosSchema])
async def get_all(
    limit: int = 10, offset: int = 0, service: ReferidosService = Depends()
):
    try:
        return await service.get_all(limit, offset)
    except Exception as e:
        return {"message": str(e), "success": False}


@router.get("/csv", response_class=StreamingResponse)
async def get_all_to_csv(service: ReferidosService = Depends()):
    try:
        return await service.get_all_to_csv()
    except Exception as e:

        return {"message": str(e), "success": False}


@router.get("/calcular-comisiones", response_class=ORJSONResponse)
async def calcular_comisiones(
    desde: str | None = Query(
        None, regex=r"^\d{4}-\d{2}$", description="Formato YYYY-MM (opcional)"
    ),
    hasta: str | None = Query(
        None, regex=r"^\d{4}-\d{2}$", description="Formato YYYY-MM (opcional)"
    ),
    service: ReferidosService = Depends(),
):
    try:
        response_comisiones: dict = await service.calcular_comisiones(
            primer_dia=desde, ultimo_dia=hasta
        )
        # if hasta:
        #     mes_str = service.month_name_spanish(f"{hasta}-01")
        # else:
        #     mes_str = service.month_name_spanish("2025-01-01")
        # zip_filename = f"Comisiones_{mes_str}.zip"

        return response_comisiones
        # return StreamingResponse(
        #     iter([zip_buffer.getvalue()]),
        #     media_type="application/x-zip-compressed",
        #     headers={"Content-Disposition": f"attachment; filename={zip_filename}"},
        # )
    except Exception as e:
        error_trace = traceback.format_exc()
        print(error_trace)
        return ORJSONResponse(
            status_code=500,
            content={"detail": str(e), "traceback": error_trace},
        )


@router.post("")
async def create_many_referidos(
    input: list[ReferidosPostRequestSchema], service: ReferidosService = Depends()
):
    try:
        return await service.create_many(input)
    except Exception as e:
        return {"message": str(e), "success": False}


@router.delete("")
async def delete_all(service: ReferidosService = Depends()):
    try:
        return await service.delete_all()
    except Exception as e:
        return {"message": str(e), "success": False}
