from fastapi import APIRouter, Depends
from services.datamart.KPIService import KPIService
from schemas.datamart.KPISchema import (
    KPISchema,
)
from fastapi.responses import ORJSONResponse
from typing import Literal

router = APIRouter()


@router.get("", response_model=list[KPISchema])
async def get_all(limit: int = 10, offset: int = 0, service: KPIService = Depends()):
    try:
        return await service.get_all(limit, offset)
    except Exception as e:
        return {"message": str(e), "success": False}


@router.get("/file/{tipo}", response_class=ORJSONResponse)
async def get_all_to_file(
    service: KPIService = Depends(),
    tipo: Literal["excel", "csv"] = "excel",
    informe: str | None = None,
):
    try:
        return await service.get_all_to_file(tipo=tipo, informe=informe)
    except Exception as e:
        return {"message": str(e), "success": False}
