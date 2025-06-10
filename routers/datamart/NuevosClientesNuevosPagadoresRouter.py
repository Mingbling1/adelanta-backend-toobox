from fastapi import APIRouter, Depends
from services.datamart.NuevosClientesNuevosPagadoresService import (
    NuevosClientesNuevosPagadoresService,
)

from fastapi.responses import ORJSONResponse
from typing import Literal


router = APIRouter()


@router.get("/file/{tipo}", response_class=ORJSONResponse)
async def get_all_to_file(
    service: NuevosClientesNuevosPagadoresService = Depends(),
    tipo: Literal["excel", "csv"] = "excel",
):
    try:
        return await service.get_all_to_file(tipo=tipo)
    except Exception as e:
        return {"message": str(e), "success": False}
