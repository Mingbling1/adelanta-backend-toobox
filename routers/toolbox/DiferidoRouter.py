from fastapi import APIRouter, Depends, UploadFile, File, Form
from services.toolbox.DiferidoService import DiferidoService
from fastapi.responses import ORJSONResponse
import traceback
from schemas.toolbox.DiferidoSchema import DiferidoCreateSchema
from config.logger import logger

router = APIRouter()


@router.post(
    "/calcular-diferido",
    response_class=ORJSONResponse,
    description="Genera el Excel en memoria con el diferido",
)
async def calcular_diferido(
    diferido: DiferidoCreateSchema = Form(...),
    archivos: UploadFile = File(...),
    service: DiferidoService = Depends(),
):
    try:
        response_diferido: dict = await service.calcular_diferido(
            file=archivos, hasta=diferido.hasta
        )
        return response_diferido
    except Exception as e:
        error_trace = traceback.format_exc()
        print(error_trace)
        return ORJSONResponse(
            status_code=500,
            content={"detail": str(e), "traceback": error_trace},
        )
