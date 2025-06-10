from fastapi import APIRouter, Depends, UploadFile, File, Form
from services.toolbox.VentasAutodetraccionesService import VentasAutodetraccionesService
from fastapi.responses import ORJSONResponse, StreamingResponse
import traceback
from schemas.toolbox.VentasAutodetraccionesSchema import (
    VentasAutodetraccionesCreateSchema,
)
from config.logger import logger

router = APIRouter()


@router.post(
    "/calcular-ventas-autodetracciones",
    response_class=ORJSONResponse,
    description="Genera el Excel en memoria con las ventas autodetracciones filtradas por mes",
)
async def calcular_ventas_autodetracciones(
    ventas_autodetracciones: VentasAutodetraccionesCreateSchema = Form(...),
    archivos: UploadFile = File(...),
    service: VentasAutodetraccionesService = Depends(),
):
    try:
        # Llama al método del service que genera el Excel filtrado por el mes indicado en 'hasta'
        response_ventas_autodetracciones: dict = (
            await service.calcular_ventas_autodetracciones(
                file=archivos, hasta=ventas_autodetracciones.hasta
            )
        )
        # Devuelve el archivo Excel como respuesta de streaming
        return response_ventas_autodetracciones
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.error(error_trace)
        return ORJSONResponse(
            status_code=500,
            content={"detail": str(e), "traceback": error_trace},
        )
