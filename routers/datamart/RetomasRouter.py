from fastapi import APIRouter, Depends, Query
from fastapi.responses import ORJSONResponse
from services.datamart.RetomasService import RetomasService
import traceback
from config.logger import logger
from datetime import datetime
from schemas.datamart.ReferidosSchema import ReferidosCreateSchema

router = APIRouter()


@router.delete("")
async def delete_all(service: RetomasService = Depends()):
    try:
        return await service.delete_all()
    except Exception as e:
        return {"message": str(e), "success": False}


@router.post(
    "/calcular-retomas",
    response_class=ORJSONResponse,
    description="Genera el Excel retomas",
)
async def calcular_retomas(
    input: ReferidosCreateSchema,
    service: RetomasService = Depends(),
):
    try:
        # Puedes convertir la cadena a datetime si es necesario antes de pasarla a la lógica
        return await service.calcular_retomas(
            fecha_corte=(
                datetime.strptime(input.fecha_corte, "%Y-%m-%d")
                if input.fecha_corte
                else None
            )
        )
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.warning(error_trace)
        return ORJSONResponse(
            status_code=500,
            content={"detail": str(e), "traceback": error_trace},
        )
