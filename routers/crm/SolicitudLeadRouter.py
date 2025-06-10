import traceback
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import ORJSONResponse
from schemas.crm.SolicitudLeadSchema import (
    SolicitudLeadCreateSchema,
    # SolicitudLeadOutputSchema,
    # SolicitudLeadUpdateSchema,
)
from services.crm.SolicitudLeadService import SolicitudLeadService
from config.logger import logger

router = APIRouter()


@router.post("/create", status_code=201)
async def create_solicitud_lead(
    input: SolicitudLeadCreateSchema, service: SolicitudLeadService = Depends()
):
    try:
        await service.create_and_send(input)
    except HTTPException as e:
        return ORJSONResponse(
            status_code=e.status_code,
            content={"detail": e.detail},
        )
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.error(f"Error: {str(e)}")
        return ORJSONResponse(
            status_code=500,
            content={"detail": str(e), "traceback": error_trace},
        )
