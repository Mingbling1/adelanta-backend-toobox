from fastapi import APIRouter, Depends, HTTPException
from typing import Union
from fastapi.responses import ORJSONResponse
from schemas.sunat.SunatSchema import EmpresaSchema, PersonaSchema, SunatTipoSchema
from services.sunat.SunatService import SunatService


router = APIRouter()


@router.post("", response_model=Union[EmpresaSchema, PersonaSchema])
async def get_info(
    input: SunatTipoSchema, service: SunatService = Depends(SunatService)
):
    try:
        return await service.get_info(input)
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