from fastapi import APIRouter, Depends, UploadFile, File
from services.datamart.TipoCambioService import TipoCambioService
from schemas.datamart.TipoCambioSchema import (
    TipoCambioPostRequestSchema,
    TipoCambioSchema,
)
from fastapi.responses import ORJSONResponse

router = APIRouter()


@router.get("", response_model=list[TipoCambioSchema])
async def get_all(
    limit: int = 10,
    offset: int = 0,
    year_month: str | None = None,
    service: TipoCambioService = Depends(),
):
    try:
        return await service.get_all(limit, offset, year_month)
    except Exception as e:
        return {"message": str(e), "success": False}


@router.get("/csv", response_class=ORJSONResponse)
async def get_all_to_csv(service: TipoCambioService = Depends()):
    try:
        return await service.get_all_to_csv()
    except Exception as e:
        return {"message": str(e), "success": False}


@router.post("")
async def create_many_tipo_cambios(
    input: list[TipoCambioPostRequestSchema], service: TipoCambioService = Depends()
):
    try:
        return await service.create_many(input)
    except Exception as e:
        return {"message": str(e), "success": False}


@router.post("/upload-csv")
async def create_many_tipo_cambios_from_csv(
    file: UploadFile = File(...), service: TipoCambioService = Depends()
):
    try:
        await service.create_many_from_csv(file)
        return {"message": "Datos insertados correctamente", "success": True}
    except Exception as e:
        return {"message": str(e), "success": False}


@router.delete("")
async def delete_all(service: TipoCambioService = Depends()):
    try:
        return await service.delete_all()
    except Exception as e:
        return {"message": str(e), "success": False}
