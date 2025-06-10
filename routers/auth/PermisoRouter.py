from fastapi import APIRouter, Depends, HTTPException, Body
from services.auth.PermisoService import PermisoService
from schemas.auth.PermisoSchema import (
    PermisoCreateSchema,
    PermisoOutputSchema,
    PermisoSearchOutputSchema,
    PermisoUpdateSchema,
    PermisoJerarquicoSchema,
)
from fastapi.responses import ORJSONResponse
import traceback

router = APIRouter()


@router.get("/search", response_model=list[PermisoSearchOutputSchema])
async def get_all_and_search(
    limit: int = 10,
    offset: int = 1,
    nombre: str = None,
    service: PermisoService = Depends(),
):
    try:
        return await service.get_all_and_search(limit, offset, nombre)
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


@router.get("/{permiso_id}", response_model=PermisoOutputSchema)
async def get_by_id(permiso_id: str, service: PermisoService = Depends()):
    try:
        return await service.get_by_id(permiso_id)
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


@router.post(
    "",
    response_model=PermisoOutputSchema,
    summary="Crear un nuevo permiso o módulo",
    description="""
    Crea un nuevo permiso en el sistema, que puede ser:
    
    1. Un módulo principal (sin módulo padre)
    2. Un submódulo (con referencia a un módulo padre)
    
    Los permisos siguen una estructura jerárquica donde los módulos principales
    contienen submódulos que representan funcionalidades específicas.
    """,
)
async def create(
    input: PermisoCreateSchema = Body(
        ...,
        examples={
            "modulo_principal": {
                "summary": "Crear módulo principal",
                "description": "Ejemplo para crear un nuevo módulo principal sin padre",
                "value": {
                    "codigo": "operaciones",
                    "nombre": "Operaciones",
                    "descripcion": "Módulo principal para gestión de operaciones",
                    "modulo_padre_id": None,
                },
            },
            "submodulo": {
                "summary": "Crear submódulo",
                "description": "Ejemplo para crear un submódulo asociado a un módulo principal",
                "value": {
                    "codigo": "operaciones.cobranza.devoluciones",
                    "nombre": "Devoluciones",
                    "descripcion": "Gestión de devoluciones de cobranza",
                    "modulo_padre_id": "5a9bd5d6-3c1e-4f2b-b789-1234567890ab",
                },
            },
        },
    ),
    service: PermisoService = Depends(),
):
    try:
        return await service.create(input)
    except Exception as e:
        return ORJSONResponse(
            status_code=500,
            content={"detail": str(e)},
        )


@router.put("/{permiso_id}", response_model=PermisoOutputSchema)
async def update_permiso(
    permiso_id: str,
    input: PermisoUpdateSchema,
    service: PermisoService = Depends(),
):
    try:
        return await service.update(permiso_id, input)
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
