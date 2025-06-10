from fastapi import APIRouter, Depends, HTTPException
from services.auth.RolService import RolService
from schemas.auth.RolSchema import (
    RolCreateSchema,
    RolOutputSchema,
    RolSearchOutputSchema,
    RolUpdateSchema,
    RolPermisosOutputSchema,
    PermisoJerarquicoSchema,
)
from fastapi.responses import ORJSONResponse
import traceback


router = APIRouter()


@router.get("/rolesPermisos", response_model=list[RolPermisosOutputSchema])
async def obtener_roles_con_permisos(
    limit: int = 10, page: int = 1, service: RolService = Depends()
):
    """
    Obtiene roles con sus permisos organizados jerárquicamente.
    Los permisos se agrupan por módulos principales, con submódulos anidados.
    """
    try:
        return await service.obtener_roles_con_permisos(limit, page)
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


@router.get("/search", response_model=list[RolSearchOutputSchema])
async def get_all_and_search(
    limit: int = 10,
    offset: int = 1,
    nombre: str = None,
    service: RolService = Depends(),
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


@router.get("/{rol_id}", response_model=RolOutputSchema)
async def get_by_id(rol_id: str, service: RolService = Depends()):
    try:
        return await service.get_by_id(rol_id)
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


@router.post("", response_model=RolOutputSchema)
async def create(input: RolCreateSchema, service: RolService = Depends()):
    try:
        return await service.create(input)
    except Exception as e:
        return ORJSONResponse(
            status_code=500,
            content={"detail": str(e)},
        )


@router.put("/{rol_id}", response_model=RolOutputSchema)
async def update_rol(
    rol_id: str,
    input: RolUpdateSchema,
    service: RolService = Depends(),
):
    try:
        return await service.update(rol_id, input)
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


@router.delete("/{rol_id}")
async def eliminar_rol(rol_id: str, service: RolService = Depends()):
    """
    Elimina un rol y todos sus permisos asociados.
    """
    try:
        result = await service.eliminar_rol_y_permisos(rol_id)
        return result
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
