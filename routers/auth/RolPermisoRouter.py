from fastapi import APIRouter, Depends, HTTPException
from services.auth.RolPermisoService import RolPermisoService
from schemas.auth.RolPermisoSchema import (
    RolPermisoCreateSchema,
    RolPermisoOutputSchema,
    RolPermisoBulkCreateSchema,
    RolPermisoUpdateSchema,
    RolSubmodulosOutputSchema,
)
from fastapi.responses import ORJSONResponse
from typing import List
from uuid import UUID
import traceback
from config.logger import logger

router = APIRouter()


@router.post("/assign", response_model=RolPermisoOutputSchema)
async def asignar_permiso_a_rol(
    input: RolPermisoCreateSchema, service: RolPermisoService = Depends()
):
    try:
        await service.asignar_permiso_a_rol(input)
        return input
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


@router.post("/assignBulk", status_code=204)
async def asignar_permisos_a_rol_masivo(
    input: RolPermisoBulkCreateSchema, service: RolPermisoService = Depends()
):
    try:
        logger.warning(input)
        await service.asignar_permisos_a_rol_masivo(input)

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


@router.post("/remove", response_model=RolPermisoOutputSchema)
async def eliminar_permiso_de_rol(
    input: RolPermisoCreateSchema, service: RolPermisoService = Depends()
):
    try:
        await service.eliminar_permiso_de_rol(input)
        return input
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


@router.get("/roles/{rol_id}/permisos", response_model=List[UUID])
async def obtener_permisos_por_rol(rol_id: str, service: RolPermisoService = Depends()):
    try:
        return await service.obtener_permisos_por_rol(rol_id)
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


@router.get("/permisos/{permiso_id}/roles", response_model=List[UUID])
async def obtener_roles_por_permiso(
    permiso_id: str, service: RolPermisoService = Depends()
):
    try:
        return await service.obtener_roles_por_permiso(permiso_id)
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


@router.put("/roles/{rol_id}/permisos")
async def actualizar_permisos_rol(
    rol_id: str, input: RolPermisoUpdateSchema, service: RolPermisoService = Depends()
):
    """
    Actualiza los permisos de un rol de forma flexible:
    - Si solo se proporciona permisos_agregar: Reemplaza todos los permisos
    - Si se proporcionan ambos: Quita y agrega los permisos indicados
    - Si solo se proporciona permisos_quitar: Solo elimina esos permisos
    """
    try:
        # Asegurar que el rol_id en la URL y en el body sean consistentes
        if str(input.rol_id) != rol_id:
            return ORJSONResponse(
                status_code=400,
                content={"detail": "El ID del rol en la URL y en el body no coinciden"},
            )

        result = await service.actualizar_permisos_rol(input)
        return result
    except HTTPException as e:
        return ORJSONResponse(
            status_code=e.status_code,
            content={"detail": e.detail},
        )
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.error(f"Error actualizando permisos del rol: {error_trace}")
        return ORJSONResponse(
            status_code=500,
            content={"detail": str(e), "traceback": error_trace},
        )


@router.get("/roles/submodulos", response_model=list[RolSubmodulosOutputSchema])
async def obtener_submodulos_por_rol(
    limit: int = 10, page: int = 1, service: RolPermisoService = Depends()
):
    """
    Obtiene directamente los submódulos asignados a un rol específico,
    sin organización jerárquica.
    """
    try:
        return await service.listar_roles_con_submodulos(limit, page)
    except HTTPException as e:
        return ORJSONResponse(
            status_code=e.status_code,
            content={"detail": e.detail},
        )
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.error(f"Error obteniendo submódulos del rol: {error_trace}")
        return ORJSONResponse(
            status_code=500,
            content={"detail": str(e), "traceback": error_trace},
        )
