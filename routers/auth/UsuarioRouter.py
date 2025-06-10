from fastapi import APIRouter, Depends, HTTPException
from services.auth.UsuarioService import UsuarioService
from services.auth.AuthService import AuthService
from schemas.auth.UsuarioSchema import (
    UsuarioCreateSchema,
    UsuarioOutputSchema,
    UsuarioUpdateSchema,
    UsuarioOutputSearchSchema,
    UsuarioAsignarRolSchema,
    UusuarioEliminarSchema,
)
from fastapi.responses import ORJSONResponse
from config.logger import logger
import traceback
from typing import Any

router = APIRouter(dependencies=[Depends(AuthService.get_current_user)])


@router.get("/search", response_model=list[UsuarioOutputSearchSchema])
async def get_all_and_search(
    limit: int = 10,
    offset: int = 1,
    username: str = None,
    email: str = None,
    service: UsuarioService = Depends(),
):
    try:
        return await service.get_all_and_search(
            limit,
            offset,
            username,
            email,
        )
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


@router.get("/verificar/{token}", response_model=dict[str, Any])
async def verificar(
    token: str,
    service: UsuarioService = Depends(),
) -> dict[str, Any]:
    """
    Endpoint para verificar el token de confirmación del usuario.
    Se deben enviar por query el email y, en la URL, el token.

    Si el token coincide, se marca al usuario como verificado.
    Si no coincide o el usuario no tenía token, se genera uno nuevo y se reenvía el correo.
    """
    result = await service.generate_verification_token(token)
    # Si result es un dict, significa que la verificación se realizó correctamente
    if isinstance(result, dict):
        return result
    # Si result es un str, se generó un nuevo token y se reenvió el correo
    return {"error": "Se volvió a enviar un correo de verificación"}


@router.post("/registrar", response_model=dict[str, Any])
async def registrar(
    usuario_data: UsuarioCreateSchema, service: UsuarioService = Depends()
):
    try:

        return await service.registrar(usuario_data)
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


@router.get("/{usuario_id}", response_model=UsuarioOutputSchema)
async def get_by_id(usuario_id: str, service: UsuarioService = Depends()):
    try:
        return await service.get_by_id(usuario_id)
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


@router.post("", response_model=UsuarioOutputSchema)
async def create(input: UsuarioCreateSchema, service: UsuarioService = Depends()):
    try:
        return await service.create(input)
    except HTTPException as e:
        return ORJSONResponse(
            status_code=e.status_code,
            content={"detail": e.detail},
        )
    except Exception as e:
        logger.debug(e)
        return ORJSONResponse(
            status_code=500,
            content={"detail": str(e)},
        )


@router.put("/{usuario_id}", response_model=UsuarioOutputSchema)
async def update_usuario(
    usuario_id: str,
    input: UsuarioUpdateSchema,
    service: UsuarioService = Depends(),
):
    try:
        return await service.update(usuario_id, input)
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


@router.put("/{usuario_id}/rol", response_model=UsuarioOutputSchema)  # Cambiar la URL
async def asignar_rol_usuario(
    usuario_id: str,
    input: UsuarioAsignarRolSchema,  # Usar el nuevo esquema
    service: UsuarioService = Depends(),
):
    """
    Asigna un rol a un usuario.

    Recibe el ID del usuario en la URL y el esquema con rol_id y updated_by en el cuerpo.
    Retorna el usuario actualizado con su nuevo rol asignado.
    """
    try:
        logger.warning(input.model_dump())
        return await service.asignar_rol(  # Renombrar el método
            usuario_id, input.rol_id, input.updated_by
        )
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


@router.put("/estado/{usuario_id}", response_model=UsuarioOutputSchema)
async def cambiar_estado_usuario(
    usuario_id: str,
    usuario: UusuarioEliminarSchema,
    service: UsuarioService = Depends(),
):
    """
    Cambia el estado de un usuario entre activo (1) e inactivo (0).

    - **usuario_id**: ID único del usuario
    - **estado**: Estado a asignar (0=inactivo, 1=activo)
    - **updated_by**: ID del usuario que realiza la modificación
    """
    return await service.cambiar_estado_usuario(
        usuario_id,
        usuario.updated_by,
        usuario.estado,
    )
