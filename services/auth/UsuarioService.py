from fastapi import Depends, HTTPException
from repositories.auth.UsuarioRepository import UsuarioRepository
from models.auth.UsuarioModel import UsuarioModel
from schemas.auth.UsuarioSchema import (
    UsuarioCreateSchema,
    UsuarioUpdateSchema,
)
import bcrypt
from uuid import UUID, uuid4
from typing import Any
from utils.mailing.Mailing import Mailing

from config.logger import logger


class UsuarioService:
    def __init__(self, usuario_repository: UsuarioRepository = Depends()) -> None:
        self.usuario_repository = usuario_repository
        self.mailing = Mailing()

    def generate_token(self) -> UUID:
        new_token = uuid4()
        return new_token

    async def generate_verification_token(
        self, provided_token: str | None = None
    ) -> str | dict[str, Any]:
        """
        Recibe el email y un token (por ejemplo, enviado en la URL de verificación).
        Si el usuario tiene un token sin verificar, lo compara con el token proporcionado:
        - Si coinciden, se marca al usuario como verificado y se elimina el token.
        - Si no coinciden, se genera un nuevo token y se actualiza en el repositorio.
        Si el usuario no tiene token registrado, se genera uno nuevo.
        """
        if not provided_token:
            return {"error": "No token provided!"}

        usuario: UsuarioModel = await self.usuario_repository.get_by_id(
            UUID(provided_token), "token", type_result="first"
        )
        if not usuario:
            return {"error": "Token not found!"}

        if usuario.token and usuario.token_verified:
            return {"success": "User already verified!"}

        if usuario.token:
            # Los tokens coinciden: marcamos el usuario como verificado y eliminamos el token
            await self.usuario_repository.update(
                usuario.usuario_id, {"token_verified": True}, "usuario_id"
            )
            await self.usuario_repository.update(
                usuario.usuario_id, {"token": None}, "usuario_id"
            )
            return {"success": "User verified!"}

        # En cualquier otro caso (no existe token o no coincide), se genera y actualiza un nuevo token
        new_token = self.generate_token()

        await self.usuario_repository.update(usuario.usuario_id, new_token, "token")
        return new_token

    async def get_by_id(self, usuario_id: str) -> UsuarioModel:
        return await self.usuario_repository.get_by_id(UUID(usuario_id), "usuario_id")

    def hash_password(self, plain_password: str) -> str:
        return bcrypt.hashpw(plain_password.encode("utf-8"), bcrypt.gensalt()).decode(
            "utf-8"
        )

    def validate_password(self, usuario_data: UsuarioCreateSchema) -> UsuarioModel:
        hashed_password = self.hash_password(usuario_data.password)
        usuario_model_dump = usuario_data.model_dump()

        usuario_model_dump["hashed_password"] = hashed_password
        del usuario_model_dump["password"]

        return usuario_model_dump

    async def create(self, usuario_data: UsuarioCreateSchema) -> UsuarioModel:
        usuario_model_dump = self.validate_password(usuario_data)

        return await self.usuario_repository.create(usuario_model_dump)

    async def registrar(self, usuario_data: UsuarioCreateSchema) -> dict[str, Any]:

        if not usuario_data.email.endswith("@adelantafactoring.com"):
            return {"error": "Error"}

        existing_user = await self.usuario_repository.get_by_id(
            usuario_data.email, "email", type_result="first"
        )

        if existing_user:
            raise HTTPException(status_code=400, detail="Email already in use!")

        usuario_model_dump = self.validate_password(usuario_data)

        new_token = self.generate_token()
        usuario_model_dump["token"] = new_token
        # Crear el usuario en la base de datos
        usuario: UsuarioModel = await self.usuario_repository.create(usuario_model_dump)

        try:
            self.mailing.enviar_confirmacion(usuario.username, usuario.email, new_token)
        except Exception as e:
            print(e)
            raise HTTPException(status_code=500, detail="Error sending email")

        return {"success": "Confirmation email sent!"}

    async def update(self, usuario_id: str, input: UsuarioUpdateSchema) -> UsuarioModel:
        return await self.usuario_repository.update(
            UUID(usuario_id), input.model_dump(), "usuario_id"
        )

    async def get_all_and_search(
        self,
        limit: int = 10,
        offset: int = 1,
        username: str = None,
        email: str = None,
    ) -> list[dict]:
        return await self.usuario_repository.get_all_and_search(
            limit, offset, username, email
        )

    async def asignar_rol(
        self, usuario_id: str, rol_id: str, updated_by: str
    ) -> UsuarioModel:
        """
        Asigna un rol a un usuario.

        Args:
            usuario_id: ID del usuario a actualizar
            rol_id: ID del rol que se asignará
            updated_by: ID del usuario que realiza la actualización

        Returns:
            UsuarioModel actualizado con el nuevo rol asignado
        """
        try:
            usuario_actualizado = await self.usuario_repository.asignar_rol_a_usuario(
                usuario_id=UUID(usuario_id),
                rol_id=UUID(rol_id),
                updated_by=UUID(updated_by),
            )
            return usuario_actualizado
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error en servicio al asignar rol: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error en el servicio al asignar rol: {str(e)}"
            )

    async def cambiar_estado_usuario(
        self,
        usuario_id: str,
        updated_by: str,
        estado: int = 0,
    ) -> UsuarioModel:
        """
        Cambia el estado de un usuario (activo/inactivo)
        """
        return await self.usuario_repository.cambiar_estado_usuario(
            UUID(usuario_id),
            UUID(updated_by),
            estado,
        )
