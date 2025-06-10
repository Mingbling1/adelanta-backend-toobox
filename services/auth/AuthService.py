from fastapi import Depends, HTTPException, status, Response
from jose import jwt, JWTError
from fastapi.security import OAuth2PasswordBearer
import bcrypt
from datetime import datetime, timedelta, timezone
from repositories.auth.UsuarioRepository import UsuarioRepository
from config.settings import settings
from schemas.auth.UsuarioSchema import UsuarioOutputSearchSchema
from typing import Annotated

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token")


class AuthService:
    def __init__(self, usuario_repository: UsuarioRepository = Depends()) -> None:
        self.usuario_repository = usuario_repository

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        return bcrypt.checkpw(
            plain_password.encode("utf-8"), hashed_password.encode("utf-8")
        )

    async def authenticate_user(self, username: str, password: str):
        user = await self.usuario_repository.get_usuario_by_email(username)
        if not user:
            return None
        if not self.verify_password(password, user.hashed_password):
            return None
        return user

    def create_access_token(
        self, response: Response, data: dict, expires_delta: timedelta | None = None
    ) -> str:
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(minutes=15)
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(
            to_encode, str(settings.SECRET_KEY), algorithm=str(settings.ALGORITHM)
        )
        response.set_cookie(
            key="token",
            value=encoded_jwt,
            httponly=True,
            expires=expire,
        )
        return encoded_jwt

    async def logout(self, response: Response, token: str = Depends(oauth2_scheme)):
        payload = jwt.decode(
            token, str(settings.SECRET_KEY), algorithms=[str(settings.ALGORITHM)]
        )

        if payload:
            response.delete_cookie(
                "token",
                httponly=True,
            )
            return {"detail": "Successfully logged out"}
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )

    @staticmethod
    async def get_current_user(
        token: str = Depends(oauth2_scheme),
        usuario_repository: UsuarioRepository = Depends(),
    ) -> UsuarioOutputSearchSchema:
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
        try:
            payload = jwt.decode(
                token, str(settings.SECRET_KEY), algorithms=[str(settings.ALGORITHM)]
            )
            email: str = payload.get("sub")
            if email is None:
                raise credentials_exception
        except JWTError:
            raise credentials_exception
        user = await usuario_repository.get_usuario_by_email_detallado(email)
        if user is None:
            raise credentials_exception
        return user


AUTH = Annotated[UsuarioOutputSearchSchema, Depends(AuthService.get_current_user)]
