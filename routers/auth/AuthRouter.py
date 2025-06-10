from fastapi import APIRouter, Depends, HTTPException, status, Response
from fastapi.security import OAuth2PasswordRequestForm
from schemas.auth.TokenSchema import TokenSchema
from schemas.auth.UsuarioSchema import UsuarioOutputSearchSchema
from config.settings import settings
from datetime import timedelta
from services.auth.AuthService import AuthService, AUTH, oauth2_scheme
from fastapi.responses import ORJSONResponse
import traceback


router = APIRouter()


@router.post("/token", response_model=TokenSchema)
async def login_for_access_token(
    response: Response,
    form_data: OAuth2PasswordRequestForm = Depends(),
    auth_service: AuthService = Depends(),
):
    user = await auth_service.authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = auth_service.create_access_token(
        response=response, data={"sub": user.email}, expires_delta=access_token_expires
    )
    response = {
        "access_token": access_token,
        "token_type": "bearer",
        "cookie_name": settings.COOKIE_NAME,
        "expires": settings.ACCESS_TOKEN_EXPIRE_MINUTES,
        "httpOnly": True,
    }

    return response


@router.get("", response_model=UsuarioOutputSearchSchema)
async def get_current_user(current_user: AUTH):
    return current_user


@router.post("/logout")
async def logout(
    response: Response,
    token: str = Depends(oauth2_scheme),
    auth_service: AuthService = Depends(),
):
    try:
        return await auth_service.logout(response, token)
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
