from config.settings import settings
from schemas.sunat.SunatSchema import SunatTipoSchema, EmpresaSchema, PersonaSchema
import requests
from fastapi import HTTPException
from pydantic import ValidationError
from typing import Union


class SunatService:
    def __init__(self):
        pass

    async def get_info(
        self, input: SunatTipoSchema
    ) -> Union[EmpresaSchema, PersonaSchema]:
        if input.tipo.sunat_tipo == "ruc":
            url: str = f"{settings.URL_API_RUC}{input.tipo.ruc}"
            headers: dict = {"Authorization": f"Bearer {settings.TOKEN_SUNAT}"}
            response = requests.get(url, headers=headers)
            try:
                return EmpresaSchema(**response.json())
            except ValidationError as e:
                raise HTTPException(status_code=400, detail=str(e))
        elif input.tipo.sunat_tipo == "dni":
            url: str = f"{settings.URL_API_DNI}{input.tipo.dni}"
            print(url, input)
            headers: dict = {"Authorization": f"Bearer {settings.TOKEN_SUNAT}"}
            response = requests.get(url, headers=headers)
            try:
                return PersonaSchema(**response.json())
            except ValidationError as e:
                raise HTTPException(status_code=400, detail=str(e))
