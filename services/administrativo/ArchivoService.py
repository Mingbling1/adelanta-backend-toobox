from repositories.administrativo.ArchivoRepository import ArchivoRepository
from models.administrativo.ArchivoModel import ArchivoModel
from schemas.administrativo.ArchivoSchema import (
    ArchivoCreateSchema,
    ArchivoUpdateSchema,
)
from fastapi import Depends
from uuid import UUID
from utils.googledrive.GoogleDrive import GoogleDriveClient


class ArchivoService:
    def __init__(
        self,
        archivo_repository: ArchivoRepository = Depends(),
        google_drive: GoogleDriveClient = Depends(),
    ):
        self.archivo_repository = archivo_repository
        self.google_drive = google_drive

    async def get_by_id(self, archivo_id: str) -> ArchivoModel:
        return await self.archivo_repository.get_by_id(UUID(archivo_id), "archivo_id")

    async def update(self, archivo_id: str, input: ArchivoUpdateSchema) -> ArchivoModel:
        return await self.archivo_repository.update(
            UUID(archivo_id), input.model_dump(), "archivo_id"
        )

    async def create(self, input: ArchivoCreateSchema) -> ArchivoModel:
        return await self.archivo_repository.create(input.model_dump())

    async def create_many_with_return(
        self, input: list[ArchivoCreateSchema]
    ) -> list[ArchivoModel]:
        return await self.archivo_repository.create_many_with_return(
            [item.model_dump() for item in input]
        )

    async def delete_all(self):
        await self.archivo_repository.delete_all()

    async def get_all(self, limit: int = 10, offset: int = 0) -> list[ArchivoModel]:
        return await self.archivo_repository.get_all(limit, offset)

    async def get_all_and_search(
        self,
        limit: int = 10,
        offset: int = 1,
        nombre_archivo: str = None,
        tipo_archivo: str = None,
        gasto_id: str | None = None,
    ) -> list[ArchivoModel]:
        return await self.archivo_repository.get_all_and_search(
            limit, offset, nombre_archivo, tipo_archivo, gasto_id
        )

    async def get_existing_folders_dict(self) -> dict:
        return await self.archivo_repository.get_existing_folders_dict()
