from fastapi import HTTPException
from repositories.administrativo.GastoRepository import GastoRepository
from repositories.administrativo.ArchivoRepository import ArchivoRepository
from repositories.administrativo.ProveedorRepository import ProveedorRepository
from repositories.administrativo.PagoRepository import PagoRepository
from models.administrativo.GastoModel import GastoModel
from models.administrativo.ArchivoModel import ArchivoModel
from schemas.administrativo.GastoSchema import (
    GastoCreateSchema,
    GastoUpdateSchema,
)
from models.administrativo.PagoModel import PagoModel
from fastapi import Depends, UploadFile
from uuid import UUID
from utils.googledrive.GoogleDrive import GoogleDriveClient
from utils.googledrive.GoogleDrive import PARENT_FOLDER_ID
import locale

locale.setlocale(locale.LC_TIME, "es_ES.UTF-8")


class GastoService:
    def __init__(
        self,
        gasto_repository: GastoRepository = Depends(),
        archivo_repository: ArchivoRepository = Depends(),
        proveedor_repository: ProveedorRepository = Depends(),
        pago_repository: PagoRepository = Depends(),
        google_drive: GoogleDriveClient = Depends(),
    ):
        self.gasto_repository = gasto_repository
        self.pago_repository = pago_repository
        self.archivo_repository = archivo_repository
        self.proveedor_repository = proveedor_repository
        self.google_drive = google_drive

    async def get_by_id(self, gasto_id: str) -> GastoModel:
        return await self.gasto_repository.get_by_id(UUID(gasto_id), "gasto_id")

    async def update(self, gasto_id: str, input: GastoUpdateSchema) -> GastoModel:
        return await self.gasto_repository.update(
            UUID(gasto_id), input.model_dump(), "gasto_id"
        )

    async def create(self, input: GastoCreateSchema) -> GastoModel:
        return await self.gasto_repository.create(input.model_dump())

    async def create_many_with_return(
        self, input: list[GastoCreateSchema]
    ) -> list[GastoModel]:
        return await self.gasto_repository.create_many_with_return(
            [item.model_dump() for item in input]
        )

    async def delete_all(self):
        await self.gasto_repository.delete_all()

    async def get_all(self, limit: int = 10, offset: int = 0) -> list[GastoModel]:
        return await self.gasto_repository.get_all(limit, offset)

    async def get_all_and_search(
        self,
        limit: int = 10,
        offset: int = 1,
        nombre_gasto: str = None,
        fecha_emision: str = None,
        gasto_id: str = None,
    ) -> list[GastoModel]:
        return await self.gasto_repository.get_all_and_search(
            limit, offset, nombre_gasto, fecha_emision, gasto_id
        )

    async def create_gasto_con_archivos(
        self,
        gasto: GastoCreateSchema,
        archivos: list[UploadFile],
    ) -> GastoModel:
        existing_folders_dict = (
            await self.archivo_repository.get_existing_folders_dict()
        )

        if (
            hasattr(gasto.tipo_gasto, "fecha_contable")
            and gasto.tipo_gasto.fecha_contable
        ):
            año = gasto.tipo_gasto.fecha_contable.strftime("%Y")
            mes = gasto.tipo_gasto.fecha_contable.strftime("%B")
        else:
            año = gasto.tipo_gasto.fecha_emision.strftime("%Y")
            mes = gasto.tipo_gasto.fecha_emision.strftime("%B")

        # Crear las carpetas solo con los criterios de año y mes
        folder_names = [año, mes]

        folder_id, path, id_path = self.google_drive.create_folders(
            folder_names,
            PARENT_FOLDER_ID,
            existing_folders_dict,
        )

        file_urls = await self.google_drive.upload_files(
            archivos, folder_id, path, id_path, str(gasto.tipo_gasto.created_by)
        )

        nuevo_gasto = await self.create(gasto.tipo_gasto)
        file_infos = [
            ArchivoModel(
                google_drive_archivo_id=file_url["google_drive_archivo_id"],
                webViewLink=file_url["webViewLink"],
                name=file_url["name"],
                path=file_url["path"],
                size=file_url["size"],
                content_type=file_url["content_type"],
                parent_folder_id=file_url["parent_folder_id"],
                path_id=file_url["path_id"],
                gasto_id=nuevo_gasto.gasto_id,
                created_by=gasto.tipo_gasto.created_by,
            )
            for file_url in file_urls
        ]

        await self.archivo_repository.create_many(file_infos)

        return nuevo_gasto

    async def eliminar_gasto_con_archivos(
        self,
        gasto_id: str,
        updated_by: str,
    ) -> GastoModel:
        # Obtener el gasto
        gasto: GastoModel = await self.gasto_repository.get_by_id(
            UUID(gasto_id), "gasto_id"
        )
        if not gasto:
            raise HTTPException(status_code=404, detail="Gasto no encontrado")

        # Obtener los archivos asociados al gasto
        archivos: list[ArchivoModel] = await self.archivo_repository.get_by_id(
            UUID(gasto_id), "gasto_id", "all"
        )

        # Eliminar archivos de Google Drive y # Actualizar los archivos (estado = 0, updated_by)
        for archivo in archivos:
            self.google_drive.delete_file(archivo.google_drive_archivo_id)
            await self.archivo_repository.update(
                archivo.archivo_id,
                {"estado": 0, "updated_by": UUID(updated_by)},
                "archivo_id",
            )

        pagos: list[PagoModel] = await self.pago_repository.get_by_id(
            UUID(gasto_id), "gasto_id", "all"
        )

        # Actualizar los pagos (estado = 0, updated_by)
        for pago in pagos:
            await self.pago_repository.update(
                pago.pago_id,
                {"estado": 0, "updated_by": UUID(updated_by)},
                "pago_id",
            )

        # Actualizar el gasto (estado = 0, updated_by)
        await self.gasto_repository.update(
            UUID(gasto_id),
            {"estado": 0, "updated_by": UUID(updated_by)},
            "gasto_id",
        )

    async def editar_gasto(
        self,
        gasto_id: str,
        input: GastoUpdateSchema,
    ) -> GastoModel:
        # Actualizar el gasto
        updated_gasto = await self.update(gasto_id, input)

        return updated_gasto

    # async def create_gasto_con_archivos(
    #     self,
    #     gasto: GastoCreateSchema,
    #     archivos: list[UploadFile],
    # ) -> GastoModel:
    #     proveedor: ProveedorModel = await self.proveedor_repository.get_by_id(
    #         gasto.tipo_gasto.proveedor_id, "proveedor_id"
    #     )
    #     existing_folders_dict = (
    #         await self.archivo_repository.get_existing_folders_dict()
    #     )

    #     if gasto.tipo_gasto.tipo_CDP == "reciboHonorarios":
    #         if gasto.tipo_gasto.porcentaje_descuento == 0.08:
    #             tipo_carpeta = "Recibo por honorarios 8%"
    #         elif gasto.tipo_gasto.porcentaje_descuento == 0:
    #             tipo_carpeta = "Recibo por honorarios 0%"
    #         else:
    #             tipo_carpeta = "Otros"
    #     elif gasto.tipo_gasto.porcentaje_descuento == 0:
    #         tipo_carpeta = "Compras descuento 0%"
    #     elif gasto.tipo_gasto.porcentaje_descuento == 0.12:
    #         tipo_carpeta = "Compras descuento 12%"
    #     else:
    #         tipo_carpeta = "Otros"

    #     if (
    #         hasattr(gasto.tipo_gasto, "fecha_contable")
    #         and gasto.tipo_gasto.fecha_contable
    #     ):
    #         año = gasto.tipo_gasto.fecha_contable.strftime("%Y")
    #         mes = gasto.tipo_gasto.fecha_contable.strftime("%B")
    #     else:
    #         año = gasto.tipo_gasto.fecha_emision.strftime("%Y")
    #         mes = gasto.tipo_gasto.fecha_emision.strftime("%B")

    #     # Manejar el caso en el que 'numero_CDP' no existe
    #     if hasattr(gasto.tipo_gasto, "numero_CDP"):
    #         folder_names = [
    #             año,
    #             mes,
    #             tipo_carpeta,
    #             f"{proveedor.nombre_proveedor}-{gasto.tipo_gasto.numero_CDP}",
    #         ]
    #     else:
    #         folder_names = [
    #             año,
    #             mes,
    #             tipo_carpeta,
    #             f"{proveedor.nombre_proveedor}",
    #         ]

    #     folder_id, path, id_path = self.google_drive.create_folders(
    #         folder_names,
    #         PARENT_FOLDER_ID,
    #         existing_folders_dict,
    #     )

    #     file_urls = await self.google_drive.upload_files(
    #         archivos, folder_id, path, id_path, str(gasto.tipo_gasto.created_by)
    #     )

    #     nuevo_gasto = await self.create(gasto.tipo_gasto)
    #     file_infos = [
    #         ArchivoModel(
    #             google_drive_archivo_id=file_url["google_drive_archivo_id"],
    #             webViewLink=file_url["webViewLink"],
    #             name=file_url["name"],
    #             path=file_url["path"],
    #             size=file_url["size"],
    #             content_type=file_url["content_type"],
    #             parent_folder_id=file_url["parent_folder_id"],
    #             path_id=file_url["path_id"],
    #             gasto_id=nuevo_gasto.gasto_id,
    #             created_by=gasto.tipo_gasto.created_by,
    #         )
    #         for file_url in file_urls
    #     ]

    #     await self.archivo_repository.create_many(file_infos)

    #     return nuevo_gasto
