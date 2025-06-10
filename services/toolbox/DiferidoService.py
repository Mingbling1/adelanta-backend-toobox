from repositories.toolbox.DiferidoRepository import DiferidoRepository
from repositories.datamart.KPIRepository import KPIRepository
from fastapi import Depends, UploadFile
import pandas as pd
from utils.adelantafactoring.calculos.diferido.DiferidoCalcular import DiferidoCalcular
from utils.decorators import create_job
from io import BytesIO
from models.datamart.KPIModel import KPIModel
from config.logger import logger
import asyncio


class DiferidoService:
    def __init__(
        self,
        diferido_repository: DiferidoRepository = Depends(),
        kpi_repository: KPIRepository = Depends(),
    ):
        self.diferido_repository = diferido_repository
        self.kpi_repository = kpi_repository

    async def calcular_diferido(self, hasta: str, file: UploadFile) -> BytesIO:
        logger.debug(f"Archivo recibido: {file.filename}")
        # Leer el contenido del archivo inmediatamente
        data = await file.read()
        # Crear un buffer en memoria a partir de los datos leídos
        persistente_file = BytesIO(data)
        persistente_file.seek(0)

        @create_job(
            name="Calcular Diferido",
            description="Genera el Excel en memoria con el diferido",
            expire=60 * 10,  # 30 minutos
            is_buffer=True,
            save_as="excel",
            capture_params=True,
        )
        async def obtener_diferido(
            hasta, persistente_file, archivo_nombre: str
        ) -> BytesIO:
            kpi_records: list[KPIModel] = await self.kpi_repository.get_all(
                limit=None, offset=0
            )
            df_interno = pd.DataFrame([record.to_dict() for record in kpi_records])
            logger.debug(f"DataFrame interno: {df_interno.head()}")

            # Instanciar DiferidoCalcular usando el buffer persistente
            diferido_calcular = DiferidoCalcular(
                file_path_externo=persistente_file, df_interno=df_interno
            )
            resultado = await diferido_calcular.calcular_diferido_async(hasta)
            # Convertir el DataFrame resultante a Excel de forma asíncrona

            excel_buffer = BytesIO()

            await asyncio.to_thread(
                resultado.to_excel, excel_buffer, engine="xlsxwriter"
            )
            excel_buffer.seek(0)
            return excel_buffer

        # Se pasa además el nombre del archivo original como keyword argument
        return await obtener_diferido(
            hasta=hasta, persistente_file=persistente_file, archivo_nombre=file.filename
        )
