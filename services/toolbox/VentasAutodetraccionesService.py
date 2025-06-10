from repositories.toolbox.VentasAutodetraccionesRepository import (
    VentasAutodetraccionesRepository,
)
from fastapi import Depends, UploadFile
import pandas as pd
from utils.decorators import create_job
from io import BytesIO
from models.datamart.TipoCambioModel import TipoCambioModel
from config.logger import logger
from repositories.datamart.TipoCambioRepository import TipoCambioRepository
from utils.adelantafactoring.calculos.VentasAutodetraccionesCalcular import (
    VentasAutodetraccionesCalcular,
)


class VentasAutodetraccionesService:
    def __init__(
        self,
        ventas_autodetracciones_repository: VentasAutodetraccionesRepository = Depends(),
        tipo_cambio_repository: TipoCambioRepository = Depends(),
    ):
        self.ventas_autodetracciones_repository = ventas_autodetracciones_repository
        self.tipo_cambio_repository = tipo_cambio_repository

    async def calcular_ventas_autodetracciones(
        self, hasta: str, file: UploadFile
    ) -> BytesIO:
        """
        Recibe un archivo con los comprobantes y un parámetro 'hasta'
        en formato "YYYY-MM". Lee el archivo, obtiene el DataFrame de tipo de cambio
        y luego genera el Excel filtrado por ese mes.
        """
        logger.debug(f"Archivo recibido: {file.filename}")
        # Leer contenido del archivo
        data = await file.read()
        persistente_file = BytesIO(data)
        persistente_file.seek(0)

        @create_job(
            name="Calcular Ventas Autodetracciones",
            description="Genera el Excel con las ventas autodetracciones filtradas por mes",
            expire=60 * 10,  # 5 minutos
            is_buffer=True,
            save_as="excel",
            capture_params=True,
        )
        async def obtener_ventas_autodetracciones(
            hasta: str, persistente_file: BytesIO, archivo_nombre: str
        ) -> BytesIO:
            # Obtener los registros de tipo cambio
            tipo_cambio_records: list[TipoCambioModel] = (
                await self.tipo_cambio_repository.get_all(limit=None, offset=0)
            )
            tipo_cambio_df = pd.DataFrame(
                [record.to_dict() for record in tipo_cambio_records]
            )
            # Leer el archivo de comprobantes (por ejemplo, un Excel)
            comprobantes_df = pd.read_excel(persistente_file, skiprows=2)
            # Instanciar la clase de cálculo pasándole los DataFrames necesarios
            calculador = VentasAutodetraccionesCalcular(
                tipo_cambio_df=tipo_cambio_df, comprobantes_df=comprobantes_df
            )
            # Generar el Excel filtrado por 'hasta' (YYYY-MM)
            excel_buffer = await calculador.generar_excel_autodetraccion(hasta)
            return excel_buffer

        return await obtener_ventas_autodetracciones(
            hasta=hasta, persistente_file=persistente_file, archivo_nombre=file.filename
        )
