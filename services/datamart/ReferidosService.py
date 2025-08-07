from repositories.datamart.ReferidosRepository import ReferidosRepository
from fastapi import Depends
from repositories.datamart.KPIRepository import KPIRepository
from schemas.datamart.ReferidosSchema import (
    ReferidosPostRequestSchema,
)
from models.datamart.ReferidosModel import ReferidosModel
import pandas as pd
from fastapi.responses import StreamingResponse
import io
from io import BytesIO
import calendar
from datetime import datetime
from models.datamart.TipoCambioModel import TipoCambioModel
from utils.decorators import create_job
from toolbox.api.comisiones_api import get_comisiones_async


class ReferidosService:
    def __init__(
        self,
        tipo_cambio_repository: ReferidosRepository = Depends(),
        kpi_repository: KPIRepository = Depends(),
    ):
        self.tipo_cambio_repository = tipo_cambio_repository
        self.kpi_repository = kpi_repository

    def month_name_spanish(self, date_str: str) -> str:
        """
        Recibe una fecha en formato 'YYYY-MM-DD' y retorna
        una cadena con el nombre del mes en español y el año, ej: 'Enero_2025'
        """
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        meses = {
            1: "Enero",
            2: "Febrero",
            3: "Marzo",
            4: "Abril",
            5: "Mayo",
            6: "Junio",
            7: "Julio",
            8: "Agosto",
            9: "Septiembre",
            10: "Octubre",
            11: "Noviembre",
            12: "Diciembre",
        }
        return f"{meses[dt.month]}_{dt.year}"

    async def get_all(self, limit: int = 10, offset: int = 0) -> list[ReferidosModel]:
        return await self.tipo_cambio_repository.get_all(limit, offset)

    async def create_many(self, input: list[ReferidosPostRequestSchema]):
        await self.tipo_cambio_repository.create_many([i.model_dump() for i in input])

    async def delete_all(self):
        await self.tipo_cambio_repository.delete_all()

    @create_job(
        name="Calcular Comisiones",
        description="Generar un zip con las comisiones de los referidos",
        expire=60 * 10 * 1,  # 10 minutos
        is_buffer=True,
        save_as="zip",
        capture_params=True,
    )
    async def calcular_comisiones(
        self,
        primer_dia: str | None = "2023-01",
        ultimo_dia: str | None = datetime.now().strftime("%Y-%m"),
    ) -> BytesIO:
        def format_date_range(
            primer_dia: str | None = "2023-01", ultimo_dia: str | None = None
        ) -> tuple[str, str]:
            """
            Convierte primer_dia y ultimo_dia en formato "YYYY-MM" a fechas completas:
            - primer_dia se convierte a "YYYY-MM-01".
            - ultimo_dia se convierte a "YYYY-MM-last_day" (último día del mes).

            """
            if primer_dia is None:
                primer_dia = "2023-01-01"
            elif len(primer_dia) == 7:
                primer_dia = f"{primer_dia}-01"

            if ultimo_dia is not None and len(ultimo_dia) == 7:
                year, month = map(int, ultimo_dia.split("-"))
                last_day = calendar.monthrange(year, month)[1]
                ultimo_dia = f"{year}-{month:02d}-{last_day:02d}"

            return primer_dia, ultimo_dia

        # obtenemos todos los KPI como lista de dicts (sin el id)
        rows = await self.kpi_repository.get_all_dicts(exclude_pk=True)
        # preservamos el orden de columnas según el primer dict
        cols = list(rows[0].keys()) if rows else []
        kpi_df = pd.DataFrame(rows, columns=cols)

        start_date, end_date = format_date_range(primer_dia, ultimo_dia)

        zip_bytes = await get_comisiones_async(
            kpi_df=kpi_df, start_date=start_date, end_date=end_date
        )
        return zip_bytes

    async def get_all_to_csv(self):
        tipo_cambio_records: list[TipoCambioModel] = (
            await self.tipo_cambio_repository.get_all(limit=None, offset=0)
        )
        df = pd.DataFrame([record.to_dict() for record in tipo_cambio_records])
        stream = io.StringIO()
        df.to_csv(stream, index=False)
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=tipo_cambio.csv"
        return response
