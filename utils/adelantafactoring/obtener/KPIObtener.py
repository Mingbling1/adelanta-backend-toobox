from .BaseObtener import BaseObtener
from utils.timing_decorator import timing_decorator
import httpx
from config.logger import logger
from datetime import datetime
from httpx import HTTPStatusError


class KPIObtener(BaseObtener):
    """
    Obtiene los datos del webservice del sistema de operaciones, debe usarse el tipo 2 siempre (detalle de anticipos y sin detalle de pagos).
    Si se necesita hacer cambios , se necesita revisa SP de la base de datos de Operaciones.
    """

    KPI_OBTNER_TIMEOUT = 120.0
    KPI_OBTENER_TOKEN_URL = "https://webservice.adelantafactoring.com/webservice/token"
    KPI_OBTENER_URL = "https://webservice.adelantafactoring.com/webservice/colocaciones"
    KPI_OBTENER_TIPO_REPORTE = 2
    KPI_OBTENER_CREDENTIALS = {
        "username": "adelantafactoring",
        "password": "jSB@$M5tR9pAXsUy",
    }

    def __init__(self):
        super().__init__()

    @timing_decorator
    async def obtener_colocaciones(
        self,
        start_date: datetime,
        end_date: datetime,
        fecha_corte: datetime,
        tipo_reporte: int = 2,
    ) -> list[dict]:
        await self.get_token(
            credentials=self.KPI_OBTENER_CREDENTIALS,
            token_url=self.KPI_OBTENER_TOKEN_URL,
        )

        reporte_url = self.KPI_OBTENER_URL
        params = {
            "desde": start_date.strftime("%Y%m%d"),
            "hasta": end_date.strftime("%Y%m%d"),
            "fechaCorte": fecha_corte.strftime("%Y%m%d"),
            "reporte": tipo_reporte,
        }

        try:
            async with httpx.AsyncClient(timeout=self.KPI_OBTNER_TIMEOUT) as client:
                results = await self.fetch_data(client, reporte_url, params)
        except HTTPStatusError as e:
            logger.debug(
                f"HTTP error occurred: {e.response.status_code} - {e.response.text}"
            )
            return
        except Exception as e:
            logger.debug(f"Error fetching data: {e}")
            return

        final_results = results if isinstance(results, list) else []

        logger.debug("Data successfully fetched from API.")

        return final_results
