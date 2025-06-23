from .BaseObtener import BaseObtener
from utils.timing_decorator import timing_decorator
from config.logger import logger
from datetime import datetime


class KPIObtener(BaseObtener):
    """
    Obtiene los datos del webservice del sistema de operaciones, debe usarse el tipo 2 siempre (detalle de anticipos y sin detalle de pagos).
    Si se necesita hacer cambios , se necesita revisa SP de la base de datos de Operaciones.
    """

    KPI_OBTENER_TOKEN_URL = "https://webservice.adelantafactoring.com/webservice/token"
    KPI_OBTENER_URL = "https://webservice.adelantafactoring.com/webservice/colocaciones"
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
        """
        Obtiene colocaciones del webservice usando el método simplificado.
        """
        try:
            data = await self.obtener_data_con_autenticacion(
                url=self.KPI_OBTENER_URL,
                params={
                    "desde": start_date.strftime("%Y%m%d"),
                    "hasta": end_date.strftime("%Y%m%d"),
                    "fechaCorte": fecha_corte.strftime("%Y%m%d"),
                    "reporte": tipo_reporte,
                },
                token_url=self.KPI_OBTENER_TOKEN_URL,
                credentials=self.KPI_OBTENER_CREDENTIALS
            )
            
            # Asegurar que devolvemos una lista
            if isinstance(data, list):
                logger.debug(f"Colocaciones obtenidas exitosamente: {len(data)} registros")
                return data
            else:
                logger.warning("La respuesta no es una lista, devolviendo lista vacía")
                return []
                
        except Exception as e:
            logger.error(f"Error obteniendo colocaciones: {e}")
            return []