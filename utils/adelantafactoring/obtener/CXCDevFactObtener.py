from .BaseObtener import BaseObtener
from utils.timing_decorator import timing_decorator
from config.logger import logger


class CXCDevFactObtener(BaseObtener):
    """
    Obtiene los datos de devoluciones de facturas del webservice de operaciones.
    Endpoint: /liquidacionDevolucion/subquery
    """

    CXC_DEVFACT_URL = "https://webservice.adelantafactoring.com/webservice/liquidacionDevolucion/subquery"

    def __init__(self):
        super().__init__()

    @timing_decorator
    async def obtener_devoluciones_facturas(self) -> list[dict]:
        """
        Obtiene devoluciones de facturas del webservice sin parámetros.
        
        Returns:
            Lista de devoluciones de facturas
        """
        try:
            data = await self.obtener_data_async_simple(self.CXC_DEVFACT_URL)
            
            # Asegurar que devolvemos una lista
            if isinstance(data, list):
                logger.debug(f"Devoluciones de facturas obtenidas exitosamente: {len(data)} registros")
                return data
            else:
                logger.warning("La respuesta no es una lista, devolviendo lista vacía")
                return []
                
        except Exception as e:
            logger.error(f"Error obteniendo devoluciones de facturas: {e}")
            return []