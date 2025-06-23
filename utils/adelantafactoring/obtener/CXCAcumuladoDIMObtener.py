from .BaseObtener import BaseObtener
from utils.timing_decorator import timing_decorator
from config.logger import logger


class CXCAcumuladoDIMObtener(BaseObtener):
    """
    Obtiene los datos acumulados DIM del webservice de operaciones.
    Endpoint: /liquidacionCab/subquery-cab
    """

    CXC_ACUMULADODIM_URL = "https://webservice.adelantafactoring/webservice/liquidacionCab/subquery-cab"

    def __init__(self):
        super().__init__()

    @timing_decorator
    async def obtener_acumulado_dim(self) -> list[dict]:
        """
        Obtiene datos acumulados DIM del webservice sin parámetros.
        
        Returns:
            Lista de datos acumulados DIM
        """
        try:
            data = await self.obtener_data_async_simple(self.CXC_ACUMULADODIM_URL)
            
            # Asegurar que devolvemos una lista
            if isinstance(data, list):
                logger.debug(f"Datos acumulados DIM obtenidos exitosamente: {len(data)} registros")
                return data
            else:
                logger.warning("La respuesta no es una lista, devolviendo lista vacía")
                return []
                
        except Exception as e:
            logger.error(f"Error obteniendo datos acumulados DIM: {e}")
            return []