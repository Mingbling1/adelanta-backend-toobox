from .BaseObtener import BaseObtener
from utils.timing_decorator import timing_decorator
from config.logger import logger


class CXCPagosFactObtener(BaseObtener):
    """
    Obtiene los datos de pagos de facturas del webservice de operaciones.
    Endpoint: /liquidacionPago/subquery
    """

    CXC_PAGOSFACT_URL = "http://localhost:8080/webservice/liquidacionPago/subquery"

    def __init__(self):
        super().__init__()

    @timing_decorator
    async def obtener_pagos_facturas(self) -> list[dict]:
        """
        Obtiene pagos de facturas del webservice sin parámetros.
        
        Returns:
            Lista de pagos de facturas
        """
        try:
            data = await self.obtener_data_async_simple(self.CXC_PAGOSFACT_URL)
            
            # Asegurar que devolvemos una lista
            if isinstance(data, list):
                logger.debug(f"Pagos de facturas obtenidos exitosamente: {len(data)} registros")
                return data
            else:
                logger.warning("La respuesta no es una lista, devolviendo lista vacía")
                return []
                
        except Exception as e:
            logger.error(f"Error obteniendo pagos de facturas: {e}")
            return []