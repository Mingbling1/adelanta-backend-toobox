from .BaseCalcular import BaseCalcular
import pandas as pd
from config.logger import logger
from ..obtener.CXCPagosFactObtener import CXCPagosFactObtener
from ..schemas.CXCPagosFactCalcularSchema import CXCPagosFactCalcularSchema


class CXCPagosFactCalcular(BaseCalcular):
    """
    Calculador para datos de pagos de facturas CXC.
    Obtiene, valida y procesa los datos de pagos.
    """
    
    def __init__(self):
        super().__init__()
        self.cxc_pagos_fact_obtener = CXCPagosFactObtener()

    def validar_datos(self, data: list[dict]) -> list[dict]:
        """
        Valida los datos usando el schema de Pydantic.
        
        Args:
            data: Lista de diccionarios con datos de pagos
            
        Returns:
            Lista de diccionarios validados
        """
        try:
            datos_validados = [CXCPagosFactCalcularSchema(**d).model_dump() for d in data]
            logger.debug(f"Datos de pagos validados exitosamente: {len(datos_validados)} registros")
            return datos_validados
        except Exception as e:
            logger.error(f"Error validando datos de pagos: {e}")
            raise e

    def procesar_datos(self, data: list[dict]) -> pd.DataFrame:
        """
        Procesa los datos validados en DataFrame.
        
        Args:
            data: Lista de diccionarios validados
            
        Returns:
            DataFrame procesado
        """
        df = pd.DataFrame(data)
        logger.debug(f"DataFrame de pagos creado con {len(df)} filas")
        return df

    async def calcular(self) -> list[dict]:
        """
        Método principal que orquesta la obtención, validación y procesamiento.
        
        Returns:
            Lista de diccionarios con datos procesados de pagos
        """
        try:
            # Obtener datos
            data = await self.cxc_pagos_fact_obtener.obtener_pagos_facturas()
            
            if not data:
                logger.warning("No se obtuvieron datos de pagos")
                return []
            
            # Validar datos
            datos_validados = self.validar_datos(data)
            
            # Procesar datos (si es necesario)
            # datos_procesados = self.procesar_datos(datos_validados)
            
            logger.info(f"Cálculo de pagos completado: {len(datos_validados)} registros")
            return datos_validados
            
        except Exception as e:
            logger.error(f"Error en cálculo de pagos: {e}")
            raise e