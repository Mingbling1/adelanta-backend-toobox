from .BaseCalcular import BaseCalcular
import pandas as pd
from config.logger import logger
from ..obtener.CXCDevFactObtener import CXCDevFactObtener
from ..schemas.CXCDevFactCalcularSchema import CXCDevFactCalcularSchema


class CXCDevFactCalcular(BaseCalcular):
    """
    Calculador para datos de devoluciones de facturas CXC.
    Obtiene, valida y procesa los datos de devoluciones.
    """
    
    def __init__(self):
        super().__init__()
        self.cxc_dev_fact_obtener = CXCDevFactObtener()

    def validar_datos(self, data: list[dict]) -> list[dict]:
        """
        Valida los datos usando el schema de Pydantic.
        
        Args:
            data: Lista de diccionarios con datos de devoluciones
            
        Returns:
            Lista de diccionarios validados
        """
        try:
            datos_validados = [CXCDevFactCalcularSchema(**d).model_dump() for d in data]
            logger.debug(f"Datos de devoluciones validados exitosamente: {len(datos_validados)} registros")
            return datos_validados
        except Exception as e:
            logger.error(f"Error validando datos de devoluciones: {e}")
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
        logger.debug(f"DataFrame de devoluciones creado con {len(df)} filas")
        return df

    async def calcular(self) -> list[dict]:
        """
        Método principal que orquesta la obtención, validación y procesamiento.
        
        Returns:
            Lista de diccionarios con datos procesados de devoluciones
        """
        try:
            # Obtener datos
            data = await self.cxc_dev_fact_obtener.obtener_devoluciones_facturas()
            
            if not data:
                logger.warning("No se obtuvieron datos de devoluciones")
                return []
            
            # Validar datos
            datos_validados = self.validar_datos(data)
            
            # Procesar datos (si es necesario)
            # datos_procesados = self.procesar_datos(datos_validados)
            
            logger.info(f"Cálculo de devoluciones completado: {len(datos_validados)} registros")
            return datos_validados
            
        except Exception as e:
            logger.error(f"Error en cálculo de devoluciones: {e}")
            raise e