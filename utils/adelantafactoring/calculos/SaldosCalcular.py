from .BaseCalcular import BaseCalcular
import pandas as pd
from config.logger import logger
from ..obtener.SaldosObtener import SaldosObtener
from ..schemas.SaldosCalcularSchema import SaldosCalcularSchema


class SaldosCalcular(BaseCalcular):
    def __init__(self):
        super().__init__()
        self.saldos_obtener = SaldosObtener()

    def validar_datos(self, data: dict) -> list[dict]:
        try:
            datos_validados = [SaldosCalcularSchema(**d).model_dump() for d in data]
            return datos_validados
        except Exception as e:
            logger.error(e)
            raise e

    def procesar_datos(self, data: dict) -> pd.DataFrame:
        df = pd.DataFrame(data)
        return df

    def calcular(self) -> list[dict]:
        data = self.saldos_obtener.obtener_saldos()
        datos_validados = self.validar_datos(data)
        # datos_procesados = self.procesar_datos(datos_validados)
        return datos_validados
