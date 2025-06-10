from .BaseCalcular import BaseCalcular
from ..schemas.SectorPagadoresCalcularSchema import SectorPagadoresCalcularSchema
import pandas as pd
from config.logger import logger
from ..obtener.SectorPagadoresObtener import SectorPagadoresObtener


class SectorPagadoresCalcular(BaseCalcular):
    def __init__(self):
        super().__init__()
        self.sector_pagadores_obtener = SectorPagadoresObtener()

    def validar_datos(self, data: pd.DataFrame) -> list[dict]:
        try:
            datos_validados = [
                SectorPagadoresCalcularSchema(**d).model_dump()
                for d in data.to_dict(orient="records")
            ]
            return datos_validados
        except Exception as e:
            logger.error(e)
            raise e

    def procesar_datos(self, data: dict) -> pd.DataFrame:
        df = pd.DataFrame(data)
        df["RUCPagador"] = df["RUC"].astype(str).str.strip()
        df["Sector"] = df["SECTOR"].str.strip()
        df["GrupoEco"] = df["GRUPO ECO."].str.strip().replace({"": pd.NA})
        df = df[["RUCPagador", "Sector", "GrupoEco"]].drop_duplicates(
            subset=["RUCPagador"]
        )
        return df

    def calcular(self) -> list[dict]:
        data = self.sector_pagadores_obtener.obtener_sector_pagadores()
        datos_procesados = self.procesar_datos(data)
        datos_validados = self.validar_datos(datos_procesados)

        return datos_validados

    def calcular_df(self) -> pd.DataFrame:
        return pd.DataFrame(self.calcular())
