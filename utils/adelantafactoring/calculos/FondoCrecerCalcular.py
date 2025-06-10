import pandas as pd
from typing import Union
from .BaseCalcular import BaseCalcular
from ..obtener.FondoCrecerObtener import FondoCrecerObtener
from ..schemas.FondoCrecerCalcularSchema import FondoCrecerCalcularSchema


class FondoCrecerCalcular(BaseCalcular):
    """
    Procesa datos de FondoCrecer, valida columnas genérico,
    renombra, parsea porcentaje y elimina duplicados.
    """

    _cols_esperadas = ["LIQUIDACION", "GARANTIA"]
    _mapa_cols = {"liquidacion": "CodigoLiquidacion", "garantia": "Garantia"}

    def __init__(self) -> None:
        super().__init__()
        self._obtener = FondoCrecerObtener()

    def calcular(self, as_df: bool = False) -> Union[pd.DataFrame, list[dict]]:
        raw = self._obtener.fondo_crecer_obtener()
        df = pd.DataFrame(raw)

        self.validar_columnas(df, self._cols_esperadas)
        df = self.renombrar_columnas(df, self._mapa_cols)

        # <-- strip de espacios en el código de liquidación
        df["CodigoLiquidacion"] = df["CodigoLiquidacion"].astype(str).str.strip()

        # parsear 'Garantia' de "75%" a 0.75
        df["Garantia"] = df["Garantia"].astype(str).str.rstrip("%").astype(float) / 100

        df = df.drop_duplicates(subset="CodigoLiquidacion", keep="last")

        if as_df:
            return df

        return [
            FondoCrecerCalcularSchema(**rec).model_dump()
            for rec in df.to_dict(orient="records")
        ]
