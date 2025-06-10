import pandas as pd
from typing import Union
from .BaseCalcular import BaseCalcular
from ..obtener.FondoPromocionalObtener import FondoPromocionalObtener
from ..schemas.FondoPromocionalCalcularSchema import FondoPromocionalCalcularSchema


class FondoPromocionalCalcular(BaseCalcular):
    """
    Procesa datos de FondoPromocional, valida columnas genérico,
    renombra y elimina duplicados.
    """

    _cols_esperadas = ["LIQUIDACION"]
    _mapa_cols = {"liquidacion": "CodigoLiquidacion"}

    def __init__(self) -> None:
        super().__init__()
        self._obtener = FondoPromocionalObtener()

    def calcular(self, as_df: bool = False) -> Union[pd.DataFrame, list[dict]]:
        raw = self._obtener.fondo_promocional_obtener()
        df = pd.DataFrame(raw)

        self.validar_columnas(df, self._cols_esperadas)
        df = self.renombrar_columnas(df, self._mapa_cols)

        # <-- strip de espacios en el código de liquidación
        df["CodigoLiquidacion"] = df["CodigoLiquidacion"].astype(str).str.strip()

        df = df.drop_duplicates(subset="CodigoLiquidacion", keep="last")

        if as_df:
            return df

        return [
            FondoPromocionalCalcularSchema(**rec).model_dump()
            for rec in df.to_dict(orient="records")
        ]
