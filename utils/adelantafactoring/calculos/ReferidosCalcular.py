import unicodedata
import pandas as pd
from typing import Union
from .BaseCalcular import BaseCalcular
from ..obtener.ReferidosObtener import ReferidosObtener
from ..schemas.ReferidosCalcularSchema import ReferidosCalcularSchema


class ReferidosCalcular(BaseCalcular):
    """
    Obtiene datos de referidos desde ReferidosObtener, valida
    columnas requeridas, renombra y formatea, y finalmente los
    valida contra un schema Pydantic.
    """

    # columnas esperadas antes de normalizar
    _cols_esperadas = ["REFERENCIA", "LIQUIDACIÓN", "EJECUTIVO", "MES"]
    # mapeo de nombre normalizado a nombre final
    _mapa_cols = {
        "referencia": "Referencia",
        "liquidacion": "CodigoLiquidacion",
        "ejecutivo": "Ejecutivo",
        "mes": "Mes",
    }

    def __init__(self) -> None:
        super().__init__()
        self._obtener = ReferidosObtener()

    def calcular(self, as_df: bool = False) -> Union[list[dict], pd.DataFrame]:
        """
        Flujo principal:
          1. Obtener lista de dicts desde ReferidosObtener.
          2. Validar que vengan las columnas necesarias.
          3. Renombrar y formatear.
          4. Eliminar duplicados por Código de Liquidación.
          5. Validar cada registro con Pydantic.

        Parámetros:
          as_df: si True devuelve un pd.DataFrame,
                 si False (por defecto) devuelve lista de dicts.

        Retorna:
          list[dict] o pd.DataFrame según as_df.
        """
        raw = self._obtener.referidos_obtener()
        df = pd.DataFrame(raw)

        # 1) Validar columnas genérico
        self.validar_columnas(df, self._cols_esperadas)

        # 2) Renombrar genérico
        df = self.renombrar_columnas(df, self._mapa_cols)

        # 3) Formatear 'Mes' dd/mm/YYYY → datetime
        df["Mes"] = pd.to_datetime(
            df["Mes"], format="%d/%m/%Y", dayfirst=True, errors="raise"
        )

        # 4) Eliminar duplicados por CodigoLiquidacion
        df = df.drop_duplicates(subset="CodigoLiquidacion", keep="last")

        if as_df:
            return df

        # 4) Validar esquemas y serializar a lista de dicts
        salida: list[dict] = []
        for rec in df.to_dict(orient="records"):
            valid = ReferidosCalcularSchema(**rec).model_dump()
            salida.append(valid)

        return salida
