from datetime import datetime
from typing import List, Dict, Union

import numpy as np
import pandas as pd
import math
from rapidfuzz import process, fuzz
from pydantic import ValidationError
from config.logger import logger
from .BaseCalcular import BaseCalcular
from .OperacionesFueraSistemaCalcular import OperacionesFueraSistemaCalcular
from .SectorPagadoresCalcular import SectorPagadoresCalcular
from .ComisionesCalcular import ComisionesCalcular
from ..obtener.KPIObtener import KPIObtener
from ..schemas.KPICalcularSchema import KPICalcularSchema


class KPICalcular(BaseCalcular):
    """
    Extrae colocaciones, las valida, formatea, enriquece y calcula todos los KPIs necesarios.
    """

    INTERESES_PEN: float = 0.14
    INTERESES_USD: float = 0.12

    def __init__(self, tipo_cambio_df: pd.DataFrame):
        super().__init__()
        self.tipo_cambio_df = tipo_cambio_df
        self.kpi_obtener = KPIObtener()
        self.operaciones_fuera_sistema_calcular = OperacionesFueraSistemaCalcular()
        self.sector_pagadores_calcular = SectorPagadoresCalcular()

    async def calcular(
        self,
        start_date: datetime,
        end_date: datetime,
        fecha_corte: datetime,
        tipo_reporte: int = 2,
        as_df: bool = False,
    ) -> Union[pd.DataFrame, List[Dict]]:
        """
        Flujo principal:
          1. Obtener colocaciones via webservice.
          2. Validar columnas y tipos mínimos.
          3. Enriquecer con operaciones fuera de sistema y unificar nombres.
          4. Formatear fechas, strings y tipos numéricos.
          5. Calcular métricas e ingresos/costos.
          6. Enriquecer con referidos.
          7. Validar esquema pydantic y serializar.

        Parametros:
          as_df: si True devuelve DataFrame, si False devuelve lista de dicts.
        """
        raw = await self.kpi_obtener.obtener_colocaciones(
            start_date, end_date, fecha_corte, tipo_reporte
        )
        df = pd.DataFrame(raw)
        df = self._validar_columnas_minimas(df)
        df = self._fusionar_fuera_sistema(df)
        df = self._formatear_campos(df)
        df = self._calcular_referidos(df)
        df = self._calcular_kpis(df)

        # 7) Validar esquema con Pydantic y serializar
        try:
            modelos = [KPICalcularSchema(**rec) for rec in df.to_dict(orient="records")]
        except ValidationError as err:
            logger.error("Error validando KPI: %s", err)
            raise

        datos = [m.model_dump() for m in modelos]
        if as_df:
            # Devolver DataFrame ya validado
            return pd.DataFrame(datos)

        # Devolver lista de dicts validados
        return datos

    def _calcular_referidos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Incorpora la columna "Referencia" usando la lógica de ComisionesCalcular.calcular_referidos().
        """
        # instanciamos ComisionesCalcular con el df ya listo
        comisiones_calcular = ComisionesCalcular(df)
        return comisiones_calcular.calcular_referidos()

    def _validar_columnas_minimas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Asegura que existan las columnas mínimas esperadas."""
        required = [
            "Ejecutivo",
            "FechaOperacion",
            "NetoConfirmado",
            "Moneda",
            "MontoDesembolso",
            "DiasEfectivo",
            "ComisionEstructuracionConIGV",
            "Interes",
            "GastosDiversosConIGV",
            "RUCCliente",
            "RUCPagador",
            "RazonSocialPagador",
            "CodigoLiquidacion",
            "NroDocumento",
        ]
        missing = set(required) - set(df.columns)
        if missing:
            msg = f"Faltan columnas obligatorias: {missing}"
            logger.error(msg)
            raise ValueError(msg)
        return df.copy()

    def _fusionar_fuera_sistema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Incorpora las operaciones fuera de sistema,
        unifica nombres de ejecutivos y concatena DataFrames.
        """
        # antes de fusionar, aseguramos fechas limpias en ambos DFs
        date_cols = ["FechaOperacion", "FechaConfirmado", "FechaDesembolso"]
        df_main = df.assign(FueraSistema="no")
        df_main = self._convertir_fechas(df_main, date_cols, "%d/%m/%Y")

        df_out = self.operaciones_fuera_sistema_calcular.calcular_df().copy()
        df_out = self._convertir_fechas(df_out, date_cols, None)

        # Filtrar y preparar
        df_out = df_out.replace({"CodigoLiquidacion": {"": np.nan}}).dropna(
            subset=["CodigoLiquidacion"]
        )
        df_out["FueraSistema"] = "si"
        df_out[["GastosDiversosConIGV", "MontoPago"]] = (
            df_out[["GastosDiversosConIGV", "MontoPago"]]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0)
        )

        # Mapear nombres de ejecutivos…
        mapping = self._map_executivos(df_main, df_out)
        df_out["Ejecutivo"] = df_out["Ejecutivo"].map(mapping)

        return pd.concat([df_main, df_out], ignore_index=True)

    def _map_executivos(
        self, df_in: pd.DataFrame, df_out: pd.DataFrame, threshold: int = 75
    ) -> Dict[str, str]:
        """Crea un mapeo 'Ejecutivo fuera' → 'Ejecutivo dentro' según fuzzy match."""
        nombres_in = df_in["Ejecutivo"].str.lower().tolist()
        mapping: Dict[str, str] = {}
        for name_out in df_out["Ejecutivo"]:
            lo = name_out.lower()
            best = process.extractOne(lo, nombres_in, scorer=fuzz.partial_ratio)
            if best and best[1] >= threshold:
                matched = df_in.loc[nombres_in.index(best[0]), "Ejecutivo"]
                mapping[name_out] = matched
            else:
                mapping[name_out] = name_out.title()
        return mapping

    def _convertir_fechas(
        self, df: pd.DataFrame, columns: list[str], fmt: str | None = None
    ) -> pd.DataFrame:
        """
        Convierte cada columna de `columns` en datetime:
         - usa `fmt` si se indica
         - elimina tz, normaliza a medianoche
        """
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce")
                df[col] = df[col].dt.tz_localize(None)
                df[col] = df[col].dt.normalize()
        return df

    def _formatear_campos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convierte tipos, fechas, limpia strings y crea columnas auxiliares."""
        df = df.copy()
        # FECHAS: dd/mm/YYYY
        # df = self._convertir_fechas(
        #     df, ["FechaOperacion", "FechaConfirmado", "FechaDesembolso"], "%d/%m/%Y"
        # )

        # Strings y códigos
        df["RUCCliente"] = df["RUCCliente"].astype(str).str.strip()
        df["RUCPagador"] = (
            df["RUCPagador"].astype(str).str.replace("[: ]", "", regex=True).str.strip()
        )
        df["CodigoLiquidacion"] = (
            df["CodigoLiquidacion"]
            .astype(str)
            .str.strip()
            .str.split("-")
            .str[:2]
            .str.join("-")
        )
        df["NroDocumento"] = (
            df["NroDocumento"].astype(str).str.replace(r"\s+", "", regex=True)
        )

        # Tasas
        df["TasaNominalMensualPorc"] = df["TasaNominalMensualPorc"].apply(
            lambda x: x / 100 if x >= 1 else x
        )

        # Mes/Año
        df["Mes"] = df["FechaOperacion"].dt.strftime("%Y-%m")
        df["Año"] = df["FechaOperacion"].dt.year.astype(str)
        df["MesAño"] = df["FechaOperacion"].dt.strftime("%B-%Y")

        return df

    def _calcular_kpis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge con tipo de cambio y sector pagadores,
        calcula ingresos, costos y utilidades.
        """
        df = df.copy()
        # Merge tipo de cambio
        df = df.merge(
            self.tipo_cambio_df,
            left_on="FechaOperacion",
            right_on="TipoCambioFecha",
            how="left",
        )
        # Merge sector pagadores
        sec = self.sector_pagadores_calcular.calcular_df()
        df = df.merge(sec, on="RUCPagador", how="left")
        df["GrupoEco"] = df["GrupoEco"].fillna(df["RazonSocialPagador"])

        # Conversiones USD → PEN
        factor = np.where(df["Moneda"] == "USD", df["TipoCambioVenta"], 1)
        df["ColocacionSoles"] = df["NetoConfirmado"] * factor
        df["MontoDesembolsoSoles"] = df["MontoDesembolso"] * factor
        df["MontoPagoSoles"] = df["MontoPago"] * factor

        # Ingresos y costos
        df["Ingresos"] = (
            df["ComisionEstructuracionConIGV"] / 1.18
            + df["Interes"]
            + df["GastosDiversosConIGV"] / 1.18
        )
        df["IngresosSoles"] = df["Ingresos"] * factor

        # Costos de fondeo
        cost_rate = np.where(
            df["Moneda"] == "PEN", self.INTERESES_PEN, self.INTERESES_USD
        )
        df["CostosFondo"] = ((1 + cost_rate) ** (df["DiasEfectivo"] / 365) - 1) * df[
            "MontoDesembolso"
        ]
        df["CostosFondoSoles"] = df["CostosFondo"] * factor

        # Totales y utilidad
        df["TotalIngresos"] = df["ComisionEstructuracionConIGV"] / 1.18 + df["Interes"]
        df["TotalIngresosSoles"] = df["TotalIngresos"] * factor
        df["Utilidad"] = df["TotalIngresosSoles"] - df["CostosFondoSoles"]

        # Semana del mes
        df["MesSemana"] = (
            df["FechaOperacion"]
            .apply(self._week_of_month)
            .apply(lambda w: f"Semana {w}")
        )
        return df

    @staticmethod
    def _week_of_month(dt: datetime) -> int:
        """Calcula número de semana dentro del mes."""
        first = dt.replace(day=1)
        dom = dt.day + first.weekday()
        return math.ceil(dom / 7)
