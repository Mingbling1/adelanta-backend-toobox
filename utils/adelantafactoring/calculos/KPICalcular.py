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
from ..schemas.KPICalcularSchema import KPICalcularSchema, KPIAcumuladoCalcularSchema


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
            # Seleccionar el esquema correcto según tipo_reporte
            if tipo_reporte == 0:
                # Para reportes acumulados (tipo_reporte = 0)
                modelos = [
                    KPIAcumuladoCalcularSchema(**rec)
                    for rec in df.to_dict(orient="records")
                ]
            else:
                # Para reportes normales (tipo_reporte = 2)
                modelos = [
                    KPICalcularSchema(**rec) for rec in df.to_dict(orient="records")
                ]
        except ValidationError as err:
            logger.error("Error validando KPI (tipo_reporte=%s): %s", tipo_reporte, err)
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
    
    @BaseCalcular.timeit
    def _fusionar_fuera_sistema(
        self, df: pd.DataFrame, date_format: str = "%d/%m/%Y"
    ) -> pd.DataFrame:
        """
        Incorpora las operaciones fuera de sistema,
        unifica nombres de ejecutivos y concatena DataFrames.
        Elimina duplicados priorizando datos 'fuera del sistema' sobre 'dentro del sistema'.
        """
        # antes de fusionar, aseguramos fechas limpias en ambos DFs
        date_cols = ["FechaOperacion", "FechaConfirmado", "FechaDesembolso"]
        df_main = df.assign(FueraSistema="no")
        df_main = self._convertir_fechas(df_main, date_cols, date_format)

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

        # NUEVA LÓGICA: Detectar y eliminar duplicados priorizando fuera del sistema
        codigos_dentro = set(df_main["CodigoLiquidacion"].dropna().astype(str))
        codigos_fuera = set(df_out["CodigoLiquidacion"].dropna().astype(str))

        # Encontrar códigos que están tanto dentro como fuera
        codigos_duplicados = codigos_dentro.intersection(codigos_fuera)

        logger.warning(f"Códigos dentro del sistema: {len(codigos_dentro)}")
        logger.warning(f"Códigos fuera del sistema: {len(codigos_fuera)}")
        logger.warning(f"Códigos duplicados encontrados: {len(codigos_duplicados)}")

        if len(codigos_duplicados) > 0:
            logger.warning(
                f"Eliminando {len(codigos_duplicados)} códigos duplicados del dataset interno"
            )

            # Eliminar de df_main los códigos que también están en df_out
            df_main = df_main[
                ~df_main["CodigoLiquidacion"].astype(str).isin(codigos_duplicados)
            ]

            logger.warning(
                f"Registros restantes en dataset interno después de limpieza: {len(df_main)}"
            )

        # Mapear nombres de ejecutivos…
        mapping = self._map_executivos(df_main, df_out)
        df_out["Ejecutivo"] = df_out["Ejecutivo"].map(mapping)

        resultado = pd.concat([df_main, df_out], ignore_index=True, sort=False)

        return resultado

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

    def _formatear_campos(
        self, df: pd.DataFrame, aplicar_formateo_fechas_legacy: bool = True
    ) -> pd.DataFrame:
        """Convierte tipos, fechas, limpia strings y crea columnas auxiliares."""
        df = df.copy()

        # FECHAS: Convertir fechas que vienen como strings del webservice
        # Este formateo es opcional para compatibilidad con CXCETLProcessor
        if aplicar_formateo_fechas_legacy:
            columnas_fecha = ["FechaOperacion", "FechaConfirmado", "FechaDesembolso"]
            for col in columnas_fecha:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                    if df[col].dt.tz is not None:
                        df[col] = df[col].dt.tz_localize(None)
                    df[col] = df[col].dt.normalize()

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

        # Tasas - CORREGIDO para manejar strings del webservice
        df["TasaNominalMensualPorc"] = pd.to_numeric(
            df["TasaNominalMensualPorc"], errors="coerce"
        ).fillna(0)
        df["TasaNominalMensualPorc"] = df["TasaNominalMensualPorc"].apply(
            lambda x: x / 100 if x >= 1 else x
        )

        # Mes/Año - Solo si FechaOperacion existe y no es null
        if "FechaOperacion" in df.columns and not df["FechaOperacion"].isna().all():
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

        # CONVERTIR COLUMNAS NUMÉRICAS QUE VIENEN COMO STRINGS DEL WEBSERVICE
        columnas_numericas = [
            "NetoConfirmado",
            "MontoDesembolso",
            "MontoPago",
            "ComisionEstructuracionConIGV",
            "Interes",
            "GastosDiversosConIGV",
            "DiasEfectivo",
            "TipoCambioVenta",
        ]
        for col in columnas_numericas:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # CORREGIR TIPOS DE FECHA PARA MERGE - Convertir TipoCambioFecha a datetime
        tipo_cambio_df_temp = self.tipo_cambio_df.copy()
        if "TipoCambioFecha" in tipo_cambio_df_temp.columns:
            tipo_cambio_df_temp["TipoCambioFecha"] = pd.to_datetime(
                tipo_cambio_df_temp["TipoCambioFecha"], errors="coerce"
            )

        # Convertir columnas numéricas del tipo_cambio_df también
        if "TipoCambioVenta" in tipo_cambio_df_temp.columns:
            tipo_cambio_df_temp["TipoCambioVenta"] = pd.to_numeric(
                tipo_cambio_df_temp["TipoCambioVenta"], errors="coerce"
            ).fillna(
                1
            )  # Default 1 para evitar divisiones por 0
        if "TipoCambioCompra" in tipo_cambio_df_temp.columns:
            tipo_cambio_df_temp["TipoCambioCompra"] = pd.to_numeric(
                tipo_cambio_df_temp["TipoCambioCompra"], errors="coerce"
            ).fillna(1)

        # Merge tipo de cambio
        df = df.merge(
            tipo_cambio_df_temp,
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

        # Semana del mes - Solo si hay fechas válidas
        if "FechaOperacion" in df.columns and not df["FechaOperacion"].isna().all():
            df["MesSemana"] = (
                df["FechaOperacion"]
                .apply(self._week_of_month)
                .apply(lambda w: f"Semana {w}")
            )
        else:
            # Si no hay fechas válidas, asignar semana por defecto
            df["MesSemana"] = "Semana 1"
        return df

    @staticmethod
    def _week_of_month(dt) -> int:
        """Calcula número de semana dentro del mes. Maneja valores NaT/None."""
        # Manejar valores nulos o NaT
        if pd.isna(dt) or dt is None:
            return 1  # Default a semana 1 si la fecha es nula

        # Convertir a datetime si es necesario
        if isinstance(dt, str):
            try:
                dt = pd.to_datetime(dt)
            except Exception:
                return 1

        try:
            first = dt.replace(day=1)
            dom = dt.day + first.weekday()
            return math.ceil(dom / 7)
        except (AttributeError, ValueError, TypeError):
            return 1  # Default a semana 1 si hay cualquier error
