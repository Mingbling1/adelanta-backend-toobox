from .BaseCalcular import BaseCalcular
import pandas as pd
from datetime import datetime
from config.logger import logger
from typing import Union
import asyncio
from ..schemas.RetomasCalcularSchema import RetomasCalcularSchema
from config.logger import logger


class RetomasCalcular(BaseCalcular):
    """
    Utiliza Datos procesados en calculos/KPICalcular.py
    """

    RUC_PAGADOR = "RUCPagador"
    RUC_CLIENTE = "RUCCliente"
    MONTO_DESEMBOLSO_SOLES = "MontoDesembolsoSoles"
    MONTO_PAGO_SOLES = "MontoPagoSoles"
    FECHA_OPERACION = "FechaOperacion"
    RAZON_SOCIAL_PAGADOR = "RazonSocialPagador"
    FECHA_PAGO = "FechaPago"

    def __init__(self, df: pd.DataFrame):
        super().__init__()

        self.df = df
        # Convertir todas las columnas de fecha a tz-naive
        self.df[self.FECHA_OPERACION] = pd.to_datetime(
            self.df[self.FECHA_OPERACION], utc=True
        ).dt.tz_localize(None)
        self.df[self.FECHA_PAGO] = pd.to_datetime(
            self.df[self.FECHA_PAGO], utc=True
        ).dt.tz_localize(None)

    def calcular_desembolsos(self, fecha_corte: datetime) -> pd.DataFrame:
        # Asegurarse de que fecha_corte sea tz-naive
        logger.warning(f"Fecha de corte: {fecha_corte} - Tipo: {type(fecha_corte)}")
        fecha_corte = fecha_corte.replace(tzinfo=None)

        return (
            self.df.loc[
                self.df[self.FECHA_OPERACION] >= fecha_corte,
                [
                    self.RUC_PAGADOR,
                    self.RAZON_SOCIAL_PAGADOR,
                    self.MONTO_DESEMBOLSO_SOLES,
                ],
            ]
            .groupby(by=[self.RUC_PAGADOR, self.RAZON_SOCIAL_PAGADOR])
            .agg({self.MONTO_DESEMBOLSO_SOLES: "sum"})
        )

    def calcular_cobranzas(self, fecha_corte: datetime) -> pd.DataFrame:
        # Asegurarse de que fecha_corte sea tz-naive
        fecha_corte = fecha_corte.replace(tzinfo=None)

        return (
            self.df.loc[
                self.df[self.FECHA_PAGO] >= fecha_corte,
                [
                    self.RUC_PAGADOR,
                    self.RAZON_SOCIAL_PAGADOR,
                    self.MONTO_PAGO_SOLES,
                ],
            ]
            .groupby(by=[self.RUC_PAGADOR, self.RAZON_SOCIAL_PAGADOR])
            .agg({self.MONTO_PAGO_SOLES: "sum"})
        )

    def procesar_datos(self, fecha_corte: datetime) -> pd.DataFrame:
        desembolsos_df = self.calcular_desembolsos(fecha_corte=fecha_corte)
        cobranzas_df = self.calcular_cobranzas(fecha_corte=fecha_corte)
        df = pd.concat(
            [cobranzas_df, desembolsos_df], keys=["Cobranzas", "Desembolsos"], axis=1
        ).fillna(0)
        df["PorRetomar"] = (
            df["Cobranzas", "MontoPagoSoles"]
            - df["Desembolsos", "MontoDesembolsoSoles"]
        )
        df = df.reset_index()
        df.columns = [
            "_".join(col).strip("_") if isinstance(col, tuple) else col.strip("_")
            for col in df.columns
        ]
        return df

    def validar_datos(self, data: pd.DataFrame) -> list[dict]:
        try:
            datos_validados = [
                RetomasCalcularSchema(**d).model_dump()
                for d in data.to_dict(orient="records")
            ]
            return datos_validados
        except Exception as e:
            logger.error(e)
            raise e

    def calcular_retomas(self, fecha_corte: datetime) -> list[dict]:
        datos_procesados = self.procesar_datos(fecha_corte)
        datos_validados = self.validar_datos(datos_procesados)
        return datos_validados

    async def calcular_retomas_async(
        self, fecha_corte: datetime, to_df: bool = False
    ) -> Union[list[dict], pd.DataFrame]:
        loop = asyncio.get_event_loop()
        retomas_result = await loop.run_in_executor(
            None, self.calcular_retomas, fecha_corte
        )
        if to_df:
            # Convertir la lista de diccionarios a DataFrame
            df_retomas = await loop.run_in_executor(None, pd.DataFrame, retomas_result)
            return df_retomas
        return retomas_result
