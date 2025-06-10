import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from dateutil.relativedelta import relativedelta
import locale
from ..BaseCalcular import BaseCalcular

locale.setlocale(locale.LC_TIME, "es_ES.UTF-8")


class DiferidoInternoCalcular(BaseCalcular):
    LISTA_COLUMNAS = [
        "CodigoLiquidacion",
        "NroDocumento",
        "FechaOperacion",
        "FechaConfirmado",
        "Moneda",
        "Interes",
        "DiasEfectivo",
    ]
    DIAS_EFECTIVO = "DiasEfectivo"
    INTERES = "Interes"
    FECHA_PAGO_CONFIRMADO = "FechaConfirmado"
    FECHA_OPERACION = "FechaOperacion"

    def __init__(self, df: pd.DataFrame) -> None:
        super().__init__()
        self.df = df  # cargar el DataFrame desde un archivo Excel o CSV

    def calcular_monto_por_mes(self, row, mes_inicio, mes_fin):
        dias_total = row[self.DIAS_EFECTIVO]
        monto_total = row[self.INTERES]

        if dias_total <= 0 or np.isnan(dias_total):
            return 0

        if (
            mes_inicio <= row[self.FECHA_PAGO_CONFIRMADO]
            and mes_fin >= row[self.FECHA_OPERACION]
        ):
            mes_inicio_ajustado = max(mes_inicio, row[self.FECHA_OPERACION])
            mes_fin_ajustado = min(mes_fin, row[self.FECHA_PAGO_CONFIRMADO])

            dias_en_mes = (mes_fin_ajustado - mes_inicio_ajustado).days + 1

            porcentaje_dias_mes = dias_en_mes / dias_total

            if porcentaje_dias_mes > 0:
                monto_mes = monto_total * porcentaje_dias_mes
                return monto_mes
            else:
                return 0
        else:
            return 0

    def last_day_of_month(self, date: datetime):
        if date.month == 12:
            next_month = date.replace(year=date.year + 1, month=1, day=1)
        else:
            next_month = date.replace(day=1, month=date.month + 1)
        return next_month - timedelta(days=1)

    def put_dates_in_columns(
        self,
        df: pd.DataFrame,
        fecha_min: datetime,
        fecha_max: datetime,
    ) -> pd.DataFrame:
        fecha_actual = fecha_min
        while fecha_actual <= fecha_max:
            mes_inicio = fecha_actual.replace(day=1)
            mes_fin = self.last_day_of_month(mes_inicio)
            col_name = mes_inicio.strftime("%B-%Y")

            df[col_name] = df.apply(
                lambda row: self.calcular_monto_por_mes(row, mes_inicio, mes_fin),
                axis=1,
            )

            # Avanzar al siguiente mes
            fecha_actual += relativedelta(months=1)
        return df

    def calcular_diferido_interno(self, hasta: str) -> pd.DataFrame:
        """
        Calcula el diferido interno considerando sólo las filas con FechaOperacion
        hasta el último día del mes indicado.

        Parámetros:
            hasta (str): Cadena obligatoria en formato "YYYY-MM". Se utiliza para filtrar
                        el DataFrame hasta el último día del mes especificado.

        Retorna:
            pd.DataFrame: DataFrame con columnas de cada mes calculadas hasta la fecha límite.
        """
        if not hasta:
            raise ValueError(
                "El parámetro 'hasta' es obligatorio y debe tener el formato 'YYYY-MM'."
            )

        df = self.df.copy()
        df = df.loc[:, self.LISTA_COLUMNAS]
        fecha_min = df[self.FECHA_OPERACION].min()
        fecha_max = df[self.FECHA_PAGO_CONFIRMADO].max()
        hasta_date = datetime.strptime(hasta, "%Y-%m")
        hasta_date = self.last_day_of_month(hasta_date)
        df = df[df[self.FECHA_OPERACION] <= hasta_date]
        df = self.put_dates_in_columns(df, fecha_min, fecha_max)
        return df

    def obtener_resumen(self) -> pd.DataFrame:
        calculos_df = self.calcular_diferido()
        resumen_df = pd.DataFrame(columns=["Mes", "PEN", "USD"])
        date_range = pd.date_range(
            self.df[self.FECHA_OPERACION].min(),
            self.df[self.FECHA_PAGO_CONFIRMADO].max(),
            freq="ME",
        )
        for date in date_range:
            col_name = date.strftime("%B-%Y")
            if col_name in calculos_df.columns:
                # Filtrar filas donde FechaOperacion corresponde al mes del cálculo
                # mes_operacion = date.strftime("%Y-%m")
                filtered_df = calculos_df[calculos_df[self.FECHA_OPERACION] <= date]

                # Sumar todas las columnas desde el mes siguiente hasta el último mes
                suma_pen = 0
                suma_usd = 0
                for next_date in date_range[date_range > date]:
                    next_col_name = next_date.strftime("%B-%Y")
                    if next_col_name in filtered_df.columns:
                        suma_pen += filtered_df[filtered_df["Moneda"] == "PEN"][
                            next_col_name
                        ].sum()
                        suma_usd += filtered_df[filtered_df["Moneda"] == "USD"][
                            next_col_name
                        ].sum()
                resumen_df = pd.concat(
                    [
                        resumen_df,
                        pd.DataFrame(
                            {"Mes": [col_name], "PEN": [suma_pen], "USD": [suma_usd]}
                        ),
                    ],
                    ignore_index=True,
                )
        return resumen_df
