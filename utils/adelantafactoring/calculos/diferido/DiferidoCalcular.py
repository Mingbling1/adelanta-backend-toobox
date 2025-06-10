from .DiferidoExternoCalcular import DiferidoExternoCalcular
from .DiferidoInternoCalcular import DiferidoInternoCalcular
import pandas as pd
import re
from ..BaseCalcular import BaseCalcular
import asyncio


class DiferidoCalcular(BaseCalcular):
    def __init__(self, file_path_externo: str, df_interno: pd.DataFrame):
        super().__init__()
        self.diferido_externo = DiferidoExternoCalcular(file_path_externo)
        self.diferido_interno = DiferidoInternoCalcular(df=df_interno)

    async def reorder_date_columns_async(self, date_cols: list[str]) -> list[str]:
        # Ejecuta la función sincrónica en un hilo separado
        return await asyncio.to_thread(self.reorder_date_columns, date_cols)

    @BaseCalcular.timeit
    def reorder_date_columns(self, date_cols: list[str]) -> list[str]:
        """
        Recibe una lista de columnas de fecha ya convertidas con formato 'mes-YYYY'
        y las retorna ordenadas cronológicamente.
        """

        def sort_key(col: str):
            try:
                # separa por guión: ej: "enero-2023"
                month_str, year_str = col.split("-")
                year = int(year_str)
                months_order = [
                    "enero",
                    "febrero",
                    "marzo",
                    "abril",
                    "mayo",
                    "junio",
                    "julio",
                    "agosto",
                    "septiembre",
                    "octubre",
                    "noviembre",
                    "diciembre",
                ]
                month_idx = (
                    months_order.index(month_str) if month_str in months_order else 99
                )
                return (year, month_idx)
            except Exception:
                return (9999, 99)

        return sorted(date_cols, key=sort_key)

    async def comparar_diferidos_async(self, df, calculado_df):
        # Ejecuta la función comparar_diferidos en un hilo separado
        return await asyncio.to_thread(self.comparar_diferidos, df, calculado_df)

    @BaseCalcular.timeit
    def comparar_diferidos(self, df, calculado_df):
        # Identificar las columnas de fecha (con el patrón "Mes-YYYY")
        date_cols_externo = set(
            col for col in df.columns if re.match(r"^[A-Za-z]+-\d{4}$", col)
        )
        date_cols_calculado = set(
            col for col in calculado_df.columns if re.match(r"^[A-Za-z]+-\d{4}$", col)
        )
        # Unión de ambas y ordenar cronológicamente
        union_date_cols = list(date_cols_externo.union(date_cols_calculado))
        date_columns = self.reorder_date_columns(union_date_cols)

        # Reordenamos las columnas fijas deseadas (nota: "CodigoLiquidacion" y "Moneda" son índices)
        # Queremos fijar el orden: NroDocumento, Interes, FechaOperacion, FechaConfirmado, DiasEfectivo
        fixed_cols = [
            "CodigoLiquidacion",
            "NroDocumento",
            "Interes",
            "FechaOperacion",
            "FechaConfirmado",
            "Moneda",
            "DiasEfectivo",
        ]
        # Reindexar ambos DataFrames para tener los fijos y las columnas de fechas en el mismo orden.
        df = df.reindex(columns=fixed_cols + date_columns, fill_value=0)
        calculado_df = calculado_df.reindex(
            columns=fixed_cols + date_columns, fill_value=0
        )

        # Definir las columnas numéricas a agrupar (todo excepto las de identificación y las fechas)
        numeric_cols = [
            col
            for col in df.columns
            if col
            not in ["CodigoLiquidacion", "Moneda", "FechaOperacion", "FechaConfirmado"]
        ]

        # Agrupación numérica: suma (solo se suman columnas numéricas)
        df_numeric_grouped = df.groupby(["CodigoLiquidacion", "Moneda"])[
            numeric_cols
        ].sum(numeric_only=True)
        calc_numeric_grouped = calculado_df.groupby(["CodigoLiquidacion", "Moneda"])[
            numeric_cols
        ].sum(numeric_only=True)

        # Agrupación de las columnas fijas no numéricas: usamos "last"
        df_fixed_grouped = df.groupby(["CodigoLiquidacion", "Moneda"])[
            ["NroDocumento", "FechaOperacion", "FechaConfirmado"]
        ].agg("last")
        calc_fixed_grouped = calculado_df.groupby(["CodigoLiquidacion", "Moneda"])[
            ["NroDocumento", "FechaOperacion", "FechaConfirmado"]
        ].agg("last")

        # Agrupación de las columnas que no estuvieron en la fija y que se quieran mantener (por ejemplo, DiasEfectivo e Interes)
        # Notar que "Interes" y "DiasEfectivo" aparecen en la suma; de allí se tomarán sus valores sumados.
        # Unir ambas agregaciones:
        df_grouped = df_numeric_grouped.join(df_fixed_grouped)
        calc_grouped = calc_numeric_grouped.join(calc_fixed_grouped)

        # Combinar en un único DataFrame usando keys "Externo" y "Calculado"
        sums = pd.concat(
            [df_grouped, calc_grouped], axis=1, keys=["Externo", "Calculado"]
        )

        # Calcular diferencias para cada columna de fechas (de meses)
        for col in date_columns:
            sums[("Diferencia_Monto", col)] = (
                sums[("Externo", col)] - sums[("Calculado", col)]
            )

        diff_cols = [("Diferencia_Monto", col) for col in date_columns]
        sums["Diferencia_Significativa"] = sums[diff_cols].abs().ge(1).any(axis=1)

        # Reordenar las columnas en cada nivel para que en "Externo" y "Calculado" aparezcan primero
        # las columnas fijas ordenadas según lo deseado, seguidas de las columnas de fechas.
        fijo_orden = [
            "NroDocumento",
            "Interes",
            "FechaOperacion",
            "FechaConfirmado",
            "DiasEfectivo",
        ]

        def reordenar_level(key):
            # Para un nivel dado (Externo o Calculado), seleccionar columnas fijas según el orden definido
            fixed_level = [(key, col) for col in fijo_orden]
            date_level = [(key, col) for col in date_columns]
            return fixed_level + date_level

        nuevos_cols = (
            reordenar_level("Externo")
            + reordenar_level("Calculado")
            + [("Diferencia_Monto", col) for col in date_columns]
            + [("Diferencia_Significativa", "")]
        )
        sums = sums[nuevos_cols]
        return sums

    def calcular_diferido(self, hasta: str) -> pd.DataFrame:
        if not hasta or not re.match(r"^\d{4}-\d{2}$", hasta):
            raise ValueError(
                "El parámetro 'hasta' es obligatorio y debe tener el formato 'YYYY-MM'."
            )
        # Calcular el diferido interno
        diferido_interno_df = self.diferido_interno.calcular_diferido_interno(
            hasta=hasta
        )
        # Calcular el diferido externo
        diferido_externo_df = self.diferido_externo.calcular_diferido_externo(
            hasta=hasta
        )
        return self.comparar_diferidos(diferido_externo_df, diferido_interno_df)

    async def calcular_diferido_async(self, hasta: str) -> pd.DataFrame:
        if not hasta or not re.match(r"^\d{4}-\d{2}$", hasta):
            raise ValueError(
                "El parámetro 'hasta' es obligatorio y debe tener el formato 'YYYY-MM'."
            )
        # Ejecutar métodos pesados en hilos separados
        diferido_interno_df = await asyncio.to_thread(
            self.diferido_interno.calcular_diferido_interno, hasta
        )
        diferido_externo_df = await asyncio.to_thread(
            self.diferido_externo.calcular_diferido_externo, hasta
        )
        resultado = await self.comparar_diferidos_async(
            diferido_externo_df, diferido_interno_df
        )
        return resultado
