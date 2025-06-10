from .BaseCalcular import BaseCalcular
import pandas as pd
from io import BytesIO
import numpy as np
import asyncio


class VentasAutodetraccionesCalcular(BaseCalcular):
    def __init__(self, tipo_cambio_df: pd.DataFrame, comprobantes_df: pd.DataFrame):
        """
        :param tipo_cambio_df: DataFrame con la información de tipo de cambio.
        :param comprobantes_df: DataFrame con los comprobantes de ventas.
        """
        super().__init__()
        self.tipo_cambio_df = tipo_cambio_df
        self.comprobantes_df = comprobantes_df

    async def generar_excel_autodetraccion(self, hasta: str) -> BytesIO:
        """
        Genera el Excel con la información filtrada por el mes indicado en 'hasta'.
        Se espera un valor en formato 'YYYY-MM'.
        """
        # Copia del DataFrame de comprobantes para no alterar el original.
        df = self.comprobantes_df.copy()
        # Convertir la columna "Fecha Emisión " a datetime (día primero)
        df["Fecha Emisión "] = pd.to_datetime(df["Fecha Emisión "], dayfirst=True)
        # Filtrar únicamente por el año y mes indicados (hasta)
        df = df[df["Fecha Emisión "].dt.strftime("%Y-%m") == hasta]

        # Procesamiento similar a tu lógica:
        estado_invalido = []
        sistema = df.loc[
            ~df["Estado Doc.Tributario"].isin(estado_invalido),
            [
                "Estado Doc.Tributario",
                "Fecha Emisión ",
                "Tipo Documento",
                "Serie-Número ",
                "Ruc Cliente",
                "Cliente",
                "Op.Gravada",
                "Op. No Gravada",
                "IGV",
                "Importe Total",
                "Moneda",
            ],
        ].fillna(0)

        sistema["FUENTE"] = "Sistema"
        sistema["Op.Gravada"] = (
            sistema["Op.Gravada"].astype(str).str.replace(",", "").astype(float)
        )
        sistema["Op. No Gravada"] = (
            sistema["Op. No Gravada"].astype(str).str.replace(",", "").astype(float)
        )
        sistema = sistema.fillna(0)
        sistema["VALOR VENTA"] = sistema["Op.Gravada"] + sistema["Op. No Gravada"]
        sistema.loc[
            sistema["Tipo Documento"] == "Nota de crédito",
            ["VALOR VENTA", "Importe Total"],
        ] *= -1
        sistema.drop(columns=["Op.Gravada", "Op. No Gravada"], inplace=True)

        sistema.rename(
            columns={
                "Fecha Emisión ": "FECHA EMISION",
                "Tipo Documento": "TIPO COMPROBANTE",
                "Serie-Número ": "COMPROBANTE",
                "Ruc Cliente": "DOCUMENTO",
                "Cliente": "RAZON SOCIAL",
                "Importe Total": "IMPORTE",
                "Moneda": "MONEDA",
            },
            inplace=True,
        )

        combined_df = sistema
        combined_df["MONEDA"] = combined_df["MONEDA"].replace(
            {"Sol": "PEN", "US Dolar": "USD"}
        )
        combined_df = combined_df.sort_values(by="FECHA EMISION", ascending=True)
        combined_df["FECHA EMISION"] = combined_df["FECHA EMISION"].dt.strftime(
            "%Y-%m-%d"
        )

        combined_df = combined_df.merge(
            self.tipo_cambio_df,
            left_on="FECHA EMISION",
            right_on="TipoCambioFecha",
            how="left",
        ).drop(columns="TipoCambioFecha")

        combined_df["VALOR VENTA"] = pd.to_numeric(combined_df["VALOR VENTA"]).fillna(0)
        combined_df["IGV"] = pd.to_numeric(
            combined_df["IGV"].replace({r",": ""}, regex=True)
        ).fillna(0)

        combined_df["VALOR VENTA a soles"] = np.where(
            combined_df["MONEDA"] == "USD",
            combined_df["VALOR VENTA"] * combined_df["TipoCambioVenta"],
            combined_df["VALOR VENTA"],
        )
        combined_df["IMPORTE a soles"] = np.where(
            combined_df["MONEDA"] == "USD",
            combined_df["IMPORTE"] * combined_df["TipoCambioVenta"],
            combined_df["IMPORTE"],
        )

        condiciones = [
            (combined_df["MONEDA"] == "PEN")
            & (combined_df["TIPO COMPROBANTE"] == "Nota de crédito"),
            (combined_df["MONEDA"] == "USD")
            & (combined_df["TIPO COMPROBANTE"] != "Nota de crédito"),
            (combined_df["MONEDA"] == "USD")
            & (combined_df["TIPO COMPROBANTE"] == "Nota de crédito"),
        ]
        valores = [
            combined_df["IGV"] * -1,
            combined_df["IGV"] * combined_df["TipoCambioVenta"],
            combined_df["IGV"] * combined_df["TipoCambioVenta"] * -1,
        ]
        combined_df["IGV a soles"] = np.select(
            condiciones, valores, default=combined_df["IGV"]
        )
        combined_df["IGV"] = pd.to_numeric(combined_df["IGV"]).fillna(0)

        detraction = combined_df.copy().loc[
            (combined_df["IGV"] > 0) & (combined_df["IMPORTE a soles"] > 700)
        ]
        detraction["AUTO-DETRACTION a soles"] = detraction["IMPORTE a soles"] * 0.12

        # Generar Excel en memoria de forma asíncrona
        excel_buffer = BytesIO()
        await asyncio.to_thread(
            self._escribir_excel, combined_df, detraction, excel_buffer
        )
        excel_buffer.seek(0)
        return excel_buffer

    def _escribir_excel(
        self,
        registro_ventas: pd.DataFrame,
        autodetraccion: pd.DataFrame,
        buffer: BytesIO,
    ):
        """Método sincrónico que escribe los DataFrames en un Excel y lo guarda en el buffer."""
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            registro_ventas.to_excel(
                writer, sheet_name="Registro de ventas", index=False
            )
            autodetraccion.to_excel(writer, sheet_name="Autodetracción", index=False)
