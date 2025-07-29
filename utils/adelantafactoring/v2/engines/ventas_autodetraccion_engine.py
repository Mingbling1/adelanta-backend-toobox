"""
⚙️ Engine V2 - Motor de cálculo para autodetracción de ventas
"""

import pandas as pd
import numpy as np
from io import BytesIO
import asyncio

from ..schemas.ventas_autodetraccion import (
    VentasAutodetraccionesRequestSchema,
    VentasAutodetraccionesResponseSchema,
    VentasAutodetraccionesRequest,
    VentasAutodetraccionesResult,
)


class VentasAutodetraccionesEngine:
    """Motor principal para cálculo de autodetracción de ventas"""

    def __init__(self):
        self.estado_invalido = []  # Estados de documentos inválidos

    async def calculate(
        self, request: VentasAutodetraccionesRequest
    ) -> VentasAutodetraccionesResult:
        """
        Calcula la autodetracción de ventas basado en los parámetros del request

        Args:
            request: Request con DataFrame de comprobantes, tipo de cambio y período

        Returns:
            VentasAutodetraccionesResult con el Excel generado y estadísticas
        """
        # Procesar el DataFrame de ventas
        registro_ventas_df = await self._procesar_registro_ventas(
            request.comprobantes_df, request.tipo_cambio_df, request.hasta
        )

        # Calcular autodetracción
        autodetraccion_df = self._calcular_autodetraccion(registro_ventas_df)

        # Generar Excel
        excel_buffer = await self._generar_excel(registro_ventas_df, autodetraccion_df)

        # Calcular estadísticas
        total_autodetraccion = (
            autodetraccion_df["AUTO-DETRACTION a soles"].sum()
            if not autodetraccion_df.empty
            else 0.0
        )

        return VentasAutodetraccionesResult(
            excel_buffer=excel_buffer,
            registro_ventas_count=len(registro_ventas_df),
            autodetraccion_count=len(autodetraccion_df),
            total_autodetraccion=float(total_autodetraccion),
            hasta=request.hasta,
        )

    async def _procesar_registro_ventas(
        self, comprobantes_df: pd.DataFrame, tipo_cambio_df: pd.DataFrame, hasta: str
    ) -> pd.DataFrame:
        """Procesa los comprobantes para generar el registro de ventas"""

        # Copia del DataFrame para no alterar el original
        df = comprobantes_df.copy()

        # Convertir fecha a datetime
        df["Fecha Emisión "] = pd.to_datetime(df["Fecha Emisión "], dayfirst=True)

        # Filtrar por período
        df = df[df["Fecha Emisión "].dt.strftime("%Y-%m") == hasta]

        # Filtrar registros válidos y seleccionar columnas necesarias
        sistema = df.loc[
            ~df["Estado Doc.Tributario"].isin(self.estado_invalido),
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

        # Agregar columna fuente
        sistema["FUENTE"] = "Sistema"

        # Limpiar y convertir campos numéricos
        sistema["Op.Gravada"] = (
            sistema["Op.Gravada"].astype(str).str.replace(",", "").astype(float)
        )
        sistema["Op. No Gravada"] = (
            sistema["Op. No Gravada"].astype(str).str.replace(",", "").astype(float)
        )
        sistema = sistema.fillna(0)

        # Calcular valor de venta
        sistema["VALOR VENTA"] = sistema["Op.Gravada"] + sistema["Op. No Gravada"]

        # Aplicar signo negativo a notas de crédito
        sistema.loc[
            sistema["Tipo Documento"] == "Nota de crédito",
            ["VALOR VENTA", "Importe Total"],
        ] *= -1

        # Eliminar columnas temporales
        sistema.drop(columns=["Op.Gravada", "Op. No Gravada"], inplace=True)

        # Renombrar columnas
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

        # Normalizar monedas
        sistema["MONEDA"] = sistema["MONEDA"].replace({"Sol": "PEN", "US Dolar": "USD"})

        # Ordenar por fecha
        sistema = sistema.sort_values(by="FECHA EMISION", ascending=True)
        sistema["FECHA EMISION"] = sistema["FECHA EMISION"].dt.strftime("%Y-%m-%d")

        # Hacer join con tipo de cambio
        combined_df = sistema.merge(
            tipo_cambio_df,
            left_on="FECHA EMISION",
            right_on="TipoCambioFecha",
            how="left",
        ).drop(columns="TipoCambioFecha")

        # Limpiar y convertir campos numéricos finales
        combined_df["VALOR VENTA"] = pd.to_numeric(combined_df["VALOR VENTA"]).fillna(0)
        combined_df["IGV"] = pd.to_numeric(
            combined_df["IGV"].replace({r",": ""}, regex=True)
        ).fillna(0)

        # Calcular valores en soles
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

        # Calcular IGV en soles con condiciones
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

        return combined_df

    def _calcular_autodetraccion(
        self, registro_ventas_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Calcula los registros que requieren autodetracción"""

        # Filtrar registros que requieren autodetracción
        # IGV > 0 e IMPORTE a soles > 700
        detraction = registro_ventas_df.copy().loc[
            (registro_ventas_df["IGV"] > 0)
            & (registro_ventas_df["IMPORTE a soles"] > 700)
        ]

        # Calcular autodetracción (12% del importe)
        detraction["AUTO-DETRACTION a soles"] = detraction["IMPORTE a soles"] * 0.12

        return detraction

    async def _generar_excel(
        self, registro_ventas_df: pd.DataFrame, autodetraccion_df: pd.DataFrame
    ) -> BytesIO:
        """Genera el archivo Excel con los resultados"""

        excel_buffer = BytesIO()
        await asyncio.to_thread(
            self._escribir_excel, registro_ventas_df, autodetraccion_df, excel_buffer
        )
        excel_buffer.seek(0)
        return excel_buffer

    def _escribir_excel(
        self,
        registro_ventas: pd.DataFrame,
        autodetraccion: pd.DataFrame,
        buffer: BytesIO,
    ):
        """Método sincrónico que escribe los DataFrames en un Excel"""
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            registro_ventas.to_excel(
                writer, sheet_name="Registro de ventas", index=False
            )
            autodetraccion.to_excel(writer, sheet_name="Autodetracción", index=False)
