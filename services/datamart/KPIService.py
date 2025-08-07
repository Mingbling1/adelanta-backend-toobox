from repositories.datamart.KPIRepository import KPIRepository
from models.datamart.KPIModel import KPIModel
from fastapi import Depends
import polars as pl
from typing import Literal
from utils.decorators import create_job
from io import BytesIO
import asyncio
from services.BaseService import BaseService
from xlsxwriter import Workbook



class KPIService(BaseService[KPIModel]):
    def __init__(self, kpi_repository: KPIRepository = Depends()):
        self.kpi_repository = kpi_repository

    async def get_all(self, limit: int = 10, offset: int = 0) -> list[KPIModel]:
        return await self.kpi_repository.get_all(limit, offset)

    async def create_many(self, input: list[dict]):
        await self.kpi_repository.create_many(input)

    async def delete_all(self):
        await self.kpi_repository.delete_all()

    @create_job(
        name="Calcular KPI",
        description="Generar un excel con los KPI calculados",
        expire=60 * 10 * 1,
        is_buffer=True,
        save_as=["excel", "csv"],
        capture_params=True,
    )
    async def get_all_to_file(
        self,
        tipo: Literal["excel", "csv"] = "excel",
        informe: str | None = None,
    ) -> BytesIO:
        data_dicts = await self.kpi_repository.get_all_dicts(exclude_pk=True)

        def _build_df():
            if not data_dicts:
                return pl.DataFrame()

            df = pl.DataFrame(data_dicts, infer_schema_length=None)

            if informe:
                columnas_esperadas = [
                    "CodigoLiquidacion",
                    "CodigoSolicitud",
                    "RUCCliente",
                    "RazonSocialCliente",
                    "RUCPagador",
                    "RazonSocialPagador",
                    "Moneda",
                    "DeudaAnterior",
                    "ObservacionLiquidacion",
                    "ObservacionSolicitud",
                    "FlagPagoInteresConfirming",
                    "FechaInteresConfirming",
                    "TipoOperacion",
                    "Estado",
                    "NroDocumento",
                    "TasaNominalMensualPorc",
                    "FinanciamientoPorc",
                    "FechaConfirmado",
                    "FechaOperacion",
                    "DiasEfectivo",
                    "NetoConfirmado",
                    "FondoResguardo",
                    "MontoComisionEstructuracion",
                    "ComisionEstructuracionIGV",
                    "ComisionEstructuracionConIGV",
                    "MontoCobrar",
                    "Interes",
                    "InteresConIGV",
                    "GastosContrato",
                    "GastoVigenciaPoder",
                    "ServicioCobranza",
                    "ServicioCustodia",
                    "GastosDiversosIGV",
                    "GastosDiversosConIGV",
                    "MontoTotalFacturado",
                    "MontoDesembolso",
                    "FacturasGeneradas",
                    "Ejecutivo",
                    "FechaPago",
                    "DiasMora",
                    "MontoCobrarPago",
                    "MontoPago",
                    "InteresPago",
                    "GastosPago",
                    "TipoPago",
                    "SaldoDeuda",
                    "ExcesoPago",
                    "ObservacionPago",
                    "FechaDesembolso",
                    "MontoDevolucion",
                    "DescuentoDevolucion",
                    "EstadoDevolucion",
                    "ObservacionDevolucion",
                    "Anticipo",
                    "TramoAnticipo",
                    "FueraSistema",
                    "GastosDiversosSinIGV",
                    "Total factura Mora",
                    "Mes",
                    "Año",
                    "MesAño",
                    "Sector",
                    "GrupoEco",
                    "Referencia",
                    "TipoCambioFecha",
                    "TipoCambioCompra",
                    "TipoCambioVenta",
                    "ColocacionSoles",
                    "MontoDesembolsoSoles",
                    "Ingresos",
                    "IngresosSoles",
                    "MesSemana",
                    "CostosFondo",
                    "TotalIngresos",
                    "CostosFondoSoles",
                    "TotalIngresosSoles",
                    "MontoPagoSoles",
                    "Utilidad",
                ]

                for col in columnas_esperadas:
                    if col not in df.columns:
                        df = df.with_columns(pl.lit(None).alias(col))
                df = df.select(columnas_esperadas)
            return df

        df = await asyncio.to_thread(_build_df)

        if tipo.lower() == "csv":
            csv_buffer = BytesIO()
            csv_content = df.write_csv()
            csv_buffer.write(csv_content.encode("utf-8"))
            csv_buffer.seek(0)
            return csv_buffer
        else:
            excel_buffer = BytesIO()
            await asyncio.to_thread(
                self._escribir_excel, df, excel_buffer
            )
            excel_buffer.seek(0)
            return excel_buffer

    def _escribir_excel(
        self,
        df: pl.DataFrame,
        buffer: BytesIO,
    ):
        """
        MÉTODO COMPLETAMENTE POLARS: Sin pandas, usando xlsxwriter nativo
        Polars puro con Excel
        """
        # xlsxwriter puede trabajar directamente con BytesIO
        with Workbook(buffer, {"in_memory": True}) as workbook:
            # Escribir hoja: KPI
            df.write_excel(
                workbook=workbook,
                worksheet="KPI",
                autofit=True,  # Ajustar ancho automáticamente
                include_header=True,
                autofilter=True,  # Agregar filtros automáticos
            )
