from repositories.datamart.KPIAcumuladoRepository import KPIAcumuladoRepository
from models.datamart.KPIAcumuladoModel import KPIAcumuladoModel
from fastapi import Depends
import polars as pl
from typing import Literal
from utils.decorators import create_job
from io import BytesIO
import asyncio
from services.BaseService import BaseService


class KPIAcumuladoService(BaseService[KPIAcumuladoModel]):
    def __init__(self, kpi_acumulado_repository: KPIAcumuladoRepository = Depends()):
        self.kpi_acumulado_repository = kpi_acumulado_repository

    async def get_all(
        self, limit: int = 10, offset: int = 0
    ) -> list[KPIAcumuladoModel]:
        return await self.kpi_acumulado_repository.get_all(limit, offset)

    async def create_many(self, input: list[dict]):
        await self.kpi_acumulado_repository.create_many(input)

    async def delete_all(self):
        await self.kpi_acumulado_repository.delete_all()

    @create_job(
        name="Calcular KPI Acumulado",
        description="Generar un excel con los KPI Acumulados calculados",
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
        data_dicts = await self.kpi_acumulado_repository.get_all_dicts(exclude_pk=True)

        def _build_df():
            if not data_dicts:
                return pl.DataFrame()

            # Convertir data_dicts a DataFrame con schema inferido más flexible
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
                    "TipoOperacionDetalle",
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
                    "FechaPagoCreacion",
                    "FechaPagoModificacion",
                    "DiasMora",
                    "MontoCobrarPago",
                    "MontoPago",
                    "FacturasMoraGeneradas",
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

        def _write_buffer() -> BytesIO:
            buf = BytesIO()
            if tipo.lower() == "csv":
                csv_content = df.write_csv()
                buf.write(csv_content.encode("utf-8"))
            else:
                df.write_excel(workbook=buf)
            buf.seek(0)
            return buf

        buffer = await asyncio.to_thread(_write_buffer)
        return buffer
