from repositories.datamart.KPIRepository import KPIRepository
from models.datamart.KPIModel import KPIModel
from fastapi import Depends
import polars as pl
from typing import Literal
from utils.decorators import create_job
from io import BytesIO
import asyncio
from services.BaseService import BaseService
from config.logger import logger


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
        logger.warning(
            f"KPI get_all_to_file iniciado - tipo: {tipo}, informe: {informe}"
        )

        data_dicts = await self.kpi_repository.get_all_dicts(exclude_pk=True)
        logger.warning(
            f"KPI datos obtenidos - registros: {len(data_dicts) if data_dicts else 0}"
        )

        def _build_df():
            if not data_dicts:
                logger.warning("KPI sin datos - retornando DataFrame vacío")
                return pl.DataFrame()

            logger.warning(f"KPI creando DataFrame con {len(data_dicts)} registros")
            df = pl.DataFrame(data_dicts, infer_schema_length=None)
            logger.warning(
                f"KPI DataFrame creado - columnas: {df.columns}, shape: {df.shape}"
            )

            if informe:
                logger.warning(f"KPI aplicando filtro informe: {informe}")
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
                logger.warning(f"KPI DataFrame filtrado - nueva shape: {df.shape}")
            return df

        logger.warning("KPI iniciando construcción de DataFrame en thread")
        df = await asyncio.to_thread(_build_df)
        logger.warning(f"KPI DataFrame final - shape: {df.shape}, tipo: {type(df)}")

        def _write_buffer() -> BytesIO:
            logger.warning(f"KPI iniciando escritura - formato: {tipo}")
            buf = BytesIO()

            try:
                if tipo.lower() == "csv":
                    logger.warning("KPI escribiendo CSV")
                    csv_content = df.write_csv()
                    buf.write(csv_content.encode("utf-8"))
                    logger.warning(
                        f"KPI CSV escrito - tamaño buffer: {buf.tell()} bytes"
                    )
                else:
                    logger.warning("KPI escribiendo Excel con df.write_excel")
                    df.write_excel(
                        autofilter=False,
                        float_precision=2,
                        workbook=buf,
                    )
                    logger.warning(
                        f"KPI Excel escrito - tamaño buffer: {buf.tell()} bytes"
                    )

                buf.seek(0)
                logger.warning("KPI buffer posicionado al inicio")
                return buf

            except Exception as e:
                logger.warning(
                    f"KPI ERROR en _write_buffer: {str(e)} - Tipo: {type(e)}"
                )
                raise

        logger.warning("KPI iniciando escritura en thread")
        buffer = await asyncio.to_thread(_write_buffer)
        logger.warning(
            f"KPI proceso completado - buffer final tamaño: {buffer.tell()} bytes"
        )
        return buffer
