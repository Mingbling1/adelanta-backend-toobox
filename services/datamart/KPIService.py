from repositories.datamart.KPIRepository import KPIRepository
from models.datamart.KPIModel import KPIModel
from fastapi import Depends
import pandas as pd 
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
        # 1) Traer todos los registros
        # records: list[KPIModel] = await self.kpi_repository.get_all(
        #     limit=None, offset=0
        # )
        # 1) Traer todos los registros como lista de dicts (sin pk)
        rows = await self.kpi_repository.get_all_dicts(exclude_pk=True)

        # 2) Construir el DataFrame en un hilo
        def _build_df():
            # df = pd.DataFrame([r.to_dict() for r in records])
            df = pd.DataFrame(rows) if rows else pd.DataFrame()
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
                        df[col] = None
                df = df[columnas_esperadas]
            return df

        df = await asyncio.to_thread(_build_df)

        # 3) Escribir el buffer en un hilo
        def _write_buffer() -> BytesIO:
            buf = BytesIO()
            if tipo.lower() == "csv":
                csv_text = df.to_csv(index=False)
                buf.write(csv_text.encode("utf-8"))
            else:
                with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                    df.to_excel(writer, index=False, sheet_name="Sheet1")
            buf.seek(0)
            return buf

        buffer = await asyncio.to_thread(_write_buffer)
        return buffer
