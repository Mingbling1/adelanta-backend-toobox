from typing import Dict, Tuple, List
import pandas as pd
import numpy as np
from datetime import datetime
from config.logger import logger
from utils.adelantafactoring.calculos.KPICalcular import KPICalcular
from ..schemas.CXCAcumuladoDIMCalcularSchema import CXCAcumuladoDIMCalcularSchema
from ..schemas.CXCPagosFactCalcularSchema import CXCPagosFactCalcularSchema
from ..schemas.CXCDevFactCalcularSchema import CXCDevFactCalcularSchema
from utils.adelantafactoring.calculos.CXCAcumuladoDIMCalcular import (
    CXCAcumuladoDIMCalcular,
)
from utils.adelantafactoring.calculos.CXCPagosFactCalcular import CXCPagosFactCalcular
from utils.adelantafactoring.calculos.CXCDevFactCalcular import CXCDevFactCalcular
from utils.adelantafactoring.calculos.SectorPagadoresCalcular import (
    SectorPagadoresCalcular,
)
from .BaseCalcular import BaseCalcular


class CXCETLProcessor(BaseCalcular):
    """🚀 PROCESADOR ETL COMPLETO CXC - ULTRA EFICIENTE CON PYDANTIC RUST"""

    # === CONSTANTES PARA ETL POWER BI ===
    # Códigos de liquidación para clasificaciones especiales (copiadas de CXCAcumuladoDIMCalcular)
    CODIGOS_MORA_MAYO = [
        "LIQ002-2021",
        "LIQ010-2022",
        "LIQ095-2022",
        "LIQ122-2022",
        "LIQ147-2022",
        "LIQ149-2022",
        "LIQ188-2022",
        "LIQ213-2022",
        "LIQ2211000149",
        "LIQ221-2022",
        "LIQ2302000043",
        "LIQ2302000044",
        "LIQ2303000070",
        "LIQ2303000082",
        "LIQ2303000129",
        "LIQ2303000144",
        "LIQ2304000013",
        "LIQ2304000031",
        "LIQ2304000107",
        "LIQ2304000117",
        "LIQ2304000123",
        "LIQ2306000105",
        "LIQ2307000164",
        "LIQ2308000014",
        "LIQ2308000077",
        "LIQ2308000126",
        "LIQ2308000137",
        "LIQ2308000139",
        "LIQ2308000189",
        "LIQ2310000033",
        "LIQ2310000036",
        "LIQ2310000072",
        "LIQ2310000082",
        "LIQ2310000093",
        "LIQ2310000164",
        "LIQ2310000186",
        "LIQ2310000192",
        "LIQ2310000193",
        "LIQ2311000129",
        "LIQ2311000130",
        "LIQ2311000131",
        "LIQ2311000133",
        "LIQ2311000134",
        "LIQ2311000233",
        "LIQ2312000022",
        "LIQ2312000097",
        "LIQ2312000135",
        "LIQ2312000144",
        "LIQ2312000145",
        "LIQ2312000146",
        "LIQ2312000154",
        "LIQ2312000183",
        "LIQ2312000197",
        "LIQ2401000066",
        "LIQ2401000125",
        "LIQ2401000126",
        "LIQ2401000132",
        "LIQ2401000133",
        "LIQ2401000161",
        "LIQ2401000163",
        "LIQ2401000164",
        "LIQ2402000088",
        "LIQ2402000112",
        "LIQ2403000197",
        "LIQ2404000017",
        "LIQ2404000030",
        "LIQ2404000125",
        "LIQ2404000156",
        "LIQ385-2022",
        "LIQ434-2021",
        "LIQ526-2021",
        "LIQ557-2021",
        "LIQ583-2021",
        "LIQ601-2021",
        "LIQ662-2021",
        "LIQ701-2021",
        "LIQ003-2022 ME",
        "LIQ014-2022 ME",
        "LIQ088-2021 ME",
        "LIQ128-2021 ME",
        "LIQ189-2022 ME",
        "LIQ199-2022 ME",
        "LIQ214-2021 ME",
        "LIQ2209000088",
        "LIQ2211000078",
        "LIQ2303000131",
        "LIQ2303000157",
        "LIQ2304000075",
        "LIQ2304000081",
        "LIQ2304000158",
        "LIQ2304000173",
        "LIQ2306000039",
        "LIQ2310000047",
        "LIQ2310000180",
        "LIQ2311000132",
        "LIQ2311000237",
        "LIQ2312000147",
        "LIQ2312000148",
        "LIQ2312000150",
        "LIQ2312000213",
        "LIQ2401000056",
        "LIQ2401000127",
        "LIQ2401000162",
        "LIQ2401000210",
        "LIQ2402000184",
        "LIQ2403000037",
        "LIQ2403000107",
        "LIQ2403000128",
        "LIQ2403000153",
        "LIQ2403000176",
    ]

    CODIGOS_COBRANZA_ESPECIAL = [
        "LIQ2302000034",
        "LIQ2309000157",
        "LIQ2307000196",
        "LIQ314-2021",
        "LIQ043-2021 ME",
        "LIQ023-2020 ME",
        "LIQ2401000124",
        "LIQ2305000186",
        "LIQ297-2021",
        "LIQ034-2021 ME",
        "LIQ248-2021 ME",
        "LIQ127-2022",
        "LIQ432-2021",
        "LIQ138-2021 ME",
        "LIQ451-2022",
        "LIQ2401000099",
        "LIQ2312000022",
        "LIQ2301000063",
        "LIQ2310000055",
        "LIQ2307000122",
        "LIQ2302000142",
        "LIQ2403000021",
        "LIQ2405000159",
        "LIQ2405000095",
        " LIQ2302000033",
        "LIQ2307000195",
        "LIQ2307000211",
        "LIQ313-2021-2",
        "LIQ2401000064",
        "LIQ2305000140",
        "LIQ296-2021",
        "LIQ251-2021 ME",
        "LIQ108-2022",
        "LIQ336-2021",
        "LIQ093-2021 ME",
        "LIQ2312000071",
        "LIQ2312000097",
        "LIQ2212000020",
        "LIQ2309000172",
        "LIQ2403000038",
        "LIQ2405000158",
        "LIQ2211000152",
        "LIQ2308000058",
        "LIQ2401000058",
        "LIQ057-2021",
        "LIQ254-2021 ME",
    ]

    def __init__(self, tipo_cambio_df: pd.DataFrame):
        super().__init__()
        self.tipo_cambio_df = tipo_cambio_df
        self.kpi_calculator = KPICalcular(tipo_cambio_df)
        self.sector_sector_pagadores_calcular = SectorPagadoresCalcular()
        # Fecha de corte para ETL Power BI (fecha actual por defecto)
        self.fecha_corte = datetime.now().date()

    async def procesar_todo_cxc(self) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        try:
            acumulado_calculador = CXCAcumuladoDIMCalcular()
            data_acumulado_raw = (
                await acumulado_calculador.cxc_acumulado_dim_obtener.obtener_acumulado_dim()
            )
            df_acumulado_raw = (
                pd.DataFrame(data_acumulado_raw)
                if data_acumulado_raw
                else pd.DataFrame()
            )

            pagos_calculador = CXCPagosFactCalcular()
            data_pagos_raw = (
                await pagos_calculador.cxc_pagos_fact_obtener.obtener_pagos_facturas()
            )
            df_pagos_raw = (
                pd.DataFrame(data_pagos_raw) if data_pagos_raw else pd.DataFrame()
            )

            dev_calculador = CXCDevFactCalcular()
            data_dev_raw = (
                await dev_calculador.cxc_dev_fact_obtener.obtener_devoluciones_facturas()
            )
            df_dev_raw = pd.DataFrame(data_dev_raw) if data_dev_raw else pd.DataFrame()

            df_acumulado_procesado = (
                await self._procesar_acumulado_dim_con_etl_kpi_exacto(
                    df_acumulado_raw, df_pagos_raw, df_dev_raw
                )
            )

            df_pagos_procesado = self._procesar_pagos_fact(df_pagos_raw)
            df_dev_procesado = self._procesar_dev_fact(df_dev_raw)

            (
                df_acumulado_procesado,
                df_pagos_procesado_fuera_sistema,
                df_dev_procesado_fuera_sistema,
            ) = await self._procesar_operaciones_fuera_sistema(df_acumulado_procesado)

            df_acumulado_procesado.to_excel("cxc_acumulado_procesado.xlsx", index=False)

            # df_pagos_procesado_fuera_sistema.to_excel(
            #     "cxc_pagos_procesado_fuera_sistema.xlsx", index=False
            # )
            # df_dev_procesado_fuera_sistema.to_excel(
            #     "cxc_dev_procesado_fuera_sistema.xlsx", index=False
            # )

            if not df_pagos_procesado_fuera_sistema.empty:
                df_pagos_procesado = pd.concat(
                    [df_pagos_procesado, df_pagos_procesado_fuera_sistema],
                    ignore_index=True,
                )

            if not df_dev_procesado_fuera_sistema.empty:
                df_dev_procesado = pd.concat(
                    [df_dev_procesado, df_dev_procesado_fuera_sistema],
                    ignore_index=True,
                )

            acumulado_final = await self._validar_con_pydantic_acumulado(
                df_acumulado_procesado
            )
            pagos_final = self._validar_con_pydantic_pagos(df_pagos_procesado)
            dev_final = self._validar_con_pydantic_dev(df_dev_procesado)

            return acumulado_final, pagos_final, dev_final

        except Exception as e:
            logger.error(f"❌ Error en procesamiento CXC: {e}")
            raise

    async def _procesar_acumulado_dim_con_etl_kpi_exacto(
        self, df_acumulado: pd.DataFrame, df_pagos: pd.DataFrame, df_dev: pd.DataFrame
    ) -> pd.DataFrame:
        """🎯 Procesa CXCAcumuladoDIM aplicando ETL KPI exacto"""
        try:
            df = self._fusionar_datos_cxc(df_acumulado, df_pagos, df_dev)
            df = self._validar_columnas_minimas(df)
            df = await self._fusionar_fuera_sistema(df)

            df = self._formatear_campos(df)
            df = self._calcular_referidos(df)
            df = self._calcular_kpis(df)
            df = self._aplicar_etl_power_bi_cxc(df, df_pagos)
            return df
        except Exception as e:
            logger.error(f"❌ Error procesando CXCAcumuladoDIM: {e}")
            raise

    def _validar_columnas_minimas(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.kpi_calculator._validar_columnas_minimas(df)

    async def _fusionar_fuera_sistema(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.kpi_calculator._fusionar_fuera_sistema(df, date_format="%Y-%m-%d")

    def _formatear_campos(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.kpi_calculator._formatear_campos(
            df, aplicar_formateo_fechas_legacy=False
        )

    def _calcular_referidos(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.kpi_calculator._calcular_referidos(df)

    def _calcular_kpis(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.kpi_calculator._calcular_kpis(df)

    def _procesar_pagos_fact(self, df_pagos: pd.DataFrame) -> pd.DataFrame:
        if df_pagos.empty:
            return pd.DataFrame()

        df = df_pagos.fillna(
            {
                "MontoPago": 0.0,
                "InteresPago": 0.0,
                "GastosPago": 0.0,
                "ExcesoPago": 0.0,
                "SaldoDeuda": 0.0,
                "DiasMora": 0,
                "TipoPago": "",
                "ObservacionPago": "",
            }
        )
        return df

    def _procesar_dev_fact(self, df_dev: pd.DataFrame) -> pd.DataFrame:
        if df_dev.empty:
            return pd.DataFrame()

        df = df_dev.fillna(
            {
                "MontoDevolucion": 0.0,
                "DescuentoDevolucion": 0.0,
                "EstadoDevolucion": 0,
                "ObservacionDevolucion": "",
            }
        )
        return df

    async def _validar_con_pydantic_acumulado(self, df: pd.DataFrame) -> List[Dict]:
        if df.empty:
            return []

        try:
            schema_fields = set(CXCAcumuladoDIMCalcularSchema.model_fields.keys())
            available_columns = set(df.columns)
            valid_columns = schema_fields.intersection(available_columns)

            df_filtered = df[list(valid_columns)].copy()
            data = df_filtered.to_dict("records")

            datos_validados = [
                CXCAcumuladoDIMCalcularSchema(**registro).model_dump(
                    exclude={"IdLiquidacionPago", "IdLiquidacionDevolucion"}
                )
                for registro in data
            ]

            return datos_validados

        except Exception as e:
            logger.error(f"❌ Error en validación de datos financieros: {e}")
            raise e

    def _validar_con_pydantic_pagos(self, df: pd.DataFrame) -> List[Dict]:
        if df.empty:
            return []

        try:
            schema_fields = set(CXCPagosFactCalcularSchema.model_fields.keys())
            available_columns = set(df.columns)
            valid_columns = schema_fields.intersection(available_columns)

            df_filtered = df[list(valid_columns)].copy()
            data = df_filtered.to_dict("records")

            datos_validados = [
                CXCPagosFactCalcularSchema(**registro).model_dump() for registro in data
            ]

            return datos_validados

        except Exception as e:
            logger.error(f"❌ Error en validación Pagos: {e}")
            raise

    def _validar_con_pydantic_dev(self, df: pd.DataFrame) -> List[Dict]:
        if df.empty:
            return []

        try:
            schema_fields = set(CXCDevFactCalcularSchema.model_fields.keys())
            available_columns = set(df.columns)
            valid_columns = schema_fields.intersection(available_columns)

            df_filtered = df[list(valid_columns)].copy()
            data = df_filtered.to_dict("records")

            datos_validados = [
                CXCDevFactCalcularSchema(**registro).model_dump() for registro in data
            ]

            return datos_validados

        except Exception as e:
            logger.error(f"❌ Error en validación Devoluciones: {e}")
            raise

    def _fusionar_datos_cxc(
        self, df_acumulado: pd.DataFrame, df_pagos: pd.DataFrame, df_dev: pd.DataFrame
    ) -> pd.DataFrame:
        df = df_acumulado.copy()

        if not df_pagos.empty and "IdLiquidacionDet" in df_pagos.columns:
            pagos_cols = [
                "IdLiquidacionDet",
                "FechaPago",
                "DiasMora",
                "MontoCobrarPago",
                "MontoPago",
                "InteresPago",
                "GastosPago",
                "TipoPago",
                "SaldoDeuda",
                "ExcesoPago",
                "FechaPagoCreacion",
                "FechaPagoModificacion",
                "ObservacionPago",
            ]
            pagos_subset = df_pagos[
                [col for col in pagos_cols if col in df_pagos.columns]
            ].copy()
            df = df.merge(pagos_subset, on="IdLiquidacionDet", how="left")
        else:
            df["FechaPago"] = None
            df["MontoPago"] = 0.0
            df["TipoPago"] = ""

        if not df_dev.empty and "IdLiquidacionDet" in df_dev.columns:
            dev_cols = [
                "IdLiquidacionDet",
                "FechaDesembolso",
                "MontoDevolucion",
                "DescuentoDevolucion",
                "EstadoDevolucion",
                "ObservacionDevolucion",
            ]
            dev_subset = df_dev[
                [col for col in dev_cols if col in df_dev.columns]
            ].copy()
            df = df.merge(dev_subset, on="IdLiquidacionDet", how="left")
        else:
            df["FechaDesembolso"] = None
            df["MontoDevolucion"] = 0.0

        return df

    def _aplicar_etl_power_bi_cxc(
        self, df: pd.DataFrame, df_pagos: pd.DataFrame
    ) -> pd.DataFrame:
        """
        🚀 Aplica ETL Power BI completo para CXC - REPLICANDO EXACTAMENTE POWER BI

        Este método obtiene los datos de pagos y sector automáticamente y aplica
        toda la lógica de transformación de Power BI.
        """

        logger.info("🚀 Iniciando ETL Power BI completo para CXC...")

        # === PASO 0: OBTENER DATOS NECESARIOS ===
        tipo_cambio = self._obtener_tipo_cambio_actual()

        # Obtener datos de sector desde el KPI calculator
        logger.debug("🏢 Obteniendo datos de sector...")
        try:
            df_sector = self.sector_pagadores_calcular.calcular_df()
            logger.debug(f"   ✅ Sectores obtenidos: {len(df_sector)} registros")
        except Exception as e:
            logger.warning(f"⚠️ Error obteniendo sectores: {e}, usando DataFrame vacío")
            df_sector = pd.DataFrame()

        # === APLICAR ETL COMPLETO ===
        df_resultado = self.aplicar_etl_power_bi(
            df_acumulado=df,
            df_pagos=df_pagos,
            df_sector=df_sector,
            tipo_cambio=tipo_cambio,
        )

        logger.info(
            f"✅ ETL Power BI completado: {len(df_resultado)} registros procesados"
        )
        return df_resultado

    def aplicar_etl_power_bi(
        self,
        df_acumulado: pd.DataFrame,
        df_pagos: pd.DataFrame,
        df_sector: pd.DataFrame,
        tipo_cambio: float,
    ) -> pd.DataFrame:
        """
        🎯 Aplica el ETL completo replicando exactamente Power BI.

        COPIADO EXACTAMENTE DE CXCAcumuladoDIMCalcular para máxima compatibilidad.

        Args:
            df_acumulado: DataFrame de datos acumulados
            df_pagos: DataFrame de pagos
            df_sector: DataFrame de sectores
            tipo_cambio: Tipo de cambio actual

        Returns:
            DataFrame con ETL aplicado
        """
        # === PASO 1: JOIN CON PAGOS (temporal para cálculos) ===
        # Hacer LEFT JOIN para obtener datos de pagos solo para cálculo
        logger.debug("=== DEBUGGING df_pagos ===")
        logger.debug(f"df_pagos.empty: {df_pagos.empty}")
        if not df_pagos.empty:
            logger.debug(f"df_pagos.shape: {df_pagos.shape}")
            logger.debug(f"df_pagos.columns: {list(df_pagos.columns)}")

        if not df_pagos.empty and "IdLiquidacionDet" in df_pagos.columns:
            required_cols = ["IdLiquidacionDet", "TipoPago", "MontoPago", "SaldoDeuda"]
            # Verificar que las columnas existen
            available_cols = [col for col in required_cols if col in df_pagos.columns]

            if len(available_cols) >= 2:  # Al menos IdLiquidacionDet + una más
                # Seleccionar y renombrar las columnas de pagos para el merge
                pagos_subset = df_pagos[available_cols].copy()

                # RENOMBRAR MANUALMENTE las columnas para agregar "_temp"
                rename_dict = {}
                for col in available_cols:
                    if col != "IdLiquidacionDet":
                        rename_dict[col] = f"{col}_temp"

                pagos_subset = pagos_subset.rename(columns=rename_dict)

                logger.debug(
                    f"Columnas de pagos después de renombrar: {list(pagos_subset.columns)}"
                )

                # Hacer el merge sin suffixes ya que ya renombramos
                df_merged = df_acumulado.merge(
                    pagos_subset, on="IdLiquidacionDet", how="left"
                )

                logger.debug(
                    f"df_merged.columns después de merge: {list(df_merged.columns)}"
                )

                # Verificar que las columnas temporales se crearon
                temp_cols = [col for col in df_merged.columns if col.endswith("_temp")]
                logger.debug(f"Columnas temporales creadas: {temp_cols}")

                # Limpiar los valores nulos que puedan haber resultado del LEFT JOIN
                for col in ["TipoPago_temp", "MontoPago_temp", "SaldoDeuda_temp"]:
                    if col in df_merged.columns:
                        if "TipoPago" in col:
                            df_merged[col] = df_merged[col].fillna("")
                        else:
                            df_merged[col] = pd.to_numeric(
                                df_merged[col], errors="coerce"
                            ).fillna(0.0)
            else:
                logger.warning(
                    "Columnas insuficientes en df_pagos, usando valores por defecto"
                )
                df_merged = df_acumulado.copy()
                df_merged["TipoPago_temp"] = ""
                df_merged["MontoPago_temp"] = 0.0
                df_merged["SaldoDeuda_temp"] = 0.0
        else:
            logger.warning(
                "DataFrame de pagos vacío o sin columna IdLiquidacionDet, usando valores por defecto"
            )
            df_merged = df_acumulado.copy()
            df_merged["TipoPago_temp"] = ""
            df_merged["MontoPago_temp"] = 0.0
            df_merged["SaldoDeuda_temp"] = 0.0

        # === PASO 2: JOIN CON SECTOR ===
        # Hacer LEFT JOIN con sector pagadores

        if not df_sector.empty and "RUCPagador" in df_sector.columns:
            sector_cols = ["RUCPagador", "Sector", "GrupoEco"]
            for col in ["Sector", "GrupoEco"]:
                if col in df_sector.columns:
                    sector_cols.append(col)

            df_merged = df_merged.merge(
                df_sector[sector_cols],
                on="RUCPagador",
                how="left",
            )
        else:
            logger.warning(
                "DataFrame de sector vacío o sin columna RUCPagador, usando valores por defecto"
            )
            df_merged["Sector"] = "SIN CLASIFICAR"
            df_merged["GrupoEco"] = ""

        # === PASO 3: CALCULAR SaldoTotal ===
        # Replicar: if [TipoPago] = "PAGO PARCIAL" then [SaldoDeuda] else if [TipoPago] = "" then [NetoConfirmado] else [NetoConfirmado] - [MontoPago]
        df_merged["SaldoTotal"] = np.where(
            df_merged.get("TipoPago_temp", "") == "PAGO PARCIAL",
            df_merged.get("SaldoDeuda_temp", 0),
            np.where(
                (df_merged.get("TipoPago_temp", "").isna())
                | (df_merged.get("TipoPago_temp", "") == ""),
                df_merged["NetoConfirmado"],
                df_merged["NetoConfirmado"]
                - df_merged.get("MontoPago_temp", 0).fillna(0),
            ),
        )

        # === PASO 4: CALCULAR SaldoTotalPen ===
        # Replicar: if [Moneda] = "PEN" then [SaldoTotal] else [SaldoTotal]*Tipo_de_Cambio
        df_merged["SaldoTotalPen"] = np.where(
            df_merged["Moneda"] == "PEN",
            df_merged["SaldoTotal"],
            df_merged["SaldoTotal"] * tipo_cambio,
        )

        # === PASO 5: CALCULAR TipoPagoReal ===
        # Replicar lógica compleja de Power BI
        df_merged["TipoPagoReal"] = np.where(
            (df_merged.get("TipoPago_temp", "") == "PAGO PARCIAL")
            | (df_merged.get("TipoPago_temp", "").isna())
            | (df_merged.get("TipoPago_temp", "") == ""),
            np.where(
                df_merged["CodigoLiquidacion"].isin(self.CODIGOS_MORA_MAYO),
                "MORA A MAYO",
                df_merged.get("TipoPago_temp", "").fillna(""),
            ),
            df_merged.get("TipoPago_temp", ""),
        )

        # === PASO 6: CALCULAR EstadoCuenta ===
        # Replicar: if [FechaConfirmado] <= Fecha_Corte then "VENCIDO" else "VIGENTE"
        df_merged["FechaConfirmado"] = pd.to_datetime(df_merged["FechaConfirmado"])
        df_merged["EstadoCuenta"] = np.where(
            df_merged["FechaConfirmado"].dt.date <= self.fecha_corte,
            "VENCIDO",
            "VIGENTE",
        )

        # === PASO 7: CALCULAR EstadoReal ===
        # Replicar lógica de cobranza especial
        df_merged["EstadoReal"] = np.where(
            df_merged["EstadoCuenta"] == "VENCIDO",
            np.where(
                df_merged["CodigoLiquidacion"].isin(self.CODIGOS_COBRANZA_ESPECIAL),
                "COBRANZA ESPECIAL",
                df_merged["EstadoCuenta"],
            ),
            df_merged["EstadoCuenta"],
        )

        # === PASO 8: LIMPIAR Y FORMATEAR ===
        # Rellenar valores nulos con valores consistentes con el modelo
        df_merged["Sector"] = df_merged["Sector"].fillna("SIN CLASIFICAR")
        df_merged["GrupoEco"] = df_merged["GrupoEco"].fillna("")

        # Asegurar que las columnas calculadas no sean nulas
        df_merged["SaldoTotal"] = df_merged["SaldoTotal"].fillna(0.0)
        df_merged["SaldoTotalPen"] = df_merged["SaldoTotalPen"].fillna(0.0)
        df_merged["TipoPagoReal"] = df_merged["TipoPagoReal"].fillna("")
        df_merged["EstadoCuenta"] = df_merged["EstadoCuenta"].fillna("VIGENTE")
        df_merged["EstadoReal"] = df_merged["EstadoReal"].fillna("VIGENTE")

        # === PASO 9: REMOVER COLUMNAS TEMPORALES ===
        # Eliminar las columnas temporales de pagos ya que no van al modelo final
        columnas_temp = [col for col in df_merged.columns if col.endswith("_temp")]
        df_merged = df_merged.drop(columns=columnas_temp)

        logger.info(
            f"ETL Power BI aplicado exitosamente. Registros procesados: {len(df_merged)}"
        )
        logger.debug(f"Columnas finales: {list(df_merged.columns)}")

        return df_merged

    def _obtener_tipo_cambio_actual(self) -> float:
        """Obtiene el tipo de cambio más reciente."""
        if self.tipo_cambio_df.empty:
            logger.warning("⚠️ No hay datos de tipo de cambio, usando 3.8 por defecto")
            return 3.8

        tc_df = self.tipo_cambio_df.copy()
        tc_df["TipoCambioFecha"] = pd.to_datetime(tc_df["TipoCambioFecha"])
        ultimo_tc = tc_df.sort_values("TipoCambioFecha", ascending=False).iloc[0]

        return float(ultimo_tc["TipoCambioVenta"])

    # ========================================================================
    # PROCESAMIENTO OPERACIONES FUERA DEL SISTEMA
    # ========================================================================

    async def _procesar_operaciones_fuera_sistema(
        self, df_acumulado: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        if df_acumulado.empty:
            return df_acumulado, pd.DataFrame(), pd.DataFrame()

        try:
            if "FueraSistema" not in df_acumulado.columns:
                return df_acumulado, pd.DataFrame(), pd.DataFrame()

            mask_fuera_sistema = df_acumulado["FueraSistema"] == "si"
            df_fuera_sistema = df_acumulado[mask_fuera_sistema].copy()
            df_dentro_sistema = df_acumulado[~mask_fuera_sistema].copy()

            if df_fuera_sistema.empty:
                return df_acumulado, pd.DataFrame(), pd.DataFrame()

            # 🔧 FILTRO ESTRATÉGICO: Eliminar duplicados ANTES de generar IDs artificiales
            # ⚡ OPTIMIZACIÓN: Evita generar IDs para registros que serán eliminados
            if (
                "CodigoLiquidacion" in df_fuera_sistema.columns
                and "NroDocumento" in df_fuera_sistema.columns
            ):
                registros_antes = len(df_fuera_sistema)

                # Aplicar drop_duplicates ANTES de generar IDs artificiales (más eficiente)
                df_fuera_sistema = df_fuera_sistema.drop_duplicates(
                    subset=["CodigoLiquidacion", "NroDocumento"], keep="last"
                )

                registros_despues = len(df_fuera_sistema)
                duplicados_eliminados = registros_antes - registros_despues

                if duplicados_eliminados > 0:
                    logger.warning(
                        f"🔧 Filtrado estratégico FueraSistema (PRE-IDs): {registros_antes} registros → {registros_despues} registros "
                        f"({duplicados_eliminados} duplicados eliminados por CodigoLiquidacion+NroDocumento, keep='last')"
                    )
                    logger.info(
                        "⚡ OPTIMIZACIÓN: IDs artificiales generados solo para registros únicos"
                    )
                else:
                    logger.info(
                        "✅ No se encontraron duplicados en operaciones FueraSistema"
                    )

            df_fuera_sistema_con_ids = self._generar_ids_artificiales(df_fuera_sistema)

            df_acumulado_actualizado = pd.concat(
                [df_dentro_sistema, df_fuera_sistema_con_ids], ignore_index=True
            )

            df_pagos_fuera_sistema = self._crear_tabla_pagos_fuera_sistema(
                df_fuera_sistema_con_ids
            )
            df_dev_fuera_sistema = self._crear_tabla_dev_fuera_sistema(
                df_fuera_sistema_con_ids
            )

            return (
                df_acumulado_actualizado,
                df_pagos_fuera_sistema,
                df_dev_fuera_sistema,
            )

        except Exception as e:
            logger.error(f"❌ Error procesando operaciones fuera del sistema: {e}")
            raise

    def _generar_ids_artificiales(self, df_fuera_sistema: pd.DataFrame) -> pd.DataFrame:
        """
        🔧 Genera IDs artificiales para operaciones fuera del sistema

        LÓGICA DE GENERACIÓN:
        - IdLiquidacionCab: Un ID único por cada CodigoLiquidacion único
        - IdLiquidacionDet: Un ID único por cada NroDocumento único dentro de cada CodigoLiquidacion
        - IdLiquidacionPago: Un ID único por cada NroDocumento (para pagos)
        - IdLiquidacionDevolucion: Un ID único por cada NroDocumento (para devoluciones)

        IMPORTANTE: Usa números grandes (1000000+) para evitar conflictos con IDs reales
        """
        logger.warning(
            "🔧 Generando IDs artificiales para operaciones fuera del sistema..."
        )

        df = df_fuera_sistema.copy()

        # Base para IDs artificiales (números grandes para evitar conflictos)
        BASE_ID_ARTIFICIAL = 1000000

        # === GENERAR IdLiquidacionCab ===
        # Un ID único por cada CodigoLiquidacion único
        codigos_unicos = df["CodigoLiquidacion"].unique()
        map_codigo_a_id_cab = {
            codigo: BASE_ID_ARTIFICIAL + i for i, codigo in enumerate(codigos_unicos)
        }

        df["IdLiquidacionCab"] = df["CodigoLiquidacion"].map(map_codigo_a_id_cab)

        logger.warning(f"   📋 Generados {len(codigos_unicos)} IdLiquidacionCab únicos")

        # === GENERAR IdLiquidacionDet ===
        # Un ID único por cada NroDocumento único dentro de cada CodigoLiquidacion
        df["CodigoLiq_NroDoc"] = (
            df["CodigoLiquidacion"].astype(str) + "_" + df["NroDocumento"].astype(str)
        )
        nro_docs_unicos = df["CodigoLiq_NroDoc"].unique()

        map_nro_doc_a_id_det = {
            nro_doc: BASE_ID_ARTIFICIAL + 100000 + i  # Offset para evitar conflictos
            for i, nro_doc in enumerate(nro_docs_unicos)
        }

        df["IdLiquidacionDet"] = df["CodigoLiq_NroDoc"].map(map_nro_doc_a_id_det)

        logger.warning(
            f"   📄 Generados {len(nro_docs_unicos)} IdLiquidacionDet únicos"
        )

        # === GENERAR IdLiquidacionPago ===
        # Un ID único por cada NroDocumento (para tabla de pagos)
        map_nro_doc_a_id_pago = {
            nro_doc: BASE_ID_ARTIFICIAL + 200000 + i  # Offset para evitar conflictos
            for i, nro_doc in enumerate(nro_docs_unicos)
        }

        df["IdLiquidacionPago"] = df["CodigoLiq_NroDoc"].map(map_nro_doc_a_id_pago)

        # === GENERAR IdLiquidacionDevolucion ===
        # Un ID único por cada NroDocumento (para tabla de devoluciones)
        map_nro_doc_a_id_dev = {
            nro_doc: BASE_ID_ARTIFICIAL + 300000 + i  # Offset para evitar conflictos
            for i, nro_doc in enumerate(nro_docs_unicos)
        }

        df["IdLiquidacionDevolucion"] = df["CodigoLiq_NroDoc"].map(map_nro_doc_a_id_dev)

        # Limpiar columna temporal
        df = df.drop("CodigoLiq_NroDoc", axis=1)

        logger.warning("✅ IDs artificiales generados exitosamente")
        logger.warning(
            f"   📋 IdLiquidacionCab: {df['IdLiquidacionCab'].min()} - {df['IdLiquidacionCab'].max()}"
        )
        logger.warning(
            f"   📄 IdLiquidacionDet: {df['IdLiquidacionDet'].min()} - {df['IdLiquidacionDet'].max()}"
        )
        logger.warning(
            f"   💰 IdLiquidacionPago: {df['IdLiquidacionPago'].min()} - {df['IdLiquidacionPago'].max()}"
        )
        logger.warning(
            f"   🔄 IdLiquidacionDevolucion: {df['IdLiquidacionDevolucion'].min()} - {df['IdLiquidacionDevolucion'].max()}"
        )

        return df

    def _crear_tabla_pagos_fuera_sistema(
        self, df_fuera_sistema: pd.DataFrame
    ) -> pd.DataFrame:
        """
        💰 Crea tabla CXCPagosFact para operaciones fuera del sistema

        Extrae y mapea los campos necesarios desde df_acumulado_procesado
        a los campos requeridos por CXCPagosFactCalcularSchema
        """
        logger.warning(
            "💰 Creando tabla CXCPagosFact para operaciones fuera del sistema..."
        )

        if df_fuera_sistema.empty:
            return pd.DataFrame()

        try:
            # Crear DataFrame para pagos con los campos mapeados
            df_pagos = pd.DataFrame()

            # === CAMPOS OBLIGATORIOS ===
            df_pagos["IdLiquidacionPago"] = df_fuera_sistema["IdLiquidacionPago"]
            df_pagos["IdLiquidacionDet"] = df_fuera_sistema["IdLiquidacionDet"]

            # FechaPago - usar FechaOperacion como base
            df_pagos["FechaPago"] = df_fuera_sistema["FechaOperacion"]

            # DiasMora - calcular o usar 0 como default
            df_pagos["DiasMora"] = (
                0  # Para operaciones fuera del sistema, inicialmente 0
            )

            # Montos relacionados con pagos
            df_pagos["MontoCobrarPago"] = df_fuera_sistema.get(
                "MontoCobrar", 0.0
            ).fillna(0.0)
            df_pagos["MontoPago"] = df_fuera_sistema.get("MontoDesembolso", 0.0).fillna(
                0.0
            )
            df_pagos["InteresPago"] = df_fuera_sistema.get("Interes", 0.0).fillna(0.0)
            df_pagos["GastosPago"] = df_fuera_sistema.get("GastosContrato", 0.0).fillna(
                0.0
            )

            # TipoPago - usar un valor por defecto para operaciones fuera del sistema
            df_pagos["TipoPago"] = "FUERA_SISTEMA"

            # Saldos
            df_pagos["SaldoDeuda"] = df_fuera_sistema.get("MontoCobrar", 0.0).fillna(
                0.0
            )
            df_pagos["ExcesoPago"] = (
                0.0  # Inicialmente 0 para operaciones fuera del sistema
            )

            # === CAMPOS OPCIONALES ===
            df_pagos["ObservacionPago"] = df_fuera_sistema.get(
                "ObservacionLiquidacion", None
            )
            df_pagos["FechaPagoCreacion"] = pd.Timestamp.now()  # Fecha actual
            df_pagos["FechaPagoModificacion"] = pd.Timestamp.now()  # Fecha actual

            logger.warning(f"✅ Tabla CXCPagosFact creada: {len(df_pagos)} registros")

            return df_pagos

        except Exception as e:
            logger.error(f"❌ Error creando tabla CXCPagosFact fuera del sistema: {e}")
            raise

    def _crear_tabla_dev_fuera_sistema(
        self, df_fuera_sistema: pd.DataFrame
    ) -> pd.DataFrame:
        """
        🔄 Crea tabla CXCDevFact para operaciones fuera del sistema

        Extrae y mapea los campos necesarios desde df_acumulado_procesado
        a los campos requeridos por CXCDevFactCalcularSchema
        """
        logger.warning(
            "🔄 Creando tabla CXCDevFact para operaciones fuera del sistema..."
        )

        if df_fuera_sistema.empty:
            return pd.DataFrame()

        try:
            # Crear DataFrame para devoluciones con los campos mapeados
            df_dev = pd.DataFrame()

            # === CAMPOS OBLIGATORIOS ===
            df_dev["IdLiquidacionDevolucion"] = df_fuera_sistema[
                "IdLiquidacionDevolucion"
            ]
            df_dev["IdLiquidacionDet"] = df_fuera_sistema["IdLiquidacionDet"]

            # FechaDesembolso - usar FechaOperacion como base
            df_dev["FechaDesembolso"] = df_fuera_sistema["FechaOperacion"]

            # Montos de devolución
            df_dev["MontoDevolucion"] = (
                0.0  # Inicialmente 0 para operaciones fuera del sistema
            )
            df_dev["DescuentoDevolucion"] = 0.0  # Inicialmente 0

            # Estado de devolución
            df_dev["EstadoDevolucion"] = 1  # Estado activo por defecto

            # === CAMPOS OPCIONALES ===
            df_dev["ObservacionDevolucion"] = df_fuera_sistema.get(
                "ObservacionLiquidacion", None
            )

            logger.warning(f"✅ Tabla CXCDevFact creada: {len(df_dev)} registros")

            return df_dev

        except Exception as e:
            logger.error(f"❌ Error creando tabla CXCDevFact fuera del sistema: {e}")
            raise
