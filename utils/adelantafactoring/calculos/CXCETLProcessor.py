from typing import Dict, Tuple, List
import pandas as pd
import numpy as np
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
from .BaseCalcular import BaseCalcular


class CXCETLProcessor(BaseCalcular):
    """🚀 PROCESADOR ETL COMPLETO CXC - ULTRA EFICIENTE CON PYDANTIC RUST"""

    def __init__(self, tipo_cambio_df: pd.DataFrame):
        super().__init__()
        self.tipo_cambio_df = tipo_cambio_df
        self.kpi_calculator = KPICalcular(tipo_cambio_df)

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
            df = self._aplicar_etl_power_bi_cxc(df)
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

    def _aplicar_etl_power_bi_cxc(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            tipo_cambio = self._obtener_tipo_cambio_actual()

            df["SaldoTotal"] = np.where(
                df.get("TipoPago", "") == "PAGO PARCIAL",
                df.get("SaldoDeuda", 0),
                np.where(
                    (df.get("TipoPago", "").isna()) | (df.get("TipoPago", "") == ""),
                    df["NetoConfirmado"],
                    df["NetoConfirmado"] - df.get("MontoPago", 0),
                ),
            )

            df["SaldoTotalPen"] = np.where(
                df["Moneda"] == "PEN", df["SaldoTotal"], df["SaldoTotal"] * tipo_cambio
            )
            df["TipoPagoReal"] = df.get("TipoPago", "").fillna("")
            df["EstadoCuenta"] = "VIGENTE"
            df["EstadoReal"] = "VIGENTE"
            df["SaldoTotal"] = df["SaldoTotal"].fillna(0.0)
            df["SaldoTotalPen"] = df["SaldoTotalPen"].fillna(0.0)

            return df
        except Exception as e:
            logger.error(f"❌ Error en ETL Power BI CXC: {e}")
            raise

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
