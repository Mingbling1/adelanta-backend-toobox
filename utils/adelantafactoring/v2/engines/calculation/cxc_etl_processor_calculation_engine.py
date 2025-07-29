"""
⚙️ CXC ETL Calculation Engine V2 - Motor unificado de cálculos ETL
Orquesta todos los motores de cálculo especializados en un pipeline cohesivo
"""

import pandas as pd
import numpy as np
from typing import Tuple, Dict, Any
from datetime import datetime

try:
    from utils.adelantafactoring.v2.engines.calculation.cxc_acumulado_dim_calculation_engine import (
        CXCAcumuladoDIMCalculationEngine,
    )
    from utils.adelantafactoring.v2.engines.calculation.cxc_pagos_fact_calculation_engine import (
        CXCPagosFactCalculationEngine,
    )
    from utils.adelantafactoring.v2.engines.calculation.cxc_dev_fact_calculation_engine import (
        CXCDevFactCalculationEngine,
    )
    from utils.adelantafactoring.v2.engines.calculation.kpi_calculation_engine import (
        KPICalculationEngine,
    )
except ImportError:
    # Fallback para desarrollo aislado
    class CXCAcumuladoDIMCalculationEngine:
        async def apply_power_bi_etl_logic(self, df_acumulado, **kwargs):
            return df_acumulado

        async def calculate_acumulado_metrics(self, df):
            return {"total_registros": len(df)}

    class CXCPagosFactCalculationEngine:
        async def process_pagos_calculations(self, df_pagos):
            return df_pagos

        def calculate_pagos_metrics(self, df):
            return {"total_pagos": len(df)}

    class CXCDevFactCalculationEngine:
        async def process_dev_calculations(self, df_dev):
            return df_dev

        def calculate_dev_metrics(self, df):
            return {"total_devoluciones": len(df)}

    class KPICalculationEngine:
        def __init__(self, tipo_cambio_df):
            self.tipo_cambio_df = tipo_cambio_df

        async def apply_kpi_logic(self, df):
            return df

        def get_tipo_cambio_actual(self):
            return 3.8


class CXCETLCalculationEngine:
    """Motor de cálculos unificado para ETL CXC completo"""

    def __init__(self, tipo_cambio_df: pd.DataFrame):
        self.tipo_cambio_df = tipo_cambio_df

        # Inicializar motores especializados
        self.acumulado_engine = CXCAcumuladoDIMCalculationEngine()
        self.pagos_engine = CXCPagosFactCalculationEngine()
        self.dev_engine = CXCDevFactCalculationEngine()
        self.kpi_engine = KPICalculationEngine(tipo_cambio_df)

        # Configuración de procesamiento
        self.include_fuera_sistema = True
        self.apply_power_bi_etl = True
        self.apply_kpi_processing = True

        # Estadísticas de procesamiento
        self.processing_stats = {
            "start_time": None,
            "end_time": None,
            "total_records_processed": 0,
            "errors_encountered": 0,
            "warnings_generated": 0,
        }

        # Configuración de IDs artificiales para operaciones fuera del sistema
        self.BASE_ID_ARTIFICIAL = 1000000

    async def process_complete_etl(
        self, df_acumulado: pd.DataFrame, df_pagos: pd.DataFrame, df_dev: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Procesa ETL completo de CXC con todos los componentes

        Returns:
            Tuple[df_acumulado_processed, df_pagos_processed, df_dev_processed]
        """
        try:
            self.processing_stats["start_time"] = datetime.now()

            # Paso 1: Fusionar datos principales
            df_merged = self._merge_cxc_data(df_acumulado, df_pagos, df_dev)

            # Paso 2: Procesar datos con KPI si está habilitado
            if self.apply_kpi_processing:
                df_merged = await self._apply_kpi_processing(df_merged)

            # Paso 3: Aplicar ETL Power BI si está habilitado
            if self.apply_power_bi_etl:
                df_merged = await self._apply_power_bi_etl_logic(df_merged)

            # Paso 4: Procesar operaciones fuera del sistema
            if self.include_fuera_sistema:
                df_merged, df_pagos_extra, df_dev_extra = (
                    await self._process_fuera_sistema(df_merged)
                )

                # Combinar datos extra con originales
                if not df_pagos_extra.empty:
                    df_pagos = pd.concat([df_pagos, df_pagos_extra], ignore_index=True)

                if not df_dev_extra.empty:
                    df_dev = pd.concat([df_dev, df_dev_extra], ignore_index=True)

            # Paso 5: Procesamiento específico de cada tabla
            df_pagos_processed = await self._process_pagos_table(df_pagos)
            df_dev_processed = await self._process_dev_table(df_dev)

            # Estadísticas finales
            self.processing_stats["end_time"] = datetime.now()
            self.processing_stats["total_records_processed"] = (
                len(df_merged) + len(df_pagos_processed) + len(df_dev_processed)
            )

            return df_merged, df_pagos_processed, df_dev_processed

        except Exception as e:
            self.processing_stats["errors_encountered"] += 1
            print(f"❌ Error en process_complete_etl: {e}")
            raise

    def _merge_cxc_data(
        self, df_acumulado: pd.DataFrame, df_pagos: pd.DataFrame, df_dev: pd.DataFrame
    ) -> pd.DataFrame:
        """Fusiona datos de acumulado con pagos y devoluciones"""
        try:
            df = df_acumulado.copy() if not df_acumulado.empty else pd.DataFrame()

            if df.empty:
                return df

            # Fusionar con pagos
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

                available_pagos_cols = [
                    col for col in pagos_cols if col in df_pagos.columns
                ]
                pagos_subset = df_pagos[available_pagos_cols].copy()

                df = df.merge(pagos_subset, on="IdLiquidacionDet", how="left")
            else:
                # Agregar columnas de pagos vacías
                df["FechaPago"] = None
                df["MontoPago"] = 0.0
                df["TipoPago"] = ""

            # Fusionar con devoluciones
            if not df_dev.empty and "IdLiquidacionDet" in df_dev.columns:
                dev_cols = [
                    "IdLiquidacionDet",
                    "FechaDesembolso",
                    "MontoDevolucion",
                    "DescuentoDevolucion",
                    "EstadoDevolucion",
                    "ObservacionDevolucion",
                ]

                available_dev_cols = [col for col in dev_cols if col in df_dev.columns]
                dev_subset = df_dev[available_dev_cols].copy()

                df = df.merge(dev_subset, on="IdLiquidacionDet", how="left")
            else:
                # Agregar columnas de devoluciones vacías
                df["FechaDesembolso"] = None
                df["MontoDevolucion"] = 0.0

            return df

        except Exception as e:
            print(f"❌ Error fusionando datos CXC: {e}")
            raise

    async def _apply_kpi_processing(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica lógica de procesamiento KPI"""
        try:
            if df.empty:
                return df

            # Usar el motor KPI para aplicar lógica
            df_processed = await self.kpi_engine.apply_kpi_logic(df)

            return df_processed

        except Exception as e:
            self.processing_stats["warnings_generated"] += 1
            print(f"⚠️ Warning en KPI processing: {e}")
            return df  # Continuar sin KPI processing en caso de error

    async def _apply_power_bi_etl_logic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica lógica ETL de Power BI"""
        try:
            if df.empty:
                return df

            tipo_cambio = self.kpi_engine.get_tipo_cambio_actual()

            # Aplicar lógica Power BI usando el motor acumulado
            df_processed = await self.acumulado_engine.apply_power_bi_etl_logic(
                df_acumulado=df, tipo_cambio=tipo_cambio
            )

            # Aplicar cálculos adicionales de CXC
            df_processed = self._apply_cxc_etl_calculations(df_processed, tipo_cambio)

            return df_processed

        except Exception as e:
            self.processing_stats["warnings_generated"] += 1
            print(f"⚠️ Warning en Power BI ETL: {e}")
            return df  # Continuar sin Power BI ETL en caso de error

    def _apply_cxc_etl_calculations(
        self, df: pd.DataFrame, tipo_cambio: float
    ) -> pd.DataFrame:
        """Aplica cálculos específicos del ETL CXC"""
        try:
            # Calcular SaldoTotal basado en lógica de pagos
            df["SaldoTotal"] = np.where(
                df.get("TipoPago", "") == "PAGO PARCIAL",
                df.get("SaldoDeuda", 0),
                np.where(
                    (df.get("TipoPago", "").isna()) | (df.get("TipoPago", "") == ""),
                    df.get("NetoConfirmado", 0),
                    df.get("NetoConfirmado", 0) - df.get("MontoPago", 0),
                ),
            )

            # Convertir a soles
            df["SaldoTotalPen"] = np.where(
                df.get("Moneda", "") == "PEN",
                df["SaldoTotal"],
                df["SaldoTotal"] * tipo_cambio,
            )

            # Campos adicionales de estado
            df["TipoPagoReal"] = df.get("TipoPago", "").fillna("")
            df["EstadoCuenta"] = "VIGENTE"
            df["EstadoReal"] = "VIGENTE"

            # Normalizar valores
            df["SaldoTotal"] = df["SaldoTotal"].fillna(0.0)
            df["SaldoTotalPen"] = df["SaldoTotalPen"].fillna(0.0)

            return df

        except Exception as e:
            print(f"❌ Error en cálculos CXC ETL: {e}")
            raise

    async def _process_fuera_sistema(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Procesa operaciones fuera del sistema"""
        try:
            if df.empty or "FueraSistema" not in df.columns:
                return df, pd.DataFrame(), pd.DataFrame()

            # Separar registros fuera del sistema
            mask_fuera_sistema = df["FueraSistema"] == "si"
            df_fuera_sistema = df[mask_fuera_sistema].copy()
            df_dentro_sistema = df[~mask_fuera_sistema].copy()

            if df_fuera_sistema.empty:
                return df, pd.DataFrame(), pd.DataFrame()

            # Eliminar duplicados y generar IDs artificiales
            df_fuera_sistema = self._remove_duplicates_fuera_sistema(df_fuera_sistema)
            df_fuera_sistema = self._generate_artificial_ids(df_fuera_sistema)

            # Crear tablas de pagos y devoluciones para operaciones fuera del sistema
            df_pagos_fuera = self._create_pagos_fuera_sistema(df_fuera_sistema)
            df_dev_fuera = self._create_dev_fuera_sistema(df_fuera_sistema)

            # Combinar datos
            df_combined = pd.concat(
                [df_dentro_sistema, df_fuera_sistema], ignore_index=True
            )

            return df_combined, df_pagos_fuera, df_dev_fuera

        except Exception as e:
            print(f"❌ Error procesando operaciones fuera del sistema: {e}")
            raise

    def _remove_duplicates_fuera_sistema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Elimina duplicados en operaciones fuera del sistema"""
        if "CodigoLiquidacion" in df.columns and "NroDocumento" in df.columns:
            records_before = len(df)
            df = df.drop_duplicates(
                subset=["CodigoLiquidacion", "NroDocumento"], keep="last"
            )
            records_after = len(df)

            duplicates_removed = records_before - records_after
            if duplicates_removed > 0:
                print(
                    f"🔧 Eliminados {duplicates_removed} duplicados en operaciones fuera del sistema"
                )

        return df

    def _generate_artificial_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera IDs artificiales para operaciones fuera del sistema"""
        try:
            df = df.copy()

            # Generar IdLiquidacionCab únicos por CodigoLiquidacion
            if "CodigoLiquidacion" in df.columns:
                codigos_unicos = df["CodigoLiquidacion"].unique()
                map_codigo_a_id_cab = {
                    codigo: self.BASE_ID_ARTIFICIAL + i
                    for i, codigo in enumerate(codigos_unicos)
                }
                df["IdLiquidacionCab"] = df["CodigoLiquidacion"].map(
                    map_codigo_a_id_cab
                )

            # Generar IdLiquidacionDet únicos por documento
            if "CodigoLiquidacion" in df.columns and "NroDocumento" in df.columns:
                df["CodigoLiq_NroDoc"] = (
                    df["CodigoLiquidacion"].astype(str)
                    + "_"
                    + df["NroDocumento"].astype(str)
                )
                docs_unicos = df["CodigoLiq_NroDoc"].unique()

                map_doc_a_id_det = {
                    doc: self.BASE_ID_ARTIFICIAL + 100000 + i
                    for i, doc in enumerate(docs_unicos)
                }
                df["IdLiquidacionDet"] = df["CodigoLiq_NroDoc"].map(map_doc_a_id_det)

                # IDs para pagos y devoluciones
                map_doc_a_id_pago = {
                    doc: self.BASE_ID_ARTIFICIAL + 200000 + i
                    for i, doc in enumerate(docs_unicos)
                }
                df["IdLiquidacionPago"] = df["CodigoLiq_NroDoc"].map(map_doc_a_id_pago)

                map_doc_a_id_dev = {
                    doc: self.BASE_ID_ARTIFICIAL + 300000 + i
                    for i, doc in enumerate(docs_unicos)
                }
                df["IdLiquidacionDevolucion"] = df["CodigoLiq_NroDoc"].map(
                    map_doc_a_id_dev
                )

                # Limpiar columna temporal
                df = df.drop("CodigoLiq_NroDoc", axis=1)

            print(
                f"✅ IDs artificiales generados para {len(df)} operaciones fuera del sistema"
            )

            return df

        except Exception as e:
            print(f"❌ Error generando IDs artificiales: {e}")
            raise

    def _create_pagos_fuera_sistema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Crea tabla de pagos para operaciones fuera del sistema"""
        try:
            if df.empty:
                return pd.DataFrame()

            df_pagos = pd.DataFrame()

            # Campos obligatorios
            df_pagos["IdLiquidacionPago"] = df["IdLiquidacionPago"]
            df_pagos["IdLiquidacionDet"] = df["IdLiquidacionDet"]
            df_pagos["FechaPago"] = df.get("FechaOperacion")
            df_pagos["DiasMora"] = 0

            # Montos
            df_pagos["MontoCobrarPago"] = df.get("MontoCobrar", 0.0).fillna(0.0)
            df_pagos["MontoPago"] = df.get("MontoDesembolso", 0.0).fillna(0.0)
            df_pagos["InteresPago"] = df.get("Interes", 0.0).fillna(0.0)
            df_pagos["GastosPago"] = df.get("GastosContrato", 0.0).fillna(0.0)

            # Campos adicionales
            df_pagos["TipoPago"] = "FUERA_SISTEMA"
            df_pagos["SaldoDeuda"] = df.get("MontoCobrar", 0.0).fillna(0.0)
            df_pagos["ExcesoPago"] = 0.0
            df_pagos["ObservacionPago"] = df.get("ObservacionLiquidacion")
            df_pagos["FechaPagoCreacion"] = pd.Timestamp.now()
            df_pagos["FechaPagoModificacion"] = pd.Timestamp.now()

            return df_pagos

        except Exception as e:
            print(f"❌ Error creando tabla pagos fuera del sistema: {e}")
            return pd.DataFrame()

    def _create_dev_fuera_sistema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Crea tabla de devoluciones para operaciones fuera del sistema"""
        try:
            if df.empty:
                return pd.DataFrame()

            df_dev = pd.DataFrame()

            # Campos obligatorios
            df_dev["IdLiquidacionDevolucion"] = df["IdLiquidacionDevolucion"]
            df_dev["IdLiquidacionDet"] = df["IdLiquidacionDet"]
            df_dev["FechaDesembolso"] = df.get("FechaOperacion")

            # Montos (inicialmente 0 para operaciones fuera del sistema)
            df_dev["MontoDevolucion"] = 0.0
            df_dev["DescuentoDevolucion"] = 0.0
            df_dev["EstadoDevolucion"] = 1
            df_dev["ObservacionDevolucion"] = df.get("ObservacionLiquidacion")

            return df_dev

        except Exception as e:
            print(f"❌ Error creando tabla devoluciones fuera del sistema: {e}")
            return pd.DataFrame()

    async def _process_pagos_table(self, df_pagos: pd.DataFrame) -> pd.DataFrame:
        """Procesa específicamente la tabla de pagos"""
        try:
            if df_pagos.empty:
                return df_pagos

            # Aplicar procesamiento específico de pagos usando el motor
            df_processed = await self.pagos_engine.process_pagos_calculations(df_pagos)

            # Normalizar campos
            df_processed = df_processed.fillna(
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

            return df_processed

        except Exception as e:
            self.processing_stats["warnings_generated"] += 1
            print(f"⚠️ Warning procesando tabla pagos: {e}")
            return df_pagos

    async def _process_dev_table(self, df_dev: pd.DataFrame) -> pd.DataFrame:
        """Procesa específicamente la tabla de devoluciones"""
        try:
            if df_dev.empty:
                return df_dev

            # Aplicar procesamiento específico de devoluciones usando el motor
            df_processed = await self.dev_engine.process_dev_calculations(df_dev)

            # Normalizar campos
            df_processed = df_processed.fillna(
                {
                    "MontoDevolucion": 0.0,
                    "DescuentoDevolucion": 0.0,
                    "EstadoDevolucion": 0,
                    "ObservacionDevolucion": "",
                }
            )

            return df_processed

        except Exception as e:
            self.processing_stats["warnings_generated"] += 1
            print(f"⚠️ Warning procesando tabla devoluciones: {e}")
            return df_dev

    async def calculate_comprehensive_metrics(
        self, df_acumulado: pd.DataFrame, df_pagos: pd.DataFrame, df_dev: pd.DataFrame
    ) -> Dict[str, Any]:
        """Calcula métricas comprehensivas del ETL"""
        try:
            metrics = {
                "processing_stats": self.processing_stats.copy(),
                "data_volume": {
                    "acumulado_records": len(df_acumulado),
                    "pagos_records": len(df_pagos),
                    "dev_records": len(df_dev),
                    "total_records": len(df_acumulado) + len(df_pagos) + len(df_dev),
                },
            }

            # Métricas específicas por motor
            if not df_acumulado.empty:
                acumulado_metrics = (
                    await self.acumulado_engine.calculate_acumulado_metrics(
                        df_acumulado
                    )
                )
                metrics["acumulado_metrics"] = acumulado_metrics

            if not df_pagos.empty:
                pagos_metrics = self.pagos_engine.calculate_pagos_metrics(df_pagos)
                metrics["pagos_metrics"] = pagos_metrics

            if not df_dev.empty:
                dev_metrics = self.dev_engine.calculate_dev_metrics(df_dev)
                metrics["dev_metrics"] = dev_metrics

            # Calcular tiempo de procesamiento
            if (
                self.processing_stats["start_time"]
                and self.processing_stats["end_time"]
            ):
                processing_time = (
                    self.processing_stats["end_time"]
                    - self.processing_stats["start_time"]
                ).total_seconds()
                metrics["processing_time_seconds"] = processing_time

                if metrics["data_volume"]["total_records"] > 0:
                    metrics["records_per_second"] = (
                        metrics["data_volume"]["total_records"] / processing_time
                    )

            return metrics

        except Exception as e:
            print(f"❌ Error calculando métricas: {e}")
            return {"error": str(e)}
