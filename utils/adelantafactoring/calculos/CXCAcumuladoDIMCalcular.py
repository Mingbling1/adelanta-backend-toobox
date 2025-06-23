from .BaseCalcular import BaseCalcular
from .SectorPagadoresCalcular import SectorPagadoresCalcular
import pandas as pd
import numpy as np
from datetime import datetime, date
from config.logger import logger
from ..obtener.CXCAcumuladoDIMObtener import CXCAcumuladoDIMObtener
from ..schemas.CXCAcumuladoDIMCalcularSchema import (
    CXCAcumuladoDIMCalcularSchema,
    CXCAcumuladoDIMRawSchema,
)


class CXCAcumuladoDIMCalcular(BaseCalcular):
    """
    Calculador ETL para datos acumulados DIM CXC.
    Replica exactamente la lógica del Power BI.
    """

    # Listas de códigos de liquidación para clasificaciones especiales
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
        "LIQ029-2022",
        "LIQ335-2021",
        "LIQ2312000180",
        "LIQ2210000084",
        "LIQ2403000063",
        "LIQ2405000211",
        "LIQ2211000037",
        "LIQ2308000069",
        "LIQ2401000042",
        "LIQ017-2021",
        "LIQ255-2021 ME",
        "LIQ009-2022",
        "LIQ2312000235",
        "LIQ2403000064",
        "LIQ2405000164",
        "LIQ2211000036",
        "LIQ2308000235",
        "LIQ2401000023",
        "LIQ001-2022 ME",
        "LIQ709-2021",
        "LIQ2401000021",
        "LIQ2403000104",
        "LIQ2309000028",
        "LIQ2312000197",
        "LIQ011-2022 ME",
        "LIQ632-2021",
        "LIQ2401000067",
        "LIQ2403000105",
        "LIQ2309000059",
        "LIQ2312000116",
        "LIQ027-2022 ME",
        "LIQ606-2021",
        "LIQ2401000107",
        "LIQ2403000186",
        "LIQ2309000113",
        "LIQ028-2022 ME",
        "LIQ600-2021",
        "LIQ2401000156",
        "LIQ2403000195",
        "LIQ046-2022 ME",
        "LIQ576-2021",
        "LIQ2402000010",
        "LIQ2404000029",
        "LIQ054-2022 ME",
        "LIQ2402000046",
        "LIQ2404000041",
        "LIQ057-2022 ME",
        "LIQ2402000106",
        "LIQ2405000223",
        "LIQ060-2022 ME",
        "LIQ2402000152",
        "LIQ061-2022 ME",
        "LIQ062-2022 ME",
        "LIQ2302000033",
        "LIQ2410000151",
        "LIQ2410000152",
        "LIQ2410000153",
        "LIQ2410000154",
        "LIQ2410000281",
        "LIQ2410000282",
        "LIQ2410000319",
        "LIQ2410000363",
        "LIQ2411000231",
        "LIQ2411000235",
        "LIQ2411000268",
        "LIQ2411000119",
        "LIQ2408000250",
        "LIQ2411000120",
        "LIQ2411000236",
        "LIQ2409000109",
        "LIQ2408000044",
        "LIQ2410000321",
        "LIQ2410000337",
        "LIQ2405000095",
        "LIQ2405000158",
        "LIQ2405000159",
        "LIQ2405000164",
        "LIQ2405000211",
        "LIQ2406000121",
        "LIQ2406000133",
        "LIQ2406000138",
        "LIQ2406000144",
        "LIQ2406000153",
        "LIQ2406000200",
        "LIQ2407000008",
        "LIQ2407000051",
        "LIQ2407000052",
        "LIQ2407000057",
        "LIQ2405000215",
    ]

    def __init__(self):
        super().__init__()
        self.cxc_acumulado_dim_obtener = CXCAcumuladoDIMObtener()
        self.sector_pagadores_calcular = SectorPagadoresCalcular()
        self.fecha_corte = datetime.now().date()  # Fecha actual como corte por defecto

    def obtener_tipo_cambio_actual(self, tipo_cambio_df: pd.DataFrame) -> float:
        """
        Obtiene el tipo de cambio más reciente.

        Args:
            tipo_cambio_df: DataFrame con tipos de cambio

        Returns:
            Tipo de cambio de venta más reciente
        """
        if tipo_cambio_df.empty:
            logger.warning("No hay datos de tipo de cambio, usando 3.8 por defecto")
            return 3.8

        # Ordenar por fecha y tomar el más reciente
        tipo_cambio_df["TipoCambioFecha"] = pd.to_datetime(
            tipo_cambio_df["TipoCambioFecha"]
        )
        ultimo_tc = tipo_cambio_df.sort_values("TipoCambioFecha", ascending=False).iloc[
            0
        ]

        logger.debug(
            f"Usando tipo de cambio: {ultimo_tc['TipoCambioVenta']} del {ultimo_tc['TipoCambioFecha']}"
        )
        return float(ultimo_tc["TipoCambioVenta"])

    def aplicar_etl_power_bi(
        self,
        df_acumulado: pd.DataFrame,
        df_pagos: pd.DataFrame,
        df_sector: pd.DataFrame,
        tipo_cambio: float,
    ) -> pd.DataFrame:
        """
        Aplica el ETL completo replicando exactamente Power BI.

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
            logger.debug(f"df_pagos.info():\n{df_pagos.info()}")

        if not df_pagos.empty and "IdLiquidacionDet" in df_pagos.columns:
            required_cols = ["IdLiquidacionDet", "TipoPago", "MontoPago", "SaldoDeuda"]
            # Seleccionar y renombrar las columnas de pagos para el merge
            pagos_subset = df_pagos[required_cols].copy()

            # RENOMBRAR MANUALMENTE las columnas para agregar "_temp"
            pagos_subset = pagos_subset.rename(
                columns={
                    "TipoPago": "TipoPago_temp",
                    "MontoPago": "MontoPago_temp",
                    "SaldoDeuda": "SaldoDeuda_temp",
                }
            )

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
            df_merged["TipoPago_temp"] = df_merged["TipoPago_temp"].fillna("")
            df_merged["MontoPago_temp"] = pd.to_numeric(
                df_merged["MontoPago_temp"], errors="coerce"
            ).fillna(0.0)
            df_merged["SaldoDeuda_temp"] = pd.to_numeric(
                df_merged["SaldoDeuda_temp"], errors="coerce"
            ).fillna(0.0)

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
            df_merged = df_merged.merge(
                df_sector[["RUCPagador", "Sector", "GrupoEco"]],
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
            df_merged["TipoPago_temp"] == "PAGO PARCIAL",
            df_merged["SaldoDeuda_temp"],
            np.where(
                (df_merged["TipoPago_temp"].isna())
                | (df_merged["TipoPago_temp"] == ""),
                df_merged["NetoConfirmado"],
                df_merged["NetoConfirmado"] - df_merged["MontoPago_temp"].fillna(0),
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
            (df_merged["TipoPago_temp"] == "PAGO PARCIAL")
            | (df_merged["TipoPago_temp"].isna())
            | (df_merged["TipoPago_temp"] == ""),
            np.where(
                df_merged["CodigoLiquidacion"].isin(self.CODIGOS_MORA_MAYO),
                "MORA A MAYO",
                df_merged["TipoPago_temp"].fillna(""),
            ),
            df_merged["TipoPago_temp"],
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

    def validar_datos_raw(self, data: list[dict]) -> pd.DataFrame:
        """
        Valida y convierte los datos RAW desde el webservice usando Pydantic.
        Esto resuelve el problema de tipos de datos desde el principio.
        """
        try:
            datos_validados = []
            errores_validacion = []

            for i, registro in enumerate(data):
                try:
                    # Validar con el schema RAW que convierte tipos automáticamente
                    registro_validado = CXCAcumuladoDIMRawSchema(**registro)
                    datos_validados.append(registro_validado.model_dump())
                except Exception as e:
                    errores_validacion.append(f"Registro {i}: {str(e)}")

            if errores_validacion:
                logger.warning(
                    f"Se encontraron {len(errores_validacion)} errores de validación RAW"
                )
                for error in errores_validacion[:3]:  # Log solo los primeros 3 errores
                    logger.warning(f"Error validación RAW: {error}")

            logger.info(
                f"Validación RAW: {len(datos_validados)}/{len(data)} registros válidos"
            )

            # Convertir a DataFrame con tipos ya correctos
            df_validado = pd.DataFrame(datos_validados)
            logger.info(
                f"DataFrame RAW creado con tipos correctos: {len(df_validado)} registros"
            )

            return df_validado

        except Exception as e:
            logger.error(f"Error crítico en validación RAW: {e}")
            raise e

    def validar_datos(self, data: list[dict]) -> list[dict]:
        """
        Valida los datos usando el schema actualizado con validación robusta.
        """
        try:
            datos_validados = []
            errores_validacion = []

            for i, registro in enumerate(data):
                try:
                    registro_validado = CXCAcumuladoDIMCalcularSchema(**registro)
                    datos_validados.append(registro_validado.model_dump())
                except Exception as e:
                    errores_validacion.append(f"Registro {i}: {str(e)}")

            if errores_validacion:
                logger.warning(
                    f"Se encontraron {len(errores_validacion)} errores de validación"
                )
                for error in errores_validacion[:5]:  # Log solo los primeros 5 errores
                    logger.warning(f"Error validación: {error}")

                if len(errores_validacion) > 5:
                    logger.warning(f"... y {len(errores_validacion) - 5} errores más")

            logger.info(
                f"Validación completada: {len(datos_validados)}/{len(data)} registros válidos"
            )
            return datos_validados

        except Exception as e:
            logger.error(f"Error crítico en validación de datos: {e}")
            raise e

    def procesar_datos(self, data: list[dict]) -> pd.DataFrame:
        """
        Procesa los datos validados en DataFrame.
        """
        df = pd.DataFrame(data)
        logger.debug(f"DataFrame acumulado DIM creado con {len(df)} filas")
        return df

    async def calcular(
        self,
        cxc_pagos_fact_df: pd.DataFrame = None,
        tipo_cambio_df: pd.DataFrame = None,
    ) -> list[dict]:
        """
        Método principal que orquesta el ETL completo.

        Args:
            cxc_pagos_fact_df: DataFrame de pagos (opcional)
            tipo_cambio_df: DataFrame de tipos de cambio (opcional)

        Returns:
            Lista de diccionarios con datos procesados y ETL aplicado
        """
        try:
            # === 1. OBTENER Y VALIDAR DATOS BASE ===
            logger.info("📊 Obteniendo datos acumulados DIM desde webservice...")
            data_acumulado = await self.cxc_acumulado_dim_obtener.obtener_acumulado_dim()

            if not data_acumulado:
                logger.warning("No se obtuvieron datos acumulados DIM")
                return []

            # ✅ VALIDAR Y CONVERTIR TIPOS INMEDIATAMENTE
            logger.info("🔍 Validando y convirtiendo tipos de datos...")
            df_acumulado = self.validar_datos_raw(data_acumulado)
            logger.info(f"📊 Datos validados: {len(df_acumulado)} registros con tipos correctos")


            # === 2. OBTENER DATOS DE PAGOS ===
            if cxc_pagos_fact_df is None or cxc_pagos_fact_df.empty:
                logger.warning(
                    "No se proporcionó DataFrame de pagos, usando datos vacíos"
                )
                df_pagos = pd.DataFrame()
            else:
                df_pagos = cxc_pagos_fact_df.copy()
                logger.info(f"📊 Datos de pagos: {len(df_pagos)} registros")

            # === 3. OBTENER SECTOR PAGADORES ===
            logger.info("📊 Obteniendo datos de sector pagadores...")
            try:
                df_sector = self.sector_pagadores_calcular.calcular_df()
                if df_sector is None or df_sector.empty:
                    logger.warning("DataFrame de sector pagadores está vacío")
                    df_sector = pd.DataFrame(
                        columns=["RUCPagador", "Sector", "GrupoEco"]
                    )
                else:
                    logger.info(f"📊 Datos de sector: {len(df_sector)} registros")
            except Exception as e:
                logger.error(f"Error obteniendo sector pagadores: {e}")
                # Crear DataFrame vacío con las columnas necesarias
                df_sector = pd.DataFrame(columns=["RUCPagador", "Sector", "GrupoEco"])
                logger.info("Usando DataFrame de sector vacío por defecto")

            # === 4. OBTENER TIPO DE CAMBIO ===
            if tipo_cambio_df is None or tipo_cambio_df.empty:
                tipo_cambio = 3.8  # Valor por defecto
                logger.warning(
                    "No se proporcionó DataFrame de tipo de cambio, usando 3.8"
                )
            else:
                tipo_cambio = self.obtener_tipo_cambio_actual(tipo_cambio_df)

            # === 5. APLICAR ETL POWER BI ===
            logger.info("🔄 Aplicando ETL Power BI...")
            df_final = self.aplicar_etl_power_bi(
                df_acumulado, df_pagos, df_sector, tipo_cambio
            )

            # === 6. CONVERTIR A LISTA DE DICCIONARIOS ===
            datos_finales = df_final.to_dict("records")

            # === 7. VALIDAR DATOS FINALES ===
            logger.info("✅ Validando datos finales con schema...")
            datos_validados = self.validar_datos(datos_finales)

            # === 8. LOG DE RESULTADOS ===
            logger.info(
                f"🎉 ETL CXC Acumulado DIM completado: {len(datos_validados)} registros válidos procesados"
            )

            # Log de estadísticas
            if datos_validados:
                df_final_validado = pd.DataFrame(datos_validados)
                estado_counts = df_final_validado["EstadoReal"].value_counts()
                logger.info(f"📈 Estados: {dict(estado_counts)}")

                moneda_counts = df_final_validado["Moneda"].value_counts()
                logger.info(f"💰 Monedas: {dict(moneda_counts)}")

                sector_counts = df_final_validado["Sector"].value_counts().head(10)
                logger.info(f"🏢 Top sectores: {dict(sector_counts)}")

            return datos_validados

        except Exception as e:
            logger.error(f"❌ Error en ETL CXC Acumulado DIM: {e}", exc_info=True)
            raise e
