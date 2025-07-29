"""
🔄 Transformer V2 - Nuevos Clientes Nuevos Pagadores

Transformer especializado para procesamiento y transformación de datos de nuevos clientes y pagadores.
Arquitectura hexagonal pura sin dependencias legacy.
"""

import pandas as pd
from typing import Dict, List, Any

from config.logger import logger
from ..engines.data.nuevos_clientes_pagadores import (
    NuevosClientesNuevosPagadoresDataEngine,
)
from ..engines.validation.nuevos_clientes_pagadores import (
    NuevosClientesNuevosPagadoresValidationEngine,
)
from ..schemas.nuevos_clientes_pagadores import (
    NuevosClientesNuevosPagadoresResponseSchema,
)


class NuevosClientesNuevosPagadoresTransformer:
    """
    🚀 Transformer principal para orquestar el procesamiento de nuevos clientes y pagadores.

    Coordina data engine y validation engine para procesamiento completo.
    """

    def __init__(self, df: pd.DataFrame):
        """
        Inicializa el transformer con DataFrame fuente.

        Args:
            df: DataFrame con datos fuente
        """
        self.df = df.copy() if not df.empty else pd.DataFrame()
        self.data_engine = NuevosClientesNuevosPagadoresDataEngine(self.df)
        self.validation_engine = NuevosClientesNuevosPagadoresValidationEngine()

        logger.info(f"🚀 Transformer inicializado con {len(self.df)} registros")

    def build_column_config(
        self,
        ruc_c_col: str = "RUCCliente",
        ruc_p_col: str = "RUCPagador",
        ruc_c_ns_col: str = "RazonSocialCliente",
        ruc_p_ns_col: str = "RazonSocialPagador",
        ejecutivo_col: str = "Ejecutivo",
        type_op_col: str = "TipoOperacion",
    ) -> Dict[str, str]:
        """
        🗂️ Construye configuración de columnas para el procesamiento.

        Args:
            ruc_c_col: Columna de RUC Cliente
            ruc_p_col: Columna de RUC Pagador
            ruc_c_ns_col: Columna de Razón Social Cliente
            ruc_p_ns_col: Columna de Razón Social Pagador
            ejecutivo_col: Columna de Ejecutivo
            type_op_col: Columna de Tipo de Operación

        Returns:
            Diccionario con configuración de columnas
        """
        config = {
            "ruc_c_col": ruc_c_col,
            "ruc_p_col": ruc_p_col,
            "ruc_c_ns_col": ruc_c_ns_col,
            "ruc_p_ns_col": ruc_p_ns_col,
            "ejecutivo_col": ejecutivo_col,
            "type_op_col": type_op_col,
        }

        logger.debug(f"🗂️ Configuración de columnas: {config}")

        return config

    def validate_input_requirements(self, config: Dict[str, str]) -> bool:
        """
        ✅ Valida que el DataFrame tenga los requisitos mínimos.

        Args:
            config: Configuración de columnas

        Returns:
            True si es válido, False en caso contrario
        """
        try:
            if self.df.empty:
                logger.error("❌ DataFrame vacío")
                return False

            # Validar columnas requeridas
            required_columns = list(config.values()) + ["Mes"]
            missing_columns = [
                col for col in required_columns if col not in self.df.columns
            ]

            if missing_columns:
                logger.error(f"❌ Faltan columnas requeridas: {missing_columns}")
                return False

            # Validar que haya datos en columnas críticas
            critical_columns = [config["ejecutivo_col"], config["type_op_col"], "Mes"]
            for col in critical_columns:
                if self.df[col].isna().all():
                    logger.error(f"❌ Columna crítica vacía: {col}")
                    return False

            logger.info("✅ Validación de requisitos de entrada exitosa")
            return True

        except Exception as e:
            logger.error(f"❌ Error validando requisitos de entrada: {str(e)}")
            return False

    def prepare_dataframe(self, config: Dict[str, str]) -> pd.DataFrame:
        """
        🧹 Prepara y limpia el DataFrame para procesamiento.

        Args:
            config: Configuración de columnas

        Returns:
            DataFrame preparado
        """
        try:
            df_prepared = self.df.copy()

            # Validar y limpiar RUCs
            df_prepared = self.validation_engine.validate_and_clean_ruc_data(
                df_prepared
            )

            # Asegurar formato de mes consistente
            if "Mes" in df_prepared.columns:
                df_prepared["Mes"] = pd.to_datetime(
                    df_prepared["Mes"], errors="coerce"
                ).dt.strftime("%Y-%m")

                # Remover registros con mes inválido
                df_prepared = df_prepared.dropna(subset=["Mes"])

            # Filtrar registros válidos para análisis
            ruc_columns = [config["ruc_c_col"], config["ruc_p_col"]]
            mask_valid_ruc = df_prepared[ruc_columns].notna().any(axis=1)
            df_prepared = df_prepared[mask_valid_ruc]

            # Filtrar ejecutivos válidos
            df_prepared = df_prepared[df_prepared[config["ejecutivo_col"]].notna()]

            logger.info(
                f"🧹 DataFrame preparado: {len(df_prepared)} registros válidos de {len(self.df)} originales"
            )

            return df_prepared

        except Exception as e:
            logger.error(f"❌ Error preparando DataFrame: {str(e)}")
            return self.df

    def transform_nuevos_clientes_pagadores(
        self, inicio: str, fin: str, **column_config
    ) -> List[Dict[str, Any]]:
        """
        🔄 Transforma datos para análisis de nuevos clientes y pagadores.

        Args:
            inicio: Fecha de inicio (YYYY-MM-DD)
            fin: Fecha de fin (YYYY-MM-DD)
            **column_config: Configuración de columnas

        Returns:
            Lista de registros transformados
        """
        try:
            logger.info(f"🔄 Iniciando transformación - Período: {inicio} a {fin}")

            # Construir configuración de columnas
            config = self.build_column_config(**column_config)

            # Validar requisitos de entrada
            if not self.validate_input_requirements(config):
                logger.error("❌ Validación de requisitos falló")
                return []

            # Preparar DataFrame
            df_prepared = self.prepare_dataframe(config)

            if df_prepared.empty:
                logger.warning("⚠️ No hay datos válidos para procesar")
                return []

            # Actualizar data engine con datos preparados
            self.data_engine.df = df_prepared

            # Procesar nuevos clientes y pagadores
            processed_data = self.data_engine.process_nuevos_clientes_pagadores(
                inicio, fin, config
            )

            logger.info(
                f"🔄 Transformación completada: {len(processed_data)} registros"
            )

            return processed_data

        except Exception as e:
            logger.error(f"❌ Error en transformación: {str(e)}")
            return []

    def process_complete_flow(
        self, request_data: Dict[str, Any], **column_config
    ) -> NuevosClientesNuevosPagadoresResponseSchema:
        """
        🎯 Procesa el flujo completo con validación y transformación.

        Args:
            request_data: Datos de request
            **column_config: Configuración de columnas

        Returns:
            Respuesta validada y transformada
        """
        try:
            logger.info("🎯 Iniciando procesamiento completo")

            # Validar request inicial
            validated_request = self.validation_engine.validate_request_data(
                request_data
            )

            # Transformar datos
            processed_data = self.transform_nuevos_clientes_pagadores(
                validated_request.inicio, validated_request.fin, **column_config
            )

            # Validar y crear respuesta final
            response = self.validation_engine.validate_complete_flow(
                request_data, self.df, processed_data
            )

            logger.info(
                f"🎯 Procesamiento completo exitoso - {response.metadata.total_registros} registros"
            )

            return response

        except Exception as e:
            logger.error(f"❌ Error en procesamiento completo: {str(e)}")
            raise

    def get_processing_summary(self) -> Dict[str, Any]:
        """
        📊 Obtiene resumen del estado actual del transformer.

        Returns:
            Diccionario con resumen de procesamiento
        """
        try:
            summary = {
                "total_registros_fuente": len(self.df),
                "columnas_disponibles": (
                    list(self.df.columns) if not self.df.empty else []
                ),
                "tipos_operacion_disponibles": [],
                "ejecutivos_disponibles": [],
                "rango_temporal": {"min": None, "max": None},
            }

            if not self.df.empty:
                # Obtener tipos de operación únicos
                if "TipoOperacion" in self.df.columns:
                    summary["tipos_operacion_disponibles"] = (
                        self.df["TipoOperacion"].dropna().unique().tolist()
                    )

                # Obtener ejecutivos únicos
                if "Ejecutivo" in self.df.columns:
                    summary["ejecutivos_disponibles"] = (
                        self.df["Ejecutivo"].dropna().unique().tolist()
                    )

                # Obtener rango temporal
                if "Mes" in self.df.columns:
                    fechas = pd.to_datetime(self.df["Mes"], errors="coerce").dropna()
                    if not fechas.empty:
                        summary["rango_temporal"] = {
                            "min": fechas.min().strftime("%Y-%m-%d"),
                            "max": fechas.max().strftime("%Y-%m-%d"),
                        }

            logger.debug(
                f"📊 Resumen generado: {summary['total_registros_fuente']} registros"
            )

            return summary

        except Exception as e:
            logger.error(f"❌ Error generando resumen: {str(e)}")
            return {"total_registros_fuente": 0, "error": str(e)}
