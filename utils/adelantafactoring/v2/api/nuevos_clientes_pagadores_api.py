"""
🌐 API V2 - Nuevos Clientes Nuevos Pagadores

Capa API para manejo de nuevos clientes y pagadores en arquitectura hexagonal.
"""

import pandas as pd
from typing import Dict, Any, Optional

from config.logger import logger
from ..core.base import BaseCalcularV2
from ..processing.nuevos_clientes_pagadores import (
    NuevosClientesNuevosPagadoresTransformer,
)
from ..schemas.nuevos_clientes_pagadores import (
    NuevosClientesNuevosPagadoresResponseSchema,
)


class NuevosClientesNuevosPagadoresCalcularV2(BaseCalcularV2):
    """
    🎯 Calculator V2 para análisis de nuevos clientes y pagadores.

    Arquitectura hexagonal pura sin dependencias legacy.
    """

    def __init__(self, df: Optional[pd.DataFrame] = None):
        """
        Inicializa calculator con DataFrame opcional.

        Args:
            df: DataFrame opcional con datos fuente
        """
        super().__init__()
        self.df = df.copy() if df is not None and not df.empty else pd.DataFrame()
        self.transformer: Optional[NuevosClientesNuevosPagadoresTransformer] = None

        if not self.df.empty:
            self.transformer = NuevosClientesNuevosPagadoresTransformer(self.df)
            logger.info(f"🎯 Calculator V2 inicializado con {len(self.df)} registros")
        else:
            logger.info("🎯 Calculator V2 inicializado sin datos")

    def set_dataframe(self, df: pd.DataFrame) -> None:
        """
        🔄 Establece DataFrame para procesamiento.

        Args:
            df: DataFrame con datos fuente
        """
        try:
            self.df = df.copy() if not df.empty else pd.DataFrame()

            if not self.df.empty:
                self.transformer = NuevosClientesNuevosPagadoresTransformer(self.df)
                logger.info(f"🔄 DataFrame establecido: {len(self.df)} registros")
            else:
                self.transformer = None
                logger.warning("⚠️ DataFrame vacío establecido")

        except Exception as e:
            logger.error(f"❌ Error estableciendo DataFrame: {str(e)}")
            raise

    def validate_calculator_state(self) -> bool:
        """
        ✅ Valida que el calculator esté en estado válido para procesamiento.

        Returns:
            True si está válido, False en caso contrario
        """
        try:
            if self.df.empty:
                logger.error("❌ Calculator sin DataFrame")
                return False

            if self.transformer is None:
                logger.error("❌ Transformer no inicializado")
                return False

            # Validar columnas mínimas requeridas
            required_columns = [
                "RUCCliente",
                "RUCPagador",
                "Ejecutivo",
                "TipoOperacion",
                "Mes",
            ]
            missing_columns = [
                col for col in required_columns if col not in self.df.columns
            ]

            if missing_columns:
                logger.error(f"❌ Faltan columnas requeridas: {missing_columns}")
                return False

            logger.debug("✅ Calculator en estado válido")
            return True

        except Exception as e:
            logger.error(f"❌ Error validando estado del calculator: {str(e)}")
            return False

    def calcular_nuevos_clientes_pagadores(
        self,
        inicio: str,
        fin: str,
        ruc_c_col: str = "RUCCliente",
        ruc_p_col: str = "RUCPagador",
        ruc_c_ns_col: str = "RazonSocialCliente",
        ruc_p_ns_col: str = "RazonSocialPagador",
        ejecutivo_col: str = "Ejecutivo",
        type_op_col: str = "TipoOperacion",
    ) -> NuevosClientesNuevosPagadoresResponseSchema:
        """
        🚀 Calcula nuevos clientes y pagadores para el período especificado.

        Args:
            inicio: Fecha de inicio (YYYY-MM-DD)
            fin: Fecha de fin (YYYY-MM-DD)
            ruc_c_col: Nombre de columna RUC Cliente
            ruc_p_col: Nombre de columna RUC Pagador
            ruc_c_ns_col: Nombre de columna Razón Social Cliente
            ruc_p_ns_col: Nombre de columna Razón Social Pagador
            ejecutivo_col: Nombre de columna Ejecutivo
            type_op_col: Nombre de columna Tipo Operación

        Returns:
            Respuesta con análisis de nuevos clientes y pagadores

        Raises:
            ValueError: Si los parámetros son inválidos
            RuntimeError: Si hay errores en el procesamiento
        """
        try:
            logger.info(
                f"🚀 Iniciando cálculo de nuevos clientes/pagadores: {inicio} a {fin}"
            )

            # Validar estado del calculator
            if not self.validate_calculator_state():
                raise RuntimeError("Calculator en estado inválido")

            # Preparar datos de request
            request_data = {
                "inicio": inicio,
                "fin": fin,
                "columnas": {
                    "ruc_cliente": ruc_c_col,
                    "ruc_pagador": ruc_p_col,
                    "razon_social_cliente": ruc_c_ns_col,
                    "razon_social_pagador": ruc_p_ns_col,
                    "ejecutivo": ejecutivo_col,
                    "tipo_operacion": type_op_col,
                },
            }

            # Procesar usando transformer
            response = self.transformer.process_complete_flow(
                request_data=request_data,
                ruc_c_col=ruc_c_col,
                ruc_p_col=ruc_p_col,
                ruc_c_ns_col=ruc_c_ns_col,
                ruc_p_ns_col=ruc_p_ns_col,
                ejecutivo_col=ejecutivo_col,
                type_op_col=type_op_col,
            )

            logger.info(
                f"🚀 Cálculo completado: {response.metadata.total_registros} registros"
            )

            return response

        except Exception as e:
            logger.error(f"❌ Error en cálculo de nuevos clientes/pagadores: {str(e)}")
            raise

    async def calcular_json(self) -> Dict[str, Any]:
        """
        📥 Implementación del método base para compatibilidad legacy.

        Returns:
            Diccionario con mensaje de error
        """
        logger.warning(
            "⚠️ calcular_json llamado - use calcular_nuevos_clientes_pagadores"
        )
        return {
            "error": "Método deprecated - use calcular_nuevos_clientes_pagadores",
            "registros": [],
        }

    def get_processing_info(self) -> Dict[str, Any]:
        """
        📊 Obtiene información sobre el estado actual del procesamiento.

        Returns:
            Diccionario con información de procesamiento
        """
        try:
            info = {
                "calculator_inicializado": True,
                "tiene_dataframe": not self.df.empty,
                "total_registros": len(self.df),
                "transformer_disponible": self.transformer is not None,
                "estado_valido": self.validate_calculator_state(),
            }

            # Obtener resumen del transformer si está disponible
            if self.transformer is not None:
                transformer_summary = self.transformer.get_processing_summary()
                info.update({"transformer_info": transformer_summary})

            logger.debug(
                f"📊 Info de procesamiento: {info['total_registros']} registros"
            )

            return info

        except Exception as e:
            logger.error(f"❌ Error obteniendo info de procesamiento: {str(e)}")
            return {"error": str(e), "calculator_inicializado": False}

    def quick_analysis(self, inicio: str, fin: str) -> Dict[str, Any]:
        """
        ⚡ Análisis rápido con configuración por defecto.

        Args:
            inicio: Fecha de inicio (YYYY-MM-DD)
            fin: Fecha de fin (YYYY-MM-DD)

        Returns:
            Análisis simplificado
        """
        try:
            logger.info(f"⚡ Iniciando análisis rápido: {inicio} a {fin}")

            if not self.validate_calculator_state():
                return {"error": "Calculator en estado inválido"}

            # Usar configuración por defecto
            response = self.calcular_nuevos_clientes_pagadores(inicio, fin)

            # Simplificar respuesta
            quick_result = {
                "periodo": {"inicio": inicio, "fin": fin},
                "resumen": {
                    "total_registros": response.metadata.total_registros,
                    "nuevos_clientes": response.metadata.nuevos_clientes,
                    "nuevos_pagadores": response.metadata.nuevos_pagadores,
                    "ejecutivos_unicos": response.metadata.ejecutivos_unicos,
                    "tipos_operacion": response.metadata.tipos_operacion,
                },
                "tiene_errores": response.errores is not None
                and len(response.errores) > 0,
            }

            logger.info(
                f"⚡ Análisis rápido completado: {quick_result['resumen']['total_registros']} registros"
            )

            return quick_result

        except Exception as e:
            logger.error(f"❌ Error en análisis rápido: {str(e)}")
            return {"error": str(e)}

    @classmethod
    def create_from_dataframe(
        cls, df: pd.DataFrame
    ) -> "NuevosClientesNuevosPagadoresCalcularV2":
        """
        🏭 Factory method para crear calculator desde DataFrame.

        Args:
            df: DataFrame con datos fuente

        Returns:
            Instancia de calculator inicializada
        """
        try:
            calculator = cls(df)
            logger.info(f"🏭 Calculator creado desde DataFrame: {len(df)} registros")
            return calculator

        except Exception as e:
            logger.error(f"❌ Error creando calculator desde DataFrame: {str(e)}")
            raise
