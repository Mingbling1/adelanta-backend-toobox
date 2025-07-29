"""
📊 Data Engine V2 - Nuevos Clientes Nuevos Pagadores

Engine especializado para procesamiento de análisis de nuevos clientes y pagadores.
Arquitectura hexagonal pura sin dependencias legacy.
"""

import pandas as pd
from typing import Dict, Set, List, Any

from config.logger import logger
from ...core.base import BaseObtenerV2


class NuevosClientesNuevosPagadoresDataEngine(BaseObtenerV2):
    """
    🔄 Engine para procesamiento de datos de nuevos clientes y pagadores.

    Analiza patrones temporales de nuevos clientes y pagadores por ejecutivo y tipo de operación.
    """

    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self.df = df.copy() if not df.empty else pd.DataFrame()
        self._validated_config = None

    def validate_dataframe_structure(self, config: Dict[str, str]) -> None:
        """
        ✅ Valida que el DataFrame tenga las columnas requeridas.

        Args:
            config: Diccionario con configuración de columnas

        Raises:
            ValueError: Si faltan columnas requeridas
        """
        try:
            required_columns = [
                config["ruc_c_col"],
                config["ruc_p_col"],
                config["ruc_c_ns_col"],
                config["ruc_p_ns_col"],
                config["ejecutivo_col"],
                config["type_op_col"],
                "Mes",  # Columna requerida para análisis temporal
            ]

            missing_columns = [
                col for col in required_columns if col not in self.df.columns
            ]

            if missing_columns:
                raise ValueError(
                    f"Faltan columnas requeridas en el DataFrame: {missing_columns}"
                )

            logger.info(
                f"✅ DataFrame validado: {len(self.df)} registros, columnas requeridas presentes"
            )

        except Exception as e:
            logger.error(f"❌ Error validando estructura del DataFrame: {str(e)}")
            raise

    def generate_date_range(self, start_date: str, end_date: str) -> List[str]:
        """
        📅 Genera rango de fechas mensuales para análisis.

        Args:
            start_date: Fecha inicio (YYYY-MM-DD)
            end_date: Fecha fin (YYYY-MM-DD)

        Returns:
            Lista de meses en formato YYYY-MM
        """
        try:
            dates = pd.date_range(start_date, end_date, freq="MS")
            months_years = dates.strftime("%Y-%m").tolist()

            logger.info(
                f"📅 Rango de fechas generado: {len(months_years)} meses ({months_years[0]} a {months_years[-1]})"
            )

            return months_years

        except Exception as e:
            logger.error(f"❌ Error generando rango de fechas: {str(e)}")
            return []

    def extract_unique_entities(
        self, config: Dict[str, str]
    ) -> Dict[str, Dict[str, str]]:
        """
        🔍 Extrae entidades únicas (clientes y pagadores) con sus razones sociales.

        Args:
            config: Configuración de columnas

        Returns:
            Diccionario con mapeos RUC -> Razón Social
        """
        try:
            # Extraer clientes únicos
            ruc_cedente = self.df[
                [config["ruc_c_col"], config["ruc_c_ns_col"]]
            ].drop_duplicates()
            ruc_cedente = ruc_cedente.dropna(subset=[config["ruc_c_col"]])

            # Extraer pagadores únicos
            ruc_pagador = self.df[
                [config["ruc_p_col"], config["ruc_p_ns_col"]]
            ].drop_duplicates()
            ruc_pagador = ruc_pagador.dropna(subset=[config["ruc_p_col"]])

            # Crear mapeos
            ruc_cedente_nombre_social = dict(
                zip(
                    ruc_cedente[config["ruc_c_col"]],
                    ruc_cedente[config["ruc_c_ns_col"]],
                )
            )
            ruc_pagador_nombre_social = dict(
                zip(
                    ruc_pagador[config["ruc_p_col"]],
                    ruc_pagador[config["ruc_p_ns_col"]],
                )
            )

            logger.info(
                f"🔍 Entidades extraídas - Clientes: {len(ruc_cedente_nombre_social)}, Pagadores: {len(ruc_pagador_nombre_social)}"
            )

            return {
                "clientes": ruc_cedente_nombre_social,
                "pagadores": ruc_pagador_nombre_social,
            }

        except Exception as e:
            logger.error(f"❌ Error extrayendo entidades únicas: {str(e)}")
            return {"clientes": {}, "pagadores": {}}

    def analyze_nuevos_clientes_pagadores(
        self,
        months_years: List[str],
        config: Dict[str, str],
        entity_mappings: Dict[str, Dict[str, str]],
    ) -> List[Dict[str, Any]]:
        """
        📊 Analiza nuevos clientes y pagadores por período temporal.

        Args:
            months_years: Lista de meses a analizar
            config: Configuración de columnas
            entity_mappings: Mapeos de entidades

        Returns:
            Lista de registros con nuevos clientes/pagadores
        """
        try:
            unique_keys = []
            rucs_nuevos_set: Set[str] = set()
            rucs_pagador_nuevos_set: Set[str] = set()

            tipos_operacion = ["Factoring", "Confirming", "Capital de Trabajo"]

            for month in months_years:
                df_month = self.df[self.df["Mes"] == month]

                if df_month.empty:
                    logger.debug(f"⚠️ No hay datos para el mes {month}")
                    continue

                ejecutivos_unicos = df_month[config["ejecutivo_col"]].dropna().unique()

                for ejecutivo in ejecutivos_unicos:
                    for tipo_operacion in tipos_operacion:
                        # Analizar nuevos clientes (Factoring)
                        if tipo_operacion == "Factoring":
                            nuevos_clientes = self._analyze_nuevos_clientes(
                                df_month,
                                month,
                                ejecutivo,
                                tipo_operacion,
                                config,
                                entity_mappings["clientes"],
                                rucs_nuevos_set,
                            )
                            unique_keys.extend(nuevos_clientes)

                        # Analizar nuevos pagadores (todos los tipos)
                        nuevos_pagadores = self._analyze_nuevos_pagadores(
                            df_month,
                            month,
                            ejecutivo,
                            tipo_operacion,
                            config,
                            entity_mappings["pagadores"],
                            rucs_pagador_nuevos_set,
                        )
                        unique_keys.extend(nuevos_pagadores)

            logger.info(
                f"📊 Análisis completado: {len(unique_keys)} registros generados"
            )

            return unique_keys

        except Exception as e:
            logger.error(f"❌ Error en análisis de nuevos clientes/pagadores: {str(e)}")
            return []

    def _analyze_nuevos_clientes(
        self,
        df_month: pd.DataFrame,
        month: str,
        ejecutivo: str,
        tipo_operacion: str,
        config: Dict[str, str],
        clientes_mapping: Dict[str, str],
        rucs_nuevos_set: Set[str],
    ) -> List[Dict[str, Any]]:
        """
        👥 Analiza nuevos clientes para Factoring.
        """
        try:
            nuevos_clientes = []

            # Obtener RUCs de clientes para este ejecutivo y tipo de operación
            clientes_mes = set(
                df_month[
                    (df_month[config["ejecutivo_col"]] == ejecutivo)
                    & (df_month[config["type_op_col"]] == tipo_operacion)
                ][config["ruc_c_col"]].dropna()
            )

            # Identificar nuevos clientes
            nuevos_ruc_clientes = clientes_mes - rucs_nuevos_set

            for ruc_cliente in nuevos_ruc_clientes:
                nombre_social = clientes_mapping.get(str(ruc_cliente), "")

                nuevos_clientes.append(
                    {
                        "Mes": month,
                        "Ejecutivo": ejecutivo,
                        "RUCCliente": str(ruc_cliente),
                        "RUCPagador": None,
                        "TipoOperacion": tipo_operacion,
                        "RazonSocial": nombre_social,
                    }
                )

                rucs_nuevos_set.add(ruc_cliente)

            return nuevos_clientes

        except Exception as e:
            logger.warning(f"⚠️ Error analizando nuevos clientes: {str(e)}")
            return []

    def _analyze_nuevos_pagadores(
        self,
        df_month: pd.DataFrame,
        month: str,
        ejecutivo: str,
        tipo_operacion: str,
        config: Dict[str, str],
        pagadores_mapping: Dict[str, str],
        rucs_pagador_nuevos_set: Set[str],
    ) -> List[Dict[str, Any]]:
        """
        💰 Analiza nuevos pagadores para todos los tipos de operación.
        """
        try:
            nuevos_pagadores = []

            # Obtener RUCs de pagadores para este ejecutivo y tipo de operación
            pagadores_mes = set(
                df_month[
                    (df_month[config["ejecutivo_col"]] == ejecutivo)
                    & (df_month[config["type_op_col"]] == tipo_operacion)
                ][config["ruc_p_col"]].dropna()
            )

            # Identificar nuevos pagadores
            nuevos_ruc_pagadores = pagadores_mes - rucs_pagador_nuevos_set

            for ruc_pagador in nuevos_ruc_pagadores:
                nombre_social = pagadores_mapping.get(str(ruc_pagador), "")

                nuevos_pagadores.append(
                    {
                        "Mes": month,
                        "Ejecutivo": ejecutivo,
                        "RUCCliente": None,
                        "RUCPagador": str(ruc_pagador),
                        "TipoOperacion": tipo_operacion,
                        "RazonSocial": nombre_social,
                    }
                )

                rucs_pagador_nuevos_set.add(ruc_pagador)

            return nuevos_pagadores

        except Exception as e:
            logger.warning(f"⚠️ Error analizando nuevos pagadores: {str(e)}")
            return []

    async def obtener_json(self) -> List[Dict[str, Any]]:
        """
        📥 Implementación del método base para compatibilidad.

        Returns:
            Lista vacía ya que este engine requiere configuración específica
        """
        logger.warning("⚠️ obtener_json llamado sin configuración específica")
        return []

    def process_nuevos_clientes_pagadores(
        self, start_date: str, end_date: str, config: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """
        🎯 Método principal para procesar nuevos clientes y pagadores.

        Args:
            start_date: Fecha inicio
            end_date: Fecha fin
            config: Configuración de columnas

        Returns:
            Lista de registros procesados
        """
        try:
            logger.info(
                f"🚀 Iniciando procesamiento de nuevos clientes/pagadores: {start_date} a {end_date}"
            )

            # Validar estructura
            self.validate_dataframe_structure(config)

            # Generar rango de fechas
            months_years = self.generate_date_range(start_date, end_date)

            if not months_years:
                logger.warning("⚠️ No se generó rango de fechas válido")
                return []

            # Extraer entidades únicas
            entity_mappings = self.extract_unique_entities(config)

            # Analizar nuevos clientes y pagadores
            processed_data = self.analyze_nuevos_clientes_pagadores(
                months_years, config, entity_mappings
            )

            logger.info(f"✅ Procesamiento completado: {len(processed_data)} registros")

            return processed_data

        except Exception as e:
            logger.error(f"❌ Error en procesamiento principal: {str(e)}")
            return []
