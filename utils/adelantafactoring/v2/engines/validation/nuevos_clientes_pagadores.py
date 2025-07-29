"""
🛡️ Validation Engine V2 - Nuevos Clientes Nuevos Pagadores

Engine especializado para validación de datos de nuevos clientes y pagadores.
Arquitectura hexagonal pura sin dependencias legacy.
"""

import pandas as pd
from typing import Dict, List, Any, Tuple
from pydantic import ValidationError

from config.logger import logger
from ...core.base import BaseCalcularV2
from ...schemas.nuevos_clientes_pagadores import (
    NuevosClientesNuevosPagadoresRequestSchema,
    NuevosClientesNuevosPagadoresResponseSchema,
    ProcessingMetadata,
)


class NuevosClientesNuevosPagadoresValidationEngine(BaseCalcularV2):
    """
    🔍 Engine para validación y transformación de datos de nuevos clientes y pagadores.

    Valida estructura de datos, rangos temporales y realiza transformaciones seguras.
    """

    def __init__(self):
        super().__init__()
        self._processing_metadata = None

    def validate_request_data(
        self, request_data: Dict[str, Any]
    ) -> NuevosClientesNuevosPagadoresRequestSchema:
        """
        ✅ Valida datos de entrada usando Pydantic schema.

        Args:
            request_data: Datos de entrada a validar

        Returns:
            Schema validado

        Raises:
            ValidationError: Si los datos no son válidos
        """
        try:
            validated_request = NuevosClientesNuevosPagadoresRequestSchema(
                **request_data
            )

            logger.info(
                f"✅ Request validado - Rango: {validated_request.inicio} a {validated_request.fin}"
            )

            return validated_request

        except ValidationError as e:
            logger.error(f"❌ Error validando request: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"❌ Error inesperado validando request: {str(e)}")
            raise ValidationError(f"Error de validación: {str(e)}")

    def validate_dataframe_columns(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        🗂️ Valida que el DataFrame tenga las columnas mínimas requeridas.

        Args:
            df: DataFrame a validar

        Returns:
            Tupla (es_válido, columnas_faltantes)
        """
        try:
            required_columns = [
                "RUCCliente",
                "RUCPagador",
                "RazonSocialCliente",
                "RazonSocialPagador",
                "Ejecutivo",
                "TipoOperacion",
                "Mes",
            ]

            missing_columns = [col for col in required_columns if col not in df.columns]

            is_valid = len(missing_columns) == 0

            if is_valid:
                logger.info(
                    f"✅ DataFrame válido - {len(df)} registros, todas las columnas presentes"
                )
            else:
                logger.warning(
                    f"⚠️ DataFrame inválido - Faltan columnas: {missing_columns}"
                )

            return is_valid, missing_columns

        except Exception as e:
            logger.error(f"❌ Error validando columnas del DataFrame: {str(e)}")
            return False, ["Error de validación"]

    def validate_temporal_range(self, inicio: str, fin: str) -> Tuple[bool, str]:
        """
        📅 Valida que el rango temporal sea coherente.

        Args:
            inicio: Fecha de inicio (YYYY-MM-DD)
            fin: Fecha de fin (YYYY-MM-DD)

        Returns:
            Tupla (es_válido, mensaje)
        """
        try:
            fecha_inicio = pd.to_datetime(inicio)
            fecha_fin = pd.to_datetime(fin)

            if fecha_inicio > fecha_fin:
                return False, "Fecha de inicio no puede ser posterior a fecha de fin"

            # Validar que no sea un rango muy extenso (más de 5 años)
            diff_years = (fecha_fin - fecha_inicio).days / 365.25
            if diff_years > 5:
                return (
                    False,
                    f"Rango temporal muy extenso: {diff_years:.1f} años (máximo 5 años)",
                )

            logger.info(
                f"✅ Rango temporal válido: {inicio} a {fin} ({diff_years:.1f} años)"
            )

            return True, "Rango temporal válido"

        except Exception as e:
            error_msg = f"Error validando rango temporal: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return False, error_msg

    def validate_and_clean_ruc_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        🧹 Valida y limpia datos de RUC en el DataFrame.

        Args:
            df: DataFrame a limpiar

        Returns:
            DataFrame limpiado
        """
        try:
            df_clean = df.copy()

            # Limpiar y validar RUCs
            ruc_columns = ["RUCCliente", "RUCPagador"]

            for col in ruc_columns:
                if col in df_clean.columns:
                    # Convertir a string y limpiar
                    df_clean[col] = df_clean[col].astype(str).str.strip()

                    # Remover valores no válidos
                    df_clean[col] = df_clean[col].replace(["nan", "None", ""], pd.NA)

                    # Validar longitud de RUC (debe ser 11 dígitos)
                    mask_valid = df_clean[col].str.len() == 11
                    invalid_count = (~mask_valid & df_clean[col].notna()).sum()

                    if invalid_count > 0:
                        logger.warning(
                            f"⚠️ {col}: {invalid_count} RUCs con longitud inválida"
                        )

            # Limpiar razones sociales
            razon_columns = ["RazonSocialCliente", "RazonSocialPagador"]

            for col in razon_columns:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].astype(str).str.strip()
                    df_clean[col] = df_clean[col].replace(
                        ["nan", "None", ""], "Sin Razón Social"
                    )

            logger.info(f"🧹 Datos limpiados - {len(df_clean)} registros procesados")

            return df_clean

        except Exception as e:
            logger.error(f"❌ Error limpiando datos de RUC: {str(e)}")
            return df

    def validate_business_rules(
        self, processed_data: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        📋 Valida reglas de negocio específicas para nuevos clientes/pagadores.

        Args:
            processed_data: Datos procesados a validar

        Returns:
            Tupla (datos_válidos, errores_encontrados)
        """
        try:
            valid_records = []
            business_errors = []

            valid_tipos_operacion = {"Factoring", "Confirming", "Capital de Trabajo"}

            for record in processed_data:
                errors_in_record = []

                # Validar tipo de operación
                tipo_op = record.get("TipoOperacion")
                if tipo_op not in valid_tipos_operacion:
                    errors_in_record.append(f"Tipo de operación inválido: {tipo_op}")

                # Validar que tenga al menos un RUC válido
                ruc_cliente = record.get("RUCCliente")
                ruc_pagador = record.get("RUCPagador")

                if not ruc_cliente and not ruc_pagador:
                    errors_in_record.append("Registro sin RUC Cliente ni RUC Pagador")

                # Validar ejecutivo
                ejecutivo = record.get("Ejecutivo")
                if not ejecutivo or ejecutivo.strip() == "":
                    errors_in_record.append("Ejecutivo vacío")

                # Validar mes
                mes = record.get("Mes")
                if (
                    not mes or not isinstance(mes, str) or len(mes) != 7
                ):  # YYYY-MM format
                    errors_in_record.append(f"Formato de mes inválido: {mes}")

                if errors_in_record:
                    business_errors.extend(errors_in_record)
                else:
                    valid_records.append(record)

            logger.info(
                f"📋 Validación de negocio - Válidos: {len(valid_records)}, Errores: {len(business_errors)}"
            )

            return valid_records, business_errors

        except Exception as e:
            logger.error(f"❌ Error en validación de reglas de negocio: {str(e)}")
            return processed_data, [f"Error de validación: {str(e)}"]

    def generate_processing_metadata(
        self, processed_data: List[Dict[str, Any]]
    ) -> ProcessingMetadata:
        """
        📊 Genera metadatos de procesamiento.

        Args:
            processed_data: Datos procesados

        Returns:
            Metadatos de procesamiento
        """
        try:
            # Contar nuevos clientes y pagadores
            nuevos_clientes = [r for r in processed_data if r.get("RUCCliente")]
            nuevos_pagadores = [r for r in processed_data if r.get("RUCPagador")]

            # Obtener ejecutivos únicos
            ejecutivos_unicos = list(
                set(r.get("Ejecutivo") for r in processed_data if r.get("Ejecutivo"))
            )

            # Contar por tipo de operación
            tipos_operacion_count = {}
            for record in processed_data:
                tipo_op = record.get("TipoOperacion", "Unknown")
                tipos_operacion_count[tipo_op] = (
                    tipos_operacion_count.get(tipo_op, 0) + 1
                )

            metadata = ProcessingMetadata(
                total_registros=len(processed_data),
                nuevos_clientes=len(nuevos_clientes),
                nuevos_pagadores=len(nuevos_pagadores),
                ejecutivos_unicos=len(ejecutivos_unicos),
                tipos_operacion=tipos_operacion_count,
            )

            logger.info(
                f"📊 Metadatos generados - Total: {metadata.total_registros}, Clientes: {metadata.nuevos_clientes}, Pagadores: {metadata.nuevos_pagadores}"
            )

            return metadata

        except Exception as e:
            logger.error(f"❌ Error generando metadatos: {str(e)}")
            return ProcessingMetadata(
                total_registros=0,
                nuevos_clientes=0,
                nuevos_pagadores=0,
                ejecutivos_unicos=0,
                tipos_operacion={},
            )

    def validate_and_transform_response(
        self,
        processed_data: List[Dict[str, Any]],
        request_data: NuevosClientesNuevosPagadoresRequestSchema,
    ) -> NuevosClientesNuevosPagadoresResponseSchema:
        """
        🔄 Valida y transforma datos para respuesta final.

        Args:
            processed_data: Datos procesados
            request_data: Datos de request originales

        Returns:
            Schema de respuesta validado
        """
        try:
            # Validar reglas de negocio
            valid_data, business_errors = self.validate_business_rules(processed_data)

            # Generar metadatos
            metadata = self.generate_processing_metadata(valid_data)

            # Crear respuesta
            response = NuevosClientesNuevosPagadoresResponseSchema(
                registros=valid_data,
                metadata=metadata,
                errores=business_errors if business_errors else None,
            )

            logger.info(
                f"🔄 Respuesta transformada - {len(valid_data)} registros válidos"
            )

            return response

        except ValidationError as e:
            logger.error(f"❌ Error de validación en respuesta: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"❌ Error transformando respuesta: {str(e)}")
            raise ValidationError(f"Error en transformación: {str(e)}")

    async def calcular_json(self) -> List[Dict[str, Any]]:
        """
        📥 Implementación del método base para compatibilidad.

        Returns:
            Lista vacía ya que este engine requiere datos específicos
        """
        logger.warning("⚠️ calcular_json llamado sin datos específicos")
        return []

    def validate_complete_flow(
        self,
        request_data: Dict[str, Any],
        df: pd.DataFrame,
        processed_data: List[Dict[str, Any]],
    ) -> NuevosClientesNuevosPagadoresResponseSchema:
        """
        🎯 Método principal para validación completa del flujo.

        Args:
            request_data: Datos de request
            df: DataFrame fuente
            processed_data: Datos procesados

        Returns:
            Respuesta validada
        """
        try:
            logger.info("🚀 Iniciando validación completa del flujo")

            # Validar request
            validated_request = self.validate_request_data(request_data)

            # Validar DataFrame
            df_valid, missing_cols = self.validate_dataframe_columns(df)
            if not df_valid:
                raise ValidationError(
                    f"DataFrame inválido - Faltan columnas: {missing_cols}"
                )

            # Validar rango temporal
            temporal_valid, temporal_msg = self.validate_temporal_range(
                validated_request.inicio, validated_request.fin
            )
            if not temporal_valid:
                raise ValidationError(temporal_msg)

            # Validar y transformar respuesta
            response = self.validate_and_transform_response(
                processed_data, validated_request
            )

            logger.info("✅ Validación completa exitosa")

            return response

        except Exception as e:
            logger.error(f"❌ Error en validación completa: {str(e)}")
            raise
