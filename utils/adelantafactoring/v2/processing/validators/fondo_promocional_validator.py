"""
✅ FondoPromocional Validator V2 - Validación especializada

Validador dedicado para reglas de negocio de FondoPromocional
"""

import pandas as pd
from typing import List, Dict, Any

# Importaciones absolutas y seguras
try:
    from utils.adelantafactoring.v2.schemas.fondo_promocional_schema import (
        FondoPromocionalSchema,
    )
    from utils.adelantafactoring.v2.schemas.base_schema import ValidationResult
except ImportError:
    # Fallback para desarrollo
    class FondoPromocionalSchema:
        model_fields = {"Liquidacion": None}

        def __init__(self, **kwargs):
            pass

        def model_dump(self):
            return {}

    class ValidationResult:
        def __init__(self, **kwargs):
            pass


from config.logger import logger


class FondoPromocionalValidator:
    """Validador especializado para FondoPromocional"""

    def __init__(self):
        self.schema = FondoPromocionalSchema

    def validate_raw_data(self, raw_data: List[Dict[str, Any]]) -> ValidationResult:
        """
        Valida datos crudos contra reglas de negocio

        Args:
            raw_data: Datos crudos a validar

        Returns:
            ValidationResult con resultado de validación
        """
        result = ValidationResult(is_valid=True, processed_count=len(raw_data))

        if not raw_data:
            result.add_warning("Lista de datos vacía")
            return result

        try:
            # Validar estructura básica
            self._validate_basic_structure(raw_data, result)

            # Validar cada registro
            valid_records = 0
            for i, record in enumerate(raw_data):
                if self._validate_single_record(record, i, result):
                    valid_records += 1

                # Límite de errores para evitar spam
                if len(result.errors) > 50:
                    result.add_warning("Demasiados errores, truncando validación")
                    break

            if valid_records == 0:
                result.add_error("No se encontraron registros válidos")
            else:
                result.add_warning(
                    f"Registros válidos: {valid_records}/{len(raw_data)}"
                )

            logger.info(
                f"Validación completada: {valid_records}/{len(raw_data)} registros válidos"
            )

        except Exception as e:
            result.add_error(f"Error durante validación: {str(e)}")
            logger.error(f"Error en validación: {e}")

        return result

    def validate_dataframe(self, df: pd.DataFrame) -> ValidationResult:
        """
        Valida DataFrame procesado

        Args:
            df: DataFrame a validar

        Returns:
            ValidationResult con resultado de validación
        """
        result = ValidationResult(is_valid=True, processed_count=len(df))

        if df.empty:
            result.add_warning("DataFrame vacío")
            return result

        try:
            # Validar columnas requeridas
            required_columns = ["CodigoLiquidacion"]
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                result.add_error(f"Faltan columnas requeridas: {missing_columns}")
                return result

            # Validar contenido
            self._validate_dataframe_content(df, result)

            logger.info(f"Validación DataFrame completada: {len(df)} registros")

        except Exception as e:
            result.add_error(f"Error validando DataFrame: {str(e)}")
            logger.error(f"Error en validación DataFrame: {e}")

        return result

    def validate_with_schema(
        self, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Valida registros usando schema Pydantic y retorna solo los válidos

        Args:
            records: Lista de registros a validar

        Returns:
            Lista de registros válidos
        """
        valid_records = []
        error_count = 0

        for i, record in enumerate(records):
            try:
                # Mapear campos para compatibilidad
                if "LIQUIDACION" in record and "Liquidacion" not in record:
                    record["Liquidacion"] = record["LIQUIDACION"]

                validated_record = self.schema(**record)
                valid_records.append(validated_record.model_dump())

            except Exception as e:
                error_count += 1
                if error_count <= 10:  # Solo loggear primeros 10 errores
                    logger.warning(f"Registro {i} inválido: {e}")

                if error_count > 100:
                    logger.warning("Demasiados errores de validación, truncando")
                    break

        if error_count > 0:
            logger.warning(f"Se encontraron {error_count} errores de validación")

        logger.info(
            f"Validación schema completada: {len(valid_records)}/{len(records)} válidos"
        )
        return valid_records

    def _validate_basic_structure(
        self, raw_data: List[Dict[str, Any]], result: ValidationResult
    ) -> None:
        """Valida estructura básica de los datos"""
        if not isinstance(raw_data, list):
            result.add_error(f"Se esperaba lista, recibido: {type(raw_data)}")
            return

        if not raw_data:
            result.add_warning("Lista de datos vacía")
            return

        # Verificar que todos los elementos sean diccionarios
        non_dict_count = sum(1 for item in raw_data if not isinstance(item, dict))
        if non_dict_count > 0:
            result.add_error(
                f"Se encontraron {non_dict_count} elementos que no son diccionarios"
            )

    def _validate_single_record(
        self, record: Dict[str, Any], index: int, result: ValidationResult
    ) -> bool:
        """Valida un solo registro"""
        if not isinstance(record, dict):
            result.add_error(f"Registro {index}: no es un diccionario")
            return False

        # Verificar campo LIQUIDACION (caso insensitive)
        liquidacion_fields = [
            key for key in record.keys() if key.upper() == "LIQUIDACION"
        ]

        if not liquidacion_fields:
            result.add_error(f"Registro {index}: falta campo LIQUIDACION")
            return False

        liquidacion_value = record[liquidacion_fields[0]]

        if liquidacion_value is None or str(liquidacion_value).strip() == "":
            result.add_error(f"Registro {index}: campo LIQUIDACION vacío")
            return False

        return True

    def _validate_dataframe_content(
        self, df: pd.DataFrame, result: ValidationResult
    ) -> None:
        """Valida contenido del DataFrame"""
        # Verificar valores nulos en CodigoLiquidacion
        null_count = df["CodigoLiquidacion"].isnull().sum()
        if null_count > 0:
            result.add_warning(
                f"Se encontraron {null_count} valores nulos en CodigoLiquidacion"
            )

        # Verificar valores vacíos
        empty_count = (df["CodigoLiquidacion"] == "").sum()
        if empty_count > 0:
            result.add_warning(
                f"Se encontraron {empty_count} valores vacíos en CodigoLiquidacion"
            )

        # Verificar duplicados
        duplicates = df["CodigoLiquidacion"].duplicated().sum()
        if duplicates > 0:
            result.add_warning(
                f"Se encontraron {duplicates} duplicados en CodigoLiquidacion"
            )
