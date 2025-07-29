"""
✅ FondoCrecer Validator V2 - Validación especializada

Validador dedicado para reglas de negocio de FondoCrecer con garantías
"""

import pandas as pd
from typing import List, Dict, Any
from config.logger import logger


# Import absolutas con fallback para compatibilidad
try:
    from utils.adelantafactoring.v2.schemas.fondo_crecer_schema import FondoCrecerSchema
    from utils.adelantafactoring.v2.schemas.base_schema import ValidationResult
except ImportError:
    # Fallback simple
    class ValidationResult:
        def __init__(self, is_valid=True, processed_count=0):
            self.is_valid = is_valid
            self.errors = []
            self.warnings = []
            self.processed_count = processed_count

        def add_error(self, message):
            self.errors.append(message)
            self.is_valid = False

        def add_warning(self, message):
            self.warnings.append(message)

    FondoCrecerSchema = None


class FondoCrecerValidator:
    """Validador especializado para FondoCrecer"""

    def __init__(self):
        self.schema = FondoCrecerSchema

    def validate_raw_data(self, raw_data: List[Dict[str, Any]]) -> ValidationResult:
        """
        Valida datos crudos contra reglas de negocio de FondoCrecer

        Args:
            raw_data: Datos crudos a validar

        Returns:
            ValidationResult con resultado de validación
        """
        result = ValidationResult(is_valid=True, processed_count=len(raw_data))

        if not raw_data:
            result.add_warning("Lista de datos FondoCrecer vacía")
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
                    result.add_warning(
                        "Demasiados errores FondoCrecer, truncando validación"
                    )
                    break

            if valid_records == 0:
                result.add_error("No se encontraron registros válidos en FondoCrecer")
            else:
                result.add_warning(
                    f"Registros FondoCrecer válidos: {valid_records}/{len(raw_data)}"
                )

            logger.info(
                f"Validación FondoCrecer completada: {valid_records}/{len(raw_data)} registros válidos"
            )

        except Exception as e:
            result.add_error(f"Error durante validación FondoCrecer: {str(e)}")
            logger.error(f"Error en validación FondoCrecer: {e}")

        return result

    def validate_dataframe(self, df: pd.DataFrame) -> ValidationResult:
        """
        Valida DataFrame procesado de FondoCrecer

        Args:
            df: DataFrame a validar

        Returns:
            ValidationResult con resultado de validación
        """
        result = ValidationResult(is_valid=True, processed_count=len(df))

        if df.empty:
            result.add_warning("DataFrame FondoCrecer vacío")
            return result

        try:
            # Validar columnas requeridas
            required_columns = ["CodigoLiquidacion", "Garantia"]
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                result.add_error(
                    f"Faltan columnas requeridas en FondoCrecer: {missing_columns}"
                )
                return result

            # Validar contenido específico de FondoCrecer
            self._validate_dataframe_content(df, result)

            logger.info(
                f"Validación DataFrame FondoCrecer completada: {len(df)} registros"
            )

        except Exception as e:
            result.add_error(f"Error validando DataFrame FondoCrecer: {str(e)}")
            logger.error(f"Error en validación DataFrame FondoCrecer: {e}")

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
        if not self.schema:
            logger.warning(
                "Schema FondoCrecer no disponible, retornando datos sin validar"
            )
            return records

        valid_records = []
        error_count = 0

        for i, record in enumerate(records):
            try:
                # Mapear campos para compatibilidad
                mapped_record = self._map_record_fields(record)

                validated_record = self.schema(**mapped_record)
                valid_records.append(validated_record.model_dump())

            except Exception as e:
                error_count += 1
                if error_count <= 10:  # Solo loggear primeros 10 errores
                    logger.warning(f"Registro FondoCrecer {i} inválido: {e}")

                if error_count > 100:
                    logger.warning(
                        "Demasiados errores de validación FondoCrecer, truncando"
                    )
                    break

        if error_count > 0:
            logger.warning(
                f"Se encontraron {error_count} errores de validación FondoCrecer"
            )

        logger.info(
            f"Validación schema FondoCrecer completada: {len(valid_records)}/{len(records)} válidos"
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
            result.add_warning("Lista de datos FondoCrecer vacía")
            return

        # Verificar que todos los elementos sean diccionarios
        non_dict_count = sum(1 for item in raw_data if not isinstance(item, dict))
        if non_dict_count > 0:
            result.add_error(
                f"Se encontraron {non_dict_count} elementos que no son diccionarios en FondoCrecer"
            )

    def _validate_single_record(
        self, record: Dict[str, Any], index: int, result: ValidationResult
    ) -> bool:
        """Valida un solo registro de FondoCrecer"""
        if not isinstance(record, dict):
            result.add_error(f"Registro FondoCrecer {index}: no es un diccionario")
            return False

        # Verificar campo LIQUIDACION (caso insensitive)
        liquidacion_fields = [
            key for key in record.keys() if key.upper() == "LIQUIDACION"
        ]
        if not liquidacion_fields:
            result.add_error(f"Registro FondoCrecer {index}: falta campo LIQUIDACION")
            return False

        liquidacion_value = record[liquidacion_fields[0]]
        if liquidacion_value is None or str(liquidacion_value).strip() == "":
            result.add_error(f"Registro FondoCrecer {index}: campo LIQUIDACION vacío")
            return False

        # Verificar campo GARANTIA (caso insensitive)
        garantia_fields = [key for key in record.keys() if key.upper() == "GARANTIA"]
        if not garantia_fields:
            result.add_error(f"Registro FondoCrecer {index}: falta campo GARANTIA")
            return False

        garantia_value = record[garantia_fields[0]]
        if garantia_value is None or str(garantia_value).strip() == "":
            result.add_error(f"Registro FondoCrecer {index}: campo GARANTIA vacío")
            return False

        # Validar formato de garantía
        if not self._validate_garantia_format(garantia_value):
            result.add_error(
                f"Registro FondoCrecer {index}: formato de GARANTIA inválido: {garantia_value}"
            )
            return False

        return True

    def _validate_garantia_format(self, garantia_value: Any) -> bool:
        """Valida que el formato de garantía sea correcto"""
        try:
            value_str = str(garantia_value).strip()

            # Si termina en %, debe ser un número válido
            if value_str.endswith("%"):
                percentage = float(value_str.rstrip("%"))
                return 0 <= percentage <= 100
            else:
                # Debe ser un decimal entre 0 y 1
                decimal_val = float(value_str)
                return 0 <= decimal_val <= 1

        except (ValueError, TypeError):
            return False

    def _validate_dataframe_content(
        self, df: pd.DataFrame, result: ValidationResult
    ) -> None:
        """Valida contenido específico del DataFrame FondoCrecer"""
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

        # Validaciones específicas de Garantia
        if "Garantia" in df.columns:
            # Verificar valores nulos en Garantia
            garantia_nulls = df["Garantia"].isnull().sum()
            if garantia_nulls > 0:
                result.add_warning(
                    f"Se encontraron {garantia_nulls} valores nulos en Garantia"
                )

            # Verificar rango de garantías
            valid_guarantees = df["Garantia"].between(0, 1, inclusive="both")
            invalid_count = (~valid_guarantees).sum()
            if invalid_count > 0:
                result.add_warning(
                    f"Se encontraron {invalid_count} garantías fuera del rango válido (0-1)"
                )

    def _map_record_fields(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Mapea campos del registro para compatibilidad con schema"""
        mapped = record.copy()

        # Mapear LIQUIDACION a Liquidacion si es necesario
        for key in list(mapped.keys()):
            if key.upper() == "LIQUIDACION" and "Liquidacion" not in mapped:
                mapped["Liquidacion"] = mapped[key]
            elif key.upper() == "GARANTIA" and "Garantia" not in mapped:
                mapped["Garantia"] = mapped[key]

        return mapped
