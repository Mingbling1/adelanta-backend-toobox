"""
🌐 Fondos API V2 - Interfaz pública simple

API simplificada:
- af.fondos.get_promocional()
- af.fondos.get_crecer()
"""

import pandas as pd
from typing import List, Dict, Any, Union
from config.logger import logger


# Importaciones absolutas con fallback para compatibilidad
try:
    from utils.adelantafactoring.v2.io.webservice.fondo_promocional_client import (
        FondoPromocionalWebservice,
    )
    from utils.adelantafactoring.v2.processing.transformers.fondo_promocional_transformer import (
        FondoPromocionalTransformer,
    )
    from utils.adelantafactoring.v2.processing.validators.fondo_promocional_validator import (
        FondoPromocionalValidator,
    )
    from utils.adelantafactoring.v2.schemas.fondo_promocional_schema import (
        FondoPromocionalSchema,
    )

    # FondoCrecer imports
    from utils.adelantafactoring.v2.io.webservice.fondo_crecer_client import (
        FondoCrecerWebservice,
    )
    from utils.adelantafactoring.v2.processing.transformers.fondo_crecer_transformer import (
        FondoCrecerTransformer,
    )
    from utils.adelantafactoring.v2.processing.validators.fondo_crecer_validator import (
        FondoCrecerValidator,
    )
    from utils.adelantafactoring.v2.schemas.fondo_crecer_schema import FondoCrecerSchema
except ImportError:
    # Fallback para compatibilidad durante desarrollo
    logger.warning("Algunos módulos V2 no disponibles, usando fallbacks")
    FondoPromocionalWebservice = None
    FondoPromocionalTransformer = None
    FondoPromocionalValidator = None
    FondoPromocionalSchema = None

    FondoCrecerWebservice = None
    FondoCrecerTransformer = None
    FondoCrecerValidator = None
    FondoCrecerSchema = None


class FondoPromocionalAPI:
    """API pública para FondoPromocional con compatibilidad v1"""

    def __init__(self):
        self._webservice = None
        self._transformer = None
        self._validator = None
        self._initialize_components()

    def _initialize_components(self):
        """Inicializa componentes con fallback"""
        try:
            if FondoPromocionalWebservice:
                self._webservice = FondoPromocionalWebservice()
            if FondoPromocionalTransformer:
                self._transformer = FondoPromocionalTransformer()
            if FondoPromocionalValidator:
                self._validator = FondoPromocionalValidator()
        except Exception as e:
            logger.warning(f"Error inicializando componentes V2: {e}")

    def get_promocional(
        self, as_df: bool = False
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Obtiene datos de FondoPromocional procesados

        Args:
            as_df: Si True retorna DataFrame, si False retorna lista de diccionarios

        Returns:
            DataFrame o lista con datos de fondo promocional
        """
        try:
            logger.info("Iniciando obtención de datos FondoPromocional V2")

            # Si los componentes V2 no están disponibles, usar fallback v1
            if not all([self._webservice, self._transformer, self._validator]):
                return self._fallback_v1(as_df)

            # Obtener datos del webservice
            raw_data = self._webservice.fetch_fondo_promocional_data()

            if not raw_data:
                logger.warning("No se obtuvieron datos del webservice")
                return pd.DataFrame() if as_df else []

            # Validar datos crudos
            validation_result = self._validator.validate_raw_data(raw_data)
            if not validation_result.is_valid:
                logger.warning(f"Errores de validación: {validation_result.errors}")

            # Transformar datos
            df = self._transformer.transform_raw_data(raw_data)

            # Validar DataFrame final
            df_validation = self._validator.validate_dataframe(df)
            if not df_validation.is_valid:
                logger.warning(f"Errores en DataFrame final: {df_validation.errors}")

            if as_df:
                logger.info(f"Datos FondoPromocional obtenidos: {len(df)} registros")
                return df

            # Convertir a lista de diccionarios con validación schema
            records = self._transformer.transform_to_dict_list(df)
            if self._validator and FondoPromocionalSchema:
                records = self._validator.validate_with_schema(records)

            logger.info(f"Datos FondoPromocional obtenidos: {len(records)} registros")
            return records

        except Exception as e:
            logger.error(f"Error obteniendo datos FondoPromocional V2: {e}")
            # Fallback a v1 en caso de error
            return self._fallback_v1(as_df)

    async def get_promocional_async(
        self, as_df: bool = False
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Versión asíncrona de get_promocional

        Args:
            as_df: Si True retorna DataFrame, si False retorna lista de diccionarios

        Returns:
            DataFrame o lista con datos de fondo promocional
        """
        try:
            if not self._webservice:
                return self._fallback_v1(as_df)

            logger.info("Iniciando obtención async de datos FondoPromocional V2")

            # Obtener datos async del webservice
            raw_data = await self._webservice.fetch_fondo_promocional_data_async()

            if not raw_data:
                logger.warning("No se obtuvieron datos del webservice async")
                return pd.DataFrame() if as_df else []

            # Usar el mismo procesamiento que la versión síncrona
            if self._transformer:
                df = self._transformer.transform_raw_data(raw_data)

                if as_df:
                    return df

                records = self._transformer.transform_to_dict_list(df)
                if self._validator and FondoPromocionalSchema:
                    records = self._validator.validate_with_schema(records)

                return records

            return raw_data

        except Exception as e:
            logger.error(f"Error obteniendo datos FondoPromocional async V2: {e}")
            return self._fallback_v1(as_df)

    def _fallback_v1(
        self, as_df: bool = False
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """Fallback a implementación v1"""
        try:
            logger.info("Usando fallback v1 para FondoPromocional")

            # Import dinámico para evitar dependencias circulares
            from utils.adelantafactoring.calculos.FondoPromocionalCalcular import (
                FondoPromocionalCalcular,
            )

            calculator = FondoPromocionalCalcular()
            return calculator.calcular(as_df=as_df)

        except Exception as e:
            logger.error(f"Error en fallback v1: {e}")
            return pd.DataFrame() if as_df else []


class FondoCrecerAPI:
    """API pública para FondoCrecer con compatibilidad v1"""

    def __init__(self):
        self._webservice = None
        self._transformer = None
        self._validator = None
        self._initialize_components()

    def _initialize_components(self):
        """Inicializa componentes con fallback"""
        try:
            if FondoCrecerWebservice:
                self._webservice = FondoCrecerWebservice()
            if FondoCrecerTransformer:
                self._transformer = FondoCrecerTransformer()
            if FondoCrecerValidator:
                self._validator = FondoCrecerValidator()
        except Exception as e:
            logger.warning(f"Error inicializando componentes FondoCrecer V2: {e}")

    def get_crecer(
        self, as_df: bool = False
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Obtiene datos de FondoCrecer procesados

        Args:
            as_df: Si True retorna DataFrame, si False retorna lista de diccionarios

        Returns:
            DataFrame o lista con datos de fondo crecer
        """
        try:
            logger.info("Iniciando obtención de datos FondoCrecer V2")

            # Si los componentes V2 no están disponibles, usar fallback v1
            if not all([self._webservice, self._transformer, self._validator]):
                return self._fallback_v1(as_df)

            # Obtener datos del webservice
            raw_data = self._webservice.fetch_fondo_crecer_data()

            if not raw_data:
                logger.warning("No se obtuvieron datos del webservice FondoCrecer")
                return pd.DataFrame() if as_df else []

            # Validar datos crudos
            validation_result = self._validator.validate_raw_data(raw_data)
            if not validation_result.is_valid:
                logger.warning(
                    f"Errores de validación FondoCrecer: {validation_result.errors}"
                )

            # Transformar datos
            df = self._transformer.transform_raw_data(raw_data)

            # Validar DataFrame final
            df_validation = self._validator.validate_dataframe(df)
            if not df_validation.is_valid:
                logger.warning(
                    f"Errores en DataFrame final FondoCrecer: {df_validation.errors}"
                )

            if as_df:
                logger.info(f"Datos FondoCrecer obtenidos: {len(df)} registros")
                return df

            # Convertir a lista de diccionarios con validación schema
            records = self._transformer.transform_to_dict_list(df)
            if self._validator and FondoCrecerSchema:
                records = self._validator.validate_with_schema(records)

            logger.info(f"Datos FondoCrecer obtenidos: {len(records)} registros")
            return records

        except Exception as e:
            logger.error(f"Error obteniendo datos FondoCrecer V2: {e}")
            # Fallback a v1 en caso de error
            return self._fallback_v1(as_df)

    async def get_crecer_async(
        self, as_df: bool = False
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Versión asíncrona de get_crecer

        Args:
            as_df: Si True retorna DataFrame, si False retorna lista de diccionarios

        Returns:
            DataFrame o lista con datos de fondo crecer
        """
        try:
            if not self._webservice:
                return self._fallback_v1(as_df)

            logger.info("Iniciando obtención async de datos FondoCrecer V2")

            # Obtener datos async del webservice
            raw_data = await self._webservice.fetch_fondo_crecer_data_async()

            if not raw_data:
                logger.warning(
                    "No se obtuvieron datos del webservice async FondoCrecer"
                )
                return pd.DataFrame() if as_df else []

            # Usar el mismo procesamiento que la versión síncrona
            if self._transformer:
                df = self._transformer.transform_raw_data(raw_data)

                if as_df:
                    return df

                records = self._transformer.transform_to_dict_list(df)
                if self._validator and FondoCrecerSchema:
                    records = self._validator.validate_with_schema(records)

                return records

            return raw_data

        except Exception as e:
            logger.error(f"Error obteniendo datos FondoCrecer async V2: {e}")
            return self._fallback_v1(as_df)

    def _fallback_v1(
        self, as_df: bool = False
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """Fallback a implementación v1"""
        try:
            logger.info("Usando fallback v1 para FondoCrecer")

            # Import dinámico para evitar dependencias circulares
            from utils.adelantafactoring.calculos.FondoCrecerCalcular import (
                FondoCrecerCalcular,
            )

            calculator = FondoCrecerCalcular()
            return calculator.calcular(as_df=as_df)

        except Exception as e:
            logger.error(f"Error en fallback v1 FondoCrecer: {e}")
            return pd.DataFrame() if as_df else []


# Instancias globales para API simple
fondo_promocional_api = FondoPromocionalAPI()
fondo_crecer_api = FondoCrecerAPI()


# Funciones de conveniencia para API simple
def get_promocional(as_df: bool = False) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
    """Función de conveniencia: af.fondos.get_promocional()"""
    return fondo_promocional_api.get_promocional(as_df)


def get_crecer(as_df: bool = False) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
    """Función de conveniencia: af.fondos.get_crecer()"""
    return fondo_crecer_api.get_crecer(as_df)


async def get_promocional_async(
    as_df: bool = False,
) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
    """Función de conveniencia async: await af.fondos.get_promocional_async()"""
    return await fondo_promocional_api.get_promocional_async(as_df)


async def get_crecer_async(
    as_df: bool = False,
) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
    """Función de conveniencia async: await af.fondos.get_crecer_async()"""
    return await fondo_crecer_api.get_crecer_async(as_df)
