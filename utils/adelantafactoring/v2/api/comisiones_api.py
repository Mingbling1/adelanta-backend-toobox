"""
🌐 Comisiones API V2 - Interfaz pública simple

API simplificada:
- af.comisiones.calculate()
- af.comisiones.get_summary()
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Union
from io import BytesIO
from config.logger import logger


# Importaciones absolutas con fallback para compatibilidad
try:
    from utils.adelantafactoring.v2.io.webservice.comisiones_client import (
        ComisionesWebserviceClient,
    )
    from utils.adelantafactoring.v2.processing.transformers.comisiones_transformer import (
        ComisionesTransformer,
    )
    from utils.adelantafactoring.v2.processing.validators.comisiones_validator import (
        ComisionesValidator,
    )
    from utils.adelantafactoring.v2.schemas.comisiones_schema import (
        ComisionesSchema,
        RegistroComisionSchema,
    )
except ImportError:
    # Fallback para compatibilidad durante desarrollo
    logger.warning("Módulos Comisiones V2 no disponibles, usando fallbacks")
    ComisionesWebserviceClient = None
    ComisionesTransformer = None
    ComisionesValidator = None
    ComisionesSchema = None
    RegistroComisionSchema = None


class ComisionesAPI:
    """API pública para Comisiones con compatibilidad v1"""

    def __init__(self):
        self._client = None
        self._transformer = None
        self._validator = None

    @property
    def client(self):
        """Lazy loading del cliente webservice"""
        if self._client is None and ComisionesWebserviceClient:
            self._client = ComisionesWebserviceClient()
        return self._client

    @property
    def transformer(self):
        """Lazy loading del transformer"""
        if self._transformer is None and ComisionesTransformer:
            self._transformer = ComisionesTransformer()
        return self._transformer

    @property
    def validator(self):
        """Lazy loading del validator"""
        if self._validator is None and ComisionesValidator:
            self._validator = ComisionesValidator()
        return self._validator

    def calculate(
        self,
        kpi_df: Optional[pd.DataFrame] = None,
        start_date: str = "2023-01-01",
        end_date: str = "2024-05-31",
        **kwargs,
    ) -> Union[BytesIO, List[Dict[str, Any]]]:
        """
        Calcula comisiones con el nuevo pipeline V2

        Args:
            kpi_df: DataFrame con datos KPI base (opcional, se obtiene del webservice si no se proporciona)
            start_date: Fecha inicial en formato YYYY-MM-DD
            end_date: Fecha final en formato YYYY-MM-DD
            **kwargs: Argumentos adicionales

        Returns:
            BytesIO con archivo ZIP de reportes o lista de datos procesados
        """
        try:
            # Validar disponibilidad de componentes V2
            if not all([self.client, self.transformer, self.validator]):
                raise ImportError("Componentes V2 no disponibles")

            # 1. Obtener datos base
            if kpi_df is None:
                kpi_df = self.client.get_kpi_data()

            if kpi_df.empty:
                logger.warning("No hay datos KPI disponibles")
                return []

            # 2. Obtener datos complementarios
            referidos_df = self.client.get_referidos_data()
            fondos_data = self.client.get_fondos_data()

            # 3. Enriquecer con referidos
            enriched_df = self.transformer.enrich_with_referidos(kpi_df, referidos_df)

            # 4. Aplicar costos de fondos especiales
            processed_df = self.transformer.apply_fondos_especiales_costs(
                enriched_df,
                fondos_data.get("fondo_crecer", pd.DataFrame()),
                fondos_data.get("fondo_promocional", pd.DataFrame()),
            )

            # 5. Filtrar por rango de fechas
            filtered_df = self.transformer.filter_by_date_range(
                processed_df, start_date, end_date
            )

            # 6. Normalizar nombres de ejecutivos
            normalized_df = self.transformer.normalize_ejecutivo_names(filtered_df)

            # 7. Clasificar operaciones
            classified_df = self.transformer.classify_operations(normalized_df)

            # 8. Validar datos finales
            validated_data = self.validator.validate_comisiones_bulk(classified_df)

            # 9. Generar reporte ZIP (si se requiere formato completo)
            if kwargs.get("return_zip", True):
                detalle_df = pd.DataFrame(validated_data)
                zip_report = self.transformer.create_zip_report(
                    classified_df, detalle_df, end_date
                )
                return zip_report

            return validated_data

        except Exception as e:
            logger.error(f"Error en cálculo de comisiones V2: {e}")
            # Fallback a V1 si está disponible
            return self._fallback_to_v1(kpi_df, start_date, end_date, **kwargs)

    async def calculate_async(
        self,
        kpi_df: Optional[pd.DataFrame] = None,
        start_date: str = "2023-01-01",
        end_date: str = "2024-05-31",
        **kwargs,
    ) -> Union[BytesIO, List[Dict[str, Any]]]:
        """Versión async del cálculo de comisiones"""
        try:
            # Para la versión async, podríamos usar el cliente async
            if self.client and hasattr(self.client, "get_kpi_data_async"):
                if kpi_df is None:
                    kpi_df = await self.client.get_kpi_data_async()

                # El resto del procesamiento es igual al sync
                return self.calculate(kpi_df, start_date, end_date, **kwargs)
            else:
                # Fallback a versión sync
                return self.calculate(kpi_df, start_date, end_date, **kwargs)

        except Exception as e:
            logger.error(f"Error en cálculo async de comisiones V2: {e}")
            return self._fallback_to_v1(kpi_df, start_date, end_date, **kwargs)

    def get_summary(
        self, kpi_df: Optional[pd.DataFrame] = None, fecha_corte: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Obtiene resumen de comisiones

        Args:
            kpi_df: DataFrame con datos KPI
            fecha_corte: Fecha de corte para el resumen

        Returns:
            Diccionario con resumen de comisiones
        """
        try:
            if not self.client:
                raise ImportError("Cliente V2 no disponible")

            if kpi_df is None:
                kpi_df = self.client.get_kpi_data(fecha_corte)

            if kpi_df.empty:
                return {"total_registros": 0, "ejecutivos": [], "montos": {}}

            # Procesar datos básicos
            if self.transformer:
                normalized_df = self.transformer.normalize_ejecutivo_names(kpi_df)
            else:
                normalized_df = kpi_df

            # Generar resumen
            summary = {
                "total_registros": len(normalized_df),
                "ejecutivos": normalized_df.get("Ejecutivo", pd.Series())
                .unique()
                .tolist(),
                "periodo": {
                    "fecha_inicio": normalized_df.get(
                        "FechaOperacion", pd.Series()
                    ).min(),
                    "fecha_fin": normalized_df.get("FechaOperacion", pd.Series()).max(),
                },
                "montos": {
                    "total_desembolso": normalized_df.get(
                        "MontoDesembolso", pd.Series()
                    ).sum(),
                    "total_comisiones": normalized_df.get(
                        "MontoComision", pd.Series(0)
                    ).sum(),
                },
            }

            return summary

        except Exception as e:
            logger.error(f"Error obteniendo resumen de comisiones V2: {e}")
            return {"error": str(e), "total_registros": 0}

    def _fallback_to_v1(self, kpi_df, start_date, end_date, **kwargs):
        """Fallback a implementación V1"""
        try:
            # Importar V1 dinámicamente con ruta absoluta
            from utils.adelantafactoring.calculos.ComisionesCalcular import (
                ComisionesCalcular,
            )

            logger.info("Usando fallback a ComisionesCalcular V1")

            if kpi_df is None:
                # Si no hay kpi_df, necesitamos crearlo vacío
                kpi_df = pd.DataFrame()

            calcular_v1 = ComisionesCalcular(kpi_df)
            return calcular_v1.calcular(start_date, end_date)

        except Exception as e:
            logger.error(f"Error en fallback V1: {e}")
            # Último recurso: datos vacíos
            return []


# Instancia global para API simple
_comisiones_api = ComisionesAPI()


# Funciones públicas para compatibilidad
def calculate(
    kpi_df: Optional[pd.DataFrame] = None,
    start_date: str = "2023-01-01",
    end_date: str = "2024-05-31",
    **kwargs,
) -> Union[BytesIO, List[Dict[str, Any]]]:
    """Función pública para calcular comisiones"""
    return _comisiones_api.calculate(kpi_df, start_date, end_date, **kwargs)


async def calculate_async(
    kpi_df: Optional[pd.DataFrame] = None,
    start_date: str = "2023-01-01",
    end_date: str = "2024-05-31",
    **kwargs,
) -> Union[BytesIO, List[Dict[str, Any]]]:
    """Función pública async para calcular comisiones"""
    return await _comisiones_api.calculate_async(kpi_df, start_date, end_date, **kwargs)


def get_summary(
    kpi_df: Optional[pd.DataFrame] = None, fecha_corte: Optional[str] = None
) -> Dict[str, Any]:
    """Función pública para obtener resumen de comisiones"""
    return _comisiones_api.get_summary(kpi_df, fecha_corte)
