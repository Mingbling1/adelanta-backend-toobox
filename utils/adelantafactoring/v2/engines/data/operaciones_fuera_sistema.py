"""
📊 Data Engine V2 - Operaciones Fuera Sistema

Engine especializado para extraer datos de operaciones fuera del sistema desde webservices
con soporte para múltiples monedas (PEN/USD) y manejo robusto de errores.
Arquitectura hexagonal pura sin dependencias legacy.
"""

import pandas as pd
import asyncio
from tenacity import retry, stop_after_attempt, wait_fixed
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime

from config import logger

try:
    from utils.adelantafactoring.v2.config.settings import settings
except ImportError:
    # Fallback básico
    class _FallbackSettings:
        WEBSERVICE_ENDPOINTS = {
            "operaciones_fuera_sistema_pen": "https://webservice.adelantafactoring.com/webservice/consultas/operacionesfuerasistema/PEN",
            "operaciones_fuera_sistema_usd": "https://webservice.adelantafactoring.com/webservice/consultas/operacionesfuerasistema/USD",
        }

    settings = _FallbackSettings()
from ...io.webservice_client import BaseWebserviceClient
from ...core.base import BaseObtenerV2


class OperacionesFueraSistemaDataEngine(BaseWebserviceClient):
    """
    🔄 Engine para obtención de datos de operaciones fuera del sistema.

    Maneja múltiples endpoints (PEN/USD) con retry automático y validación robusta.
    """

    def __init__(self):
        super().__init__()
        self.pen_url = settings.WEBSERVICE_ENDPOINTS["operaciones_fuera_sistema_pen"]
        self.usd_url = settings.WEBSERVICE_ENDPOINTS["operaciones_fuera_sistema_usd"]
        self.column_mapping = self._get_column_mapping()

    def _get_column_mapping(self) -> Dict[str, str]:
        """
        🗺️ Mapeo de columnas desde webservice a schema normalizado.
        Mantiene compatibilidad con el sistema legacy.
        """
        return {
            "Liquidación": "CodigoLiquidacion",
            "LIQUIDACIÓN": "CodigoLiquidacion",  # Variación de caso
            "Liquidacion": "CodigoLiquidacion",
            "N° Factura/ Letra": "NroDocumento",
            "N° FACTURA/ LETRA": "NroDocumento",
            "Nombre Cliente": "RazonSocialCliente",
            "NOMBRE CLIENTE": "RazonSocialCliente",
            "RUC Cliente": "RUCCliente",
            "RUC CLIENTE": "RUCCliente",
            "Nombre Deudor": "RazonSocialPagador",
            "NOMBRE DEUDOR": "RazonSocialPagador",
            "RUC DEUDOR": "RUCPagador",
            "RUC Deudor": "RUCPagador",
            "TNM Op": "TasaNominalMensualPorc",
            "TNM OP": "TasaNominalMensualPorc",
            "% Finan": "FinanciamientoPorc",
            "% FINAN": "FinanciamientoPorc",
            "Fecha de Op": "FechaOperacion",
            "FECHA DE OP": "FechaOperacion",
            "F.Pago Confirmada": "FechaConfirmado",
            "F.PAGO CONFIRMADA": "FechaConfirmado",
            "Días Efect": "DiasEfectivo",
            "DÍAS EFECT": "DiasEfectivo",
            "Moneda": "Moneda",
            "MONEDA": "Moneda",
            "Neto Confirmado": "NetoConfirmado",
            "NETO CONFIRMADO": "NetoConfirmado",
            "Comisión de Estructuracion": "MontoComisionEstructuracion",
            "COMISIÓN DE ESTRUCTURACION": "MontoComisionEstructuracion",
            "IGV Comisión": "ComisionEstructuracionIGV",
            "IGV COMISIÓN": "ComisionEstructuracionIGV",
            "Comision Con IGV": "ComisionEstructuracionConIGV",
            "COMISION CON IGV": "ComisionEstructuracionConIGV",
            "Fondo Resguardo": "FondoResguardo",
            "FONDO RESGUARDO": "FondoResguardo",
            "Neto a Financiar": "MontoCobrar",
            "NETO A FINANCIAR": "MontoCobrar",
            "EJECUTIVO": "Ejecutivo",
            "Ejecutivo": "Ejecutivo",
            "TIPO DE OPERACIÓN": "TipoOperacion",
            "Tipo de Operación": "TipoOperacion",
            "TIPO DE OPERACION": "TipoOperacion",
        }

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def fetch_data_by_currency(self, currency: str) -> Optional[pd.DataFrame]:
        """
        🌍 Obtiene datos por moneda específica (PEN/USD) con retry automático.

        Args:
            currency: "PEN" o "USD"

        Returns:
            DataFrame con datos de la moneda especificada o None si falla
        """
        url = self.pen_url if currency == "PEN" else self.usd_url

        try:
            logger.info(f"🔄 Obteniendo datos de operaciones fuera sistema: {currency}")

            # Obtener datos del webservice
            raw_data = await self.get_data_async(url)

            if not raw_data:
                logger.warning(f"⚠️ No se obtuvieron datos para {currency}")
                return None

            # Crear DataFrame
            df = pd.DataFrame(raw_data)

            if df.empty:
                logger.warning(f"⚠️ DataFrame vacío para {currency}")
                return None

            logger.info(f"✅ {currency}: {len(df)} registros obtenidos")

            # Aplicar mapeo de columnas
            df = self._apply_column_mapping(df)

            # Agregar metadatos
            df["_currency_source"] = currency
            df["_extracted_at"] = datetime.now()

            return df

        except Exception as e:
            logger.error(f"❌ Error obteniendo datos {currency}: {str(e)}")
            raise

    def _apply_column_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        🔄 Aplica mapeo de columnas con fallbacks para variaciones de caso.

        Args:
            df: DataFrame original

        Returns:
            DataFrame con columnas mapeadas
        """
        try:
            # Crear diccionario de mapeo dinámico basado en columnas existentes
            available_mapping = {}

            for original_col in df.columns:
                if original_col in self.column_mapping:
                    mapped_col = self.column_mapping[original_col]
                    available_mapping[original_col] = mapped_col
                    logger.debug(f"📝 Mapeo: '{original_col}' -> '{mapped_col}'")

            # Aplicar mapeo solo a columnas existentes
            if available_mapping:
                df = df.rename(columns=available_mapping)
                logger.info(f"✅ Columnas mapeadas: {len(available_mapping)}")

            return df

        except Exception as e:
            logger.warning(f"⚠️ Error en mapeo de columnas: {str(e)}")
            return df

    async def fetch_all_data(
        self,
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        🌐 Obtiene datos de ambas monedas en paralelo.

        Returns:
            Tuple (pen_df, usd_df) con DataFrames de cada moneda
        """
        try:
            logger.info("🚀 Iniciando obtención paralela de datos PEN/USD")

            # Ejecutar ambas consultas en paralelo
            pen_task = self.fetch_data_by_currency("PEN")
            usd_task = self.fetch_data_by_currency("USD")

            pen_df, usd_df = await asyncio.gather(
                pen_task, usd_task, return_exceptions=True
            )

            # Manejar excepciones
            if isinstance(pen_df, Exception):
                logger.error(f"❌ Error en datos PEN: {pen_df}")
                pen_df = None

            if isinstance(usd_df, Exception):
                logger.error(f"❌ Error en datos USD: {usd_df}")
                usd_df = None

            # Log resultados
            pen_count = len(pen_df) if pen_df is not None else 0
            usd_count = len(usd_df) if usd_df is not None else 0

            logger.info(f"📊 Datos obtenidos - PEN: {pen_count}, USD: {usd_count}")

            return pen_df, usd_df

        except Exception as e:
            logger.error(f"❌ Error en obtención paralela: {str(e)}")
            return None, None

    def combine_currency_data(
        self, pen_df: Optional[pd.DataFrame], usd_df: Optional[pd.DataFrame]
    ) -> pd.DataFrame:
        """
        🔗 Combina datos de ambas monedas en un DataFrame unificado.

        Args:
            pen_df: DataFrame con datos PEN
            usd_df: DataFrame con datos USD

        Returns:
            DataFrame combinado con todos los registros
        """
        try:
            dataframes = []

            if pen_df is not None and not pen_df.empty:
                dataframes.append(pen_df)
                logger.info(f"✅ Agregando {len(pen_df)} registros PEN")

            if usd_df is not None and not usd_df.empty:
                dataframes.append(usd_df)
                logger.info(f"✅ Agregando {len(usd_df)} registros USD")

            if not dataframes:
                logger.warning("⚠️ No hay datos para combinar")
                return pd.DataFrame()

            # Combinar DataFrames
            combined_df = pd.concat(dataframes, ignore_index=True, sort=False)

            logger.info(f"🔗 Datos combinados: {len(combined_df)} registros totales")

            return combined_df

        except Exception as e:
            logger.error(f"❌ Error combinando datos: {str(e)}")
            return pd.DataFrame()

    async def get_processed_data(self) -> pd.DataFrame:
        """
        📋 Método principal para obtener datos procesados de operaciones fuera del sistema.

        Returns:
            DataFrame con todos los datos procesados y normalizados
        """
        try:
            logger.info(
                "🎯 Iniciando obtención de datos de operaciones fuera del sistema"
            )

            # Obtener datos de ambas monedas
            pen_df, usd_df = await self.fetch_all_data()

            # Combinar datos
            combined_df = self.combine_currency_data(pen_df, usd_df)

            if combined_df.empty:
                logger.warning(
                    "⚠️ No se obtuvieron datos de operaciones fuera del sistema"
                )
                return pd.DataFrame()

            logger.info(f"✅ Procesamiento completado: {len(combined_df)} registros")

            return combined_df

        except Exception as e:
            logger.error(f"❌ Error en get_processed_data: {str(e)}")
            return pd.DataFrame()


# Compatibilidad con sistema legacy
class OperacionesFueraSistemaObtener(BaseObtenerV2):
    """
    🔄 Wrapper de compatibilidad para el sistema legacy V2.
    Mantiene la misma interfaz pero usa arquitectura hexagonal pura.
    """

    def __init__(self):
        super().__init__()
        self.data_engine = OperacionesFueraSistemaDataEngine()

    async def obtener_json(self) -> List[Dict[str, Any]]:
        """
        📥 Obtiene datos en formato JSON compatible con sistema legacy.

        Returns:
            Lista de diccionarios con los datos
        """
        try:
            # Obtener DataFrame procesado
            df = await self.data_engine.get_processed_data()

            if df.empty:
                return []

            # Convertir a lista de diccionarios
            return df.to_dict(orient="records")

        except Exception as e:
            logger.error(f"❌ Error en obtener_json: {str(e)}")
            return []
