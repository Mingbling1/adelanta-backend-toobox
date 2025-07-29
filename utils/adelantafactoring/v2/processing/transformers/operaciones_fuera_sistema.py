"""
🔄 Transformer V2 - Operaciones Fuera Sistema

Transformer especializado para conversión de datos entre formatos de operaciones fuera del sistema
con optimizaciones para ETL y compatibilidad legacy.
"""

import pandas as pd
from typing import List, Dict, Any
from datetime import datetime

from config import logger
from ...schemas.operaciones_fuera_sistema import OperacionesFueraSistemaProcessedSchema


class OperacionesFueraSistemaTransformer:
    """
    🔄 Transformer para datos de operaciones fuera del sistema.

    Convierte entre diferentes formatos manteniendo integridad financiera.
    """

    def __init__(self):
        self.excel_column_mapping = self._get_excel_mapping()

    def _get_excel_mapping(self) -> Dict[str, str]:
        """
        📊 Mapeo de columnas para exportación a Excel compatible con reportes.

        Returns:
            Diccionario con mapeo schema -> Excel
        """
        return {
            "CodigoLiquidacion": "Código Liquidación",
            "NroDocumento": "N° Documento",
            "RazonSocialCliente": "Razón Social Cliente",
            "RUCCliente": "RUC Cliente",
            "RazonSocialPagador": "Razón Social Pagador",
            "RUCPagador": "RUC Pagador",
            "TasaNominalMensualPorc": "Tasa Nominal Mensual %",
            "FinanciamientoPorc": "Financiamiento %",
            "FechaOperacion": "Fecha Operación",
            "FechaConfirmado": "Fecha Confirmado",
            "DiasEfectivo": "Días Efectivo",
            "Moneda": "Moneda",
            "NetoConfirmado": "Neto Confirmado",
            "MontoComisionEstructuracion": "Comisión Estructuración",
            "ComisionEstructuracionIGV": "IGV Comisión",
            "ComisionEstructuracionConIGV": "Comisión con IGV",
            "FondoResguardo": "Fondo Resguardo",
            "MontoCobrar": "Monto a Cobrar",
            "Interes": "Interés",
            "InteresConIGV": "Interés con IGV",
            "GastosContrato": "Gastos Contrato",
            "ServicioCustodia": "Servicio Custodia",
            "ServicioCobranza": "Servicio Cobranza",
            "GastoVigenciaPoder": "Gasto Vigencia Poder",
            "GastosDiversosSinIGV": "Gastos Diversos sin IGV",
            "GastosDiversosIGV": "Gastos Diversos IGV",
            "GastosDiversosConIGV": "Gastos Diversos con IGV",
            "MontoTotalFacturado": "Monto Total Facturado",
            "FacturasGeneradas": "Facturas Generadas",
            "MontoDesembolso": "Monto Desembolso",
            "FechaPago": "Fecha Pago",
            "Estado": "Estado",
            "DiasMora": "Días Mora",
            "InteresPago": "Interés Pago",
            "GastosPago": "Gastos Pago",
            "MontoCobrarPago": "Monto Cobrar Pago",
            "MontoPago": "Monto Pago",
            "ExcesoPago": "Exceso Pago",
            "FechaDesembolso": "Fecha Desembolso",
            "Ejecutivo": "Ejecutivo",
            "TipoOperacion": "Tipo Operación",
        }

    def to_dataframe(
        self, processed_data: OperacionesFueraSistemaProcessedSchema
    ) -> pd.DataFrame:
        """
        📊 Convierte datos procesados a DataFrame para análisis o exportación.

        Args:
            processed_data: Schema con datos procesados

        Returns:
            DataFrame optimizado para análisis
        """
        try:
            logger.info(
                f"📊 Convirtiendo {len(processed_data.data)} registros a DataFrame"
            )

            if not processed_data.data:
                logger.warning("⚠️ No hay datos para convertir")
                return pd.DataFrame()

            # Convertir schemas a diccionarios
            records = [record.model_dump() for record in processed_data.data]

            # Crear DataFrame
            df = pd.DataFrame(records)

            # Optimizaciones de tipos de datos
            df = self._optimize_dataframe_types(df)

            logger.info(
                f"✅ DataFrame creado: {len(df)} filas, {len(df.columns)} columnas"
            )

            return df

        except Exception as e:
            logger.error(f"❌ Error creando DataFrame: {str(e)}")
            return pd.DataFrame()

    def _optimize_dataframe_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        ⚡ Optimiza tipos de datos del DataFrame para mejor rendimiento.

        Args:
            df: DataFrame original

        Returns:
            DataFrame con tipos optimizados
        """
        try:
            # Campos categóricos
            categorical_fields = ["Moneda", "Estado", "TipoOperacion"]
            for field in categorical_fields:
                if field in df.columns:
                    df[field] = df[field].astype("category")

            # Campos enteros
            integer_fields = ["DiasEfectivo", "DiasMora"]
            for field in integer_fields:
                if field in df.columns:
                    df[field] = (
                        pd.to_numeric(df[field], errors="coerce")
                        .fillna(0)
                        .astype("int32")
                    )

            # Campos de fecha
            date_fields = [
                "FechaOperacion",
                "FechaConfirmado",
                "FechaPago",
                "FechaDesembolso",
            ]
            for field in date_fields:
                if field in df.columns:
                    df[field] = pd.to_datetime(df[field], errors="coerce")

            # Campos flotantes
            float_fields = [
                "TasaNominalMensualPorc",
                "FinanciamientoPorc",
                "NetoConfirmado",
                "MontoComisionEstructuracion",
                "ComisionEstructuracionIGV",
                "ComisionEstructuracionConIGV",
                "FondoResguardo",
                "MontoCobrar",
            ]
            for field in float_fields:
                if field in df.columns:
                    df[field] = pd.to_numeric(df[field], errors="coerce").fillna(0)

            logger.debug("⚡ Tipos de datos optimizados")

            return df

        except Exception as e:
            logger.warning(f"⚠️ Error optimizando tipos: {str(e)}")
            return df

    def to_excel_format(
        self, processed_data: OperacionesFueraSistemaProcessedSchema
    ) -> pd.DataFrame:
        """
        📋 Convierte datos a formato Excel con columnas amigables para reportes.

        Args:
            processed_data: Schema con datos procesados

        Returns:
            DataFrame formateado para Excel
        """
        try:
            logger.info(
                f"📋 Formateando {len(processed_data.data)} registros para Excel"
            )

            # Crear DataFrame base
            df = self.to_dataframe(processed_data)

            if df.empty:
                return pd.DataFrame()

            # Aplicar mapeo de columnas para Excel
            df_excel = df.rename(columns=self.excel_column_mapping)

            # Formatear fechas para Excel
            date_columns = [
                "Fecha Operación",
                "Fecha Confirmado",
                "Fecha Pago",
                "Fecha Desembolso",
            ]
            for col in date_columns:
                if col in df_excel.columns:
                    df_excel[col] = df_excel[col].dt.strftime("%Y-%m-%d")

            # Formatear campos monetarios
            money_columns = [
                "Neto Confirmado",
                "Comisión Estructuración",
                "IGV Comisión",
                "Comisión con IGV",
                "Fondo Resguardo",
                "Monto a Cobrar",
            ]
            for col in money_columns:
                if col in df_excel.columns:
                    df_excel[col] = df_excel[col].round(2)

            logger.info(f"✅ Formato Excel aplicado: {len(df_excel.columns)} columnas")

            return df_excel

        except Exception as e:
            logger.error(f"❌ Error formateando para Excel: {str(e)}")
            return pd.DataFrame()

    def to_legacy_format(
        self, processed_data: OperacionesFueraSistemaProcessedSchema
    ) -> List[Dict[str, Any]]:
        """
        🔄 Convierte datos a formato legacy para compatibilidad con sistema existente.

        Args:
            processed_data: Schema con datos procesados

        Returns:
            Lista de diccionarios compatible con sistema legacy
        """
        try:
            logger.info(
                f"🔄 Convirtiendo {len(processed_data.data)} registros a formato legacy"
            )

            legacy_records = []

            for record in processed_data.data:
                legacy_record = record.model_dump()

                # Transformaciones específicas para legacy
                # Fechas como strings ISO
                for field in [
                    "FechaOperacion",
                    "FechaConfirmado",
                    "FechaPago",
                    "FechaDesembolso",
                ]:
                    if legacy_record.get(field):
                        if hasattr(legacy_record[field], "isoformat"):
                            legacy_record[field] = legacy_record[field].isoformat()

                # Campos numéricos como float explícito
                numeric_fields = [
                    "TasaNominalMensualPorc",
                    "FinanciamientoPorc",
                    "NetoConfirmado",
                    "MontoComisionEstructuracion",
                    "MontoCobrar",
                ]
                for field in numeric_fields:
                    if legacy_record.get(field) is not None:
                        legacy_record[field] = float(legacy_record[field])

                legacy_records.append(legacy_record)

            logger.info(
                f"✅ Conversión legacy completada: {len(legacy_records)} registros"
            )

            return legacy_records

        except Exception as e:
            logger.error(f"❌ Error en conversión legacy: {str(e)}")
            return []

    def filter_by_currency(
        self, processed_data: OperacionesFueraSistemaProcessedSchema, currency: str
    ) -> OperacionesFueraSistemaProcessedSchema:
        """
        💱 Filtra datos por moneda específica.

        Args:
            processed_data: Schema con datos procesados
            currency: Moneda a filtrar ("PEN" o "USD")

        Returns:
            Schema filtrado por moneda
        """
        try:
            logger.info(f"💱 Filtrando por moneda: {currency}")

            filtered_records = [
                record
                for record in processed_data.data
                if record.Moneda and record.Moneda.upper() == currency.upper()
            ]

            # Crear nueva respuesta filtrada
            filtered_response = OperacionesFueraSistemaProcessedSchema(
                data=filtered_records,
                total_records=len(filtered_records),
                pen_records=len(filtered_records) if currency.upper() == "PEN" else 0,
                usd_records=len(filtered_records) if currency.upper() == "USD" else 0,
                filtered_records=0,
                processing_timestamp=datetime.now(),
            )

            logger.info(
                f"✅ Filtrado completado: {len(filtered_records)} registros {currency}"
            )

            return filtered_response

        except Exception as e:
            logger.error(f"❌ Error filtrando por moneda: {str(e)}")
            return processed_data

    def get_summary_stats(
        self, processed_data: OperacionesFueraSistemaProcessedSchema
    ) -> Dict[str, Any]:
        """
        📈 Genera estadísticas resumidas de los datos procesados.

        Args:
            processed_data: Schema con datos procesados

        Returns:
            Diccionario con estadísticas resumidas
        """
        try:
            if not processed_data.data:
                return {"error": "No hay datos para analizar"}

            df = self.to_dataframe(processed_data)

            stats = {
                "total_registros": len(df),
                "registros_pen": processed_data.pen_records,
                "registros_usd": processed_data.usd_records,
                "monto_total_cobrar": (
                    df["MontoCobrar"].sum() if "MontoCobrar" in df.columns else 0
                ),
                "promedio_dias_efectivo": (
                    df["DiasEfectivo"].mean() if "DiasEfectivo" in df.columns else 0
                ),
                "ejecutivos_unicos": (
                    df["Ejecutivo"].nunique() if "Ejecutivo" in df.columns else 0
                ),
                "tipos_operacion": (
                    df["TipoOperacion"].nunique()
                    if "TipoOperacion" in df.columns
                    else 0
                ),
                "timestamp_procesamiento": processed_data.processing_timestamp.isoformat(),
            }

            logger.info(
                f"📈 Estadísticas generadas: {stats['total_registros']} registros"
            )

            return stats

        except Exception as e:
            logger.error(f"❌ Error generando estadísticas: {str(e)}")
            return {"error": f"Error generando estadísticas: {str(e)}"}
