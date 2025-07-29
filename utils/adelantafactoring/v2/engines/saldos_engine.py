"""
Motor de cálculo para saldos
"""

import pandas as pd
from typing import List, Dict

from ..schemas.saldos_schema import (
    SaldosRequest,
    SaldosResult,
    SaldoRecord,
    SaldosCalcularSchema,
)
from ..io.saldos_data_source import SaldosDataSource


class SaldosEngine:
    """Motor principal para procesamiento de saldos de caja"""

    def __init__(self):
        self.data_source = SaldosDataSource()

    def calculate(self, request: SaldosRequest) -> SaldosResult:
        """
        Procesa los datos de saldos

        Args:
            request: Request con configuración de procesamiento

        Returns:
            Resultado del procesamiento
        """
        try:
            # Obtener datos desde la fuente
            raw_data = self.data_source._obtener_sincrono()

            # Procesar los datos
            processed_df = self._procesar_datos(raw_data)

            # Validar con Pydantic
            validated_records = self._validar_datos(processed_df)

            # Generar estadísticas
            total_saldo = sum(record.saldo_total_caja for record in validated_records)
            fecha_ultimo = max(
                (record.fecha_operacion for record in validated_records), default=None
            )

            return SaldosResult(
                records=validated_records,
                records_count=len(validated_records),
                total_saldo_caja=total_saldo,
                fecha_ultimo_saldo=fecha_ultimo,
            )

        except Exception:
            # Retornar resultado vacío en caso de error
            return SaldosResult(
                records=[],
                records_count=0,
                total_saldo_caja=0.0,
                fecha_ultimo_saldo=None,
            )

    def _procesar_datos(self, raw_data: Dict) -> pd.DataFrame:
        """Procesa los datos raw obtenidos de la fuente"""

        # Manejar tanto listas directas como estructura con 'data'
        if isinstance(raw_data, dict) and "data" in raw_data:
            data_list = raw_data["data"]
        elif isinstance(raw_data, list):
            data_list = raw_data
        else:
            data_list = [raw_data] if raw_data else []

        df = pd.DataFrame(data_list)

        # Si el DataFrame está vacío, devolver estructura básica
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "FechaOperacion",
                    "EvolucionCaja",
                    "CostoExcesoCaja",
                    "IngresoNoRecibidoPorExcesoCaja",
                    "MontoOvernight",
                    "IngresosOvernightNeto",
                    "SaldoTotalCaja",
                ]
            )

        # Normalizar nombres de columnas (Google Scripts puede variar)
        column_mapping = {
            "fechaoperacion": "FechaOperacion",
            "fecha_operacion": "FechaOperacion",
            "FechaOperacion": "FechaOperacion",
            "evolucioncaja": "EvolucionCaja",
            "evolucion_caja": "EvolucionCaja",
            "EvolucionCaja": "EvolucionCaja",
            "costoexcesocaja": "CostoExcesoCaja",
            "costo_exceso_caja": "CostoExcesoCaja",
            "CostoExcesoCaja": "CostoExcesoCaja",
            "ingresonorecibidoporexcesocaja": "IngresoNoRecibidoPorExcesoCaja",
            "ingreso_no_recibido_exceso_caja": "IngresoNoRecibidoPorExcesoCaja",
            "IngresoNoRecibidoPorExcesoCaja": "IngresoNoRecibidoPorExcesoCaja",
            "montoovernight": "MontoOvernight",
            "monto_overnight": "MontoOvernight",
            "MontoOvernight": "MontoOvernight",
            "ingresosovernightneto": "IngresosOvernightNeto",
            "ingresos_overnight_neto": "IngresosOvernightNeto",
            "IngresosOvernightNeto": "IngresosOvernightNeto",
            "saldototalcaja": "SaldoTotalCaja",
            "saldo_total_caja": "SaldoTotalCaja",
            "SaldoTotalCaja": "SaldoTotalCaja",
        }

        # Renombrar columnas encontradas
        for col in df.columns:
            col_lower = col.lower().replace(" ", "")
            if col_lower in column_mapping:
                df = df.rename(columns={col: column_mapping[col_lower]})

        return df

    def _validar_datos(self, df: pd.DataFrame) -> List[SaldoRecord]:
        """Valida los datos con Pydantic y convierte a schema moderno"""
        if df.empty:
            return []

        records = []
        for _, row in df.iterrows():
            try:
                # Mapear a estructura moderna
                record_data = {
                    "fecha_operacion": row.get("FechaOperacion"),
                    "evolucion_caja": row.get("EvolucionCaja", 0.0),
                    "costo_exceso_caja": row.get("CostoExcesoCaja", 0.0),
                    "ingreso_no_recibido_exceso_caja": row.get(
                        "IngresoNoRecibidoPorExcesoCaja", 0.0
                    ),
                    "monto_overnight": row.get("MontoOvernight", 0.0),
                    "ingresos_overnight_neto": row.get("IngresosOvernightNeto", 0.0),
                    "saldo_total_caja": row.get("SaldoTotalCaja", 0.0),
                }

                record = SaldoRecord(**record_data)
                records.append(record)

            except Exception as e:
                # Continuar con el siguiente registro si hay error
                print(f"Error validando registro: {e}")
                continue

        return records

    # Métodos de compatibilidad con versión original
    def validar_datos_legacy(self, data: List[Dict]) -> List[Dict]:
        """Método de compatibilidad para validación con schema original"""
        validated = []
        for item in data:
            try:
                schema_item = SaldosCalcularSchema(**item)
                validated.append(schema_item.model_dump())
            except Exception as e:
                print(f"Error validando item: {e}")
                continue
        return validated
