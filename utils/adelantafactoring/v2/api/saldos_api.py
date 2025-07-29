"""
API de compatibilidad para SaldosCalcular
"""

from typing import Dict, List, Union
import asyncio

from ..engines.saldos_engine import SaldosEngine
from ..schemas.saldos_schema import SaldosRequest


class SaldosAPI:
    """
    API de compatibilidad para mantener la interfaz original de SaldosCalcular
    """

    def __init__(self):
        self.engine = SaldosEngine()

    def calcular(self, *args, **kwargs) -> List[Dict]:
        """
        Método principal de compatibilidad - mantiene interfaz original
        """
        try:
            # Crear request básico
            request = SaldosRequest()

            # Ejecutar cálculo
            result = self.engine.calculate(request)

            # Convertir a formato legacy esperado
            legacy_data = []
            for record in result.records:
                # Convertir de estructura moderna a legacy
                legacy_item = {
                    "FechaOperacion": (
                        record.fecha_operacion.strftime("%d/%m/%Y")
                        if record.fecha_operacion
                        else ""
                    ),
                    "EvolucionCaja": record.evolucion_caja,
                    "CostoExcesoCaja": record.costo_exceso_caja,
                    "IngresoNoRecibidoPorExcesoCaja": record.ingreso_no_recibido_exceso_caja,
                    "MontoOvernight": record.monto_overnight,
                    "IngresosOvernightNeto": record.ingresos_overnight_neto,
                    "SaldoTotalCaja": record.saldo_total_caja,
                }
                legacy_data.append(legacy_item)

            # Validar con schema original para compatibilidad
            return self.validar_datos(legacy_data)

        except Exception:
            return []

    def validar_datos(self, data: List[Dict]) -> List[Dict]:
        """
        Validación de datos usando schema original
        """
        return self.engine.validar_datos_legacy(data)

    def procesar_datos(self, data: Union[Dict, List]) -> List[Dict]:
        """
        Procesamiento de datos - método de compatibilidad
        """
        try:
            # Si recibimos datos directamente, simular el flujo
            if isinstance(data, (dict, list)):
                # Usar el engine para procesar
                request = SaldosRequest()
                result = self.engine.calculate(request)

                # Convertir a formato legacy
                legacy_data = []
                for record in result.records:
                    legacy_item = {
                        "FechaOperacion": (
                            record.fecha_operacion.strftime("%d/%m/%Y")
                            if record.fecha_operacion
                            else ""
                        ),
                        "EvolucionCaja": record.evolucion_caja,
                        "CostoExcesoCaja": record.costo_exceso_caja,
                        "IngresoNoRecibidoPorExcesoCaja": record.ingreso_no_recibido_exceso_caja,
                        "MontoOvernight": record.monto_overnight,
                        "IngresosOvernightNeto": record.ingresos_overnight_neto,
                        "SaldoTotalCaja": record.saldo_total_caja,
                    }
                    legacy_data.append(legacy_item)

                return self.validar_datos(legacy_data)

            return []

        except Exception:
            return []

    # Métodos adicionales de compatibilidad si son necesarios
    async def calcular_async(self) -> List[Dict]:
        """Versión asíncrona del cálculo"""
        return await asyncio.to_thread(self.calcular)

    def get_stats(self) -> Dict:
        """Obtener estadísticas del último cálculo"""
        try:
            request = SaldosRequest()
            result = self.engine.calculate(request)

            return {
                "total_records": result.records_count,
                "total_saldo_caja": result.total_saldo_caja,
                "fecha_ultimo_saldo": (
                    result.fecha_ultimo_saldo.strftime("%d/%m/%Y")
                    if result.fecha_ultimo_saldo
                    else None
                ),
            }
        except Exception:
            return {
                "total_records": 0,
                "total_saldo_caja": 0.0,
                "fecha_ultimo_saldo": None,
            }
