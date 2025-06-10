from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
from ..schemas.SolicitudLeadSchema import SolicitudLeadResultado


class SolicitudLeadCalcular:
    """
    Calcula los valores de adelanto de facturas para leads de factoring.
    Implementa la lógica de cálculo acordada para el simulador online.
    """

    # Constantes para el cálculo
    TASA_MENSUAL: float = 2.0  # Porcentaje mensual fijo
    COMISION_BASE: float = 5.5  # Comisión operacional base
    DIAS_MES: int = 30  # Días por mes para cálculos

    def calcular_adelanto(
        self,
        monto: float,
        fecha_vencimiento: date,
        moneda: str = "PEN",
        fecha_actual: date = None,
    ) -> SolicitudLeadResultado:
        """
        Calcula el adelanto de factoring basado en el monto y fecha de vencimiento.

        Args:
            monto: Monto de la factura a adelantar
            fecha_vencimiento: Fecha de vencimiento de la factura
            moneda: Moneda de la operación ('PEN' para soles, 'USD' para dólares)
            fecha_actual: Fecha actual (por defecto es hoy)

        Returns:
            SolicitudLeadResultado con los valores calculados
        """
        # Si no se proporciona fecha actual, usar la fecha de hoy
        if fecha_actual is None:
            fecha_actual = date.today()

        # Asegurarse de que trabajamos con objetos date
        if isinstance(fecha_vencimiento, datetime):
            fecha_vencimiento = fecha_vencimiento.date()

        if isinstance(fecha_actual, datetime):
            fecha_actual = fecha_actual.date()

        # 1. Calcular días de crédito: (fecha_vencimiento - fecha_actual) - 1
        dias_credito = (fecha_vencimiento - fecha_actual).days - 1
        if dias_credito <= 0:
            raise ValueError(
                "La fecha de vencimiento debe ser posterior a la fecha actual"
            )

        # 2. Calcular interés basado en la tasa mensual y días
        tasa_diaria = Decimal(self.TASA_MENSUAL) / Decimal(self.DIAS_MES) / Decimal(100)
        interes = (Decimal(monto) * tasa_diaria * Decimal(dias_credito)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

        # 3. Agregar comisión operacional
        comision = Decimal(self.COMISION_BASE).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

        # 4. Calcular descuento total
        descuento_total = interes + comision

        # 5. Calcular monto neto a recibir
        monto_recibir = (Decimal(monto) - descuento_total).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

        # 6. Crear respuesta estructurada - Actualizado con moneda
        return SolicitudLeadResultado(
            monto=Decimal(monto).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            dias_credito=dias_credito,
            tasa_mensual=Decimal(self.TASA_MENSUAL).quantize(
                Decimal("0.1"), rounding=ROUND_HALF_UP
            ),
            comision_operacional=comision,
            monto_total_recibir=monto_recibir,
            moneda=moneda,
        )


# # Ejemplo de prueba
# if __name__ == "__main__":
#     # Crear instancia del calculador
#     calculador = SolicitudLeadCalcular()

#     # Definir datos de prueba
#     monto_prueba = 10000.0
#     fecha_actual_prueba = datetime(2025, 6, 4)
#     fecha_vencimiento_prueba = datetime(2025, 7, 30)

#     # Ejemplo en soles
#     resultado_pen = calculador.calcular_adelanto(
#         monto=monto_prueba,
#         fecha_vencimiento=fecha_vencimiento_prueba,
#         moneda="PEN",
#         fecha_actual=fecha_actual_prueba,
#     )

#     # Ejemplo en dólares
#     resultado_usd = calculador.calcular_adelanto(
#         monto=monto_prueba,
#         fecha_vencimiento=fecha_vencimiento_prueba,
#         moneda="USD",
#         fecha_actual=fecha_actual_prueba,
#     )

#     # Mostrar resultados en formato JSON (model_dump)
#     print("Resultado en soles (model_dump):")
#     print(resultado_pen.model_dump_json(indent=2))

#     print("\nResultado en dólares (model_dump):")
#     print(resultado_usd.model_dump_json(indent=2))

#     # Mostrar valores directamente
#     print("\nValores directos (soles):")
#     print(f"Monto: {resultado_pen.monto}")
#     print(f"Monto total a recibir: {resultado_pen.monto_total_recibir}")
