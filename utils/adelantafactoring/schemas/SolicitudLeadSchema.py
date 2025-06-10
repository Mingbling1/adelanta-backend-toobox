from typing import Dict, Any
from pydantic import BaseModel, model_serializer
from decimal import Decimal

class SolicitudLeadResultado(BaseModel):
    monto: Decimal
    dias_credito: int
    tasa_mensual: Decimal
    comision_operacional: Decimal
    monto_total_recibir: Decimal
    moneda: str = "PEN"  # Por defecto soles

    @model_serializer
    def serializer_model(self) -> Dict[str, Any]:
        # Determinar el símbolo de la moneda
        simbolo = (
            "S/ " if self.moneda == "PEN" else "$ " if self.moneda == "USD" else "$/ "
        )

        return {
            "monto": f"{simbolo}{self.monto:,.2f}",
            "dias_credito": self.dias_credito,
            "tasa_mensual": f"{self.tasa_mensual}%",
            "comision_operacional": f"{simbolo}{self.comision_operacional:,.2f}",
            "monto_total_recibir": f"{simbolo}{self.monto_total_recibir:,.2f}",
            "moneda": self.moneda,
        }
