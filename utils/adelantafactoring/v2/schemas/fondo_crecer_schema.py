"""
📊 Schemas Pydantic V2 - FondoCrecer

Mantiene compatibilidad con v1 mientras mejora validación financiera
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional
from decimal import Decimal, ROUND_HALF_UP


class FondoCrecerSchema(BaseModel):
    """Schema para datos de Fondo Crecer con validación de garantía"""

    Liquidacion: str = Field(..., description="Código de liquidación", min_length=1)

    Garantia: Decimal = Field(
        ...,
        description="Porcentaje de garantía como decimal (0.75 para 75%)",
        ge=0,  # Mayor o igual a 0
        le=1,  # Menor o igual a 1 (100%)
    )

    # Alias para compatibilidad con v1
    CodigoLiquidacion: Optional[str] = Field(None, description="Alias para Liquidacion")

    @field_validator("Liquidacion", mode="before")
    @classmethod
    def validate_liquidacion(cls, v):
        """Normaliza el código de liquidación"""
        if v is None:
            raise ValueError("Liquidacion no puede ser None")
        return str(v).strip().upper()

    @field_validator("Garantia", mode="before")
    @classmethod
    def validate_garantia(cls, v):
        """
        Recibe "75%" y devuelve 0.75 como Decimal
        Mantiene compatibilidad con v1 pero mejora precisión
        """
        if v is None:
            raise ValueError("Garantia no puede ser None")

        # Si es string con porcentaje
        if isinstance(v, str):
            v = v.strip()
            if v.endswith("%"):
                percentage_val = float(v.rstrip("%"))
                decimal_val = Decimal(percentage_val / 100)
            else:
                decimal_val = Decimal(v)
        else:
            decimal_val = Decimal(str(v))

        # Redondear a 4 decimales para precisión financiera
        return decimal_val.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    def model_post_init(self, __context) -> None:
        """Post-validación para mantener compatibilidad"""
        if self.CodigoLiquidacion is None:
            self.CodigoLiquidacion = self.Liquidacion


# Alias para compatibilidad con v1
FondoCrecerCalcularSchema = FondoCrecerSchema
