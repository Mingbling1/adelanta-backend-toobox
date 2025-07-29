"""
📊 Schemas Pydantic V2 - FondoPromocional

Mantiene compatibilidad con v1 mientras mejora validación
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional


class FondoPromocionalSchema(BaseModel):
    """Schema para datos de Fondo Promocional"""

    Liquidacion: str = Field(..., description="Código de liquidación", min_length=1)

    # Alias para compatibilidad con v1
    CodigoLiquidacion: Optional[str] = Field(None, description="Alias para Liquidacion")

    @field_validator("Liquidacion", mode="before")
    @classmethod
    def validate_liquidacion(cls, v):
        """Normaliza el código de liquidación"""
        if v is None:
            raise ValueError("Liquidacion no puede ser None")
        return str(v).strip().upper()

    def model_post_init(self, __context) -> None:
        """Post-validación para mantener compatibilidad"""
        if self.CodigoLiquidacion is None:
            self.CodigoLiquidacion = self.Liquidacion


# Alias para compatibilidad con v1
FondoPromocionalCalcularSchema = FondoPromocionalSchema
