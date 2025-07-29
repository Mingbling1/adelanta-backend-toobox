"""
🏗️ Base Schema V2 - Schemas comunes y validadores

Validadores financieros reutilizables con precisión decimal
"""

from pydantic import BaseModel, Field, field_validator
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, date
from typing import Optional
import pandas as pd


class BaseFinancialSchema(BaseModel):
    """Schema base para entidades financieras"""

    model_config = {
        "str_strip_whitespace": True,
        "validate_assignment": True,
        "use_enum_values": True,
    }

    @field_validator("*", mode="before")
    @classmethod
    def normalize_financial_values(cls, v, info):
        """Normaliza valores financieros manteniendo precisión"""
        # Normalización de strings
        if isinstance(v, str):
            v = v.strip()
            if v.lower() in ["", "nan", "null", "none"]:
                return None

        # Normalización de valores pandas NaN
        if pd.isna(v):
            return None

        return v

    @classmethod
    def validate_currency_amount(cls, v) -> Optional[Decimal]:
        """Valida y convierte montos monetarios"""
        if v is None or pd.isna(v):
            return None

        try:
            decimal_val = Decimal(str(v))
            return decimal_val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        except (ValueError, TypeError):
            raise ValueError(f"Valor monetario inválido: {v}")

    @classmethod
    def validate_financial_date(cls, v) -> Optional[date]:
        """Valida y normaliza fechas financieras"""
        if v is None or pd.isna(v):
            return None

        if isinstance(v, (date, datetime)):
            return v.date() if isinstance(v, datetime) else v

        if isinstance(v, str):
            # Intentar múltiples formatos
            formats = ["%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"]
            for fmt in formats:
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue

        raise ValueError(f"Fecha inválida: {v}")


class ValidationResult(BaseModel):
    """Resultado de validación con detalles de errores"""

    is_valid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    processed_count: int = 0

    def add_error(self, message: str):
        """Añade un error"""
        self.errors.append(message)
        self.is_valid = False

    def add_warning(self, message: str):
        """Añade una advertencia"""
        self.warnings.append(message)
