from pydantic import BaseModel, field_validator


class FondoCrecerCalcularSchema(BaseModel):
    Liquidacion: str
    Garantia: float

    @field_validator("Garantia", mode="before")
    @classmethod
    def parsear_garantia(cls, v):
        """
        Recibe "75%" y devuelve 0.75
        """
        if isinstance(v, str) and v.endswith("%"):
            return float(v.rstrip("%")) / 100
        return float(v)
