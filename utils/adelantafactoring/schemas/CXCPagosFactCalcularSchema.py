from pydantic import BaseModel, Field
from datetime import date, datetime
from typing import Optional


class CXCPagosFactCalcularSchema(BaseModel):
    IdLiquidacionPago: int = Field(..., description="ID de liquidación de pago")
    IdLiquidacionDet: int = Field(..., description="ID de detalle de liquidación")
    FechaPago: date = Field(..., description="Fecha del pago")
    DiasMora: int = Field(..., description="Días de mora")
    MontoCobrarPago: float = Field(..., description="Monto a cobrar del pago")
    MontoPago: float = Field(..., description="Monto del pago")
    ObservacionPago: Optional[str] = Field(None, description="Observación del pago")
    InteresPago: float = Field(..., description="Interés del pago")
    GastosPago: float = Field(..., description="Gastos del pago")
    TipoPago: str = Field(..., description="Tipo de pago")
    SaldoDeuda: float = Field(..., description="Saldo de la deuda")
    ExcesoPago: float = Field(..., description="Exceso del pago")
    FechaPagoCreacion: Optional[datetime] = Field(None, description="Fecha de creación del pago")
    FechaPagoModificacion: Optional[datetime] = Field(None, description="Fecha de modificación del pago")

    class Config:
        from_attributes = True
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
            datetime: lambda v: v.isoformat() if v else None,
        }