from schemas.BaseSchema import BaseCreateSchema, BaseOutputSchema, BaseUpdateSchema
from pydantic import field_validator, EmailStr, Field
from uuid import UUID
from datetime import date, datetime
import re


class SolicitudLeadCreateSchema(BaseCreateSchema):
    monto: float = Field(..., description="Monto de la solicitud")
    moneda: str = Field(..., description="Moneda de la solicitud (PEN, USD)")
    ruc_cliente: str = Field(..., description="RUC del cliente")
    ruc_empresa: str = Field(..., description="RUC de tu empresa")
    fecha_vencimiento: date = Field(..., description="Fecha de vencimiento")
    celular: str = Field(..., description="Número de celular")
    email: EmailStr = Field(..., description="Correo electrónico")
    acepta_terminos: bool = Field(
        ..., description="Aceptación de términos y condiciones"
    )

    @field_validator("monto")
    @classmethod
    def validate_monto(cls, v: float) -> float:
        if v < 1000:
            raise ValueError("El monto debe ser mayor a 1000")
        return v

    @field_validator("moneda")
    @classmethod
    def validate_moneda(cls, v: str) -> str:
        if not v or len(v) < 1:
            raise ValueError("Selecciona una moneda")
        if v not in ["PEN", "USD"]:
            raise ValueError("Moneda inválida, debe ser PEN o USD")
        return v

    @field_validator("ruc_cliente", "ruc_empresa")
    @classmethod
    def validate_ruc(cls, v: str) -> str:
        if not v or len(v) != 11:
            raise ValueError("Ingresa un RUC válido de 11 dígitos")
        if not v.isdigit():
            raise ValueError("El RUC debe contener solo números")
        return v

    @field_validator("celular")
    @classmethod
    def validate_celular(cls, v: str) -> str:
        # Validación de número peruano: +51 seguido de 9 dígitos
        patron = r"^(?:\+51)?9\d{8}$"
        if not re.match(patron, v):
            raise ValueError(
                "Ingresa un número válido (formato: +519XXXXXXXX o 9XXXXXXXX)"
            )
        return v

    @field_validator("acepta_terminos")
    @classmethod
    def validate_terminos(cls, v: bool) -> bool:
        if not v:
            raise ValueError("Debes aceptar los términos y condiciones")
        return v

    @field_validator("fecha_vencimiento", mode="before")
    @classmethod
    def parse_fecha_vencimiento(cls, v) -> date:
        if isinstance(v, str):
            try:
                # Intenta convertir datetime string a objeto date
                dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                return dt.date()
            except ValueError:
                # Si no es posible, deja que Pydantic maneje el error
                pass
        return v


class SolicitudLeadOutputSchema(BaseOutputSchema):
    solicitud_lead_id: UUID
    monto: float
    moneda: str
    ruc_cliente: str
    ruc_empresa: str
    fecha_vencimiento: date
    celular: str
    email: str
    acepta_terminos: bool


class SolicitudLeadUpdateSchema(BaseUpdateSchema):
    monto: float | None = None
    moneda: str | None = None
    ruc_cliente: str | None = None
    ruc_empresa: str | None = None
    fecha_vencimiento: date | None = None
    celular: str | None = None
    email: EmailStr | None = None
    acepta_terminos: bool | None = None
