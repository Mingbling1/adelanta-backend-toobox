from pydantic import BaseModel


class RetomasCalcularSchema(BaseModel):
    RUCPagador: str | None
    RazonSocialPagador: str | None
    Cobranzas_MontoPagoSoles: float
    Desembolsos_MontoDesembolsoSoles: float
    PorRetomar: float
