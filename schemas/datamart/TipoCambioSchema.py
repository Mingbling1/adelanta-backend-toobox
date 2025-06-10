from pydantic import BaseModel, field_validator
from datetime import datetime
from dateutil.parser import parse


class TipoCambioPostRequestSchema(BaseModel):
    TipoCambioFecha: str | datetime
    TipoCambioCompra: float
    TipoCambioVenta: float

    @field_validator("TipoCambioFecha", mode="before")
    @classmethod
    def validate_fecha(cls, v):
        if isinstance(v, datetime):
            return v.strftime("%Y-%m-%d")
        elif isinstance(v, str):
            try:
                parsed_date = parse(v)
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                raise ValueError("TipoCambioFecha must be in the format yyyy-mm-dd")
        else:
            raise ValueError(
                "TipoCambioFecha must be a datetime object or a string in the format yyyy-mm-dd"
            )


class TipoCambioSchema(TipoCambioPostRequestSchema):
    id: int | None = None
