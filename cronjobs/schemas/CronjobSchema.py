from pydantic import BaseModel, Field, field_validator
from typing import Literal, Union
from datetime import datetime
from dateutil.parser import parse


class CronjobConfigDayOfWeekSchema(BaseModel):
    day_of_week: str | None = Field(default="0-6", description="Days of the week")


class CronjobConfigSchema(CronjobConfigDayOfWeekSchema):
    times: list[tuple[int, int]] = Field(
        default=[(0, 0)], description="List Hour and minute"
    )


class ActualizarTablaRetomaConfigSchema(BaseModel):
    job_id: Literal["actualizartablaretomacronjob"]
    fecha_corte: str | None = Field(
        default=None, description="Fecha de corte in format %Y-%m-%d"
    )

    @field_validator("fecha_corte", mode="before")
    @classmethod
    def validate_fecha(cls, v):
        if v is None:
            return v
        if isinstance(v, datetime):
            return v.strftime("%Y-%m-%d")
        elif isinstance(v, str):
            try:
                parsed_date = parse(v)
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                raise ValueError("Date must be in the format yyyy-mm-dd")
        else:
            raise ValueError(
                "Date must be a datetime object or a string in the format yyyy-mm-dd"
            )


class ActualizarTablaKPIAcumuladoConfigSchema(BaseModel):
    job_id: Literal["actualizartablakpiacumuladocronjob"]


class ActualizarTablasReportesConfigSchema(BaseModel):
    job_id: Literal["actualizartablasreportescronjob"]


class ActualizarTipoCambioCronjobConfigSchema(BaseModel):
    job_id: Literal["actualizartipocambiocronjob"]


class CronjobSchema(CronjobConfigSchema):
    cronjob_config: Union[
        ActualizarTablaRetomaConfigSchema,
        ActualizarTablaKPIAcumuladoConfigSchema,
        ActualizarTablasReportesConfigSchema,
        ActualizarTipoCambioCronjobConfigSchema,
    ] = Field(..., discriminator="job_id")


class CronjobNowSchema(BaseModel):
    cronjob_config: Union[
        ActualizarTablaRetomaConfigSchema,
        ActualizarTablaKPIAcumuladoConfigSchema,
        ActualizarTablasReportesConfigSchema,
        ActualizarTipoCambioCronjobConfigSchema,
    ] = Field(..., discriminator="job_id")
