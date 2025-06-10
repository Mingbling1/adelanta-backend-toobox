from pydantic import BaseModel, field_validator


class SectorPagadoresCalcularSchema(BaseModel):
    RUCPagador: str
    Sector: str
    GrupoEco: str | None

    @field_validator("GrupoEco", mode="before")
    def convertir_valores_vacios_a_none(cls, v):
        if v == "":
            return None
        return v
