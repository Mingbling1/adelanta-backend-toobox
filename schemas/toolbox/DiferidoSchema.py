from pydantic import model_validator, BaseModel
from fastapi import Query
import json


class DiferidoCreateSchema(BaseModel):
    hasta: str | None = Query(
        None, regex=r"^\d{4}-\d{2}$", description="Formato YYYY-MM (opcional)"
    )

    @model_validator(mode="before")
    @classmethod
    def validate_to_json(cls, value):
        if isinstance(value, str):
            return cls(**json.loads(value))
        return value
