from pydantic import BaseModel, ConfigDict
from datetime import datetime
from uuid import UUID


class BaseCreateSchema(BaseModel):
    estado: int = 1
    created_by: UUID | None = None
    model_config = ConfigDict(from_attributes=True)


class BaseUpdateSchema(BaseModel):
    estado: int = 1
    updated_by: UUID | None = None
    model_config = ConfigDict(from_attributes=True)


class BaseOutputSchema(BaseModel):
    estado: int | str
    created_by: str | UUID
    created_at: datetime
    updated_by: str | UUID | None
    updated_at: datetime | None
    model_config = ConfigDict(from_attributes=True)
