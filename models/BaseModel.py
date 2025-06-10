from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import DateTime, func, Integer
from datetime import datetime
from uuid import UUID
from sqlalchemy import Uuid


class BaseModel:
    __abstract__ = True

    estado: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    # Audit columns
    created_by: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=func.now()
    )
    updated_by: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), onupdate=func.now()
    )
