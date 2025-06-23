from config.db_mysql import Base
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import DateTime, Integer, SmallInteger
from datetime import datetime

# from uuid import UUID
# from sqlalchemy import Uuid


class BaseModel(Base):
    __abstract__ = True

    Estado: Mapped[bool] = mapped_column("Estado", SmallInteger, nullable=False)
    IdUsuarioCreacion: Mapped[int] = mapped_column(
        "IdUsuarioCreacion", Integer, nullable=False
    )
    FechaCreacion: Mapped[datetime] = mapped_column(
        "FechaCreacion", DateTime, nullable=False
    )
    IdUsuarioModificacion: Mapped[int] = mapped_column(
        "IdUsuarioModificacion", Integer, nullable=True
    )
    FechaModificacion: Mapped[datetime] = mapped_column(
        "FechaModificacion", DateTime, nullable=True
    )
