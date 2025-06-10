from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Integer, DateTime
from config.db_mysql import Base
from datetime import datetime


class NuevosClientesNuevosPagadoresModel(Base):
    __tablename__ = "nuevos_clientes_nuevos_pagadores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    FechaOperacion: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    Ejecutivo: Mapped[str | None] = mapped_column(String(255), nullable=True)
    RUCCliente: Mapped[str | None] = mapped_column(String(255), nullable=True)
    RUCPagador: Mapped[str | None] = mapped_column(String(255), nullable=True)
    TipoOperacion: Mapped[str | None] = mapped_column(String(255), nullable=True)
    RazonSocial: Mapped[str | None] = mapped_column(String(255), nullable=True)

    def to_dict(self) -> dict:
        return {
            "FechaOperacion": self.FechaOperacion,
            "Ejecutivo": self.Ejecutivo,
            "RUCCliente": self.RUCCliente,
            "RUCPagador": self.RUCPagador,
            "TipoOperacion": self.TipoOperacion,
            "RazonSocial": self.RazonSocial,
        }
