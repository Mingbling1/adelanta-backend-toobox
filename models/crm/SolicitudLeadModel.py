from sqlalchemy import String, Numeric, Date, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from uuid import UUID, uuid4
from datetime import date
from models.crm.CRMBaseModel import CRMBaseModel

# from typing import Optional


class SolicitudLeadModel(CRMBaseModel):
    """
    Modelo para almacenar las solicitudes/leads ingresadas desde la landing page.
    Captura información básica de clientes potenciales y sus necesidades de financiamiento.
    """

    __tablename__ = "solicitud_lead"

    # Identificador único
    solicitud_lead_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)

    # Datos de la solicitud
    monto: Mapped[float] = mapped_column(Numeric(18, 2), nullable=False)
    moneda: Mapped[str] = mapped_column(String(3), nullable=False)  # PEN, USD

    # Información de cliente y empresa
    ruc_cliente: Mapped[str] = mapped_column(String(11), nullable=False)
    ruc_empresa: Mapped[str] = mapped_column(String(11), nullable=False)
    fecha_vencimiento: Mapped[date] = mapped_column(Date, nullable=False)

    # Información de contacto
    celular: Mapped[str] = mapped_column(String(15), nullable=False)
    email: Mapped[str] = mapped_column(String(255), nullable=False)

    # Términos y estado
    acepta_terminos: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )

    # # Campos adicionales útiles para CRM
    # estado: Mapped[str] = mapped_column(String(20), nullable=False, default="nuevo")
    # asignado_a: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    # fecha_seguimiento: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    # comentarios: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # # Campos para tracking de la fuente
    # utm_source: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    # utm_medium: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    # utm_campaign: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    # ip_address: Mapped[Optional[str]] = mapped_column(String(45), nullable=True)
    # user_agent: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
