from repositories.crm.SolicitudLeadRepository import SolicitudLeadRepository
from services.BaseService import BaseService
from fastapi import Depends
from models.crm.SolicitudLeadModel import SolicitudLeadModel
from utils.mailing.Mailing import Mailing
from config.queue import email_queue
from utils.adelantafactoring.calculos.SolicitudLeadCalcular import (
    SolicitudLeadCalcular,
)
from schemas.crm.SolicitudLeadSchema import (
    SolicitudLeadCreateSchema,
)
from config.logger import logger


class SolicitudLeadService(BaseService[SolicitudLeadModel]):
    """
    Servicio para manejar las operaciones relacionadas con SolicitudLead.
    Hereda de BaseService para proporcionar operaciones CRUD estándar.
    """

    def __init__(
        self, solicitud_lead_repository: SolicitudLeadRepository = Depends()
    ) -> None:
        super().__init__(solicitud_lead_repository)
        self.solicitud_lead_repository = solicitud_lead_repository
        self.mailing = Mailing()
        self.solicitud_lead_calcular = SolicitudLeadCalcular()

    async def create_and_send(
        self, entity: SolicitudLeadCreateSchema, autocommit: bool = True
    ) -> SolicitudLeadModel:
        """
        Crea una nueva solicitud de lead y envía un correo con los datos.

        Args:
            entity_data: Diccionario con los datos de la solicitud
            autocommit: Si se debe hacer commit automáticamente

        Returns:
            La entidad creada
        """
        await self.create(entity.model_dump(), autocommit)

        resultado_solicitud_lead_calcular = (
            self.solicitud_lead_calcular.calcular_adelanto(
                monto=entity.monto,
                fecha_vencimiento=entity.fecha_vencimiento,
                moneda=entity.moneda,
            )
        )

        # Encolar el envío de email (se procesará en segundo plano)
        email_queue.enqueue(
            self.mailing.enviar_solicitud_lead,  # Pasar la referencia a la función, sin ejecutarla
            **resultado_solicitud_lead_calcular.model_dump(),  # Expandir el diccionario como kwargs
            email=entity.email,  # Añadir el email como kwarg adicional
        )
        logger.info(
            f"SolicitudLead creada y correo encolado para enviar a {entity.email}"
        )
