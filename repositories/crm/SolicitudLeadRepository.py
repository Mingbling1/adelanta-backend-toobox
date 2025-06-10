from models.crm.SolicitudLeadModel import SolicitudLeadModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql_crm import DB_CRM


class SolicitudLeadRepository(BaseRepository[SolicitudLeadModel]):
    def __init__(self, db: DB_CRM) -> None:
        super().__init__(SolicitudLeadModel, db)
