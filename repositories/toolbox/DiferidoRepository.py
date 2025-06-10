# from models.datamart.DiferidoModel import DiferidoModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql import DB
from models.datamart.KPIModel import KPIModel


class DiferidoRepository(BaseRepository):
    def __init__(self, db: DB) -> None:
        super().__init__(KPIModel, db)
