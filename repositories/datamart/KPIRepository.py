from models.datamart.KPIModel import KPIModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql import DB


class KPIRepository(BaseRepository):
    def __init__(self, db: DB) -> None:
        super().__init__(KPIModel, db)
