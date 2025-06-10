from models.datamart.KPIAcumuladoModel import KPIAcumuladoModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql import DB


class KPIAcumuladoRepository(BaseRepository):
    def __init__(self, db: DB) -> None:
        super().__init__(KPIAcumuladoModel, db)
