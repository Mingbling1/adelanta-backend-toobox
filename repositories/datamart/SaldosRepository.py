from models.datamart.SaldosModel import SaldosModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql import DB


class SaldosRepository(BaseRepository):
    def __init__(self, db: DB) -> None:
        super().__init__(SaldosModel, db)
