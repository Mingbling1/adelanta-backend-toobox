from models.datamart.TipoCambioModel import TipoCambioModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql import DB


class TipoCambioRepository(BaseRepository):
    def __init__(self, db: DB) -> None:
        super().__init__(TipoCambioModel, db)
