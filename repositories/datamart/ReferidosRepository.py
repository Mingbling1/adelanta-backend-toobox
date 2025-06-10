from models.datamart.ReferidosModel import ReferidosModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql import DB


class ReferidosRepository(BaseRepository):
    def __init__(self, db: DB) -> None:
        super().__init__(ReferidosModel, db)
