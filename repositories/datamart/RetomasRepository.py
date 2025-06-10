from models.datamart.RetomasModel import RetomasModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql import DB


class RetomasRepository(BaseRepository):
    def __init__(self, db: DB) -> None:
        super().__init__(RetomasModel, db)
