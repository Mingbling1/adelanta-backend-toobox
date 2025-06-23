from models.datamart.CXCDevFactModel import CXCDevFactModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql import DB


class CXCDevFactRepository(BaseRepository[CXCDevFactModel]):
    def __init__(self, db: DB) -> None:
        super().__init__(CXCDevFactModel, db)
