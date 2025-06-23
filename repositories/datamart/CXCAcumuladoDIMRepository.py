from models.datamart.CXCAcumuladoDIMModel import CXCAcumuladoDIMModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql import DB


class CXCAcumuladoDIMRepository(BaseRepository[CXCAcumuladoDIMModel]):
    def __init__(self, db: DB) -> None:
        super().__init__(CXCAcumuladoDIMModel, db)
