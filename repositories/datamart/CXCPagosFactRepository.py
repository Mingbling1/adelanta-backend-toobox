from models.datamart.CXCPagosFactModel import CXCPagosFactModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql import DB


class CXCPagosFactRepository(BaseRepository[CXCPagosFactModel]):
    def __init__(self, db: DB) -> None:
        super().__init__(CXCPagosFactModel, db)
