from models.datamart.ActualizacionReportesModel import ActualizacionReportesModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql import DB


class ActualizacionReportesRepository(BaseRepository):
    def __init__(self, db: DB) -> None:
        super().__init__(ActualizacionReportesModel, db)
