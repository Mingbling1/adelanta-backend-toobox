from models.datamart.NuevosClientesNuevosPagadoresModel import (
    NuevosClientesNuevosPagadoresModel,
)
from repositories.BaseRepository import BaseRepository
from config.db_mysql import DB


class NuevosClientesNuevosPagadoresRepository(
    BaseRepository[NuevosClientesNuevosPagadoresModel]
):
    def __init__(self, db: DB) -> None:
        super().__init__(NuevosClientesNuevosPagadoresModel, db)
