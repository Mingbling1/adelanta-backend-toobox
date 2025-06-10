from models.auth.PermisoModel import PermisoModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql_administrativo import DB_ADMINISTRATIVO
from sqlalchemy.future import select
from sqlalchemy import and_, func, or_
from models.master.TablaMaestraModel import TablaMaestraModel
from models.master.TablaMaestraDetalleModel import TablaMaestraDetalleModel
from sqlalchemy.orm import aliased
from config.logger import logger
from fastapi import HTTPException


class PermisoRepository(BaseRepository[PermisoModel]):
    def __init__(self, db: DB_ADMINISTRATIVO) -> None:
        super().__init__(PermisoModel, db)

    async def get_all_and_search(
        self,
        limit: int = 10,
        page: int = 1,
        nombre: str = None,
    ):
        """
        Obtiene solo los submódulos (permisos con módulo padre) aplicando paginación y filtros.

        Args:
            limit: Número de registros por página
            page: Número de página (iniciando en 1)
            nombre: Filtro opcional por nombre

        Returns:
            list: Lista de submódulos con el total de páginas agregado
        """
        # Calcular el offset basado en la página (empezando desde 1)
        offset = (page - 1) * limit

        # Construir la consulta con filtros
        query = select(self.entity_class)

        # Filtro para devolver SOLO submódulos (permisos con módulo padre)
        filters = [self.entity_class.modulo_padre_id.is_not(None)]

        # Filtro adicional por nombre si se proporciona
        if nombre:
            search_term = f"%{nombre}%"
            filters.append(self.entity_class.nombre.ilike(search_term))

        query = query.where(and_(*filters)).offset(offset).limit(limit)

        # Obtener el número total de submódulos que coinciden con los filtros
        total_query = (
            select(func.count()).select_from(self.entity_class).where(and_(*filters))
        )
        total_result = await self.db.execute(total_query)
        total_records = total_result.scalar()

        # Calcular el número total de páginas
        total_pages = (total_records + limit - 1) // limit

        # Obtener los submódulos paginados
        result = await self.db.execute(query)
        records = result.scalars().all()

        # Añadir total_pages a cada instancia
        for record in records:
            record.total_pages = total_pages

        return records
