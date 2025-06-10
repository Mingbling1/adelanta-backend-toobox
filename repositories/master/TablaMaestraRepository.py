from models.master.TablaMaestraModel import TablaMaestraModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql_administrativo import DB_ADMINISTRATIVO
from sqlalchemy.future import select
from sqlalchemy import and_, func, or_
from models.auth.UsuarioModel import UsuarioModel
from sqlalchemy.orm import aliased


class TablaMaestraRepository(BaseRepository):
    def __init__(self, db: DB_ADMINISTRATIVO) -> None:
        super().__init__(TablaMaestraModel, db)

    async def get_all_and_search(
        self,
        limit: int = 10,
        page: int = 1,
        tabla_nombre: str = None,
        tipo: str = None,
    ):
        # Calcular el offset basado en la página (empezando desde 1)
        offset = (page - 1) * limit

        # Construir la consulta con filtros
        query = select(self.entity_class).offset(offset).limit(limit)

        filters = []
        if tabla_nombre:
            search_term = f"%{tabla_nombre}%"
            filters.append(
                or_(
                    self.entity_class.tabla_nombre.ilike(search_term),
                    self.entity_class.tipo.ilike(search_term),
                )
            )
        if tipo:
            filters.append(self.entity_class.tipo.ilike(f"%{tipo}%"))

        if filters:
            query = query.where(and_(*filters))

        # Obtener el número total de registros que coinciden con los filtros
        total_query = select(func.count()).select_from(self.entity_class)
        if filters:
            total_query = total_query.where(and_(*filters))
        total_result = await self.db.execute(total_query)
        total_records = total_result.scalar()

        # Calcular el número total de páginas
        total_pages = (total_records + limit - 1) // limit

        # Ajustar la consulta para incluir la tabla de usuarios
        user_alias = aliased(UsuarioModel)
        query = query.join(
            user_alias, self.entity_class.created_by == user_alias.usuario_id
        )
        query = query.add_columns(user_alias.username)

        # Obtener los registros paginados
        result = await self.db.execute(query)
        records = result.all()

        # Añadir total_pages y username a cada instancia
        result_list = []
        for record, username in records:
            record.total_pages = total_pages
            record.created_by = username
            result_list.append(record)

        return result_list
