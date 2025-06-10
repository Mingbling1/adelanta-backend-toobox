from models.master.TablaMaestraDetalleModel import TablaMaestraDetalleModel
from models.master.TablaMaestraModel import TablaMaestraModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql_administrativo import DB_ADMINISTRATIVO
from sqlalchemy.future import select
from sqlalchemy import and_, func, or_
from models.auth.UsuarioModel import UsuarioModel
from sqlalchemy.orm import aliased


class TablaMaestraDetalleRepository(BaseRepository):
    def __init__(self, db: DB_ADMINISTRATIVO) -> None:
        super().__init__(TablaMaestraDetalleModel, db)

    async def get_all_and_search(
        self,
        limit: int = 10,
        page: int = 1,
        codigo: int = None,
        valor: str = None,
        descripcion: str = None,
        tabla_maestra_id: str = None,
        tabla_nombre: str = None,
        tipo: str = None,
    ):
        # Calcular el offset basado en la página (empezando desde 1)
        offset = (page - 1) * limit

        # Construir la consulta con filtros
        query = select(self.entity_class).offset(offset).limit(limit)

        filters = []
        if codigo:
            filters.append(self.entity_class.codigo == codigo)
        if valor:
            filters.append(self.entity_class.valor.ilike(f"%{valor}%"))
        if descripcion:
            filters.append(self.entity_class.descripcion.ilike(f"%{descripcion}%"))
        if tabla_maestra_id:
            filters.append(self.entity_class.tabla_maestra_id == tabla_maestra_id)

        if filters:
            query = query.where(and_(*filters))

        # Ajustar la consulta para incluir la tabla maestra si se proporciona tabla_nombre y tipo
        if tabla_nombre and tipo:
            tabla_maestra_alias = aliased(TablaMaestraModel)
            query = query.join(
                tabla_maestra_alias,
                and_(
                    tabla_maestra_alias.tabla_maestra_id
                    == self.entity_class.tabla_maestra_id,
                    tabla_maestra_alias.tabla_nombre == tabla_nombre,
                    tabla_maestra_alias.tipo == tipo,
                ),
            )

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
