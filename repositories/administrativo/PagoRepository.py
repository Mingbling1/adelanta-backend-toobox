from models.administrativo.PagoModel import PagoModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql_administrativo import DB_ADMINISTRATIVO
from sqlalchemy.future import select
from sqlalchemy import and_, func, or_
from models.auth.UsuarioModel import UsuarioModel
from models.master.TablaMaestraModel import TablaMaestraModel
from models.master.TablaMaestraDetalleModel import TablaMaestraDetalleModel
from sqlalchemy.orm import aliased
from uuid import UUID


class PagoRepository(BaseRepository):
    def __init__(self, db: DB_ADMINISTRATIVO) -> None:
        super().__init__(PagoModel, db)

    async def get_all_and_search(
        self,
        limit: int = 10,
        page: int = 1,
        nombre_pago: str = None,
        pago_monto: float = None,
        pago_fecha: str = None,
        gasto_id: str | None = None,
    ):
        # Calcular el offset basado en la página (empezando desde 1)
        offset = (page - 1) * limit

        # Construir la consulta con filtros
        query = select(self.entity_class).offset(offset).limit(limit)

        filters = []

        filters.append(self.entity_class.estado == 1)

        if nombre_pago:
            search_term = f"%{nombre_pago}%"
            filters.append(
                or_(
                    self.entity_class.pago_descripcion.ilike(search_term),
                    self.entity_class.pago_monto.ilike(search_term),
                    self.entity_class.pago_fecha.ilike(search_term),
                ),
            )

        if pago_monto:
            filters.append(self.entity_class.pago_monto == pago_monto)

        if pago_fecha:
            filters.append(self.entity_class.pago_fecha.ilike(f"%{pago_fecha}%"))

        if gasto_id:
            filters.append(self.entity_class.gasto_id == UUID(gasto_id))

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

        # Subconsulta para obtener el valor del estado
        subquery = (
            select(TablaMaestraDetalleModel.valor)
            .join(
                TablaMaestraModel,
                and_(
                    TablaMaestraModel.tabla_maestra_id
                    == TablaMaestraDetalleModel.tabla_maestra_id,
                    TablaMaestraModel.tabla_nombre == "all",
                    TablaMaestraModel.tipo == "estado",
                ),
            )
            .where(TablaMaestraDetalleModel.codigo == self.entity_class.estado)
            .limit(1)
            .scalar_subquery()
        )

        # Añadir la subconsulta al query principal
        query = query.add_columns(subquery.label("estado_valor"))

        # Obtener los registros paginados
        result = await self.db.execute(query)
        records = result.all()

        # Añadir total_pages y username a cada instancia
        result_list = []
        for record, username, estado_valor in records:
            record.total_pages = total_pages
            record.created_by = username
            record.estado = estado_valor
            result_list.append(record)

        return result_list
