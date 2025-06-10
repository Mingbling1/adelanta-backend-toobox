from models.administrativo.GastoModel import GastoModel
from models.administrativo.ProveedorModel import ProveedorModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql_administrativo import DB_ADMINISTRATIVO
from sqlalchemy.future import select
from sqlalchemy import and_, func, or_
from models.auth.UsuarioModel import UsuarioModel
from models.master.TablaMaestraModel import TablaMaestraModel
from models.master.TablaMaestraDetalleModel import TablaMaestraDetalleModel
from uuid import UUID


class GastoRepository(BaseRepository):
    def __init__(self, db: DB_ADMINISTRATIVO) -> None:
        super().__init__(GastoModel, db)

    async def get_all_and_search(
        self,
        limit: int = 10,
        page: int = 1,
        nombre_gasto: str = None,
        pago_fecha: str = None,
        gasto_id: str = None,
    ):
        # Calcular el offset basado en la página (empezando desde 1)
        offset = (page - 1) * limit

        # Construir la consulta con filtros y joins
        query = (
            select(self.entity_class, UsuarioModel, ProveedorModel)
            .join(
                UsuarioModel,
                self.entity_class.created_by == UsuarioModel.usuario_id,
            )
            .join(
                ProveedorModel,
                self.entity_class.proveedor_id == ProveedorModel.proveedor_id,
            )
            .offset(offset)
            .limit(limit)
        )

        filters = []
        filters.append(self.entity_class.estado == 1)
        if nombre_gasto:
            search_term = f"%{nombre_gasto}%"
            filters.append(
                or_(
                    self.entity_class.importe.ilike(search_term),
                    self.entity_class.tipo_gasto.ilike(search_term),
                    self.entity_class.tipo_CDP.ilike(search_term),
                    self.entity_class.moneda.ilike(search_term),
                ),
            )

        if pago_fecha:
            filters.append(self.entity_class.pago_fecha == pago_fecha)

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

        # Subconsulta para obtener el valor del estado
        subquery_estado = (
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

        subquery_gasto_estado = (
            select(TablaMaestraDetalleModel.valor)
            .join(
                TablaMaestraModel,
                and_(
                    TablaMaestraModel.tabla_maestra_id
                    == TablaMaestraDetalleModel.tabla_maestra_id,
                    TablaMaestraModel.tabla_nombre == "gasto",
                    TablaMaestraModel.tipo == "estado",
                ),
            )
            .where(TablaMaestraDetalleModel.codigo == self.entity_class.gasto_estado)
            .limit(1)
            .scalar_subquery()
        )

        # Añadir la subconsulta al query principal
        query = query.add_columns(subquery_estado.label("estado_valor"))
        query = query.add_columns(subquery_gasto_estado.label("gasto_estado_valor"))

        # Obtener los registros paginados
        result = await self.db.execute(query)
        records = result.all()

        # Añadir total_pages y username a cada instancia
        result_list = []
        for record, usuario, proveedor, estado_valor, gasto_estado_valor in records:
            record_data = record.__dict__
            record_data["total_pages"] = total_pages
            record_data["created_by"] = usuario.username
            record_data["estado"] = estado_valor
            record_data["gasto_estado"] = gasto_estado_valor
            record_data["proveedor"] = proveedor.__dict__

            result_list.append(record_data)

        return result_list
