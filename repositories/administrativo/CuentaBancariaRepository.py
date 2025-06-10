from models.administrativo.CuentaBancariaModel import CuentaBancariaModel
from models.administrativo.ProveedorModel import ProveedorModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql_administrativo import DB_ADMINISTRATIVO
from sqlalchemy import and_, func, or_
from sqlalchemy.future import select
from models.auth.UsuarioModel import UsuarioModel
from models.master.TablaMaestraModel import TablaMaestraModel
from models.master.TablaMaestraDetalleModel import TablaMaestraDetalleModel
from sqlalchemy.orm import aliased
from uuid import UUID


class CuentaBancariaRepository(BaseRepository):
    def __init__(self, db: DB_ADMINISTRATIVO) -> None:
        super().__init__(CuentaBancariaModel, db)

    async def get_all_and_search(
        self,
        limit: int = 10,
        page: int = 1,
        nombre_cuenta_bancaria: str | None = None,
        banco: str | None = None,
        moneda: str | None = None,
        tipo_cuenta: str | None = None,
        proveedor_id: str | None = None,
        nombre_proveedor: str | None = None,
        numero_documento: str | None = None,
    ) -> list[CuentaBancariaModel]:
        # Calcular el offset basado en la página (empezando desde 1)
        offset = (page - 1) * limit

        # Construir la consulta con filtros
        query = select(self.entity_class).offset(offset).limit(limit)

        filters = []
        # Ajustar la consulta para incluir la tabla de proveedores
        proveedor_alias = aliased(ProveedorModel)
        query = query.join(
            proveedor_alias,
            self.entity_class.proveedor_id == proveedor_alias.proveedor_id,
        )
        if nombre_cuenta_bancaria:
            search_term = f"%{nombre_cuenta_bancaria}%"
            filters.append(
                or_(
                    self.entity_class.banco.ilike(search_term),
                    self.entity_class.moneda.ilike(search_term),
                    self.entity_class.tipo_cuenta.ilike(search_term),
                    proveedor_alias.nombre_proveedor.ilike(search_term),
                    proveedor_alias.numero_documento.ilike(search_term),
                )
            )

        if banco:
            search_term = f"%{banco}%"
            filters.append(
                or_(
                    self.entity_class.banco.ilike(search_term),
                    self.entity_class.moneda.ilike(search_term),
                    self.entity_class.tipo_cuenta.ilike(search_term),
                )
            )
        if moneda:
            filters.append(self.entity_class.moneda.ilike(f"%{moneda}%"))
        if tipo_cuenta:
            filters.append(self.entity_class.tipo_cuenta.ilike(f"%{tipo_cuenta}%"))
        if proveedor_id:
            filters.append(self.entity_class.proveedor_id == UUID(proveedor_id))

        if nombre_proveedor:
            filters.append(
                proveedor_alias.nombre_proveedor.ilike(f"%{nombre_proveedor}%")
            )
        if numero_documento:
            filters.append(
                proveedor_alias.numero_documento.ilike(f"%{numero_documento}%")
            )

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

        proveedor_ids = [record[0].proveedor_id for record in records]

        proveedor_query = select(ProveedorModel).where(
            ProveedorModel.proveedor_id.in_(proveedor_ids)
        )
        proveedor_result = await self.db.execute(proveedor_query)
        proveedores: dict[str, ProveedorModel] = {
            row.proveedor_id: row for row in proveedor_result.scalars().all()
        }

        # Añadir total_pages y username a cada instancia
        result_list = []
        for record, username, estado_valor in records:
            record.total_pages = total_pages
            record.created_by = username
            record.estado = estado_valor
            record.proveedor = proveedores.get(record.proveedor_id)
            result_list.append(record)

        return result_list
