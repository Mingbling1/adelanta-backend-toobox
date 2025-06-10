from models.administrativo.ArchivoModel import ArchivoModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql_administrativo import DB_ADMINISTRATIVO
from sqlalchemy.future import select
from sqlalchemy import and_, func
from models.auth.UsuarioModel import UsuarioModel
from models.master.TablaMaestraModel import TablaMaestraModel
from models.master.TablaMaestraDetalleModel import TablaMaestraDetalleModel
from sqlalchemy.orm import aliased
from uuid import UUID


class ArchivoRepository(BaseRepository):
    def __init__(
        self,
        db: DB_ADMINISTRATIVO,
    ) -> None:
        super().__init__(ArchivoModel, db)

    async def get_all_and_search(
        self,
        limit: int = 10,
        page: int = 1,
        nombre_archivo: str | None = None,
        tipo_archivo: str | None = None,
        gasto_id: str | None = None,
    ):
        # Calcular el offset basado en la página (empezando desde 1)
        offset = (page - 1) * limit

        # Construir la consulta con filtros
        query = select(self.entity_class).offset(offset).limit(limit)

        filters = []
        if nombre_archivo:
            search_term = f"%{nombre_archivo}%"
            filters.append(self.entity_class.nombre_archivo.ilike(search_term))
        if tipo_archivo:
            filters.append(self.entity_class.tipo_archivo.ilike(f"%{tipo_archivo}%"))

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

    async def get_existing_folders_dict(self) -> dict:
        query = select(ArchivoModel.path, ArchivoModel.path_id)
        result = await self.db.execute(query)
        records = result.all()
        existing_folders_dict = {record.path: record.path_id for record in records}
        return existing_folders_dict

    # async def get_existing_folders_dict(self) -> dict:
    #     query = select(ArchivoModel.path, ArchivoModel.path_id).filter(
    #         ArchivoModel.path.isnot(None), ArchivoModel.path_id.isnot(None)
    #     )
    #     result = await self.db.execute(query)
    #     records = result.all()
    #     existing_folders_dict = {record.path: record.path_id for record in records}
    #     return existing_folders_dict

    async def create_many(
        self,
        archivos: list[ArchivoModel],
    ) -> list[ArchivoModel]:
        new_archivos = []
        for archivo_info in archivos:
            self.db.add(archivo_info)
            new_archivos.append(archivo_info)
        await self.db.commit()
        return new_archivos
