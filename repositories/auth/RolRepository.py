from models.auth.RolModel import RolModel
from models.auth.RolPermisoModel import RolPermisoModel
from repositories.BaseRepository import BaseRepository
from repositories.auth.RolPermisoRepository import RolPermisoRepository
from config.db_mysql_administrativo import DB_ADMINISTRATIVO
from sqlalchemy.future import select
from sqlalchemy import and_, func
from uuid import UUID
from sqlalchemy.orm import joinedload
from fastapi import HTTPException, Depends
from config.logger import logger
from models.auth.PermisoModel import PermisoModel


class RolRepository(BaseRepository):
    def __init__(
        self,
        db: DB_ADMINISTRATIVO,
        rol_permiso_repository: RolPermisoRepository = Depends(),
    ) -> None:
        super().__init__(RolModel, db)
        self.rol_permiso_repository = rol_permiso_repository

    async def get_all_and_search(
        self,
        limit: int = 10,
        page: int = 1,
        nombre: str = None,
    ):
        # Calcular el offset basado en la página (empezando desde 1)
        offset = (page - 1) * limit

        # Construir la consulta con filtros
        query = select(self.entity_class).offset(offset).limit(limit)

        filters = []
        if nombre:
            search_term = f"%{nombre}%"
            filters.append(self.entity_class.nombre.ilike(search_term))

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

        # Obtener los registros paginados
        result = await self.db.execute(query)
        records = result.scalars().all()

        # Añadir total_pages a cada instancia

        for record in records:
            record.total_pages = total_pages

        return records

    async def obtener_roles_con_permisos(
        self,
        limit: int = 10,
        page: int = 1,
    ):
        """
        Obtiene roles con sus permisos asociados, organizados jerárquicamente.

        Los permisos se organizan en una estructura jerárquica donde:
        - Los módulos principales (sin módulo padre) aparecen como elementos de primer nivel
        - Los submódulos aparecen agrupados dentro de sus módulos padre correspondientes

        Args:
            limit: Número de registros por página
            page: Número de página (iniciando en 1)

        Returns:
            list: Lista de diccionarios, cada uno representando un rol con:
                - Información básica del rol
                - Lista de permisos jerárquicos (módulos con sus submódulos)
                - Total de páginas disponibles
        """
        # Calcular el offset basado en la página (empezando desde 1)
        offset = (page - 1) * limit

        # Construir la consulta con paginación y join
        query = (
            select(RolModel)
            .options(joinedload(RolModel.permisos).joinedload(RolPermisoModel.permiso))
            .offset(offset)
            .limit(limit)
        )

        # Obtener el número total de registros
        total_query = select(func.count()).select_from(RolModel)
        total_result = await self.db.execute(total_query)
        total_records = total_result.scalar()

        # Calcular el número total de páginas
        total_pages = (total_records + limit - 1) // limit

        # Obtener los registros paginados
        result = await self.db.execute(query)
        roles = result.unique().scalars().all()

        # Obtener TODOS los permisos para poder mostrar la jerarquía completa
        # Usamos modelo PermisoModel directamente para obtener todos los permisos

        stmt = select(PermisoModel).order_by(PermisoModel.nombre)
        result_permisos = await self.db.execute(stmt)
        todos_permisos = result_permisos.scalars().all()

        # Separar módulos principales y submódulos de TODOS los permisos
        todos_modulos_principales = [p for p in todos_permisos if not p.modulo_padre_id]
        todos_submodulos = [p for p in todos_permisos if p.modulo_padre_id]

        # Crear una lista con los roles y sus permisos
        result_list = []
        for rol in roles:
            rol_data = {
                k: v for k, v in rol.__dict__.items() if k != "_sa_instance_state"
            }

            # Obtener IDs de permisos asignados al rol para verificación
            permisos_rol_ids = {permiso.permiso.permiso_id for permiso in rol.permisos}

            # Organizar jerárquicamente, mostrando todos los módulos principales
            permisos_jerarquicos = []
            for modulo in todos_modulos_principales:
                # Verificar si este módulo principal está asignado al rol
                modulo_asignado = modulo.permiso_id in permisos_rol_ids

                modulo_dict = {
                    k: v
                    for k, v in modulo.__dict__.items()
                    if k != "_sa_instance_state"
                }
                # Agregar campo para indicar si está asignado al rol
                modulo_dict["asignado_al_rol"] = modulo_asignado

                # Encontrar submódulos para este módulo principal
                modulo_dict["submodulos"] = []
                for sub in todos_submodulos:
                    if sub.modulo_padre_id == modulo.permiso_id:
                        sub_dict = {
                            k: v
                            for k, v in sub.__dict__.items()
                            if k != "_sa_instance_state"
                        }
                        # Verificar si este submódulo está asignado al rol
                        sub_dict["asignado_al_rol"] = sub.permiso_id in permisos_rol_ids
                        modulo_dict["submodulos"].append(sub_dict)

                permisos_jerarquicos.append(modulo_dict)

            rol_data["permisos"] = permisos_jerarquicos
            rol_data["total_pages"] = total_pages
            result_list.append(rol_data)

        return result_list

    async def eliminar_rol_y_sus_permisos(self, rol_id: UUID):
        """
        Elimina un rol y todos sus permisos asociados en una sola transacción.
        """
        try:
            # Verificar que el rol existe
            rol = await self.get_by_id(rol_id, "rol_id")
            if not rol:
                raise HTTPException(
                    status_code=404, detail=f"Rol con ID {rol_id} no encontrado"
                )

            try:
                # Iniciar transacción manual
                # 1. Primero eliminamos permisos del rol sin autocommit
                permisos_eliminados = (
                    await self.rol_permiso_repository.eliminar_permisos_por_rol(
                        rol_id, autocommit=False
                    )
                )

                # 2. Luego eliminamos el rol sin autocommit
                await self.delete_by_id(rol_id, "rol_id", autocommit=False)

                # 3. Si todo fue exitoso, commit
                await self.db.commit()

                return {
                    "mensaje": f"Rol '{rol.nombre}' eliminado correctamente",
                    "permisos_eliminados": permisos_eliminados,
                }
            except Exception:
                # 4. Si algo falló, rollback
                await self.db.rollback()
                raise

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error al eliminar rol y sus permisos: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error al eliminar rol y sus permisos: {str(e)}",
            )
