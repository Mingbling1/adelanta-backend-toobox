from uuid import UUID
from sqlalchemy import and_, insert, delete
from sqlalchemy.future import select
from sqlalchemy import func
from config.db_mysql_administrativo import DB_ADMINISTRATIVO
from models.auth.RolPermisoModel import RolPermisoModel
from repositories.BaseRepository import BaseRepository
from fastapi import HTTPException
from config.logger import logger
from models.auth.PermisoModel import PermisoModel


class RolPermisoRepository(BaseRepository[RolPermisoModel]):
    def __init__(self, db: DB_ADMINISTRATIVO) -> None:
        super().__init__(RolPermisoModel, db)

    async def asignar_permiso_a_rol(self, rol_id: UUID, permiso_id: UUID):
        """Asigna un permiso a un rol."""
        try:
            stmt = insert(self.entity_class).values(
                rol_id=rol_id, permiso_id=permiso_id
            )
            await self.db.execute(stmt)
            await self.db.commit()
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error al asignar permiso a rol: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error al asignar permiso: {str(e)}"
            )

    async def asignar_permisos_a_rol_masivo(self, rol_id: UUID, permisos: list[UUID]):
        """Asigna múltiples permisos a un rol."""
        try:
            stmt = insert(self.entity_class).values(
                [
                    {"rol_id": rol_id, "permiso_id": permiso_id}
                    for permiso_id in permisos
                ]
            )
            await self.db.execute(stmt)
            await self.db.commit()
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error al asignar permisos masivamente: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error al asignar permisos: {str(e)}"
            )

    async def eliminar_permiso_de_rol(self, rol_id: UUID, permiso_id: UUID):
        """Elimina un permiso de un rol."""
        try:
            stmt = delete(self.entity_class).where(
                and_(
                    self.entity_class.rol_id == rol_id,
                    self.entity_class.permiso_id == permiso_id,
                )
            )
            await self.db.execute(stmt)
            await self.db.commit()
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error al eliminar permiso del rol: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error al eliminar permiso: {str(e)}"
            )

    async def obtener_permisos_por_rol(self, rol_id: UUID):
        """Obtiene todos los permisos asociados a un rol."""
        try:
            stmt = select(self.entity_class.permiso_id).where(
                self.entity_class.rol_id == rol_id
            )
            result = await self.db.execute(stmt)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error al obtener permisos por rol: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error al obtener permisos: {str(e)}"
            )

    async def obtener_roles_por_permiso(self, permiso_id: UUID):
        """Obtiene todos los roles asociados a un permiso."""
        try:
            stmt = select(self.entity_class.rol_id).where(
                self.entity_class.permiso_id == permiso_id
            )
            result = await self.db.execute(stmt)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error al obtener roles por permiso: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error al obtener roles: {str(e)}"
            )

    async def eliminar_permisos_por_rol(self, rol_id: UUID, autocommit: bool = True):
        """
        Elimina todos los permisos asociados a un rol específico.

        Args:
            rol_id: ID del rol cuyos permisos se eliminarán
            autocommit: Si se debe hacer commit automáticamente

        Returns:
            int: Número de registros eliminados

        Raises:
            HTTPException: Si ocurre algún error durante el proceso
        """
        try:
            # Usar delete sin autocommit para que forme parte de una transacción más grande
            stmt = delete(self.entity_class).where(self.entity_class.rol_id == rol_id)
            result = await self.db.execute(stmt)

            if autocommit:
                await self.db.commit()

            return result.rowcount

        except Exception as e:
            if autocommit:
                await self.db.rollback()
            logger.error(f"Error al eliminar permisos del rol: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error al eliminar permisos del rol: {str(e)}"
            )

    async def actualizar_permisos_rol(
        self,
        rol_id: UUID,
        permisos_agregar: list[UUID] = None,
        permisos_quitar: list[UUID] = None,
    ):
        """
        Actualiza los permisos de un rol, permitiendo solo submódulos.

        Casos de uso:
        - Si solo se proporciona permisos_agregar: Establece exactamente esos permisos
        (elimina todos los existentes y agrega los nuevos)
        - Si se proporcionan ambos: Quita los permisos especificados y agrega los nuevos
        - Si solo se proporciona permisos_quitar: Solo elimina los permisos indicados

        Args:
            rol_id: ID del rol a actualizar
            permisos_agregar: Lista de IDs de permisos a establecer/agregar (solo submódulos)
            permisos_quitar: Lista de IDs de permisos a quitar (solo submódulos)

        Returns:
            dict: Información sobre los cambios realizados

        Raises:
            HTTPException: Si se intentan asignar módulos principales en lugar de submódulos
        """
        try:
            # Verificar que solo se estén agregando/quitando submódulos
            if permisos_agregar or permisos_quitar:

                # Combinar permisos a verificar
                permisos_verificar = []
                if permisos_agregar:
                    permisos_verificar.extend(permisos_agregar)
                if permisos_quitar:
                    permisos_verificar.extend(permisos_quitar)

                if permisos_verificar:
                    # Obtener todos los permisos mencionados
                    stmt = select(PermisoModel).where(
                        PermisoModel.permiso_id.in_(permisos_verificar)
                    )
                    result = await self.db.execute(stmt)
                    permisos = result.scalars().all()

                    # Verificar si alguno es un módulo principal (sin padre)
                    modulos_padres = [p for p in permisos if p.modulo_padre_id is None]
                    if modulos_padres:
                        modulos_padre_ids = [str(p.permiso_id) for p in modulos_padres]
                        raise HTTPException(
                            status_code=400,
                            detail=f"Solo se permiten submódulos IDs para agregar o quitar. "
                            f"Módulos padre detectados: {', '.join(modulos_padre_ids)}",
                        )

            # Inicializar contadores
            permisos_eliminados = 0
            permisos_agregados = 0

            # Para todos los casos, necesitamos saber los permisos actuales
            permisos_actuales = await self.obtener_permisos_por_rol(rol_id)

            # Caso 1: Reemplazar todos los permisos (eliminar actuales y agregar nuevos)
            if permisos_agregar is not None and permisos_quitar is None:
                # Eliminar todos los permisos existentes sin autocommit
                permisos_eliminados = await self.eliminar_permisos_por_rol(
                    rol_id, autocommit=False
                )

                # Agregar los nuevos permisos si la lista no está vacía
                if permisos_agregar:
                    stmt = insert(self.entity_class).values(
                        [
                            {"rol_id": rol_id, "permiso_id": permiso_id}
                            for permiso_id in permisos_agregar
                        ]
                    )
                    await self.db.execute(stmt)
                    permisos_agregados = len(permisos_agregar)

                await self.db.commit()

            # Caso 2: Actualización selectiva (quitar algunos permisos y/o agregar otros)
            else:
                # Normalizar listas para evitar NoneType
                permisos_agregar = permisos_agregar or []
                permisos_quitar = permisos_quitar or []

                # Eliminar permisos específicos en una sola operación
                if permisos_quitar:
                    stmt = delete(self.entity_class).where(
                        and_(
                            self.entity_class.rol_id == rol_id,
                            self.entity_class.permiso_id.in_(permisos_quitar),
                        )
                    )
                    result = await self.db.execute(stmt)
                    permisos_eliminados = result.rowcount

                # Agregar permisos nuevos (evitando duplicados)
                if permisos_agregar:
                    # Calcular qué permisos realmente agregar (los que no existan ya)
                    permisos_existentes = set(permisos_actuales)
                    permisos_a_agregar = [
                        {"rol_id": rol_id, "permiso_id": p_id}
                        for p_id in permisos_agregar
                        if p_id not in permisos_existentes
                    ]

                    # Solo ejecutar el insert si hay permisos nuevos
                    if permisos_a_agregar:
                        stmt = insert(self.entity_class).values(permisos_a_agregar)
                        await self.db.execute(stmt)
                        permisos_agregados = len(permisos_a_agregar)

                await self.db.commit()

            # Obtener estado final después de todos los cambios
            permisos_finales = await self.obtener_permisos_por_rol(rol_id)

            # Crear respuesta estandarizada
            return {
                "rol_id": str(rol_id),
                "permisos_eliminados": permisos_eliminados,
                "permisos_agregados": permisos_agregados,
                "total_permisos": len(permisos_finales),
                "operacion_exitosa": True,
            }

        except HTTPException as e:
            await self.db.rollback()
            logger.error(f"Error al actualizar permisos del rol: {str(e.detail)}")
            raise e
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error al actualizar permisos del rol: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error al actualizar permisos del rol: {str(e)}",
            )

    async def listar_roles_con_submodulos(
        self,
        limit: int = 10,
        page: int = 1,
    ):
        """
        Obtiene una lista paginada de roles con sus submódulos asignados.

        Args:
            limit: Número de roles por página
            page: Número de página (iniciando en 1)

        Returns:
            list: Lista de roles con sus submódulos asignados paginados
        """
        try:
            # Calcular el offset basado en la página
            offset = (page - 1) * limit

            # 1. Obtener roles con paginación
            from models.auth.RolModel import RolModel

            # Contar total de roles para paginación
            count_query = select(func.count()).select_from(RolModel)
            total_result = await self.db.execute(count_query)
            total_records = total_result.scalar()
            total_pages = (total_records + limit - 1) // limit

            # Obtener roles paginados
            roles_query = (
                select(RolModel).order_by(RolModel.nombre).offset(offset).limit(limit)
            )
            roles_result = await self.db.execute(roles_query)
            roles = roles_result.scalars().all()

            # 2. Para cada rol, obtener sus submódulos asignados
            from models.auth.PermisoModel import PermisoModel

            result_list = []
            for rol in roles:
                # Datos básicos del rol
                rol_data = {
                    k: v for k, v in rol.__dict__.items() if k != "_sa_instance_state"
                }

                # Obtener submódulos asignados a este rol
                submodulos_query = (
                    select(PermisoModel)
                    .join(
                        self.entity_class,
                        and_(
                            self.entity_class.permiso_id == PermisoModel.permiso_id,
                            self.entity_class.rol_id == rol.rol_id,
                        ),
                    )
                    .where(PermisoModel.modulo_padre_id.is_not(None))  # Solo submódulos
                    .order_by(PermisoModel.nombre)
                )

                submodulos_result = await self.db.execute(submodulos_query)
                submodulos = submodulos_result.scalars().all()

                # Convertir submódulos a diccionarios
                submodulos_list = [
                    {k: v for k, v in sub.__dict__.items() if k != "_sa_instance_state"}
                    for sub in submodulos
                ]

                # Agregar submódulos al rol
                rol_data["submodulos"] = submodulos_list
                rol_data["total_pages"] = total_pages

                result_list.append(rol_data)

            return result_list

        except Exception as e:
            logger.error(f"Error al obtener roles con submódulos: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error al obtener roles con submódulos: {str(e)}",
            )
