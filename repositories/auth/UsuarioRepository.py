from models.auth.UsuarioModel import UsuarioModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql_administrativo import DB_ADMINISTRATIVO
from sqlalchemy.future import select
from sqlalchemy import and_, func
from models.master.TablaMaestraModel import (
    TablaMaestraModel,
)
from repositories.auth.RolRepository import RolRepository
from models.master.TablaMaestraDetalleModel import TablaMaestraDetalleModel
from config.logger import logger
from fastapi import HTTPException, Depends
from sqlalchemy.orm import joinedload
from models.auth.RolModel import RolModel
from models.auth.RolPermisoModel import RolPermisoModel
from models.auth.PermisoModel import PermisoModel


class UsuarioRepository(BaseRepository[UsuarioModel]):
    def __init__(
        self, db: DB_ADMINISTRATIVO, rol_repository: RolRepository = Depends()
    ) -> None:
        super().__init__(UsuarioModel, db)
        self.rol_repository = rol_repository

    async def get_usuario_by_email(self, email: str) -> UsuarioModel:
        statement = select(self.entity_class).where(self.entity_class.email == email)
        result = await self.db.execute(statement)
        found = result.scalars().first()
        return found

    async def get_usuario_by_email_detallado(self, email: str) -> dict:
        """
        Obtiene un usuario por su email, incluyendo información básica de su rol
        y los submódulos asignados.
        """
        # 1) Subconsulta para obtener el valor del estado
        subq_estado = (
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

        # 2) Consulta principal con join de rol (simplificada, sin cargar todos los permisos)
        statement = (
            select(self.entity_class, subq_estado.label("estado_valor"))
            .where(self.entity_class.email == email)
            .options(joinedload(self.entity_class.rol))
        )

        # 3) Ejecutar consulta
        result = await self.db.execute(statement)
        record = result.unique().first()

        # 4) Si no hay resultados, retornar None
        if not record:
            return None

        usuario, estado_valor = record

        # 5) Formatear los datos básicos del usuario
        data = {**usuario.__dict__}
        if "_sa_instance_state" in data:
            del data["_sa_instance_state"]

        # 6) Añadir valor del estado
        data["estado"] = estado_valor

        # 7) Agregar información del rol y sus submódulos asignados si existe
        if usuario.rol:
            rol_data = {**usuario.rol.__dict__}
            if "_sa_instance_state" in rol_data:
                del rol_data["_sa_instance_state"]

            submodulos_query = (
                select(PermisoModel)
                .join(
                    RolPermisoModel,
                    and_(
                        RolPermisoModel.permiso_id == PermisoModel.permiso_id,
                        RolPermisoModel.rol_id == usuario.rol.rol_id,
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
            data["rol"] = rol_data

        return data

    async def get_all_and_search(
        self,
        limit: int = 10,
        page: int = 1,
        username: str = None,
        email: str = None,
    ) -> list[dict]:
        # 1) Offset
        offset = (page - 1) * limit

        # 2) Condiciones
        conditions = []
        if username:
            conditions.append(self.entity_class.username.ilike(f"%{username}%"))
        if email:
            conditions.append(self.entity_class.email.ilike(f"%{email}%"))

        # 3) Conteo total
        base_query = select(func.count()).select_from(self.entity_class)
        if conditions:
            base_query = base_query.where(and_(*conditions))
        total_records = (await self.db.execute(base_query)).scalar() or 0
        total_pages = (total_records + limit - 1) // limit

        # 4) Subconsulta para obtener el valor del estado
        subq_estado = (
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

        # 5) Consulta principal con paginación, valor de estado y join de rol
        main_query = select(
            self.entity_class, subq_estado.label("estado_valor")
        ).options(joinedload(self.entity_class.rol))
        if conditions:
            main_query = main_query.where(and_(*conditions))
        main_query = main_query.offset(offset).limit(limit)

        # Ordenar por estado de forma descendente (1 primero, luego 0)
        # y luego por username para un orden consistente
        main_query = main_query.order_by(
            self.entity_class.estado.desc(), self.entity_class.username.asc()
        )

        result = await self.db.execute(main_query)
        records = result.unique().all()  # Usar unique() para evitar duplicados

        # 6) Armar salida
        output = []
        for usuario, estado_valor in records:
            # Datos básicos del usuario
            data = {**usuario.__dict__}
            if "_sa_instance_state" in data:
                del data["_sa_instance_state"]

            data["estado"] = estado_valor
            data["total_pages"] = total_pages

            # 7) Agregar información del rol y sus submódulos si existe
            if usuario.rol:
                rol_data = {**usuario.rol.__dict__}
                if "_sa_instance_state" in rol_data:
                    del rol_data["_sa_instance_state"]

                # Obtener submódulos asignados a este rol directamente
                from models.auth.PermisoModel import PermisoModel

                submodulos_query = (
                    select(PermisoModel)
                    .join(
                        RolPermisoModel,
                        and_(
                            RolPermisoModel.permiso_id == PermisoModel.permiso_id,
                            RolPermisoModel.rol_id == usuario.rol.rol_id,
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
                data["rol"] = rol_data

            output.append(data)

        return output

    async def asignar_rol_a_usuario(
        self, usuario_id: str, rol_id: str, updated_by: str
    ) -> UsuarioModel:
        """Asigna un rol a un usuario."""
        try:
            # 1) Verificar que el usuario existe
            usuario = await self.get_by_id(usuario_id, "usuario_id")
            if not usuario:
                raise HTTPException(
                    status_code=404, detail=f"Usuario con ID {usuario_id} no encontrado"
                )

            rol = await self.rol_repository.get_by_id(rol_id, "rol_id")
            if not rol:
                raise HTTPException(
                    status_code=404, detail=f"Rol con ID {rol_id} no encontrado"
                )

            # 3) Actualizar usuario con el rol y updated_by
            input_data = {"rol_id": rol_id, "updated_by": updated_by}
            return await self.update(
                entity_id=usuario_id, input=input_data, id_column="usuario_id"
            )

        except HTTPException:
            raise
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error al asignar rol: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error al asignar rol: {str(e)}"
            )

    async def cambiar_estado_usuario(
        self,
        usuario_id: str,
        updated_by: str,
        estado: int = 0,
    ) -> UsuarioModel:
        """
        Actualiza el estado de un usuario (activo/inactivo).

        Args:
            usuario_id: ID único del usuario
            estado: Nuevo estado del usuario (0=inactivo, 1=activo)
            updated_by: ID del usuario que realiza la modificación

        Returns:
            UsuarioModel: Usuario actualizado
        """
        try:
            # Verificar que el usuario existe
            usuario = await self.get_by_id(usuario_id, "usuario_id")
            if not usuario:
                raise HTTPException(
                    status_code=404, detail=f"Usuario con ID {usuario_id} no encontrado"
                )

            # Verificar si el usuario ya tiene el estado que se quiere asignar
            if usuario.estado == estado:
                estado_txt = "inactivo" if estado == 0 else "activo"
                raise HTTPException(
                    status_code=400, detail=f"El usuario ya se encuentra {estado_txt}"
                )

            # Preparar datos de actualización
            input_data = {
                "estado": estado,
                "updated_by": updated_by,
            }

            # Utilizar el método base update
            return await self.update(
                entity_id=usuario_id, input=input_data, id_column="usuario_id"
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error al cambiar estado del usuario: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error al cambiar estado del usuario: {str(e)}"
            )
