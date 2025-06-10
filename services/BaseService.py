from repositories.BaseRepository import BaseRepository
from fastapi import HTTPException
from typing import TypeVar, Generic, List, Union, Dict, Any, Optional
from config.logger import logger
from sqlalchemy.exc import NoResultFound

T = TypeVar("T")  # Tipo para los modelos


class BaseService(Generic[T]):
    """
    Clase base para servicios que provee operaciones CRUD estándar.
    Encapsula la interacción con el repositorio y maneja errores comunes.
    """

    def __init__(self, repository: BaseRepository[T]):
        self.repository = repository

    async def create(self, entity_data: dict, autocommit: bool = True) -> T:
        """
        Crea una nueva entidad con los datos proporcionados.

        Args:
            entity_data: Diccionario con los datos de la entidad
            autocommit: Si se debe hacer commit automáticamente

        Returns:
            La entidad creada

        Raises:
            HTTPException: Si ocurre un error al crear la entidad
        """
        try:
            return await self.repository.create(entity_data, autocommit)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error en service.create: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error al crear: {str(e)}")

    async def create_many(
        self, items: List[Dict[str, Any]], autocommit: bool = True
    ) -> None:
        """
        Crea múltiples entidades en una sola operación.

        Args:
            items: Lista de diccionarios con los datos de las entidades
            autocommit: Si se debe hacer commit automáticamente

        Raises:
            HTTPException: Si ocurre un error al crear las entidades
        """
        try:
            await self.repository.create_many(items, autocommit)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error en service.create_many: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error al crear múltiples registros: {str(e)}"
            )

    async def delete(self, entity: T, autocommit: bool = True) -> None:
        """
        Elimina una entidad.

        Args:
            entity: La entidad a eliminar
            autocommit: Si se debe hacer commit automáticamente

        Raises:
            HTTPException: Si ocurre un error al eliminar la entidad
        """
        try:
            await self.repository.delete(entity, autocommit)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error en service.delete: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error al eliminar: {str(e)}")

    async def get_by_id(
        self,
        entity_id: Union[int, str],
        id_column: str = "id",
        type_result: str = "first",
    ) -> Union[T, List[T]]:
        """
        Obtiene una entidad por su ID.

        Args:
            entity_id: El ID de la entidad
            id_column: El nombre de la columna ID
            type_result: 'first' para una entidad, 'all' para una lista

        Returns:
            La entidad encontrada o una lista de entidades

        Raises:
            HTTPException: Si no se encuentra la entidad o ocurre un error
        """
        try:
            result = await self.repository.get_by_id(
                entity_id, id_column, type_result=type_result
            )
            if not result and type_result == "first":
                raise HTTPException(status_code=404, detail="Elemento no encontrado")
            return result
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error en service.get_by_id: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error al obtener por ID: {str(e)}"
            )

    async def get_all(self, limit: Optional[int] = 10, offset: int = 0) -> List[T]:
        """
        Obtiene todas las entidades con paginación.

        Args:
            limit: Límite de resultados (None = sin límite)
            offset: Desplazamiento para paginación

        Returns:
            Lista de entidades

        Raises:
            HTTPException: Si ocurre un error al obtener las entidades
        """
        try:
            return await self.repository.get_all(limit, offset)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error en service.get_all: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error al obtener todos los registros: {str(e)}",
            )

    async def get_all_dicts(self, exclude_pk: bool = True) -> List[Dict[str, Any]]:
        """
        Obtiene todas las entidades como diccionarios.

        Args:
            exclude_pk: Si se deben excluir las columnas de clave primaria

        Returns:
            Lista de diccionarios con los datos de las entidades

        Raises:
            HTTPException: Si ocurre un error al obtener las entidades
        """
        try:
            return await self.repository.get_all_dicts(exclude_pk)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error en service.get_all_dicts: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error al obtener todos los registros como diccionarios: {str(e)}",
            )

    async def update(
        self,
        entity_id: Union[int, str],
        input_data: dict,
        id_column: str = "id",
        autocommit: bool = True,
    ) -> T:
        """
        Actualiza una entidad por su ID.

        Args:
            entity_id: El ID de la entidad
            input_data: Diccionario con los datos a actualizar
            id_column: El nombre de la columna ID
            autocommit: Si se debe hacer commit automáticamente

        Returns:
            La entidad actualizada

        Raises:
            HTTPException: Si no se encuentra la entidad o ocurre un error
        """
        try:
            result = await self.repository.update(
                entity_id, input_data, id_column, autocommit
            )
            return result
        except NoResultFound:
            raise HTTPException(
                status_code=404, detail="Elemento no encontrado para actualizar"
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error en service.update: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error al actualizar: {str(e)}"
            )

    async def create_many_with_return(
        self, items: List[Dict[str, Any]], column_search: str
    ) -> List[T]:
        """
        Crea múltiples entidades y las retorna.

        Args:
            items: Lista de diccionarios con los datos de las entidades
            column_search: Columna para buscar las entidades creadas

        Returns:
            Lista de entidades creadas

        Raises:
            HTTPException: Si ocurre un error al crear las entidades
        """
        try:
            return await self.repository.create_many_with_return(items, column_search)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error en service.create_many_with_return: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error al crear y retornar múltiples registros: {str(e)}",
            )

    async def delete_by_id(
        self, entity_id: Union[int, str], id_column: str = "id", autocommit: bool = True
    ) -> None:
        """
        Elimina una entidad por su ID.

        Args:
            entity_id: El ID de la entidad
            id_column: El nombre de la columna ID
            autocommit: Si se debe hacer commit automáticamente

        Raises:
            HTTPException: Si no se encuentra la entidad o ocurre un error
        """
        try:
            await self.repository.delete_by_id(entity_id, id_column, autocommit)
        except NoResultFound:
            raise HTTPException(
                status_code=404, detail="Elemento no encontrado para eliminar"
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error en service.delete_by_id: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Error al eliminar por ID: {str(e)}"
            )

    async def delete_all(self, autocommit: bool = True) -> None:
        """
        Elimina todas las entidades.

        Args:
            autocommit: Si se debe hacer commit automáticamente

        Raises:
            HTTPException: Si ocurre un error al eliminar las entidades
        """
        try:
            await self.repository.delete_all(autocommit)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error en service.delete_all: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error al eliminar todos los registros: {str(e)}",
            )
