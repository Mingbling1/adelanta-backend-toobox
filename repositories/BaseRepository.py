from sqlalchemy.future import select
from sqlalchemy import update, delete, insert
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import (
    AsyncSession,
)
from sqlalchemy.exc import IntegrityError, OperationalError
from fastapi import HTTPException
from typing import TypeVar, Generic, List, Union, Type, Dict, Any
from config.logger import logger
import sqlalchemy as sa

T = TypeVar("T")  # Tipo para los modelos


class BaseRepository(Generic[T]):
    def __init__(self, entity_class: Type[T], db: AsyncSession) -> None:
        self.entity_class = entity_class
        self.db = db

    async def create(self, entity_data, autocommit: bool = True) -> T:
        entity = self.entity_class(**entity_data)
        self.db.add(entity)
        try:
            await self.db.flush()  # Enviamos los cambios a la base de datos
            await self.db.refresh(entity)
            if autocommit:
                await self.db.commit()
            return entity
        except IntegrityError as e:
            if autocommit:
                await self.db.rollback()
            logger.error(f"Error en create: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail=f"Error: {str(e.orig)}",
            )
        except Exception as e:
            if autocommit:
                await self.db.rollback()
            logger.error(f"Error inesperado en create: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error inesperado: {str(e)}",
            )

    async def create_many(
        self, input: List[Dict[str, Any]], autocommit: bool = True
    ) -> None:
        try:
            await self.db.execute(self.entity_class.__table__.insert(), input)
            if autocommit:
                await self.db.commit()
        except Exception as e:
            if autocommit:
                await self.db.rollback()
            logger.error(f"Error en create_many: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error al crear múltiples registros: {str(e)}",
            )

    async def delete(self, entity: T, autocommit: bool = True) -> None:
        try:
            await self.db.delete(entity)
            if autocommit:
                await self.db.commit()
        except Exception as e:
            if autocommit:
                await self.db.rollback()
            logger.error(f"Error en delete: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error al eliminar: {str(e)}",
            )

    async def get_by_id(
        self, entity_id: Union[int, str], id_column: str, type_result: str = "first"
    ) -> Union[T, List[T]]:
        query = select(self.entity_class).where(
            getattr(self.entity_class, id_column) == entity_id
        )
        result = await self.db.execute(query)

        if type_result == "all":
            found = result.scalars().all()
        elif type_result == "first":
            found = result.scalars().first()
        else:
            raise ValueError("type_result must be 'all' or 'first'")

        # if not found:
        #     raise NoResultFound(f"{self.entity_class.__name__} no encontrado.")

        return found

    async def get_all(self, limit: int | None = 10, offset: int = 0) -> list[T]:
        try:
            query = select(self.entity_class).offset(offset).limit(limit)
            result = await self.db.execute(query)
            return result.scalars().all()
        except OperationalError as e:
            await self.db.rollback()
            logger.error(f"OperationalError en get_all: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error de conexión a la base de datos: {str(e)}",
            )
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error inesperado en get_all: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error inesperado: {str(e)}",
            )

    async def get_all_dicts(self, exclude_pk: bool = True) -> list[dict]:
        """
        Devuelve todos los registros como lista de dicts,
        respetando el orden de columnas en el modelo.
        Si exclude_pk=True, omite las columnas que son primary_key.
        """
        # Extraemos nombres de columnas según el modelo
        cols = [
            col.name
            for col in self.entity_class.__table__.columns
            if not (exclude_pk and col.primary_key)
        ]
        # Construimos el SELECT genérico
        stmt = sa.select(*[getattr(self.entity_class, c) for c in cols])
        result = await self.db.execute(stmt)
        # Cada row._mapping es un dict {col: valor}
        return [dict(row._mapping) for row in result]

    async def update(
        self,
        entity_id: Union[int, str],
        input: dict,
        id_column: str,
        autocommit: bool = True,
    ) -> T:
        """ "Actualizar un registro por ID y retornarlo."""
        try:
            query = (
                update(self.entity_class)
                .where(getattr(self.entity_class, id_column) == entity_id)
                .values(**input)
                .execution_options(synchronize_session="fetch")
            )
            result = await self.db.execute(query)
            if result.rowcount == 0:
                raise NoResultFound(f"{self.entity_class.__name__} no encontrado.")
            if autocommit:
                await self.db.commit()
            return await self.get_by_id(entity_id, id_column)
        except Exception as e:
            if autocommit:
                await self.db.rollback()
            logger.error(f"Error en update: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error al actualizar: {str(e)}",
            )

    async def create_many_with_return(
        self, input: List[Dict[str, Any]], column_search: str = None
    ) -> List[T]:
        """Crear multiples registros y retornarlos. Sin embargo, se debe especificar una columna para buscar los registros creados.
        Debido a que no se puede retornar los registros creados con el id generado por la base de datos puesto que estamos usando self.entity_class.__table__.insert().
        Aunque podríamos utilizar self.add_all para retornar los ids generados por la base de datos.
        """
        await self.db.execute(self.entity_class.__table__.insert(), input)
        await self.db.commit()

        # Obtener los valores únicos de la columna de búsqueda
        search_values = [item[column_search] for item in input]

        query = select(self.entity_class).where(
            getattr(self.entity_class, column_search).in_(search_values)
        )
        result = await self.db.execute(query)
        return result.scalars().all()

    async def delete_by_id(
        self, entity_id: Union[int, str], id_column: str, autocommit: bool = True
    ) -> None:
        try:
            query = delete(self.entity_class).where(
                getattr(self.entity_class, id_column) == entity_id
            )
            result = await self.db.execute(query)
            if result.rowcount == 0:
                raise NoResultFound(f"{self.entity_class.__name__} no encontrado.")
            if autocommit:
                await self.db.commit()
        except Exception as e:
            if autocommit:
                await self.db.rollback()
            logger.error(f"Error en delete_by_id: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error al eliminar por ID: {str(e)}",
            )

    async def delete_all(self, autocommit: bool = True):
        try:
            statement = delete(self.entity_class)
            await self.db.execute(statement)
            if autocommit:
                await self.db.commit()
        except Exception as e:
            if autocommit:
                await self.db.rollback()
            logger.error(f"Error en delete_all: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error al eliminar todos los registros: {str(e)}",
            )

    def _chunks(self, lst, n):
        """Divide una lista en lotes de tamaño n"""
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    async def bulk_insert_chunked(
        self,
        records: List[Dict[str, Any]],
        chunk_size: int = 1000,
        autocommit: bool = True,
    ) -> None:
        """
        Inserta en chunks secuenciales usando la misma sesión async (self.db).
        """
        total = len(records)
        logger.warning(f"Total de registros a insertar: {total}")

        # df = pd.DataFrame(records)
        # df.to_excel("output.xlsx", index=False, engine="openpyxl", sheet_name="Sheet1")
        for start in range(0, total, chunk_size):
            chunk = records[start : start + chunk_size]
            stmt = insert(self.entity_class).values(chunk)
            await self.db.execute(stmt)
            if autocommit:
                await self.db.commit()

    async def delete_and_bulk_insert_chunked(
        self,
        records: List[Dict[str, Any]],
        chunk_size: int = 1000,
        autocommit: bool = True,
    ) -> None:
        """
        Borra todo (delete_all) y luego lanza el bulk_insert_chunked.
        """
        # 1) Trunca la tabla usando el helper existente
        await self.delete_all(autocommit=autocommit)
        # 2) Inserta en chunks
        await self.bulk_insert_chunked(
            records=records,
            chunk_size=chunk_size,
            autocommit=autocommit,
        )
