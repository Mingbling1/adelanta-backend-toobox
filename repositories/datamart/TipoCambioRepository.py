from models.datamart.TipoCambioModel import TipoCambioModel
from repositories.BaseRepository import BaseRepository
from config.db_mysql import DB
from config.logger import logger
from fastapi import HTTPException
from sqlalchemy.exc import OperationalError
from sqlalchemy import select


class TipoCambioRepository(BaseRepository[TipoCambioModel]):
    def __init__(self, db: DB) -> None:
        super().__init__(TipoCambioModel, db)

    async def obtener_todos_con_filtro_fecha(
        self, limit: int | None = 10, offset: int = 0, year_month: str | None = None
    ) -> list[TipoCambioModel]:
        """
        Obtener tipos de cambio con filtro opcional por año-mes.
        Filtra por el campo TipoCambioFecha.

        Args:
            limit: Límite de registros
            offset: Offset para paginación
            year_month: Formato "YYYY-MM" para filtrar (ej: "2025-02")

        Returns:
            Lista de TipoCambioModel ordenados por fecha ascendente
        """
        try:
            # Query base ordenado por fecha
            query = select(TipoCambioModel).order_by(
                TipoCambioModel.TipoCambioFecha.asc()
            )

            # Aplicar filtro por año-mes si se proporciona
            if year_month:
                try:
                    # Validar formato
                    if len(year_month) != 7 or year_month[4] != "-":
                        raise ValueError("Formato inválido")

                    year = int(year_month[:4])
                    month = int(year_month[5:7])

                    # Validar valores
                    if month < 1 or month > 12:
                        raise ValueError("Mes debe estar entre 1 y 12")

                    # Filtrar usando LIKE para el campo string TipoCambioFecha
                    # Como TipoCambioFecha es string formato "YYYY-MM-DD"
                    filter_pattern = f"{year:04d}-{month:02d}-%"
                    query = query.where(
                        TipoCambioModel.TipoCambioFecha.like(filter_pattern)
                    )

                except (ValueError, IndexError) as ve:
                    logger.error(
                        f"Formato year_month inválido: {year_month} - {str(ve)}"
                    )
                    raise HTTPException(
                        status_code=400,
                        detail="Formato year_month debe ser 'YYYY-MM'. Ejemplo: '2025-02'",
                    )

            # Aplicar paginación
            if limit is not None:
                query = query.offset(offset).limit(limit)
            else:
                query = query.offset(offset)

            # Ejecutar query
            result = await self.db.execute(query)
            registros = result.scalars().all()

            logger.info(
                f"Obtenidos {len(registros)} registros de tipo_cambio con filtro year_month={year_month}"
            )
            return registros

        except HTTPException:
            # Re-lanzar HTTPException tal como está
            raise
        except OperationalError as e:
            await self.db.rollback()
            logger.error(
                f"OperationalError en obtener_todos_con_filtro_fecha: {str(e)}"
            )
            raise HTTPException(
                status_code=500,
                detail=f"Error de conexión a la base de datos: {str(e)}",
            )
        except Exception as e:
            await self.db.rollback()
            logger.error(
                f"Error inesperado en obtener_todos_con_filtro_fecha: {str(e)}"
            )
            raise HTTPException(
                status_code=500,
                detail=f"Error inesperado: {str(e)}",
            )
