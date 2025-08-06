"""
🏭 Factory de Repositories para Celery Tasks
Crea instancias frescas de repositories sin singletons
"""

from config.db_mysql import DatabaseSessionManager, get_db
from config.settings import settings
from repositories.datamart.TipoCambioRepository import TipoCambioRepository
from repositories.datamart.KPIAcumuladoRepository import KPIAcumuladoRepository
from repositories.datamart.KPIRepository import KPIRepository
from repositories.datamart.SaldosRepository import SaldosRepository
from repositories.datamart.NuevosClientesNuevosPagadoresRepository import (
    NuevosClientesNuevosPagadoresRepository,
)
from repositories.datamart.ActualizacionReportesRepository import (
    ActualizacionReportesRepository,
)
from repositories.datamart.CXCAcumuladoDIMRepository import CXCAcumuladoDIMRepository
from repositories.datamart.CXCPagosFactRepository import CXCPagosFactRepository
from repositories.datamart.CXCDevFactRepository import CXCDevFactRepository


class RepositoryFactory:
    """
    Factory para crear repositories con sesiones aisladas
    Cada task de Celery obtiene sus propias instancias
    """

    def __init__(self):
        # Crear session manager con configuración optimizada para Celery
        self.session_manager = DatabaseSessionManager(
            host=str(settings.DATABASE_MYSQL_URL),
            engine_kwargs={
                "echo": False,  # Sin logging detallado en tasks
                "future": True,
                "pool_size": 10,  # Pool más pequeño por worker
                "max_overflow": 5,  # Overflow reducido
                "pool_recycle": 1800,  # Reciclar conexiones cada 30 min
            },
        )

    async def get_db_session(self):
        """Obtener sesión de base de datos"""
        async for session in get_db(self.session_manager):
            return session

    async def create_tipo_cambio_repository(self) -> TipoCambioRepository:
        """Crear repository de TipoCambio"""
        db_session = await self.get_db_session()
        return TipoCambioRepository(db=db_session)

    async def create_kpi_acumulado_repository(self) -> KPIAcumuladoRepository:
        """Crear repository de KPI Acumulado"""
        db_session = await self.get_db_session()
        return KPIAcumuladoRepository(db=db_session)

    async def create_kpi_repository(self) -> KPIRepository:
        """Crear repository de KPI"""
        db_session = await self.get_db_session()
        return KPIRepository(db=db_session)

    async def create_saldos_repository(self) -> SaldosRepository:
        """Crear repository de Saldos"""
        db_session = await self.get_db_session()
        return SaldosRepository(db=db_session)

    async def create_nuevos_clientes_nuevos_pagadores_repository(
        self,
    ) -> NuevosClientesNuevosPagadoresRepository:
        """Crear repository de NuevosClientesNuevosPagadores"""
        db_session = await self.get_db_session()
        return NuevosClientesNuevosPagadoresRepository(db=db_session)

    async def create_actualizacion_reportes_repository(
        self,
    ) -> ActualizacionReportesRepository:
        """Crear repository de ActualizacionReportes"""
        db_session = await self.get_db_session()
        return ActualizacionReportesRepository(db=db_session)

    async def create_cxc_acumulado_dim_repository(self) -> CXCAcumuladoDIMRepository:
        """Crear repository de CXCAcumuladoDIM"""
        db_session = await self.get_db_session()
        return CXCAcumuladoDIMRepository(db=db_session)

    async def create_cxc_pagos_fact_repository(self) -> CXCPagosFactRepository:
        """Crear repository de CXCPagosFact"""
        db_session = await self.get_db_session()
        return CXCPagosFactRepository(db=db_session)

    async def create_cxc_dev_fact_repository(self) -> CXCDevFactRepository:
        """Crear repository de CXCDevFact"""
        db_session = await self.get_db_session()
        return CXCDevFactRepository(db=db_session)

    async def cleanup(self):
        """Limpiar recursos del factory"""
        try:
            await self.session_manager.close()
        except Exception as e:
            from config.logger import logger

            logger.warning(f"Error cerrando session manager: {e}")


# Instancia global para reuso en tasks
repository_factory = RepositoryFactory()
