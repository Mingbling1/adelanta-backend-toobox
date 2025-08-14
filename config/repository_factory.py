from config.db_mysql import DatabaseSessionManager
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
from config.logger import logger


class RepositoryFactory:
    """
    Factory para crear repositories con sesiones aisladas
    Cada task de Celery obtiene sus propias instancias
    """

    def __init__(self):
        # Crear session manager con configuración optimizada para Celery con más recursos
        self.session_manager = DatabaseSessionManager(
            host=str(settings.DATABASE_MYSQL_URL),
            engine_kwargs={
                "echo": False,  # Sin logging detallado en tasks
                "future": True,
                "pool_size": 5,  # Pool más grande para múltiples workers
                "max_overflow": 3,  # Overflow aumentado
                "pool_recycle": 1800,  # Reciclar conexiones cada 30 min
                "pool_pre_ping": True,  # Verificar conexiones antes de usar
                "pool_timeout": 20,  # Timeout aumentado para evitar race conditions
                "connect_args": {
                    "connect_timeout": 15,  # Timeout aumentado
                    "charset": "utf8mb4",
                    "autocommit": False,  # Control explícito de transacciones
                },
            },
        )

    async def get_db_session(self):
        """
        🔄 Generador de sesiones con auto-cleanup (MEJOR PRÁCTICA)
        Cada sesión se cierra automáticamente después del uso
        """
        async with self.session_manager.session() as session:
            yield session


    async def create_tipo_cambio_repository(self) -> TipoCambioRepository:
        """Crear repository de TipoCambio"""
        async for db_session in self.get_db_session():
            return TipoCambioRepository(db=db_session)

    async def create_kpi_acumulado_repository(self) -> KPIAcumuladoRepository:
        """Crear repository de KPI Acumulado"""
        async for db_session in self.get_db_session():
            return KPIAcumuladoRepository(db=db_session)

    async def create_kpi_repository(self) -> KPIRepository:
        """Crear repository de KPI"""
        async for db_session in self.get_db_session():
            return KPIRepository(db=db_session)

    async def create_saldos_repository(self) -> SaldosRepository:
        """Crear repository de Saldos"""
        async for db_session in self.get_db_session():
            return SaldosRepository(db=db_session)

    async def create_nuevos_clientes_nuevos_pagadores_repository(
        self,
    ) -> NuevosClientesNuevosPagadoresRepository:
        """Crear repository de NuevosClientesNuevosPagadores"""
        async for db_session in self.get_db_session():
            return NuevosClientesNuevosPagadoresRepository(db=db_session)

    async def create_actualizacion_reportes_repository(
        self,
    ) -> ActualizacionReportesRepository:
        """Crear repository de ActualizacionReportes"""
        async for db_session in self.get_db_session():
            return ActualizacionReportesRepository(db=db_session)

    async def create_cxc_acumulado_dim_repository(self) -> CXCAcumuladoDIMRepository:
        """Crear repository de CXCAcumuladoDIM"""
        async for db_session in self.get_db_session():
            return CXCAcumuladoDIMRepository(db=db_session)

    async def create_cxc_pagos_fact_repository(self) -> CXCPagosFactRepository:
        """Crear repository de CXCPagosFact"""
        async for db_session in self.get_db_session():
            return CXCPagosFactRepository(db=db_session)

    async def create_cxc_dev_fact_repository(self) -> CXCDevFactRepository:
        """Crear repository de CXCDevFact"""
        async for db_session in self.get_db_session():
            return CXCDevFactRepository(db=db_session)

    async def cleanup(self):
        """Limpiar recursos del factory de forma segura"""
        try:
            # Con el patrón yield, las sesiones se auto-limpian
            # Solo necesitamos cerrar el session manager
            await self.session_manager.close()
            logger.info("✅ RepositoryFactory: Recursos limpiados exitosamente")

        except Exception as e:
            logger.warning(f"Error cerrando repository factory: {e}")
        finally:
            # Asegurar que se cierre
            try:
                await self.session_manager.close()
            except Exception:
                pass


def create_repository_factory() -> RepositoryFactory:
    """
    Crear una nueva instancia de RepositoryFactory para cada task
    Esto evita problemas de estado compartido en Celery
    """
    return RepositoryFactory()
