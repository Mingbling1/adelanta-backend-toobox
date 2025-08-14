import asyncio
import weakref
from contextlib import suppress
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
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


# 🛠️ Registry global para cleanup automático
_active_factories = weakref.WeakSet()


class RepositoryFactory:
    """
    Factory para crear repositories con sesiones aisladas
    Cada task de Celery obtiene sus propias instancias
    🚀 MEJORADO: Configuración específica para evitar event loop warnings
    """

    def __init__(self):
        # Registrar para cleanup automático
        _active_factories.add(self)

        # 🛡️ CONFIGURACIÓN OPTIMIZADA PARA CELERY + ASYNC
        # Parámetros ajustados para evitar conexiones huérfanas
        self.session_manager = DatabaseSessionManager(
            host=str(settings.DATABASE_MYSQL_URL),
            engine_kwargs={
                "echo": False,  # Sin logging detallado en tasks
                "future": True,
                "pool_size": 3,
                "max_overflow": 1,
                "pool_recycle": 15,
                "pool_reset_on_return": "commit",  # Reset estado al devolver conexión
                "connect_args": {
                    "connect_timeout": 8,  # REDUCIDO: Timeout más agresivo
                    "charset": "utf8mb4",
                },
            },
        )
        self._session = None
        self._closed = False
        logger.info("🏭 RepositoryFactory creado con configuración Celery-optimizada")

    async def get_db_session(self):
        """
        Obtener sesión de base de datos reutilizable
        🛡️ MEJORADO: Verificación de estado y manejo de errores robusto
        """
        if self._closed:
            raise RuntimeError(
                "RepositoryFactory ya está cerrado - no se pueden crear más sesiones"
            )

        if self._session is None:
            try:
                # Verificar que el session manager esté disponible
                if self.session_manager._engine is None:
                    raise RuntimeError("DatabaseSessionManager no está inicializado")

                self._session = self.session_manager._sessionmaker()
                logger.debug("📁 Nueva sesión de BD creada exitosamente")

            except Exception as e:
                logger.error(f"❌ Error crítico creando sesión de BD: {e}")
                raise RuntimeError(f"No se pudo crear sesión de BD: {e}") from e

        return self._session

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
        """
        🛡️ MEJORADO: Limpiar recursos del factory de forma ultra-segura
        Maneja específicamente el caso de event loop cerrado en Celery
        """
        if self._closed:
            logger.debug("RepositoryFactory ya está cerrado, saltando cleanup")
            return  # Ya limpiado

        logger.info("🧹 Iniciando cleanup robusto de RepositoryFactory...")

        try:
            # 1. 🔒 Marcar como cerrado inmediatamente para evitar uso concurrente
            self._closed = True

            # 2. 🗂️ Cerrar sesión activa de forma segura
            if self._session is not None:
                try:
                    # Verificar si hay un event loop activo
                    try:
                        loop = asyncio.get_running_loop()
                        if loop.is_closed():
                            logger.warning(
                                "⚠️ Event loop cerrado, usando cleanup directo de sesión"
                            )
                            # Si el loop está cerrado, intentar cierre directo sin await
                            with suppress(Exception):
                                if (
                                    hasattr(self._session, "_connection")
                                    and self._session._connection
                                ):
                                    self._session._connection.close()
                        else:
                            # Event loop activo, cierre normal
                            await self._session.close()
                            logger.debug("✅ Sesión de BD cerrada correctamente")
                    except RuntimeError as e:
                        if "no running event loop" in str(e).lower():
                            logger.debug(
                                "🔄 No hay event loop activo, saltando cierre async de sesión"
                            )
                        else:
                            raise  # Re-lanzar si es otro tipo de RuntimeError

                except Exception as e:
                    logger.warning(f"⚠️ Error cerrando sesión de BD: {e}")
                finally:
                    self._session = None

            # 3. 🏭 Cerrar session manager de forma segura
            if hasattr(self, "session_manager") and self.session_manager:
                try:
                    # Verificar si hay un event loop activo
                    try:
                        loop = asyncio.get_running_loop()
                        if loop.is_closed():
                            logger.warning(
                                "⚠️ Event loop cerrado, saltando cleanup de session manager"
                            )
                        else:
                            await self.session_manager.close()
                            logger.debug("✅ Session manager cerrado correctamente")
                    except RuntimeError as e:
                        if "no running event loop" in str(e).lower():
                            logger.debug(
                                "🔄 No hay event loop activo, saltando cierre async de session manager"
                            )
                        else:
                            raise  # Re-lanzar si es otro tipo de RuntimeError

                except Exception as e:
                    logger.warning(f"⚠️ Error cerrando session manager: {e}")

        except Exception as e:
            logger.error(f"❌ Error crítico durante cleanup: {e}")
        finally:
            logger.info("✅ RepositoryFactory cleanup completado")

    def __del__(self):
        """
        🛡️ Destructor seguro: Previene warnings en garbage collection
        NO realiza operaciones async aquí para evitar problemas con event loop cerrado
        """
        if not self._closed and hasattr(self, "_session") and self._session is not None:
            logger.warning(
                "⚠️ RepositoryFactory no se cerró explícitamente antes de destructor"
            )
            # Intentar cleanup síncrono básico sin await
            with suppress(Exception):
                if hasattr(self._session, "_connection") and self._session._connection:
                    # Cierre directo de la conexión sin async
                    self._session._connection.close()
                self._session = None
            self._closed = True


def create_repository_factory() -> RepositoryFactory:
    """
    Crear una nueva instancia de RepositoryFactory para cada task
    Esto evita problemas de estado compartido en Celery
    """
    return RepositoryFactory()


# 🛠️ Registry global para cleanup automático (sync)
_active_sync_factories = weakref.WeakSet()


class RepositoryFactorySync:
    """
    Factory SÍNCRONO para crear repositories con sesiones aisladas
    Réplica síncrona de RepositoryFactory para casos específicos
    """

    def __init__(self):
        # Registrar para cleanup automático
        _active_sync_factories.add(self)

        # 🛡️ CONFIGURACIÓN SÍNCRONA - URL sin aiomysql
        sync_url = str(settings.DATABASE_MYSQL_URL).replace(
            "mysql+aiomysql://", "mysql+pymysql://"
        )

        # Motor síncrono con configuración optimizada
        self._engine = create_engine(
            sync_url,
            echo=False,
            pool_size=3,
            max_overflow=1,
            pool_recycle=600,
            pool_pre_ping=True,
            pool_reset_on_return="commit",
            connect_args={
                "connect_timeout": 8,
                "charset": "utf8mb4",
            },
        )

        # Session maker síncrono
        self._sessionmaker = sessionmaker(
            autocommit=False, autoflush=False, bind=self._engine
        )

        self._session = None
        self._closed = False
        logger.info("🏭 RepositoryFactorySync creado con configuración síncrona")

    def get_db_session(self) -> Session:
        """
        Obtener sesión de base de datos síncrona reutilizable
        """
        if self._closed:
            raise RuntimeError(
                "RepositoryFactorySync ya está cerrado - no se pueden crear más sesiones"
            )

        if self._session is None:
            try:
                self._session = self._sessionmaker()
                logger.debug("📁 Nueva sesión síncrona de BD creada exitosamente")
            except Exception as e:
                logger.error(f"❌ Error crítico creando sesión síncrona de BD: {e}")
                raise RuntimeError(
                    f"No se pudo crear sesión síncrona de BD: {e}"
                ) from e

        return self._session

    def create_tipo_cambio_repository(self) -> TipoCambioRepository:
        """Crear repository de TipoCambio (síncrono)"""
        db_session = self.get_db_session()
        return TipoCambioRepository(db=db_session)

    def create_kpi_acumulado_repository(self) -> KPIAcumuladoRepository:
        """Crear repository de KPI Acumulado (síncrono)"""
        db_session = self.get_db_session()
        return KPIAcumuladoRepository(db=db_session)

    def create_kpi_repository(self) -> KPIRepository:
        """Crear repository de KPI (síncrono)"""
        db_session = self.get_db_session()
        return KPIRepository(db=db_session)

    def create_saldos_repository(self) -> SaldosRepository:
        """Crear repository de Saldos (síncrono)"""
        db_session = self.get_db_session()
        return SaldosRepository(db=db_session)

    def create_nuevos_clientes_nuevos_pagadores_repository(
        self,
    ) -> NuevosClientesNuevosPagadoresRepository:
        """Crear repository de NuevosClientesNuevosPagadores (síncrono)"""
        db_session = self.get_db_session()
        return NuevosClientesNuevosPagadoresRepository(db=db_session)

    def create_actualizacion_reportes_repository(
        self,
    ) -> ActualizacionReportesRepository:
        """Crear repository de ActualizacionReportes (síncrono)"""
        db_session = self.get_db_session()
        return ActualizacionReportesRepository(db=db_session)

    def create_cxc_acumulado_dim_repository(self) -> CXCAcumuladoDIMRepository:
        """Crear repository de CXCAcumuladoDIM (síncrono)"""
        db_session = self.get_db_session()
        return CXCAcumuladoDIMRepository(db=db_session)

    def create_cxc_pagos_fact_repository(self) -> CXCPagosFactRepository:
        """Crear repository de CXCPagosFactRepository (síncrono)"""
        db_session = self.get_db_session()
        return CXCPagosFactRepository(db=db_session)

    def create_cxc_dev_fact_repository(self) -> CXCDevFactRepository:
        """Crear repository de CXCDevFactRepository (síncrono)"""
        db_session = self.get_db_session()
        return CXCDevFactRepository(db=db_session)

    def cleanup(self):
        """
        Cleanup síncrono de recursos
        Cierra sesión y motor de BD de forma segura
        """
        if self._closed:
            logger.debug("🔄 RepositoryFactorySync ya estaba cerrado")
            return

        logger.info("🧹 Iniciando cleanup de RepositoryFactorySync...")

        try:
            # 1. 📁 Cerrar sesión síncrona
            if hasattr(self, "_session") and self._session is not None:
                try:
                    self._session.close()
                    logger.debug("✅ Sesión síncrona de BD cerrada correctamente")
                except Exception as e:
                    logger.warning(f"⚠️ Error cerrando sesión síncrona de BD: {e}")
                finally:
                    self._session = None

            # 2. 🏭 Cerrar motor síncrono
            if hasattr(self, "_engine") and self._engine:
                try:
                    self._engine.dispose()
                    logger.debug("✅ Motor síncrono de BD cerrado correctamente")
                except Exception as e:
                    logger.warning(f"⚠️ Error cerrando motor síncrono de BD: {e}")

        except Exception as e:
            logger.error(f"❌ Error crítico durante cleanup síncrono: {e}")
        finally:
            self._closed = True
            logger.info("✅ RepositoryFactorySync cleanup completado")

    def __del__(self):
        """
        Destructor seguro para cleanup síncrono
        """
        if not self._closed and hasattr(self, "_session") and self._session is not None:
            logger.warning(
                "⚠️ RepositoryFactorySync no se cerró explícitamente antes de destructor"
            )
            with suppress(Exception):
                if self._session:
                    self._session.close()
                self._session = None
            self._closed = True


def create_repository_factory_sync() -> RepositoryFactorySync:
    """
    Crear una nueva instancia de RepositoryFactorySync
    Para casos que requieren conexión síncrona a BD
    """
    return RepositoryFactorySync()
