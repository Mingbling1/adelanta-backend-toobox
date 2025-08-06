"""
🏭 Factory de Repositories para Celery Tasks
Proporciona context managers para trabajar con repositories
"""

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
from contextlib import asynccontextmanager
from typing import AsyncIterator

# 🔄 REUTILIZAR el session manager existente en lugar de crear uno nuevo
from config.db_mysql import sessionmanager


class RepositoryFactory:
    """
    Factory para crear repositories con sesiones aisladas
    Cada task de Celery obtiene sus propias instancias
    """

    def __init__(self):
        self.session_manager = sessionmanager

    @asynccontextmanager
    async def create_tipo_cambio_repository(
        self,
    ) -> AsyncIterator[TipoCambioRepository]:
        """Crear repository de TipoCambio con context manager"""
        async with self.session_manager.session() as db_session:
            yield TipoCambioRepository(db=db_session)

    @asynccontextmanager
    async def create_kpi_acumulado_repository(
        self,
    ) -> AsyncIterator[KPIAcumuladoRepository]:
        """Crear repository de KPI Acumulado con context manager"""
        async with self.session_manager.session() as db_session:
            yield KPIAcumuladoRepository(db=db_session)

    @asynccontextmanager
    async def create_kpi_repository(self) -> AsyncIterator[KPIRepository]:
        """Crear repository de KPI con context manager"""
        async with self.session_manager.session() as db_session:
            yield KPIRepository(db=db_session)

    @asynccontextmanager
    async def create_saldos_repository(self) -> AsyncIterator[SaldosRepository]:
        """Crear repository de Saldos con context manager"""
        async with self.session_manager.session() as db_session:
            yield SaldosRepository(db=db_session)

    @asynccontextmanager
    async def create_nuevos_clientes_nuevos_pagadores_repository(
        self,
    ) -> AsyncIterator[NuevosClientesNuevosPagadoresRepository]:
        """Crear repository de NuevosClientesNuevosPagadores con context manager"""
        async with self.session_manager.session() as db_session:
            yield NuevosClientesNuevosPagadoresRepository(db=db_session)

    @asynccontextmanager
    async def create_actualizacion_reportes_repository(
        self,
    ) -> AsyncIterator[ActualizacionReportesRepository]:
        """Crear repository de ActualizacionReportes con context manager"""
        async with self.session_manager.session() as db_session:
            yield ActualizacionReportesRepository(db=db_session)

    @asynccontextmanager
    async def create_cxc_acumulado_dim_repository(
        self,
    ) -> AsyncIterator[CXCAcumuladoDIMRepository]:
        """Crear repository de CXCAcumuladoDIM con context manager"""
        async with self.session_manager.session() as db_session:
            yield CXCAcumuladoDIMRepository(db=db_session)

    @asynccontextmanager
    async def create_cxc_pagos_fact_repository(
        self,
    ) -> AsyncIterator[CXCPagosFactRepository]:
        """Crear repository de CXCPagosFact con context manager"""
        async with self.session_manager.session() as db_session:
            yield CXCPagosFactRepository(db=db_session)

    @asynccontextmanager
    async def create_cxc_dev_fact_repository(
        self,
    ) -> AsyncIterator[CXCDevFactRepository]:
        """Crear repository de CXCDevFact con context manager"""
        async with self.session_manager.session() as db_session:
            yield CXCDevFactRepository(db=db_session)

    async def cleanup(self):
        """Limpiar recursos del factory - No cerrar el session manager global"""
        pass  # El session manager global se mantiene vivo


# Instancia global para reuso en tasks
repository_factory = RepositoryFactory()
