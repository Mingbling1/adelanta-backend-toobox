from dependency_injector import containers, providers
from config.settings import settings
from config.db_mysql import DatabaseSessionManager, get_db
from repositories.datamart.TipoCambioRepository import TipoCambioRepository
from services.datamart.TipoCambioService import TipoCambioService
from repositories.datamart.KPIRepository import KPIRepository
from services.datamart.KPIService import KPIService
from repositories.datamart.KPIAcumuladoRepository import KPIAcumuladoRepository
from services.datamart.KPIAcumuladoService import KPIAcumuladoService
from repositories.datamart.RetomasRepository import RetomasRepository
from services.datamart.RetomasService import RetomasService
from repositories.datamart.NuevosClientesNuevosPagadoresRepository import (
    NuevosClientesNuevosPagadoresRepository,
)
from services.toolbox.NuevosClientesNuevosPagadoresService import (
    NuevosClientesNuevosPagadoresService,
)
from repositories.datamart.SaldosRepository import SaldosRepository
from services.datamart.SaldosService import SaldosService

from repositories.datamart.ActualizacionReportesRepository import (
    ActualizacionReportesRepository,
)


from repositories.datamart import (
    CXCAcumuladoDIMRepository,
    CXCDevFactRepository,
    CXCPagosFactRepository,
)


class Container(containers.DeclarativeContainer):
    wiring_config = containers.WiringConfiguration(
        modules=[
            "cronjobs.datamart.ActualizarTipoCambioCronjob",
            "cronjobs.datamart.ActualizarTablaRetomaCronjob",
            "cronjobs.datamart.ActualizarTablasCXCCronjob",
            "cronjobs.datamart.ActualizarCXCETLCronjob",
            # Background processors NO necesitan wiring, solo tasks con repository_factory
        ]
    )

    config = providers.Configuration()

    db_session_manager = providers.ThreadSafeSingleton(
        DatabaseSessionManager,
        host=str(settings.DATABASE_MYSQL_URL),
        engine_kwargs={
            "echo": True,
            "future": True,
            "pool_size": 40,
            "max_overflow": 20,
            "pool_recycle": 3600,
        },
    )

    db_session = providers.Resource(get_db)

    tipo_cambio_repository = providers.Singleton(TipoCambioRepository, db=db_session)

    tipo_cambio_service = providers.Singleton(
        TipoCambioService, tipo_cambio_repository=tipo_cambio_repository
    )

    kpi_repository = providers.Singleton(KPIRepository, db=db_session)

    kpi_service = providers.Singleton(KPIService, kpi_repository=kpi_repository)

    kpi_acumulado_repository = providers.Singleton(
        KPIAcumuladoRepository, db=db_session
    )

    kpi_acumulado_service = providers.Singleton(
        KPIAcumuladoService, kpi_acumulado_repository=kpi_acumulado_repository
    )

    retomas_repository = providers.Singleton(RetomasRepository, db=db_session)
    retomas_service = providers.Singleton(
        RetomasService, retomas_repository=retomas_repository
    )

    nuevos_clientes_nuevos_pagadores_repository = providers.Singleton(
        NuevosClientesNuevosPagadoresRepository, db=db_session
    )
    nuevos_clientes_nuevos_pagadores_service = providers.Singleton(
        NuevosClientesNuevosPagadoresService,
        nuevos_clientes_nuevos_pagadores_repository=nuevos_clientes_nuevos_pagadores_repository,
    )

    saldos_repository = providers.Singleton(SaldosRepository, db=db_session)
    saldos_service = providers.Singleton(
        SaldosService, saldos_repository=saldos_repository
    )

    actualizacion_reportes_repository = providers.Singleton(
        ActualizacionReportesRepository, db=db_session
    )

    cxc_acumulado_dim_repository = providers.Singleton(
        CXCAcumuladoDIMRepository, db=db_session
    )

    cxc_pagos_fact_repository = providers.Singleton(
        CXCPagosFactRepository, db=db_session
    )
    cxc_dev_fact_repository = providers.Singleton(CXCDevFactRepository, db=db_session)


container = Container()
