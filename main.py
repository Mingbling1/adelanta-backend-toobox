from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from config.cronjob import cronjob_manager
import time
import pytz
from config.db_mysql import sessionmanager
from config.logger import logger
from routers.cronjob import CronjobRouter  # CRONJOB
from routers.datamart import (
    TipoCambioRouter,
    ReferidosRouter,
    KPIRouter,
    RetomasRouter,
    NuevosClientesNuevosPagadoresRouter,
)  # DATAMART
from routers.toolbox import DiferidoRouter  # TOOLBOX
from routers.toolbox import VentasAutodetraccionesRouter  # TOOLBOX
from routers.auth import (
    UsuarioRouter,
    AuthRouter,
    PermisoRouter,
    RolRouter,
    RolPermisoRouter,
)  # AUTH
from routers.administrativo import (
    GastoRouter,
    PagoRouter,
    ArchivoRouter,
    ProveedorRouter,
    CuentaBancariaRouter,
)  # ADMINISTRATIVO
from routers.sunat import SunatRouter
from routers.master import TablaMaestraDetalleRouter, TablaMaestraRouter  # MASTER
from routers.crm import SolicitudLeadRouter  # CRM
from cronjobs.BaseCronjob import BaseCronjob
from config.container import container
from fastapi.responses import ORJSONResponse


peru_tz = pytz.timezone("America/Lima")


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    logger.info(f"Iniciando el servidor {app.title}")

    container.init_resources()


    # cronjob_manager.wakeup()
    BaseCronjob.register_all_cronjobs()
    await cronjob_manager.start()
    yield
    await cronjob_manager.shutdown()
    logger.info("Servidor detenido")

    if sessionmanager._engine is not None:
        logger.info("Disconnecting from database!")
        await sessionmanager.close()


app = FastAPI(
    lifespan=app_lifespan, default_response_class=ORJSONResponse, root_path="/api"
)
# CRONJOB
app.include_router(
    CronjobRouter.router,
    prefix="/cronjob",
    tags=["Cronjob"],
)
# DATAMART
app.include_router(
    TipoCambioRouter.router,
    prefix="/datamart/tipoCambio",
    tags=["Datamart", "TipoCambio"],
)
app.include_router(
    ReferidosRouter.router,
    prefix="/datamart/referidos",
    tags=["Datamart", "Referidos"],
)

app.include_router(
    KPIRouter.router,
    prefix="/datamart/kpi",
    tags=["Datamart", "KPI"],
)
app.include_router(
    RetomasRouter.router,
    prefix="/datamart/retomas",
    tags=["Datamart", "Retomas"],
)
app.include_router(
    NuevosClientesNuevosPagadoresRouter.router,
    prefix="/datamart/nuevosClientesNuevosPagadores",
    tags=["Datamart", "NuevosClientesNuevosPagadores"],
)
# TOOLBOX
app.include_router(
    DiferidoRouter.router, prefix="/toolbox" + "/diferido", tags=["Toolbox"]
)
app.include_router(
    VentasAutodetraccionesRouter.router,
    prefix="/toolbox" + "/ventasAutodetracciones",
    tags=["Toolbox"],
)
# AUTH
app.include_router(AuthRouter.router, prefix="/auth", tags=["Auth"])
app.include_router(UsuarioRouter.router, prefix="/auth" + "/usuario", tags=["Usuario"])
app.include_router(PermisoRouter.router, prefix="/auth" + "/permiso", tags=["Permiso"])
app.include_router(RolRouter.router, prefix="/auth" + "/rol", tags=["Rol"])
app.include_router(
    RolPermisoRouter.router, prefix="/auth" + "/rolPermiso", tags=["RolPermiso"]
)

# ADMINISTRATIVO
app.include_router(
    GastoRouter.router,
    prefix="/administrativo/gasto",
    tags=["Administrativo", "Gasto"],
)
app.include_router(
    PagoRouter.router,
    prefix="/administrativo/pago",
    tags=["Administrativo", "Pago"],
)
app.include_router(
    ArchivoRouter.router,
    prefix="/administrativo/archivo",
    tags=["Administrativo", "Archivo"],
)
app.include_router(
    ProveedorRouter.router,
    prefix="/administrativo/proveedor",
    tags=["Administrativo", "Proveedor"],
)
app.include_router(
    CuentaBancariaRouter.router,
    prefix="/administrativo/cuentaBancaria",
    tags=["Administrativo", "CuentaBancaria"],
)

# SUNAT
app.include_router(SunatRouter.router, prefix="/sunat", tags=["Sunat"])

# MASTER
app.include_router(
    TablaMaestraRouter.router, prefix="/master" + "/tablaMaestra", tags=["TablaMaestra"]
)
app.include_router(
    TablaMaestraDetalleRouter.router,
    prefix="/master" + "/tablaMaestraDetalle",
    tags=["TablaMaestraDetalle"],
)

# CRM
app.include_router(
    SolicitudLeadRouter.router, prefix="/crm" + "/solicitudLead", tags=["CRM"]
)


@app.get("/")
def read_root():
    return "Server is running"


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = time.perf_counter() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


origins = [
    "http://localhost:3000",
    "https://localhost:3000",
    "https://toolbox.adelantafactoring.com",
    "http://toolbox.adelantafactoring.com",
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)
