from .actualizar_colocaciones import ColocacionesDataFetcher
from .actualizar_tipo_cambio import actualizar_tipo_cambio
from datetime import datetime
import pytz
from sqlalchemy.ext.asyncio import AsyncSession

__all__ = [
    "actualizar_colocaciones",
    "actualizar_tipo_cambio",
]


peru_tz = pytz.timezone("America/Lima")


def obtener_fecha_inicio() -> datetime:
    return datetime(2019, 7, 1, tzinfo=peru_tz)


def obtener_fecha_fin() -> datetime:
    # Retornar la fecha actual
    return datetime.now(peru_tz)


def obtener_fecha_corte() -> str:
    # Retornar la fecha de corte en formato "YYYYMMDD"
    return datetime.now(peru_tz).strftime("%Y%m%d")


async def cronjob_colocaciones(db: AsyncSession):
    print("Job obtener colocaciones started")
    try:
        actualizar_colocaciones = ColocacionesDataFetcher(
            db,
            token_url="https://webservice.adelantafactoring.com/webservice/token",
            credentials={
                "username": "adelantafactoring",
                "password": "jSB@$M5tR9pAXsUy",
            },
            timeout=120.0,
        )

        await actualizar_colocaciones.obtener_colocaciones(
            obtener_fecha_inicio(), obtener_fecha_fin(), obtener_fecha_corte()
        )
        print("Job obtener colocaciones completed successfully")
    except Exception as e:
        print(f"Job obtener colocaciones failed: {e}")


async def cronjob_tipo_cambio():
    print("Job obtener tipo de cambio started")
    try:
        await actualizar_tipo_cambio.obtener_tipo_cambio(
            obtener_fecha_inicio(), obtener_fecha_fin()
        )
        print("Job obtener tipo de cambio completed successfully")
    except Exception as e:
        print(f"Job obtener tipo de cambio failed: {e}")
