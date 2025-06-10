from fastapi import HTTPException
from services.datamart.TipoCambioService import TipoCambioService
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
import httpx
from schemas.datamart.TipoCambioSchema import TipoCambioPostRequestSchema
import asyncio
from cronjobs.BaseCronjob import BaseCronjob
from config.logger import logger
from dependency_injector.wiring import inject, Provide
from config.container import Container


class ActualizarTipoCambioCronjob(BaseCronjob):
    URL = "https://api.apis.net.pe/v1/tipo-cambio-sunat"

    def __init__(self):
        super().__init__(
            description="Actualiza el tipo de cambio desde la API de SUNAT"
        )

    async def get_all_days(self, start_date: datetime, end_date: datetime):
        start_date = start_date.replace(tzinfo=None)
        end_date = end_date.replace(tzinfo=None)
        delta = end_date - start_date
        return (start_date + timedelta(days=i) for i in range(delta.days + 1))

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def get_exchange_rate(self, date_str: str):
        url = f"{self.URL}?fecha={date_str}"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)

            if response.status_code != 200:
                logger.error(
                    f"API request failed with status {response.status_code} for date {date_str}"
                )
                raise HTTPException(
                    status_code=500,
                    detail=f"API request failed with status {response.status_code}",
                )
            if not response.text:
                logger.error(f"API response is empty for date {date_str}")
                raise HTTPException(status_code=500, detail="API response is empty")
            json = response.json()
            logger.debug(f"Obteniendo tipo de cambio {date_str}: {json}")
            data = {
                "TipoCambioFecha": json["fecha"],
                "TipoCambioCompra": float(json["compra"]),
                "TipoCambioVenta": float(json["venta"]),
            }
            new_exchange_rate = TipoCambioPostRequestSchema(**data)
        except Exception as e:
            logger.error(f"Exception occurred for date {date_str}: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
        return new_exchange_rate

    @inject
    async def run(
        self,
        tipo_cambio_service: TipoCambioService = Provide[Container.tipo_cambio_service],
        batch_size: int = 1,
    ):
        start_date = BaseCronjob.obtener_datetime_fecha_inicio()
        end_date = BaseCronjob.obtener_datetime_fecha_fin()

        logger.debug(f"{start_date} - {end_date}")

        all_exchange_rate = [
            i.to_dict() for i in await tipo_cambio_service.get_all(limit=None)
        ]

        existing_dates = set(
            exchange_rate["TipoCambioFecha"] for exchange_rate in all_exchange_rate
        )
        all_dates = set(
            date.strftime("%Y-%m-%d")
            for date in await self.get_all_days(start_date, end_date)
        )

        missing_dates = list(all_dates - existing_dates)
        logger.debug(f"Missing dates: {missing_dates}")

        final_results = []

        for i in range(0, len(missing_dates), batch_size):
            batch = missing_dates[i : i + batch_size]
            tasks = [self.get_exchange_rate(date) for date in batch]
            try:
                new_exchange_rates = await asyncio.gather(
                    *tasks, return_exceptions=True
                )
            except RetryError as e:
                logger.error(f"RetryError occurred: {str(e)}")
                continue

            for new_exchange_rate in new_exchange_rates:
                if isinstance(new_exchange_rate, Exception):
                    logger.debug(f"Falló en el primer intento: {new_exchange_rate}")
                else:
                    if isinstance(new_exchange_rate, TipoCambioPostRequestSchema):
                        final_results.append(new_exchange_rate)
                        logger.debug("Todo bien!")
                    else:
                        logger.debug(f"Último intento falló: {new_exchange_rate}")

            await asyncio.sleep(5)

        if final_results:
            try:
                await tipo_cambio_service.create_many(final_results)
            except Exception as e:
                logger.error(f"Error al crear tipos de cambio: {str(e)}")
