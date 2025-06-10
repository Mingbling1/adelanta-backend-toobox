from fastapi import HTTPException
from datetime import datetime, timedelta
from config.db_mongo import connection
from models.crm.tipo_de_cambio import ExchangeRate
import httpx
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential


class ExchangeRateUpdater:
    def __init__(self, url: str):
        self.connection = connection
        self.collection = connection.webservice.tipo_cambio
        self.url = url

    async def get_all_days(self, start_date: datetime, end_date: datetime):

        start_date = start_date.replace(tzinfo=None)
        end_date = end_date.replace(tzinfo=None)
        delta = end_date - start_date
        return (start_date + timedelta(days=i) for i in range(delta.days + 1))

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def get_exchange_rate(self, date: str):
        url = f"{self.url}?fecha={date}"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
            if response.status_code != 200:
                raise HTTPException(
                    status_code=500,
                    detail=f"API request failed with status {response.status_code}",
                )
            if not response.text:
                raise HTTPException(status_code=500, detail="API response is empty")
            json = response.json()
            print(f"Obteniendo tipo de cambio {date}: {json}")
            data = {
                "tcFecha": datetime.strptime(json["fecha"], "%Y-%m-%d"),
                "tcCompra": float(json["compra"]),
                "tcVenta": float(json["venta"]),
            }

            new_exchange_rate = ExchangeRate(**data)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        return new_exchange_rate

    async def obtener_tipo_cambio(
        self, start_date: datetime, end_date: datetime, batch_size: int = 1
    ):
        cursor = self.collection.find()
        all_exchange_rate = await cursor.to_list(length=None)

        existing_dates = set(
            exchange_rate["tcFecha"].replace(tzinfo=None)
            for exchange_rate in all_exchange_rate
        )

        all_dates = set(
            date.replace(tzinfo=None)
            for date in await self.get_all_days(start_date, end_date)
        )

        missing_dates = all_dates - existing_dates

        missing_dates = list(missing_dates)
        final_results = []  # Lista para almacenar los resultados

        for i in range(0, len(missing_dates), batch_size):
            batch = missing_dates[i : i + batch_size]
            tasks = [
                self.get_exchange_rate(date.strftime("%Y-%m-%d")) for date in batch
            ]
            new_exchange_rates = await asyncio.gather(*tasks, return_exceptions=True)

            for new_exchange_rate in new_exchange_rates:
                if isinstance(new_exchange_rate, Exception):
                    print(f"Falló en el primer intento: {new_exchange_rate}")
                else:
                    if isinstance(new_exchange_rate, ExchangeRate):
                        final_results.append(new_exchange_rate.model_dump())
                        print("Todo bien!")
                    else:
                        print(f"Último intento falló: {new_exchange_rate}")

            # Espera de 10 segundos entre cada lote de consultas
            await asyncio.sleep(10)

        # Insertar todos los resultados de una vez usando insert_many
        if final_results:
            await self.collection.insert_many(final_results)


actualizar_tipo_cambio = ExchangeRateUpdater(
    url="https://api.apis.net.pe/v1/tipo-cambio-sunat"
)
