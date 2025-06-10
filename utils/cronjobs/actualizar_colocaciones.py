import httpx
import pandas as pd
from datetime import datetime, timedelta
import time
from tenacity import retry, stop_after_attempt, wait_exponential
from config.db_mongo import connection
from schemas.datamart.KPISchema import ColocacionesSchema
from models.webservice.nuevos_clientes_pagadores import ClientesPagadores
from models.webservice.saldos import Saldos
from models.webservice.retomas import RetomasOutput
from utils.calculos import (
    KPI,
    sector_pagadores,
    NuevosClientesPagadores,
    saldos,
    Retomas,
)

from services.datamart.colocaciones_service import ColocacionesService
from fastapi import Depends


class ColocacionesDataFetcher:
    def __init__(
        self,
        token_url,
        credentials,
        timeout: float = 240.0,
        colocaciones_service: ColocacionesService = Depends(),
    ):
        self.token_url = token_url
        self.credentials = credentials
        self.timeout = timeout
        self.token = None
        self.collection_colocaciones = connection.webservice.colocaciones
        self.collection_nuevos_clientes_pagadores = (
            connection.webservice.nuevos_clientes_pagadores
        )
        self.collection_saldos = connection.webservice.saldos
        self.collection_retomas = connection.webservice.retomas
        self.colocaciones_service = colocaciones_service

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def fetch_data(
        self, client, start_date: datetime, end_date: datetime, fecha_corte: str
    ):
        start_time = time.time()
        print(
            f"Fetching data from {start_date.strftime('%Y%m%d')} to {end_date.strftime('%Y%m%d')}"
        )
        reporte_url = "https://webservice.adelantafactoring.com/webservice/colocaciones"
        params = {
            "desde": start_date.strftime("%Y%m%d"),
            "hasta": end_date.strftime("%Y%m%d"),
            "fechaCorte": fecha_corte,
            "reporte": 2,
        }
        headers = {"Authorization": f"Bearer {self.token}"}

        response = await client.get(reporte_url, headers=headers, params=params)
        response.raise_for_status()
        end_time = time.time()
        print(
            f"Completed fetching data from {start_date.strftime('%Y%m%d')} to {end_date.strftime('%Y%m%d')} in {end_time - start_time:.2f} seconds"
        )
        return response.json()

    async def get_token(self):
        start_time = time.time()
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            print("Requesting access token...")
            response = await client.post(self.token_url, data=self.credentials)
            response.raise_for_status()
            self.token = response.json().get("access_token")
            print("Access token obtained")
        end_time = time.time()
        print(f"Token request took {end_time - start_time:.2f} seconds")

    async def obtener_colocaciones(self, start_date, end_date, fecha_corte):
        start_time = time.time()
        await self.get_token()

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                results = await self.fetch_data(
                    client, start_date, end_date, fecha_corte
                )
        except Exception as e:
            print(f"Error fetching data: {e}")
            return
        except Exception as e:
            print(f"Error fetching data: {e}")
            return

        final_results = results if isinstance(results, list) else []

        # KPI
        await self.collection_colocaciones.delete_many({})

        colocaciones_df = pd.DataFrame(final_results)

        print("Data successfully fetched from API.")

        kpi_start_time = time.time()
        pagadores_df = sector_pagadores.obtener_data()
        kpi = KPI(colocaciones_df)
        kpi_df = await kpi.calcular_kpi()
        # print(kpi_df.info())

        # nan_rows = kpi_df[kpi_df.isna().any(axis=1)]
        # print(nan_rows)
        kpi_df = kpi_df.drop(columns=["_id"])
        kpi_df = kpi_df.merge(pagadores_df, on="RUCPagador", how="left")
        kpi_df["GrupoEco"] = kpi_df["GrupoEco"].fillna(kpi_df["RazonSocialPagador"])

        kpi_end_time = time.time()
        print(f"KPI calculation took {kpi_end_time - kpi_start_time:.2f} seconds")

        # Conversión a dict
        dict_conversion_start_time = time.time()
        kpi_data: list[dict] = kpi_df.to_dict("records")
        dict_conversion_end_time = time.time()
        print(
            f"Conversion to dict took {dict_conversion_end_time - dict_conversion_start_time:.2f} seconds"
        )

        # Conversión a model_dump()
        model_dump_start_time = time.time()
        colocaciones_data = [
            ColocacionesSchema(**item).model_dump() for item in kpi_data
        ]

        model_dump_end_time = time.time()
        print(
            f"Model dump conversion took {model_dump_end_time - model_dump_start_time:.2f} seconds"
        )
        # Aquí
        insert_start_time = time.time()
        await self.collection_colocaciones.delete_many(
            {}
        )  # borrar todo en esta tabla que se llama colocaciones

        await self.colocaciones_service.delete_all()
        if colocaciones_data:
            await self.colocaciones_service.create_many(kpi_data)
            print("Data successfully inserted into MySQL.")

        if colocaciones_data:
            await self.collection_colocaciones.insert_many(
                colocaciones_data
            )  # insertar en mySQL
            print("Data successfully inserted into MongoDB.")
        insert_end_time = time.time()
        print(f"Data insertion took {insert_end_time - insert_start_time:.2f} seconds")

        # Retomas
        await self.collection_retomas.delete_many({})
        retomas_start_time = time.time()
        retomas = Retomas(df=kpi_df)
        retomas_df = retomas.obtener_resumen(cut_date=end_date - timedelta(days=15))
        retomas_df = retomas_df.reset_index()
        retomas_df.columns = [
            "_".join(col).strip("_") if isinstance(col, tuple) else col.strip("_")
            for col in retomas_df.columns
        ]
        retomas_data: list[dict] = retomas_df.to_dict("records")
        retomas_data = [RetomasOutput(**item).model_dump() for item in retomas_data]

        if retomas_data:
            await self.collection_retomas.insert_many(retomas_data)

        retomas_end_time = time.time()
        print(
            f"Retomas calculation took {retomas_end_time - retomas_start_time:.2f} seconds"
        )

        # Nuevos clientes pagadores
        await self.collection_nuevos_clientes_pagadores.delete_many({})
        nuevos_clientes_start_time = time.time()
        nuevos_clientes_pagadores = NuevosClientesPagadores(kpi_df)
        nuevos_clientes_pagadores_df = (
            nuevos_clientes_pagadores.calcular_nuevos_clientes_pagadores(
                start_date=kpi_df["FechaOperacion"].min().strftime("%Y-%m-%d"),
                end_date=datetime.now().strftime("%Y-%m-%d"),
                ruc_c_col="RUCCliente",
                ruc_p_col="RUCPagador",
                ruc_c_ns_col="RazonSocialCliente",
                ruc_p_ns_col="RazonSocialPagador",
                ejecutivo_col="Ejecutivo",
                type_op_col="TipoOperacion",
            )
        )

        nuevos_clientes_end_time = time.time()
        print(
            f"New clients and pagadores calculation took {nuevos_clientes_end_time - nuevos_clientes_start_time:.2f} seconds"
        )
        nuevos_clientes_pagadores_data: list[dict] = (
            nuevos_clientes_pagadores_df.to_dict("records")
        )
        nuevos_clientes_pagadores_data = [
            ClientesPagadores(**item).model_dump()
            for item in nuevos_clientes_pagadores_data
        ]
        if nuevos_clientes_pagadores_data:
            await self.collection_nuevos_clientes_pagadores.insert_many(
                nuevos_clientes_pagadores_data
            )
        nuevos_clientes_end_time = time.time()
        print(
            f"New clients and pagadores calculation took {nuevos_clientes_end_time - nuevos_clientes_start_time:.2f} seconds"
        )
        nuevos_clientes_pagadores_data: list[dict] = (
            nuevos_clientes_pagadores_df.to_dict("records")
        )
        nuevos_clientes_pagadores_data = [
            ClientesPagadores(**item).model_dump()
            for item in nuevos_clientes_pagadores_data
        ]
        if nuevos_clientes_pagadores_data:
            await self.collection_nuevos_clientes_pagadores.insert_many(
                nuevos_clientes_pagadores_data
            )

        # Saldos
        await self.collection_saldos.delete_many({})

        saldos_data_df = saldos.obtener_data()
        print(saldos_data_df)
        saldos_data: list[dict] = saldos_data_df.to_dict("records")

        saldos_data = [Saldos(**item).model_dump() for item in saldos_data]

        if saldos_data:
            await self.collection_saldos.insert_many(saldos_data)

        print("Saldos updated successfully")

        end_time = time.time()
        print(f"Total processing time: {end_time - start_time:.2f} seconds")
