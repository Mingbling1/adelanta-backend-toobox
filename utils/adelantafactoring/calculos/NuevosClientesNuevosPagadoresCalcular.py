import pandas as pd
import numpy as np
from config.logger import logger
from ..schemas.NuevosClientesNuevosPagadoresCalcularSchema import (
    NuevosClientesNuevosPagadoresCalcularSchema,
)


class NuevosClientesNuevosPagadoresCalcular:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def procesar_datos(
        self,
        start_date: str,
        end_date: str,
        ruc_c_col: str,
        ruc_p_col: str,
        ruc_c_ns_col: str,
        ruc_p_ns_col: str,
        ejecutivo_col: str,
        type_op_col: str,
    ) -> pd.DataFrame:
        dates = pd.date_range(start_date, end_date, freq="MS")
        months_years = dates.strftime("%Y-%m")
        unique_keys = []
        ruc_cedente = self.df[[ruc_c_col, ruc_c_ns_col]].drop_duplicates()
        ruc_pagador = self.df[[ruc_p_col, ruc_p_ns_col]].drop_duplicates()
        ruc_cedente_nombre_social = dict(
            zip(ruc_cedente[ruc_c_col], ruc_cedente[ruc_c_ns_col])
        )
        ruc_pagador_nombre_social = dict(
            zip(ruc_pagador[ruc_p_col], ruc_pagador[ruc_p_ns_col])
        )

        rucs_nuevos_set = set()
        rucs_pagador_nuevos_set = set()
        for month in months_years:
            df_month = self.df[self.df["Mes"] == month]
            for ejecutivo in df_month[ejecutivo_col].unique():
                for type in ["Factoring", "Confirming", "Capital de Trabajo"]:
                    if type == "Factoring":
                        news_ids_f_ruc = set(
                            df_month[
                                (df_month[ejecutivo_col] == ejecutivo)
                                & (df_month[type_op_col] == type)
                            ][ruc_c_col]
                        )
                        news_ids_f_ruc_pagador = set(
                            df_month[
                                (df_month[ejecutivo_col] == ejecutivo)
                                & (df_month[type_op_col] == type)
                            ][ruc_p_col]
                        )
                        # Factoring case - RUC cliente nuevo
                        for ruc in news_ids_f_ruc:
                            if ruc not in rucs_nuevos_set:
                                nombre_social = ruc_cedente_nombre_social[ruc]
                                unique_keys.append(
                                    {
                                        "Mes": month,
                                        "Ejecutivo": ejecutivo,
                                        "RUCCliente": str(ruc),
                                        "RUCPagador": np.nan,
                                        "TipoOperacion": type,
                                        "RazonSocial": nombre_social,
                                    }
                                )
                                rucs_nuevos_set.add(ruc)

                        # Factoring case - RUCPagador nuevo
                        for ruc_pagador in news_ids_f_ruc_pagador:
                            if ruc_pagador not in rucs_pagador_nuevos_set:
                                nombre_social = ruc_pagador_nombre_social[ruc_pagador]
                                unique_keys.append(
                                    {
                                        "Mes": month,
                                        "Ejecutivo": ejecutivo,
                                        "RUCCliente": np.nan,
                                        "RUCPagador": ruc_pagador,
                                        "TipoOperacion": type,
                                        "RazonSocial": nombre_social,
                                    }
                                )
                                rucs_pagador_nuevos_set.add(ruc_pagador)

                    else:
                        # Código para Confirming o Capital de Trabajo
                        news_ids_c = (
                            df_month[
                                (df_month[ejecutivo_col] == ejecutivo)
                                & (df_month[type_op_col] == type)
                            ][ruc_p_col]
                            .unique()
                            .tolist()
                        )

                        for ruc_pagador in news_ids_c:
                            if ruc_pagador not in rucs_pagador_nuevos_set:
                                nombre_social = ruc_pagador_nombre_social[ruc_pagador]
                                unique_keys.append(
                                    {
                                        "Mes": month,
                                        "Ejecutivo": ejecutivo,
                                        "RUCCliente": np.nan,
                                        "RUCPagador": str(ruc_pagador),
                                        "TipoOperacion": type,
                                        "RazonSocial": nombre_social,
                                    }
                                )
                                rucs_pagador_nuevos_set.add(ruc_pagador)
        df_result = pd.DataFrame(unique_keys)
        df_result["FechaOperacion"] = pd.to_datetime(df_result["Mes"])
        return df_result

    def validar_datos(self, data: pd.DataFrame) -> list[dict]:
        try:
            datos_validados = [
                NuevosClientesNuevosPagadoresCalcularSchema(**d).model_dump()
                for d in data.to_dict(orient="records")
            ]
            return datos_validados
        except Exception as e:
            logger.error(e)
            raise e

    def calcular(
        self,
        start_date: str,
        end_date: str,
        ruc_c_col: str,
        ruc_c_ns_col: str,
        ruc_p_col: str,
        ruc_p_ns_col: str,
        ejecutivo_col: str,
        type_op_col: str,
    ) -> list[dict]:
        datos_procesados = self.procesar_datos(
            start_date,
            end_date,
            ruc_c_col,
            ruc_p_col,
            ruc_c_ns_col,
            ruc_p_ns_col,
            ejecutivo_col,
            type_op_col,
        )

        datos_validados = self.validar_datos(datos_procesados)
        return datos_validados
