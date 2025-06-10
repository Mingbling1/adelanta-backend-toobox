import pandas as pd
from datetime import datetime
from .BaseCalcular import BaseCalcular
from config.logger import logger
import numpy as np
from io import BytesIO
import zipfile
import calendar
from typing import Dict, Tuple, Union, List
from rapidfuzz import fuzz
from ..schemas.ComisionesCalcularSchema import (
    RegistroComisionSchema,
    PromocionSchema,
)
from ..calculos.ReferidosCalcular import ReferidosCalcular
from ..calculos.FondoCrecerCalcular import FondoCrecerCalcular
from ..calculos.FondoPromocionalCalcular import FondoPromocionalCalcular


class ComisionesCalcular(BaseCalcular):
    INTERES_PROMOCIONAL_PEN = 0.08
    INTERES_PROMOCIONAL_USD = 0.08

    TASA_FONDO_CRECER = 0.08  # Nueva constante para el cálculo de Fondo Crecer

    name_map = {
        "LEO": "Leonardo, Castillo",
        "CRISTIAN": "Cristian, Stanbury",
        "GUADALUPE": "Guadalupe, Campos",
        "MARTÍN": "Martin, Huaccharaqui",
        "MIGUEL": "Miguel , Del Solar",
        "REYNALDO": "Reynaldo, Santiago",
        "ROBERTO": "ROBERTO NUÑEZ",
        "MARÍA": "MARIA GARCIA",
        "ROSA MARIA AGREDA": "Rosa Maria, Agreda",
        "JULISSA": "Julissa, Tito",
        "GABRIEL": "GABRIEL ARREDONDO",
        "IVAN": "IVAN FERNANDO, ZUAZO",
        "PALOMA": "Paloma, Landeo",
        "ARIAN": "Arian, Aguirre",
        "FRANCO": "Franco, Moreano",
        "FABIOLA": "Fabiola, Farro",
        "ROSELYS": "Roselys, Acosta",
    }

    def __init__(
        self,
        kpi_df: pd.DataFrame,
    ) -> None:
        self.kpi_df = kpi_df
        self.referidos_df = ReferidosCalcular().calcular(as_df=True)
        self.fondo_crecer_df = FondoCrecerCalcular().calcular(as_df=True)
        self.fondo_promocional_df = FondoPromocionalCalcular().calcular(as_df=True)

    def calcular(
        self, start_date: str = "2023-01-01", end_date: str = "2024-05-31"
    ) -> BytesIO:
        """
        Flujo principal para el cálculo y generación de comisiones:
        1. Obtener y enriquecer datos base con referidos --google sheets externo--.
        2. Aplicar costos especiales de fondos (Crecer, Promocional).
        3. Procesar lógica de comisiones por mes y ejecutivo.
        4. Filtrar operaciones por fecha relevante (mes actual y anterior).
        5. Clasificar operaciones como "Lista Actual" o "Lista Anterior".
        6. Determinar si cada operación es "Nueva" o "Recurrente".
        7. Calcular comisiones según reglas específicas por tipo de ejecutivo.
        8. Incorporar operaciones con anticipos.
        9. Obtener resumen de comisiones por ejecutivo.
        10. Generar archivo ZIP con informes Excel (global, por ejecutivo, detalle).

        Parámetros:
        start_date: Fecha inicial en formato 'YYYY-MM-DD'. Default: "2023-01-01"
        end_date: Fecha final en formato 'YYYY-MM-DD'. Default: "2024-05-31"

        Retorna:
        BytesIO: Buffer en memoria conteniendo archivo ZIP con reportes Excel.
        """
        # 1. Obtener datos base con referidos
        sistema_df = self.calcular_referidos()

        # 2. Aplicar costos especiales (Fondo Crecer, Promocional)
        sistema_df = self.aplicar_costos_fondos_especiales(sistema_df)

        # 3. Procesar lógica de comisiones
        lista, promos = self.logica_comisiones(
            df=sistema_df,
            start_date=start_date,
            end_date=end_date,
        )
        comisiones_detalle_df = pd.DataFrame([r.model_dump() for r in lista])

        # 4. Filtrar por fecha relevante
        lower_bound, upper_bound = self.get_filter_bounds(end_date)
        sistema_filtrado_df = sistema_df[
            (sistema_df["FechaOperacion"] >= lower_bound)
            & (sistema_df["FechaOperacion"] <= upper_bound)
        ]

        # 5. Clasificar operaciones (Lista Actual/Anterior)
        comisiones_df = self.marcar_lista_pasada(
            sistema_filtrado_df, comisiones_detalle_df
        )

        # 6. Determinar tipo de operación (Nuevo/Recurrente)
        comisiones_df = self.seleccionar_y_clasificar_operaciones(
            comisiones_df, comisiones_detalle_df
        )

        # 7. Calcular comisiones según reglas por tipo de ejecutivo
        comisiones_df = self.calcular_comisiones_v1(comisiones_df)

        # 8. Incorporar operaciones con anticipos
        comisiones_df = self.obtener_comisiones_con_anticipos(comisiones_df, end_date)

        # 9. Obtener resumen de comisiones por ejecutivo
        comisiones_detalle_df = self.obtener_detalle_comisiones(comisiones_df)

        # 10. Generar ZIP con reportes Excel
        zip_bytes = self.generar_zip_con_excels(
            comisiones_df,
            comisiones_detalle_df,
            end_date,
        )

        return zip_bytes

    def aplicar_costos_fondos_especiales(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # 1. Fondo Crecer
        df["CostosFondoCrecer"] = 0.0
        if not self.fondo_crecer_df.empty:
            # mapeo código → garantía
            garantias = dict(
                zip(
                    self.fondo_crecer_df["CodigoLiquidacion"],
                    self.fondo_crecer_df["Garantia"],
                )
            )
            # serie de garantías alineada con df; 0 para los no encontrados
            garantia_s = df["CodigoLiquidacion"].map(garantias).fillna(0)
            mask_crecer = garantia_s > 0

            adicional = (
                df["TasaNominalMensualPorc"]
                * 12
                * self.TASA_FONDO_CRECER
                / 360
                * df["DiasEfectivo"]
                * df["MontoDesembolso"]
                * garantia_s
                * 1.18
            )

            df["CostosFondoCrecer"] = np.where(mask_crecer, adicional, 0.0)

        # 2. Fondo Promocional
        df["CostosFondoPromocional"] = 0.0
        if not self.fondo_promocional_df.empty:
            promo_codes = set(self.fondo_promocional_df["CodigoLiquidacion"])
            mask_prom = df["CodigoLiquidacion"].isin(promo_codes)

            # Vectorizado con np.where anidado
            df["CostosFondoPromocional"] = np.where(
                mask_prom & (df["Moneda"] == "PEN"),
                ((1 + self.INTERES_PROMOCIONAL_PEN) ** (df["DiasEfectivo"] / 365) - 1)
                * df["MontoDesembolso"],
                0.0,
            )
            df["CostosFondoPromocional"] = np.where(
                mask_prom & (df["Moneda"] != "PEN"),
                ((1 + self.INTERES_PROMOCIONAL_USD) ** (df["DiasEfectivo"] / 365) - 1)
                * df["MontoDesembolso"],
                df["CostosFondoPromocional"],
            )

        # 3. Costos totales
        # Si hay promoción, ese costo reemplaza al total; sino sumamos base + crecer
        mask_prom = df["CostosFondoPromocional"] > 0
        df["CostosFondoTotal"] = np.where(
            mask_prom,
            df["CostosFondoPromocional"],
            df["CostosFondo"] + df["CostosFondoCrecer"],
        )
        # Versión en soles
        df["CostosFondoTotalSoles"] = np.where(
            df["Moneda"] == "PEN",
            df["CostosFondoTotal"],
            df["CostosFondoTotal"] * df["TipoCambioVenta"],
        )
        # Utilidad en soles sobre el nuevo total
        df["UtilidadTotalSoles"] = (
            df["TotalIngresosSoles"] - df["CostosFondoTotalSoles"]
        )

        # 4. Etiqueta del tipo de fondo aplicado
        #   COFIDE si hubo CostosFondoCrecer, Promocional si hubo CostosFondoPromocional,
        #   Normal en caso contrario
        df["EtiquetaFondo"] = np.select(
            [df["CostosFondoCrecer"] > 0, df["CostosFondoPromocional"] > 0],
            ["COFIDE", "Promocional"],
            default="Normal",
        )

        return df

    def calcular_referidos(
        self,
    ) -> pd.DataFrame:
        def fuzzy_map(name: str) -> str:
            candidate = name.strip()
            # Mapeo directo: si el candidato coincide (sin distinción de mayúsculas) con alguna clave en name_map, lo usamos
            for key, canonical in self.name_map.items():
                if candidate.upper() == key.upper():
                    return canonical
            # En caso de no coincidir directamente, se hace matching difuso con los valores canónicos
            best_score = 0
            best_match = candidate
            for canonical in self.name_map.values():
                score = fuzz.ratio(candidate.lower(), canonical.lower())
                if score > best_score:
                    best_score = score
                    best_match = canonical
            # Umbral ajustable (e.g., 60)
            if best_score >= 60:
                return best_match
            # Si no se encuentra un parecido aceptable, se registra un warning
            logger.warning(
                f"No se encontró mapeo adecuado para '{candidate}' (mejor score: {best_score})"
            )
            return candidate

        referidos_df = self.referidos_df.copy()
        # Aplicar la función de mapeo difuso a la columna "Referencia"
        referidos_df["Referencia"] = referidos_df["Referencia"].apply(fuzzy_map)

        # referidos_df["Referencia"] = referidos_df["Referencia"].map(self.name_map)

        referidos_df = referidos_df[
            ["CodigoLiquidacion", "Referencia"]
        ].drop_duplicates(subset="CodigoLiquidacion", keep="last")

        # referidos_df.to_excel(r"C:\Jimmy\main-backend\referidos.xlsx", index=False)
        # Verificar si "Referencia" existe antes de intentar eliminarla
        if "Referencia" in self.kpi_df.columns:
            logger.debug("Columna 'Referencia' encontrada, eliminando antes de merge")
            kpi_df = self.kpi_df.drop(columns=["Referencia"]).merge(
                referidos_df, on="CodigoLiquidacion", how="left"
            )
        else:
            logger.debug("Columna 'Referencia' no encontrada, realizando merge directo")
            kpi_df = self.kpi_df.merge(referidos_df, on="CodigoLiquidacion", how="left")

        kpi_df["Referencia"] = kpi_df["Referencia"].fillna(kpi_df["Ejecutivo"])
        # Reemplazar valores mayúsculas por la versión con la primera letra en mayúscula
        kpi_df["TipoOperacion"] = kpi_df["TipoOperacion"].replace(
            {"FACTORING": "Factoring", "CONFIRMING": "Confirming"}
        )

        # kpi_df.to_excel(r"C:\Jimmy\main-backend\kpi.xlsx", index=False)

        return kpi_df

    def _rango_mensual(self, fecha_inicio: str, fecha_fin: str) -> List[str]:
        """
        Genera la lista de meses entre fecha_inicio y fecha_fin (ambos inclusive),
        en formato "YYYY-MM".
        """
        # Construye un DatetimeIndex con el primer día de cada mes
        meses = pd.date_range(start=fecha_inicio, end=fecha_fin, freq="MS")
        # Devuelve la representación "YYYY-MM" de cada fecha
        return [mes.strftime("%Y-%m") for mes in meses]

    def _fecha_ultima_operacion(
        self, df: pd.DataFrame, columna: str, valor: str
    ) -> pd.Timestamp | None:
        """
        Retorna la fecha más reciente de 'FechaOperacion' en 'df'
        donde df[columna] == valor. Si no hay coincidencias, retorna None.
        """
        df_filtrado = df[df[columna] == valor]
        return df_filtrado["FechaOperacion"].max() if not df_filtrado.empty else None

    def _fecha_primera_operacion(
        self, df: pd.DataFrame, columna: str, valor: str
    ) -> pd.Timestamp | None:
        """
        Retorna la fecha de la primera ocurrencia de 'FechaOperacion' en 'df'
        donde df[columna] == valor. Si no hay coincidencias, retorna None.
        """
        df_filtrado = df[df[columna] == valor]
        return df_filtrado["FechaOperacion"].iloc[0] if not df_filtrado.empty else None

    def _diferencia_dias_operacion(
        self, df_pasado: pd.DataFrame, df_actual: pd.DataFrame, columna: str, valor: str
    ) -> int:
        """
        Calcula cuántos días pasaron entre la última operación registrada
        en df_pasado y la primera en df_actual para un mismo 'valor' en 'columna'.
        Si falta cualquiera de las dos fechas, devuelve 0.
        """
        fecha_ant = self._fecha_ultima_operacion(df_pasado, columna, valor)
        fecha_act = self._fecha_primera_operacion(df_actual, columna, valor)
        if fecha_ant is None or fecha_act is None:
            return 0
        return (fecha_act - fecha_ant).days

    def logica_comisiones(
        self,
        df: pd.DataFrame,
        start_date: str,
        end_date: str,
    ) -> Tuple[
        List[RegistroComisionSchema], Dict[Union[Tuple[str, str], str], PromocionSchema]
    ]:
        registros: List[RegistroComisionSchema] = []
        # Diccionario para acumular promociones entre meses
        promociones_acumuladas: Dict[Union[Tuple[str, str], str], PromocionSchema] = {}
        meses = self._rango_mensual(start_date, end_date)

        for mes in meses:
            # Promociones específicas de este mes
            promociones_mes: Dict[Tuple[str, str], PromocionSchema] = {}

            df_mes = df[df["Mes"] == mes]
            df_anteriores = df[df["Mes"] < mes]

            for ejec in df_mes["Ejecutivo"].unique():
                for tipo in df_mes["TipoOperacion"].unique():
                    if tipo == "Factoring":
                        # Usar promociones acumuladas hasta ahora
                        recs, promo = self._procesar_factoring(
                            df_mes,
                            df_anteriores,
                            ejec,
                            tipo,
                            mes,
                            promociones_acumuladas,
                        )
                    else:
                        recs, promo = self._procesar_confirming_o_ct(
                            df_mes,
                            df_anteriores,
                            ejec,
                            tipo,
                            mes,
                            promociones_acumuladas,
                        )

                    # Agregar registros
                    for r in recs:
                        registros.append(RegistroComisionSchema(**r))

                    # Convertir promo_dict a objetos PromocionSchema
                    for key, val in promo.items():
                        promociones_mes[key] = PromocionSchema(
                            **{
                                "Ejecutivo": val["Ejecutivo"],
                                "TipoOperacion": val["TipoOperacion"],
                                "FechaExpiracion": val["Fecha de expiración"],
                            }
                        )

            # Actualizar promociones acumuladas con las de este mes
            promociones_acumuladas.update(promociones_mes)

        # Al final, devolver los registros y todas las promociones acumuladas
        return registros, promociones_acumuladas

    def _procesar_factoring(
        self,
        df_mes: pd.DataFrame,
        df_anteriores: pd.DataFrame,
        ejecutivo: str,
        tipo: str,
        mes: str,
        promos_existentes: Dict[Union[Tuple[str, str], str], PromocionSchema] = None,
    ) -> Tuple[list[dict], dict]:
        """
        Genera registros de Factoring (Nuevo/Recurrente) y actualiza expiraciones.
        """
        nuevos = df_mes[
            (df_mes["Ejecutivo"] == ejecutivo) & (df_mes["TipoOperacion"] == tipo)
        ]
        anteriores = df_anteriores[df_anteriores["TipoOperacion"] == tipo]

        ids_nuevos = set(zip(nuevos["RUCCliente"], nuevos["RUCPagador"]))
        ids_anteriores = set(zip(anteriores["RUCCliente"], anteriores["RUCPagador"]))
        promo_dict = {}
        registros = []

        # Convertir mes a datetime para comparar con fechas de expiración
        fecha_mes = datetime.strptime(mes, "%Y-%m")

        for ruc, pagador in ids_nuevos:
            # PRIMERA PRIORIDAD: Verificar promociones activas de forma flexible
            tiene_promo_vigente = False
            detalle = ""
            tipo_oper = None

            # 1. Verificar el par exacto (RUCCliente, RUCPagador)
            if promos_existentes and (ruc, pagador) in promos_existentes:
                promo = promos_existentes[(ruc, pagador)]
                if promo.FechaExpiracion > fecha_mes and promo.Ejecutivo == ejecutivo:
                    tiene_promo_vigente = True
                    tipo_oper = "Nuevo"
                    detalle = f"Promoción vigente hasta {promo.FechaExpiracion.strftime('%d/%m/%Y')}"

            # 2. Si no tiene, verificar si el cliente está en alguna otra promoción
            if not tiene_promo_vigente and promos_existentes:
                # Filtrar solo promociones de Factoring
                for key, promo in promos_existentes.items():
                    if (
                        promo.TipoOperacion == "Factoring"
                        and isinstance(key, tuple)
                        and len(key) == 2
                    ):
                        cliente, otro_pagador = key
                        if (
                            cliente == ruc
                            and promo.FechaExpiracion > fecha_mes
                            and promo.Ejecutivo == ejecutivo
                        ):
                            tiene_promo_vigente = True
                            tipo_oper = "Nuevo"
                            detalle = f"Cliente con promoción vigente hasta {promo.FechaExpiracion.strftime('%d/%m/%Y')}"
                            break

            # 3. Si no tiene, verificar si el pagador está en alguna otra promoción
            if not tiene_promo_vigente and promos_existentes:
                # Filtrar solo promociones de Factoring
                for key, promo in promos_existentes.items():
                    if (
                        promo.TipoOperacion == "Factoring"
                        and isinstance(key, tuple)
                        and len(key) == 2
                    ):
                        otro_cliente, pag = key
                        if (
                            pag == pagador
                            and promo.FechaExpiracion > fecha_mes
                            and promo.Ejecutivo == ejecutivo
                        ):
                            tiene_promo_vigente = True
                            tipo_oper = "Nuevo"
                            detalle = f"Pagador con promoción vigente hasta {promo.FechaExpiracion.strftime('%d/%m/%Y')}"
                            break
            # Si no tiene promoción vigente, aplicar reglas normales
            if not tiene_promo_vigente:
                if (ruc, pagador) in ids_anteriores:
                    # Ambos ya existen → puede ser Recurrente o Reactivado
                    dc = self._diferencia_dias_operacion(
                        anteriores, nuevos, "RUCCliente", ruc
                    )
                    dp = self._diferencia_dias_operacion(
                        anteriores, nuevos, "RUCPagador", pagador
                    )
                    if max(dc, dp) > 180:
                        tipo_oper = "Nuevo"
                        detalle = self._detalle_reactivacion(dc, dp)
                        fecha_exp = self._fecha_expiracion(nuevos, ruc, pagador)
                        promo_dict[(ruc, pagador)] = {
                            "Ejecutivo": ejecutivo,
                            "TipoOperacion": tipo,
                            "Fecha de expiración": fecha_exp,
                        }
                    else:
                        tipo_oper = "Recurrente"
                        detalle = "Cliente y Pagador recurrentes"
                else:
                    # Al menos uno es totalmente nuevo
                    tipo_oper = "Nuevo"
                    detalle = self._detalle_nuevo(ruc, pagador, anteriores)
                    fecha_exp = self._fecha_expiracion(nuevos, ruc, pagador)
                    promo_dict[(ruc, pagador)] = {
                        "Ejecutivo": ejecutivo,
                        "TipoOperacion": tipo,
                        "Fecha de expiración": fecha_exp,
                    }

            # Agregar el registro con tipo y detalle apropiados
            registros.append(
                {
                    "RUCCliente": ruc,
                    "RUCPagador": pagador,
                    "Tipo": tipo_oper,
                    "Detalle": detalle,
                    "Mes": mes,
                    "TipoOperacion": tipo,
                    "Ejecutivo": ejecutivo,
                }
            )

        return registros, promo_dict

    def _procesar_confirming_o_ct(
        self,
        df_mes: pd.DataFrame,
        df_anteriores: pd.DataFrame,
        ejecutivo: str,
        tipo: str,
        mes: str,
        promos_existentes: Dict[Union[Tuple[str, str], str], PromocionSchema] = None,
    ) -> Tuple[list[dict], dict]:
        """
        Genera registros de Confirming o Capital de Trabajo (Nuevo/Recurrente).
        """
        nuevos = df_mes[
            (df_mes["Ejecutivo"] == ejecutivo) & (df_mes["TipoOperacion"] == tipo)
        ]
        anteriores = df_anteriores[df_anteriores["TipoOperacion"] == tipo]

        pag_nuevos = set(nuevos["RUCPagador"])
        pag_anteriores = set(anteriores["RUCPagador"])
        promo_dict = {}
        registros = []

        # Convertir mes a datetime para comparar con fechas de expiración
        fecha_mes = datetime.strptime(mes, "%Y-%m")

        for pag in pag_nuevos:
            # PRIMERA PRIORIDAD: Verificar si el pagador tiene promoción vigente
            tiene_promo_vigente = False
            detalle = ""
            tipo_oper = None

            # 1. Verificar promoción directa para este pagador
            if promos_existentes and pag in promos_existentes:
                promo = promos_existentes[pag]
                if promo.FechaExpiracion > fecha_mes and promo.Ejecutivo == ejecutivo:
                    tiene_promo_vigente = True
                    tipo_oper = "Nuevo"
                    detalle = f"Promoción vigente hasta {promo.FechaExpiracion.strftime('%d/%m/%Y')}"

            # 2. Verificar si el pagador está en alguna promoción de Factoring
            if not tiene_promo_vigente and promos_existentes:
                for key, promo in promos_existentes.items():
                    # Solo verificar promociones en tuplas (Factoring)
                    if (
                        promo.TipoOperacion == "Factoring"
                        and isinstance(key, tuple)
                        and len(key) == 2
                    ):
                        _, pagador_en_tupla = key
                        if (
                            pagador_en_tupla == pag
                            and promo.FechaExpiracion > fecha_mes
                            and promo.Ejecutivo == ejecutivo
                        ):
                            tiene_promo_vigente = True
                            tipo_oper = "Nuevo"
                            detalle = f"Pagador con promoción vigente hasta {promo.FechaExpiracion.strftime('%d/%m/%Y')}"
                            break

            # Si no tiene promoción vigente, aplicar reglas normales
            if not tiene_promo_vigente:
                if pag in pag_anteriores:
                    tipo_oper = "Recurrente"
                    detalle = "Pagador recurrente"
                else:
                    tipo_oper = "Nuevo"
                    detalle = "Pagador nuevo"
                    fecha_exp = nuevos[nuevos["RUCPagador"] == pag][
                        "FechaOperacion"
                    ].iloc[0] + pd.DateOffset(months=6)
                    promo_dict[pag] = {
                        "Ejecutivo": ejecutivo,
                        "TipoOperacion": tipo,
                        "Fecha de expiración": fecha_exp,
                    }

            # Agregar el registro con tipo y detalle apropiados
            registros.append(
                {
                    "RUCCliente": np.nan,
                    "RUCPagador": pag,
                    "Tipo": tipo_oper,
                    "Detalle": detalle,
                    "Mes": mes,
                    "TipoOperacion": tipo,
                    "Ejecutivo": ejecutivo,
                }
            )

        return registros, promo_dict

    def _detalle_reactivacion(self, dias_cliente: int, dias_pagador: int) -> str:
        partes = []
        if dias_cliente > 180:
            partes.append(f"Cliente reactivado {dias_cliente} días")
        if dias_pagador > 180:
            partes.append(f"Pagador reactivado {dias_pagador} días")
        return " - ".join(partes)

    def _detalle_nuevo(self, ruc: str, pagador: str, anteriores: pd.DataFrame) -> str:
        existe_ruc = ruc in set(anteriores["RUCCliente"])
        existe_pag = pagador in set(anteriores["RUCPagador"])
        if existe_ruc and not existe_pag:
            return "Cliente recurrente - Pagador nuevo"
        if not existe_ruc and existe_pag:
            return "Cliente nuevo - Pagador recurrente"
        return "Cliente y Pagador nuevos"

    def _fecha_expiracion(
        self, df_nuevos: pd.DataFrame, ruc: str, pagador: str
    ) -> pd.Timestamp:
        return df_nuevos[
            (df_nuevos["RUCCliente"] == ruc) & (df_nuevos["RUCPagador"] == pagador)
        ]["FechaOperacion"].iloc[0] + pd.DateOffset(months=6)

    def marcar_lista_pasada(
        self, df: pd.DataFrame, comisiones_detalle: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Clasifica cada registro en 'Lista Actual' o 'Lista Anterior' según si la liquidación
        aparece en el detalle de comisiones del mismo mes.

        Parámetros:
            df: DataFrame original con todas las operaciones. Debe tener columnas:
                - 'Mes': cadena "YYYY-MM"
                - 'Ejecutivo': nombre del ejecutivo
                - 'TipoOperacion': uno de "Factoring", "Confirming", "Capital de Trabajo"
                - 'RUCCliente', 'RUCPagador'
            comisiones_detalle: DataFrame con el listado de comisiones generadas,
                mismas columnas de clave para comparar.

        Retorna:
            Un nuevo DataFrame donde se agrega o actualiza la columna 'Comisiones' con:
            - "Lista Actual" si la liquidación existe en el detalle del mes.
            - "Lista Anterior" en caso contrario.
        """
        df = df.copy()
        # Iterar por mes y por ejecutivo para comparar cada subgrupo
        for mes in df["Mes"].unique():
            for ejec in df[df["Mes"] == mes]["Ejecutivo"].unique():
                # Claves únicas del mes actual en detalle: tupla (RUCCliente, RUCPagador)
                claves_factoring = comisiones_detalle[
                    (comisiones_detalle["Ejecutivo"] == ejec)
                    & (comisiones_detalle["Mes"] == mes)
                    & (comisiones_detalle["TipoOperacion"] == "Factoring")
                ][["RUCCliente", "RUCPagador"]].apply(tuple, axis=1)

                # Claves RUCPagador para Confirming y Capital de Trabajo
                claves_confirming = comisiones_detalle[
                    (comisiones_detalle["Ejecutivo"] == ejec)
                    & (comisiones_detalle["Mes"] == mes)
                    & (comisiones_detalle["TipoOperacion"] == "Confirming")
                ]["RUCPagador"].tolist()

                claves_ct = comisiones_detalle[
                    (comisiones_detalle["Ejecutivo"] == ejec)
                    & (comisiones_detalle["Mes"] == mes)
                    & (comisiones_detalle["TipoOperacion"] == "Capital de Trabajo")
                ]["RUCPagador"].tolist()

                # Máscaras para filtrar el DataFrame principal por tipo de operación
                mask_f = (
                    (df["Ejecutivo"] == ejec)
                    & (df["TipoOperacion"] == "Factoring")
                    & (df["Mes"] == mes)
                )
                mask_c = (
                    (df["Ejecutivo"] == ejec)
                    & (df["TipoOperacion"] == "Confirming")
                    & (df["Mes"] == mes)
                )
                mask_ct = (
                    (df["Ejecutivo"] == ejec)
                    & (df["TipoOperacion"] == "Capital de Trabajo")
                    & (df["Mes"] == mes)
                )

                # Asignar "Lista Actual" o "Lista Anterior" usando numpy.where
                df.loc[mask_f, "Comisiones"] = np.where(
                    df[mask_f][["RUCCliente", "RUCPagador"]]
                    .apply(tuple, axis=1)
                    .isin(claves_factoring),
                    "Lista Actual",
                    "Lista Anterior",
                )
                df.loc[mask_c, "Comisiones"] = np.where(
                    df[mask_c]["RUCPagador"].isin(claves_confirming),
                    "Lista Actual",
                    "Lista Anterior",
                )
                df.loc[mask_ct, "Comisiones"] = np.where(
                    df[mask_ct]["RUCPagador"].isin(claves_ct),
                    "Lista Actual",
                    "Lista Anterior",
                )

        return df

    def seleccionar_y_clasificar_operaciones(
        self, df: pd.DataFrame, comisiones_detalle: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Determina para cada operación si es 'Nuevo' o 'Recurrente'
        comparando con el detalle de comisiones del mismo mes.

        Parámetros:
            df: DataFrame con operaciones a clasificar. Columnas requeridas:
                - 'Mes', 'Ejecutivo', 'TipoOperacion', 'RUCCliente', 'RUCPagador'
            comisiones_detalle: DataFrame con registros ya procesados, con columna 'Tipo'
                que indica si es 'Nuevo' en el detalle original.

        Retorna:
            DataFrame con la columna 'Tipo' actualizada:
            - "Nuevo" si la operación aparece como tal en el detalle mensual.
            - "Recurrente" en caso contrario.
        """
        # Recorrer cada mes y ejecutivo para mantener agrupaciones coherentes
        for mes in df["Mes"].unique():
            for ejec in df[df["Mes"] == mes]["Ejecutivo"].unique():
                # Claves que se marcaron inicialmente como 'Nuevo' en el detalle
                claves_factoring = comisiones_detalle[
                    (comisiones_detalle["Ejecutivo"] == ejec)
                    & (comisiones_detalle["Mes"] == mes)
                    & (comisiones_detalle["Tipo"] == "Nuevo")
                    & (comisiones_detalle["TipoOperacion"] == "Factoring")
                ][["RUCCliente", "RUCPagador"]].apply(tuple, axis=1)
                claves_confirming = comisiones_detalle[
                    (comisiones_detalle["Ejecutivo"] == ejec)
                    & (comisiones_detalle["Mes"] == mes)
                    & (comisiones_detalle["Tipo"] == "Nuevo")
                    & (comisiones_detalle["TipoOperacion"] == "Confirming")
                ]["RUCPagador"].tolist()
                claves_ct = comisiones_detalle[
                    (comisiones_detalle["Ejecutivo"] == ejec)
                    & (comisiones_detalle["Mes"] == mes)
                    & (comisiones_detalle["Tipo"] == "Nuevo")
                    & (comisiones_detalle["TipoOperacion"] == "Capital de Trabajo")
                ]["RUCPagador"].tolist()

                # Definir máscaras por tipo de operación
                mask_f = (
                    (df["Ejecutivo"] == ejec)
                    & (df["TipoOperacion"] == "Factoring")
                    & (df["Mes"] == mes)
                )
                mask_c = (
                    (df["Ejecutivo"] == ejec)
                    & (df["TipoOperacion"] == "Confirming")
                    & (df["Mes"] == mes)
                )
                mask_ct = (
                    (df["Ejecutivo"] == ejec)
                    & (df["TipoOperacion"] == "Capital de Trabajo")
                    & (df["Mes"] == mes)
                )

                # Actualizar columna 'Tipo' según pertenencia a claves 'Nuevo'
                df.loc[mask_f, "Tipo"] = np.where(
                    df[mask_f][["RUCCliente", "RUCPagador"]]
                    .apply(tuple, axis=1)
                    .isin(claves_factoring),
                    "Nuevo",
                    "Recurrente",
                )
                df.loc[mask_c, "Tipo"] = np.where(
                    df[mask_c]["RUCPagador"].isin(claves_confirming),
                    "Nuevo",
                    "Recurrente",
                )
                df.loc[mask_ct, "Tipo"] = np.where(
                    df[mask_ct]["RUCPagador"].isin(claves_ct),
                    "Nuevo",
                    "Recurrente",
                )

        return df

    def get_filter_bounds(self, end_date: str) -> Tuple[str, str]:
        """
        Retorna lower_bound y upper_bound en formato "YYYY-MM-DD" para filtrar 'FechaOperacion'.

        Se asume que end_date viene en formato "YYYY-MM-DD". Se toma el rango:
        - lower_bound: primer día del mes anterior a end_date.
        - upper_bound: último día del mes de end_date.

        Ejemplo: end_date = "2024-12-01" → lower_bound = "2024-11-01" y upper_bound = "2024-12-31".
        """
        year, month, _ = map(int, end_date.split("-"))

        # Calcular lower_bound: primer día del mes anterior
        if month == 1:
            lower_year = year - 1
            lower_month = 12
        else:
            lower_year = year
            lower_month = month - 1
        lower_bound = f"{lower_year}-{lower_month:02d}-01"

        # Calcular upper_bound: último día del mes de end_date
        last_day = calendar.monthrange(year, month)[1]
        upper_bound = f"{year}-{month:02d}-{last_day:02d}"

        return lower_bound, upper_bound

    def obtener_comisiones_con_anticipos(
        self, df: pd.DataFrame, end_date: str
    ) -> pd.DataFrame:
        # Extraer "YYYY-MM" de end_date (ignora el día)
        month_str = end_date[:7]  # Ej. "2024-12"
        # Calcular el mes anterior en formato "YYYY-MM"
        prev_month_full = self.obtener_primer_dia_mes_anterior(
            f"{month_str}-01"
        )  # Ej. "2024-11-01"
        prev_month = prev_month_full[:7]  # Ej. "2024-11"

        # Filtrar los CódigoLiquidacion del dataframe para el mes indicado (YYYY-MM)
        comisiones_end_date_df = df.loc[df["Mes"] == month_str]
        comisiones_end_date_unicos = comisiones_end_date_df.loc[
            :, "CodigoLiquidacion"
        ].unique()

        # Seleccionar los registros del mes anterior (prev_month) que tengan "Anticipo" == "Sí"
        # y cuyo CódigoLiquidacion esté entre los obtenidos para el mes actual.
        anticipos_mask = (
            (df["Mes"] == prev_month)
            & (df["Anticipo"] == "Sí")
            & (df["CodigoLiquidacion"].isin(comisiones_end_date_unicos))
        )
        comisiones_anticipos_mes_anterior_df = df[anticipos_mask]

        # Retorna la concatenación del dataframe original con los registros adicionales.
        comisiones_df = pd.concat(
            [comisiones_end_date_df, comisiones_anticipos_mes_anterior_df]
        )

        return comisiones_df

    def obtener_detalle_comisiones(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        comisiones_detalle_columnas_mask: list = [
            "Ejecutivo",
            "TotalIngresosSoles",
            "CostosFondoSoles",
            "Utilidad",
            "Comision",
        ]

        comisiones_columna_ejecutivo_mask: list = ["Ejecutivo"]

        detalle_df = (
            df.loc[:, comisiones_detalle_columnas_mask]
            .groupby(by=comisiones_columna_ejecutivo_mask, as_index=False)
            .sum()
        )
        return detalle_df

    def generar_zip_con_excels(
        self,
        comisiones: pd.DataFrame,
        detalle: pd.DataFrame,
        fecha_corte: str,
    ) -> BytesIO:
        """
        Crea un ZIP que contiene:
        - Un Excel global de todas las comisiones.
        - Un Excel por cada ejecutivo.
        - Un Excel con el detalle de comisiones.
        Los nombres de archivo incluyen mes y año en español extraídos de fecha_corte.
        """
        mes_str = self.obtener_nombre_mes_es(fecha_corte)
        archivo_zip = BytesIO()

        with zipfile.ZipFile(
            archivo_zip, mode="w", compression=zipfile.ZIP_DEFLATED
        ) as zf:
            # 1) Excel global
            excel_global = self._crear_excel_en_memoria(comisiones, "COMISIONES")
            zf.writestr(f"Comisiones_{mes_str}.xlsx", excel_global.getvalue())

            # 2) Excel por ejecutivo
            for ejecutivo in comisiones["Ejecutivo"].unique():
                df_ej = comisiones[comisiones["Ejecutivo"] == ejecutivo]
                excel_ej = self._crear_excel_en_memoria(df_ej, "COMISIONES")
                zf.writestr(
                    f"Comisiones_{ejecutivo}_{mes_str}.xlsx", excel_ej.getvalue()
                )

            # 3) Excel de detalle
            excel_detalle = self._crear_excel_en_memoria(detalle, "DETALLE COMISIONES")
            zf.writestr(f"Detalle_Comisiones_{mes_str}.xlsx", excel_detalle.getvalue())

        archivo_zip.seek(0)
        return archivo_zip

    def _crear_excel_en_memoria(self, df: pd.DataFrame, nombre_hoja: str) -> BytesIO:
        """
        Genera en memoria un archivo Excel con la hoja 'nombre_hoja'
        que contiene los datos de 'df'. Retorna el BytesIO listo para escribir.
        """
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=nombre_hoja, index=False)
        buffer.seek(0)
        return buffer

    def calcular_comisiones_v1(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula comisiones personalizadas según categoría de ejecutivo.

        Implementa diferentes reglas de cálculo:
        - Ejecutivos internos: tasas variables según tipo de operación
        - Ejecutivos externos: tasas fijas según persona

        Parámetros:
            df: DataFrame con datos de liquidaciones y utilidades

        Retorna:
            DataFrame con columna 'Comision' calculada
        """
        df = df.copy()

        # Constantes
        IGV_FACTOR = 1.18
        TASAS_EJECUTIVOS_EXTERNOS = {
            "Marcos, Vargas": 0.15,
            "Giancarlo, La Torre": 0.09,
            "IVAN FERNANDO, ZUAZO": 0.07,
        }

        # Clasificación de ejecutivos
        ejecutivos_externos = list(TASAS_EJECUTIVOS_EXTERNOS.keys())
        ejecutivos_especiales = ["Ricardo Franco", "Red, Capital"]  # Se procesan aparte
        todos_ejecutivos = df["Ejecutivo"].unique().tolist()
        ejecutivos_internos = [
            ejecutivo
            for ejecutivo in todos_ejecutivos
            if ejecutivo not in ejecutivos_externos + ejecutivos_especiales
        ]

        # 1. Cálculo para ejecutivos internos (tasas variables según tipo de operación)
        df_internos = df[df["Ejecutivo"].isin(ejecutivos_internos)].copy()
        if not df_internos.empty:
            conditions = [
                # Nuevos, mismo referente y ejecutivo
                (df_internos["Comisiones"] == "Lista Actual")
                & (df_internos["Tipo"] == "Nuevo")
                & (df_internos["Ejecutivo"] == df_internos["Referencia"]),
                # Nuevos, referente distinto al ejecutivo
                (df_internos["Comisiones"] == "Lista Actual")
                & (df_internos["Tipo"] == "Nuevo")
                & (df_internos["Ejecutivo"] != df_internos["Referencia"]),
                # Lista anterior
                (df_internos["Comisiones"] == "Lista Anterior"),
            ]
            choices = [0.11, 0.07, 0.09]
            df_internos.loc[:, "Tasa"] = np.select(conditions, choices, default=0.06)
            df_internos["Comision"] = (
                df_internos["Tasa"] * df_internos["UtilidadTotalSoles"]
            )

        # 2. Ejecutivos externos con tasas fijas
        dfs_externos = []
        for ejecutivo, tasa in TASAS_EJECUTIVOS_EXTERNOS.items():
            df_ejecutivo = df[df["Ejecutivo"] == ejecutivo].copy()
            if not df_ejecutivo.empty:
                df_ejecutivo.loc[:, "Comision"] = np.where(
                    pd.isna(df_ejecutivo["ComisionEstructuracionConIGV"])
                    | (df_ejecutivo["ComisionEstructuracionConIGV"] == 0),
                    df_ejecutivo["UtilidadTotalSoles"] * tasa,
                    df_ejecutivo["ComisionEstructuracionConIGV"] / IGV_FACTOR,
                )
                dfs_externos.append(df_ejecutivo)

        # 3. Casos especiales (Ricardo Franco y Red Capital)
        for ejecutivo in ejecutivos_especiales:
            df_especial = df[df["Ejecutivo"] == ejecutivo].copy()
            if not df_especial.empty:
                df_especial.loc[:, "Comision"] = np.where(
                    pd.isna(df_especial["ComisionEstructuracionConIGV"])
                    | (df_especial["ComisionEstructuracionConIGV"] == 0),
                    np.where(
                        df_especial["Moneda"] == "USD",
                        df_especial["ComisionEstructuracionConIGV"]
                        / IGV_FACTOR
                        * df_especial["TipoCambioVenta"],
                        df_especial["ComisionEstructuracionConIGV"] / IGV_FACTOR,
                    ),
                    df_especial["UtilidadTotalSoles"],
                )
                dfs_externos.append(df_especial)

        # Unir todos los DataFrames
        dfs_a_unir = [df_internos] + dfs_externos
        dfs_no_vacios = [df for df in dfs_a_unir if not df.empty]

        return pd.concat(dfs_no_vacios, axis=0, ignore_index=True)
