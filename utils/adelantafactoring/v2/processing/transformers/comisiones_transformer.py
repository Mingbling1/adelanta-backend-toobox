"""
🔄 Transformer V2 - Comisiones

Transformaciones especializadas para datos de comisiones
"""

import pandas as pd
from datetime import datetime, timedelta
import calendar
from io import BytesIO
import zipfile


class ComisionesTransformer:
    """Transformador para procesar datos de comisiones"""

    # Constantes de cálculo
    INTERES_PROMOCIONAL_PEN = 0.08
    INTERES_PROMOCIONAL_USD = 0.08
    TASA_FONDO_CRECER = 0.08

    # Mapeo de nombres de ejecutivos
    NAME_MAP = {
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

    def __init__(self):
        """Inicializa el transformador"""
        pass

    def enrich_with_referidos(
        self, kpi_df: pd.DataFrame, referidos_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Enriquece datos KPI con información de referidos

        Args:
            kpi_df: DataFrame base con datos KPI
            referidos_df: DataFrame con datos de referidos

        Returns:
            DataFrame enriquecido con referidos
        """
        if referidos_df.empty:
            kpi_df = kpi_df.copy()
            kpi_df["Referencia"] = None
            return kpi_df

        # Merge con datos de referidos
        resultado = kpi_df.merge(
            referidos_df[["RUCCliente", "Referencia"]], on="RUCCliente", how="left"
        )

        return resultado

    def apply_fondos_especiales_costs(
        self,
        df: pd.DataFrame,
        fondo_crecer_df: pd.DataFrame,
        fondo_promocional_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Aplica costos de fondos especiales (Crecer y Promocional)

        Args:
            df: DataFrame base
            fondo_crecer_df: DataFrame con datos de Fondo Crecer
            fondo_promocional_df: DataFrame con datos de Fondo Promocional

        Returns:
            DataFrame con costos aplicados
        """
        df = df.copy()

        # 1. Fondo Crecer
        df["CostosFondoCrecer"] = 0.0
        if not fondo_crecer_df.empty:
            garantias = dict(
                zip(
                    fondo_crecer_df["CodigoLiquidacion"],
                    fondo_crecer_df["Garantia"],
                )
            )
            garantia_s = df["CodigoLiquidacion"].map(garantias).fillna(0)
            mask_crecer = garantia_s > 0

            if mask_crecer.any():
                adicional = (
                    df.loc[mask_crecer, "TasaNominalMensualPorc"]
                    * 12
                    * self.TASA_FONDO_CRECER
                    / 360
                    * df.loc[mask_crecer, "DiasEfectivo"]
                    * df.loc[mask_crecer, "MontoDesembolso"]
                )
                df.loc[mask_crecer, "CostosFondoCrecer"] = adicional

        # 2. Fondo Promocional
        df["CostosFondoPromocional"] = 0.0
        if not fondo_promocional_df.empty:
            codigos_promocional = set(fondo_promocional_df["CodigoLiquidacion"])
            mask_promocional = df["CodigoLiquidacion"].isin(codigos_promocional)

            if mask_promocional.any():
                # Aplicar costo promocional (lógica simplificada)
                promocional_cost = (
                    df.loc[mask_promocional, "MontoDesembolso"] * 0.01  # 1% ejemplo
                )
                df.loc[mask_promocional, "CostosFondoPromocional"] = promocional_cost

        return df

    def normalize_ejecutivo_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza nombres de ejecutivos usando el mapeo definido

        Args:
            df: DataFrame con columna 'Ejecutivo'

        Returns:
            DataFrame con nombres normalizados
        """
        df = df.copy()
        df["EjecutivoNormalizado"] = (
            df["Ejecutivo"].map(self.NAME_MAP).fillna(df["Ejecutivo"])
        )
        return df

    def filter_by_date_range(
        self,
        df: pd.DataFrame,
        start_date: str,
        end_date: str,
        date_column: str = "FechaOperacion",
    ) -> pd.DataFrame:
        """
        Filtra DataFrame por rango de fechas

        Args:
            df: DataFrame a filtrar
            start_date: Fecha inicial (YYYY-MM-DD)
            end_date: Fecha final (YYYY-MM-DD)
            date_column: Nombre de la columna de fecha

        Returns:
            DataFrame filtrado
        """
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        mask = (df[date_column] >= start_dt) & (df[date_column] <= end_dt)
        return df[mask].copy()

    def get_filter_bounds(self, end_date: str) -> tuple:
        """
        Obtiene límites de filtrado basados en fecha final

        Args:
            end_date: Fecha final en formato YYYY-MM-DD

        Returns:
            Tupla con (fecha_inicio, fecha_fin)
        """
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Mes actual y anterior
        start_month = end_dt.replace(day=1) - timedelta(days=31)
        start_month = start_month.replace(day=1)

        # Último día del mes de end_date
        last_day = calendar.monthrange(end_dt.year, end_dt.month)[1]
        upper_bound = end_dt.replace(day=last_day)

        return start_month, upper_bound

    def classify_operations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clasifica operaciones como Nueva/Recurrente

        Args:
            df: DataFrame con operaciones

        Returns:
            DataFrame con clasificación
        """
        df = df.copy()

        # Lógica simplificada para clasificación
        # En V1 esto es más complejo, aquí mantenemos compatibilidad básica
        df["TipoClasificacion"] = "Recurrente"  # Default

        # Ejemplo de lógica: primera operación del cliente = Nueva
        first_operations = df.groupby("RUCCliente")["FechaOperacion"].idxmin()
        df.loc[first_operations, "TipoClasificacion"] = "Nuevo"

        return df

    def create_zip_report(
        self, comisiones_df: pd.DataFrame, detalle_df: pd.DataFrame, end_date: str
    ) -> BytesIO:
        """
        Crea archivo ZIP con reportes Excel

        Args:
            comisiones_df: DataFrame de comisiones
            detalle_df: DataFrame de detalle
            end_date: Fecha final para nombres de archivo

        Returns:
            BytesIO con archivo ZIP
        """
        zip_buffer = BytesIO()

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            # Reporte general
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                comisiones_df.to_excel(writer, sheet_name="Comisiones", index=False)
                detalle_df.to_excel(writer, sheet_name="Detalle", index=False)

            zip_file.writestr(
                f"reporte_comisiones_{end_date}.xlsx", excel_buffer.getvalue()
            )

        zip_buffer.seek(0)
        return zip_buffer
