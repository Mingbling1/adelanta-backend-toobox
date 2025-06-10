from ..Base import Base
from datetime import datetime, date
from typing import Union
import unicodedata
import pandas as pd


class BaseCalcular(Base):
    def __init__(self) -> None:
        pass

    MESES_ES = {
        1: "Enero",
        2: "Febrero",
        3: "Marzo",
        4: "Abril",
        5: "Mayo",
        6: "Junio",
        7: "Julio",
        8: "Agosto",
        9: "Septiembre",
        10: "Octubre",
        11: "Noviembre",
        12: "Diciembre",
    }

    def obtener_nombre_mes_es(self, fecha: Union[str, date, datetime]) -> str:
        """
        Dada una fecha ('YYYY-MM-DD' o date/datetime),
        devuelve 'Mes_AAAA' en español, ej: 'Enero_2025'.
        """
        if isinstance(fecha, str):
            dt = datetime.strptime(fecha, "%Y-%m-%d").date()
        elif isinstance(fecha, datetime):
            dt = fecha.date()
        else:
            dt = fecha

        nombre = self.MESES_ES.get(dt.month, str(dt.month))
        return f"{nombre}_{dt.year}"

    def obtener_primer_dia_mes_anterior(self, fecha: Union[str, date, datetime]) -> str:
        """
        Dada una fecha ('YYYY-MM-DD' o date/datetime),
        retorna una cadena 'YYYY-MM-01' del mes anterior.
        """
        if isinstance(fecha, str):
            dt = datetime.strptime(fecha, "%Y-%m-%d").date()
        elif isinstance(fecha, datetime):
            dt = fecha.date()
        else:
            dt = fecha

        year, month = dt.year, dt.month
        if month == 1:
            new_year, new_month = year - 1, 12
        else:
            new_year, new_month = year, month - 1

        return f"{new_year}-{new_month:02d}-01"

    def _normalize(self, text: str) -> str:
        """Quita diacríticos y deja minúsculas."""
        nf = unicodedata.normalize("NFD", text)
        return "".join(c for c in nf if unicodedata.category(c) != "Mn").lower()

    def validar_columnas(self, df: pd.DataFrame, expected: list[str]) -> None:
        """
        Verifica que el DataFrame tenga todas las columnas en 'expected',
        comparando nombres normalizados.
        Lanza ValueError con la lista de faltantes.
        """
        present = {self._normalize(c) for c in df.columns}
        faltan = [col for col in expected if self._normalize(col) not in present]
        if faltan:
            raise ValueError(f"Faltan columnas requeridas: {faltan}")

    def renombrar_columnas(
        self, df: pd.DataFrame, mapping: dict[str, str]
    ) -> pd.DataFrame:
        """
        Renombra las columnas según mapping, donde la key es el nombre
        normalizado y el value el nombre final.
        """
        renames: dict[str, str] = {}
        for c in df.columns:
            key = self._normalize(c)
            if key in mapping:
                renames[c] = mapping[key]
        return df.rename(columns=renames)
