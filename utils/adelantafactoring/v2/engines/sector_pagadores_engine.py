"""
Motor de cálculo para sector pagadores
"""

import pandas as pd
from typing import List, Dict

from ..schemas.sector_pagadores_schema import (
    SectorPagadoresRequest,
    SectorPagadoresResult,
    SectorPagadorRecord,
    SectorPagadoresCalcularSchema,
)
from ..io.sector_pagadores_data_source import SectorPagadoresDataSource


class SectorPagadoresEngine:
    """Motor principal para procesamiento de sector pagadores"""

    def __init__(self):
        self.data_source = SectorPagadoresDataSource()

    def calculate(self, request: SectorPagadoresRequest) -> SectorPagadoresResult:
        """
        Procesa los datos de sector pagadores

        Args:
            request: Request con configuración de procesamiento

        Returns:
            SectorPagadoresResult con los datos procesados y estadísticas
        """
        # Obtener datos desde la fuente
        raw_data = self.data_source._obtener_sincrono()

        # Procesar datos
        processed_df = self._procesar_datos(raw_data)

        # Validar con Pydantic
        validated_records = self._validar_datos(processed_df)

        # Generar estadísticas
        unique_sectors = sorted(
            list(set(record.sector for record in validated_records))
        )
        unique_grupos = sorted(
            list(
                set(
                    record.grupo_eco for record in validated_records if record.grupo_eco
                )
            )
        )

        return SectorPagadoresResult(
            records=validated_records,
            records_count=len(validated_records),
            unique_sectors=unique_sectors,
            unique_grupos=unique_grupos,
        )

    def _procesar_datos(self, raw_data: Dict) -> pd.DataFrame:
        """Procesa los datos raw obtenidos de la fuente"""

        # Extraer los datos de la estructura Google Scripts
        if "data" in raw_data:
            data_list = raw_data["data"]
        else:
            data_list = raw_data

        df = pd.DataFrame(data_list)

        # Si el DataFrame está vacío, devolver estructura básica
        if df.empty:
            return pd.DataFrame(columns=["RUCPagador", "Sector", "GrupoEco"])

        # Procesar según la lógica original con mapeo de columnas flexible
        column_mapping = {
            "RUC": "RUC",
            "ruc": "RUC",
            "sector": "SECTOR",
            "SECTOR": "SECTOR",
            "Sector": "SECTOR",
            "descripcion": "GRUPO ECO.",
            "GRUPO ECO.": "GRUPO ECO.",
            "grupo_eco": "GRUPO ECO.",
        }

        # Normalizar nombres de columnas
        for col in df.columns:
            if col in column_mapping:
                df = df.rename(columns={col: column_mapping[col]})

        # Procesar datos con valores por defecto
        df["RUCPagador"] = df.get("RUC", pd.Series(dtype=str)).astype(str).str.strip()
        df["Sector"] = df.get("SECTOR", pd.Series(dtype=str)).astype(str).str.strip()
        df["GrupoEco"] = (
            df.get("GRUPO ECO.", pd.Series(dtype=str))
            .astype(str)
            .str.strip()
            .replace({"": pd.NA})
        )

        # Filtrar y eliminar duplicados
        df = df[["RUCPagador", "Sector", "GrupoEco"]].drop_duplicates(
            subset=["RUCPagador"]
        )

        return df

    def _validar_datos(self, df: pd.DataFrame) -> List[SectorPagadorRecord]:
        """Valida los datos con Pydantic y convierte a schema moderno"""

        # Primero validar con el schema de compatibilidad
        validated_data = []
        for record in df.to_dict(orient="records"):
            try:
                schema_record = SectorPagadoresCalcularSchema(**record)
                validated_data.append(schema_record.model_dump())
            except Exception as e:
                # Log del error pero continúa con otros registros
                print(f"Error validando registro {record}: {e}")
                continue

        # Convertir a schemas modernos
        modern_records = []
        for record in validated_data:
            modern_record = SectorPagadorRecord(
                ruc_pagador=record["RUCPagador"],
                sector=record["Sector"],
                grupo_eco=record["GrupoEco"],
            )
            modern_records.append(modern_record)

        return modern_records

    def get_legacy_format(self, result: SectorPagadoresResult) -> List[Dict]:
        """Convierte el resultado al formato legacy para compatibilidad"""
        return [
            {
                "RUCPagador": record.ruc_pagador,
                "Sector": record.sector,
                "GrupoEco": record.grupo_eco,
            }
            for record in result.records
        ]

    def get_legacy_dataframe(self, result: SectorPagadoresResult) -> pd.DataFrame:
        """Convierte el resultado a DataFrame para compatibilidad"""
        legacy_data = self.get_legacy_format(result)
        return pd.DataFrame(legacy_data)
