"""Retomas calculation engine for v2 architecture."""

import asyncio
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Union
from config.logger import logger
from utils.adelantafactoring.v2.io.retomas_data_source import RetomasDataSource
from utils.adelantafactoring.v2.schemas.retomas_schema import RetomasCalcularResponse


class RetomasEngine:
    """Engine for retomas financial calculations."""
    
    def __init__(self, data_source: RetomasDataSource):
        """Initialize engine with data source."""
        self.data_source = data_source
    
    def procesar_datos(self, fecha_corte: datetime) -> pd.DataFrame:
        """Process retomas data combining desembolsos and cobranzas."""
        desembolsos_df = self.data_source.calcular_desembolsos(fecha_corte=fecha_corte)
        cobranzas_df = self.data_source.calcular_cobranzas(fecha_corte=fecha_corte)
        
        # Combine both DataFrames with hierarchical columns
        df = pd.concat(
            [cobranzas_df, desembolsos_df], 
            keys=["Cobranzas", "Desembolsos"], 
            axis=1
        ).fillna(0)
        
        # Calculate PorRetomar (difference between cobranzas and desembolsos)
        df["PorRetomar"] = (
            df["Cobranzas", "MontoPagoSoles"]
            - df["Desembolsos", "MontoDesembolsoSoles"]
        )
        
        # Reset index and flatten column names
        df = df.reset_index()
        df.columns = [
            "_".join(col).strip("_") if isinstance(col, tuple) else col.strip("_")
            for col in df.columns
        ]
        
        return df
    
    def validar_datos(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Validate data using Pydantic schemas."""
        try:
            datos_validados = [
                RetomasCalcularResponse(**d).model_dump()
                for d in data.to_dict(orient="records")
            ]
            return datos_validados
        except Exception as e:
            logger.error(f"Error validating retomas data: {e}")
            raise e
    
    def calcular_retomas(self, fecha_corte: datetime) -> List[Dict[str, Any]]:
        """Calculate retomas for given fecha_corte."""
        datos_procesados = self.procesar_datos(fecha_corte)
        datos_validados = self.validar_datos(datos_procesados)
        return datos_validados
    
    async def calcular_retomas_async(
        self, fecha_corte: datetime, to_df: bool = False
    ) -> Union[List[Dict[str, Any]], pd.DataFrame]:
        """Calculate retomas asynchronously."""
        loop = asyncio.get_event_loop()
        retomas_result = await loop.run_in_executor(
            None, self.calcular_retomas, fecha_corte
        )
        
        if to_df:
            # Convert list of dictionaries to DataFrame
            df_retomas = await loop.run_in_executor(None, pd.DataFrame, retomas_result)
            return df_retomas
        
        return retomas_result
