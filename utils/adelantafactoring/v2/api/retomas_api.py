"""Retomas API for v2 architecture."""

from datetime import datetime
from typing import List, Dict, Any
import pandas as pd
from utils.adelantafactoring.v2.io.retomas_data_source import RetomasDataSource
from utils.adelantafactoring.v2.engines.retomas_engine import RetomasEngine
from utils.adelantafactoring.v2.schemas.retomas import (
    RetomasCalcularRequest,
    RetomasCalcularResponse,
    RetomasCalcularListResponse
)


class RetomasAPI:
    """API interface for retomas calculations."""
    
    def __init__(self, dataframe: pd.DataFrame):
        """Initialize API with DataFrame."""
        self.data_source = RetomasDataSource(dataframe)
        self.engine = RetomasEngine(self.data_source)
    
    def calcular_retomas(
        self, 
        request: RetomasCalcularRequest
    ) -> RetomasCalcularListResponse:
        """Calculate retomas based on request."""
        fecha_corte = datetime.fromisoformat(request.fecha_corte.replace('Z', '+00:00'))
        
        # Remove timezone info to make it tz-naive as per original logic
        if fecha_corte.tzinfo is not None:
            fecha_corte = fecha_corte.replace(tzinfo=None)
        
        retomas_data = self.engine.calcular_retomas(fecha_corte)
        
        return RetomasCalcularListResponse(
            retomas=[RetomasCalcularResponse(**item) for item in retomas_data],
            total_records=len(retomas_data)
        )
    
    async def calcular_retomas_async(
        self, 
        request: RetomasCalcularRequest,
        to_df: bool = False
    ) -> RetomasCalcularListResponse:
        """Calculate retomas asynchronously."""
        fecha_corte = datetime.fromisoformat(request.fecha_corte.replace('Z', '+00:00'))
        
        # Remove timezone info to make it tz-naive as per original logic
        if fecha_corte.tzinfo is not None:
            fecha_corte = fecha_corte.replace(tzinfo=None)
        
        retomas_result = await self.engine.calcular_retomas_async(fecha_corte, to_df=to_df)
        
        if to_df and isinstance(retomas_result, pd.DataFrame):
            # Convert DataFrame back to list format for response
            retomas_data = retomas_result.to_dict(orient="records")
        else:
            retomas_data = retomas_result
        
        return RetomasCalcularListResponse(
            retomas=[RetomasCalcularResponse(**item) for item in retomas_data],
            total_records=len(retomas_data)
        )
    
    def calcular_retomas_raw(self, fecha_corte: datetime) -> List[Dict[str, Any]]:
        """Calculate retomas returning raw data (backward compatibility)."""
        return self.engine.calcular_retomas(fecha_corte)
    
    async def calcular_retomas_raw_async(
        self, 
        fecha_corte: datetime, 
        to_df: bool = False
    ) -> List[Dict[str, Any]]:
        """Calculate retomas asynchronously returning raw data (backward compatibility)."""
        return await self.engine.calcular_retomas_async(fecha_corte, to_df=to_df)
