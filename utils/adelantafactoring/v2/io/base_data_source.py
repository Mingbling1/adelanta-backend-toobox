"""Base data source interface for v2 architecture."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List
import pandas as pd


class BaseDataSource(ABC):
    """Abstract base class for data sources."""
    
    @abstractmethod
    def get_data(self, **filters) -> List[Dict[str, Any]]:
        """Get data as list of dictionaries with optional filters."""
        pass
    
    @abstractmethod
    def get_dataframe(self) -> pd.DataFrame:
        """Get data as pandas DataFrame."""
        pass
